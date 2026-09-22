# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"

module Backstage::Adapters::LocalFiles
  class ArtifactStore
    Records = Backstage::Domain::Records
    SecretGuard = Backstage::Support::SecretGuard
    attr_reader :root

    def initialize(root, secret_guard: SecretGuard.new)
      @root = File.expand_path(root)
      @secret_guard = secret_guard
      FileUtils.mkdir_p(@root)
    end

    def write(work_item_id:, run_id:, name:, content:, kind:, provenance:)
      safe_name = File.basename(name)
      data = content.is_a?(String) ? content : JSON.pretty_generate(content)
      @secret_guard.check!(data)
      directory = File.join(@root, work_item_id, run_id)
      FileUtils.mkdir_p(directory)
      path = File.join(directory, safe_name)
      File.open(path, "w", 0o600) { |file| file.write(data) }
      Records.artifact(
        work_item_id: work_item_id,
        run_id: run_id,
        kind: kind,
        path: path,
        sha256: Digest::SHA256.hexdigest(data),
        provenance: provenance
      )
    end

    # Opens a directory a runtime's output is captured into, chunk by chunk.
    #
    # Layout: `<root>/<work_item_id>/<run_id>/streams/<index>-<step>/{0.log,1.log,…,stream.json}`.
    # One file per chunk rather than one appended file, deliberately: a torn append is a real
    # failure mode that leaves a file nobody can tell is incomplete, while a create-fsync-rename
    # either happened or did not. `start_offset` seeds the byte position, so resuming from a
    # checkpoint continues the same coordinate system rather than restarting at zero.
    #
    # `committed_chunks` is how many chunks a caller has already *acknowledged* — committed an
    # event for. It is the line between bytes that are history and bytes that are merely on the
    # disk, and only the caller knows where it falls, so it is passed rather than counted from the
    # directory: a crash between a chunk's rename and its commit leaves a file at the next index
    # that nothing references.
    def open_stream(work_item_id:, run_id:, stream_id:, kind:, provenance:, start_offset: 0,
                    chain_sha256: nil, committed_chunks: 0)
      directory = File.join(@root, work_item_id.to_s, run_id.to_s, "streams", segment(stream_id))
      FileUtils.mkdir_p(directory)
      sync_directory(File.dirname(directory))
      StreamHandle.new(
        directory: directory, work_item_id: work_item_id.to_s, run_id: run_id.to_s,
        stream_id: stream_id.to_s, kind: kind.to_s, provenance: provenance,
        secret_guard: @secret_guard, start_offset: start_offset, chain_sha256: chain_sha256,
        committed_chunks: committed_chunks
      )
    end

    # `run-a1b2:1:2:harness` names the second stream of a run's `harness` step, so the directory
    # is `2-harness`. Anything that is not a four-part stream id falls back to its own sanitized
    # text rather than colliding two streams into one directory.
    def segment(stream_id)
      parts = stream_id.to_s.split(":")
      name = parts.length == 4 ? "#{parts[2]}-#{parts[3]}" : stream_id.to_s
      sanitized = name.gsub(/[^A-Za-z0-9._-]/, "-")
      sanitized.empty? ? "stream" : sanitized
    end

    def sync_directory(directory)
      File.open(directory) { |handle| handle.fsync }
    rescue SystemCallError, IOError
      nil
    end

    # The write side of one captured stream. It knows nothing about activity, commits, or
    # interpretation — its whole job is that when `append` returns, the bytes it was given are on
    # the disk under a name that means "complete", and that the caller can therefore commit an
    # event referencing them.
    class StreamHandle
      Records = Backstage::Domain::Records

      attr_reader :directory, :stream_id, :offset, :chain_sha256, :chunk_count

      def initialize(directory:, work_item_id:, run_id:, stream_id:, kind:, provenance:,
                     secret_guard:, start_offset: 0, chain_sha256: nil, committed_chunks: 0)
        @directory = directory
        @work_item_id = work_item_id
        @run_id = run_id
        @stream_id = stream_id
        @kind = kind
        @provenance = provenance
        @secret_guard = secret_guard
        @offset = Integer(start_offset)
        @chain_sha256 = chain_sha256
        @committed_chunks = Integer(committed_chunks)
        @chunk_count = 0
        @last = nil
      end

      # Writes one chunk durably and returns its description. Write, fsync, rename, fsync the
      # directory: the `.part` name never appears in a returned description, so a crash at any
      # point leaves either a complete `<index>.log` or an orphan `.part` that nothing references
      # and a cleaner may remove.
      #
      # The secret check here is a second line of defence, not the primary one — redaction runs
      # upstream where the offsets are decided. If it ever fires, the chunk is not written at all.
      # Appending the same index with the same bytes twice returns the first description
      # unchanged. That is what lets a caller whose commit failed retry the whole flush: the file
      # is already durable and the offsets it was given must not shift underneath the retry.
      #
      # Bytes an event already claims are never overwritten with different ones. A handle only
      # remembers the chunk *it* last wrote, so idempotence checked against that alone would be
      # blind to a second handle over the same stream. So the file itself is consulted — but only
      # below `committed_chunks`, and the distinction is the whole point: an acknowledged chunk is
      # history and a rewrite of it is a CaptureError raised before anything on disk changes,
      # while a chunk at or above that line is the orphan the design's own crash window produces
      # (renamed, never committed) and a resumed writer is entitled to rebuild it. Refusing there
      # too would turn a designed-for, recoverable crash into a stream that can never resume: a
      # resumed run rebuilds that chunk from wherever its reads and its poll happen to land, which
      # is the same byte range only for a size flush.
      def append(bytes, index:)
        data = bytes.to_s.dup.force_encoding(Encoding::BINARY)
        digest = Digest::SHA256.hexdigest(data)
        if @last && @last.fetch("index") == index
          return @last if @last.fetch("sha256") == digest

          raise Backstage::CaptureError.new(
            "chunk #{index} of #{@stream_id} was already written with different bytes",
            stream_id: @stream_id, offset: @offset
          )
        end

        @secret_guard.check!(data)
        part = File.join(@directory, "#{index}.log.part")
        final = File.join(@directory, "#{index}.log")
        existing = File.exist?(final) ? Digest::SHA256.file(final).hexdigest : nil
        if existing && existing != digest && index < @committed_chunks
          raise Backstage::CaptureError.new(
            "chunk #{index} of #{@stream_id} is already durable with different bytes",
            stream_id: @stream_id, offset: @offset
          )
        end

        # Already durable byte for byte: the rename is not repeated. Otherwise the file is written
        # under the incomplete name and renamed over whatever orphan was there.
        unless existing == digest
          File.open(part, File::WRONLY | File::CREAT | File::TRUNC, 0o600) do |file|
            file.binmode
            file.write(data)
            file.flush
            file.fsync
          end
          File.rename(part, final)
          sync_directory(@directory)
        end

        start_offset = @offset
        @offset += data.bytesize
        @chunk_count += 1
        @chain_sha256 = Digest::SHA256.hexdigest("#{@chain_sha256}:#{digest}")
        @last = {
          "index" => index,
          "path" => final,
          "sha256" => digest,
          "chain_sha256" => @chain_sha256,
          "bytes" => data.bytesize,
          "start_offset" => start_offset,
          "end_offset" => @offset
        }.freeze
      end

      # An artifact record for one chunk. The id is derived from the stream and the chunk index so
      # an in-process retry of a failed commit references the same artifact and reconciles, rather
      # than minting a second record for bytes that are already on disk.
      def chunk_artifact(chunk)
        Records.artifact(
          work_item_id: @work_item_id, run_id: @run_id, kind: @kind,
          path: chunk.fetch("path"), sha256: chunk.fetch("sha256"), provenance: chunk_provenance(chunk)
        ).merge(
          "id" => self.class.chunk_artifact_id(@stream_id, chunk.fetch("index")),
          "stream_id" => @stream_id,
          "chunk_index" => chunk.fetch("index"),
          "bytes" => chunk.fetch("bytes"),
          "start_offset" => chunk.fetch("start_offset"),
          "end_offset" => chunk.fetch("end_offset")
        )
      end

      def self.chunk_artifact_id(stream_id, index)
        "artifact-#{Digest::SHA256.hexdigest("chunk:#{stream_id}:#{index}")[0, 12]}"
      end

      def self.manifest_artifact_id(stream_id)
        "artifact-#{Digest::SHA256.hexdigest("stream:#{stream_id}")[0, 12]}"
      end

      # Writes `stream.json`, the manifest that makes the chunk files readable as one stream, and
      # returns its artifact record. Same durability rule as a chunk.
      def finalize(summary)
        manifest = {
          "schema_version" => 1,
          "stream_id" => @stream_id,
          "work_item_id" => @work_item_id,
          "run_id" => @run_id,
          "kind" => @kind,
          "directory" => @directory,
          "chain_sha256" => @chain_sha256
        }.merge(JSON.parse(JSON.generate(summary || {})))
        @secret_guard.check!(manifest)
        data = JSON.pretty_generate(manifest)
        part = File.join(@directory, "stream.json.part")
        final = File.join(@directory, "stream.json")
        File.open(part, File::WRONLY | File::CREAT | File::TRUNC, 0o600) do |file|
          file.write(data)
          file.flush
          file.fsync
        end
        File.rename(part, final)
        sync_directory(@directory)
        Records.artifact(
          work_item_id: @work_item_id, run_id: @run_id, kind: "#{@kind}_manifest",
          path: final, sha256: Digest::SHA256.hexdigest(data), provenance: manifest_provenance
        ).merge("id" => self.class.manifest_artifact_id(@stream_id), "stream_id" => @stream_id)
      end

      private

      def chunk_provenance(chunk)
        base = @provenance.is_a?(Hash) ? JSON.parse(JSON.generate(@provenance)) : { "adapter" => @provenance.to_s }
        base.merge("stream_id" => @stream_id, "chunk_index" => chunk.fetch("index"))
      end

      def manifest_provenance
        base = @provenance.is_a?(Hash) ? JSON.parse(JSON.generate(@provenance)) : { "adapter" => @provenance.to_s }
        base.merge("stream_id" => @stream_id)
      end

      def sync_directory(directory)
        File.open(directory) { |handle| handle.fsync }
      rescue SystemCallError, IOError
        nil
      end
    end
  end
end

Backstage::ArtifactStore = Backstage::Adapters::LocalFiles::ArtifactStore unless defined?(Backstage::ArtifactStore)
