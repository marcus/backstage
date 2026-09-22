# frozen_string_literal: true

require "base64"

module Backstage
  module Support
    # Turns an arbitrary byte stream into complete newline-delimited records. It is pure: no IO,
    # no clock, no store, and its whole state round-trips through a JSON-safe hash, which is what
    # lets a checkpoint resume framing exactly where a crash left it.
    #
    # Offsets are byte positions in the stream the framer was fed — the *redacted* stream, since
    # redaction runs first — counted from 0 at open. `start_offset` is the first byte of the
    # record, `end_offset` the byte after its terminating newline, so frames tile the stream
    # without gaps and a chunk's byte range and a record's byte range are the same coordinates.
    #
    # A frame:
    #
    #   { "index" => 41, "start_offset" => 8192, "end_offset" => 8231,
    #     "text" => "...", "encoding" => "utf-8" | "replaced", "truncated" => false }
    #
    # Three things it refuses to do badly:
    #
    # - A multibyte character split across two reads is not mangled. Bytes accumulate binary and a
    #   line is only transcoded once its newline has arrived, so the tail of a UTF-8 sequence is
    #   simply part of a pending record.
    # - Bytes that are not valid UTF-8 are not dropped and not raised on. The record is scrubbed to
    #   U+FFFD and flagged `encoding: "replaced"`, because a runtime printing broken bytes is
    #   information, not a reason to lose the line.
    # - A process printing a gigabyte with no newline cannot exhaust memory. At `max_record_bytes`
    #   the pending line is emitted `truncated: true` and everything up to the next newline is
    #   discarded, so pending bytes never exceed that bound.
    class RecordFramer
      DEFAULT_MAX_RECORD_BYTES = 64 * 1024

      NEWLINE = "\n".b

      attr_reader :max_record_bytes, :record_index, :byte_offset

      def initialize(max_record_bytes: DEFAULT_MAX_RECORD_BYTES, record_index: 0, byte_offset: 0,
                     partial: nil, truncating: false)
        @max_record_bytes = Integer(max_record_bytes)
        raise Backstage::ContractError, "max_record_bytes must be positive" unless @max_record_bytes.positive?

        @record_index = Integer(record_index)
        @byte_offset = Integer(byte_offset)
        @buffer = binary(partial)
        @truncating = truncating ? true : false
      end

      # Rebuilds a framer from `state`. `max_record_bytes` is a configuration choice rather than
      # stream history, so it is passed rather than persisted: changing the bound between runs
      # must not be silently overridden by an old checkpoint.
      def self.restore(state, max_record_bytes: DEFAULT_MAX_RECORD_BYTES)
        row = state || {}
        new(
          max_record_bytes: max_record_bytes,
          record_index: row["record_index"] || row[:record_index] || 0,
          byte_offset: row["byte_offset"] || row[:byte_offset] || 0,
          partial: decode(row["partial_b64"] || row[:partial_b64]),
          truncating: row["truncating"] || row[:truncating] || false
        )
      end

      def self.decode(encoded)
        return nil if encoded.nil? || encoded.to_s.empty?

        Base64.strict_decode64(encoded.to_s)
      end

      # Feeds bytes in. Returns the complete records they completed, oldest first, possibly none.
      def push(bytes)
        scan(bytes, emit: true)
      end

      # Consumes bytes exactly as `push` does — same record boundaries, same truncation decisions,
      # same index and offset arithmetic — and builds no frames. It exists so a caller can keep a
      # second framer over the bytes it has actually made durable, and checkpoint *that* one,
      # without transcoding every record twice. Sharing one scan loop is the point: a second
      # implementation of the framing rules would be free to drift from this one, and a checkpoint
      # that disagrees with the framer by a single record boundary renumbers history on resume.
      #
      # Returns self.
      def advance(bytes)
        scan(bytes, emit: false)
        self
      end

      # Ends the stream. Returns the trailing record when the last line had no newline, which is
      # the ordinary case for a process that exits mid-line.
      def finish
        return [] if @buffer.empty?

        if @truncating
          # The record was already emitted truncated; this is the tail being discarded.
          @byte_offset += @buffer.bytesize
          @buffer = binary(nil)
          return []
        end

        [take(@buffer.bytesize, truncated: false)]
      end

      # Bytes held for a record that has not ended yet. Bounded by `max_record_bytes`.
      def pending_bytes
        @buffer.bytesize
      end

      # Whether the framer is discarding the tail of an oversized record.
      def truncating?
        @truncating
      end

      # JSON-safe, bounded, and complete: restoring this hash resumes framing byte for byte.
      def state
        {
          "partial_b64" => Base64.strict_encode64(@buffer),
          "record_index" => @record_index,
          "byte_offset" => @byte_offset,
          "truncating" => @truncating
        }
      end

      private

      # The one framing loop. `emit` decides only whether a frame is built for each record; every
      # boundary, index and offset decision is the same either way.
      def scan(bytes, emit:)
        @buffer << binary(bytes)
        frames = []
        loop do
          if @truncating
            break unless skip_to_newline

            next
          end

          newline = @buffer.index(NEWLINE)
          if newline && newline <= @max_record_bytes
            frame = take(newline + 1, truncated: false, emit: emit)
            frames << frame if frame
            next
          end
          # Oversized, whether or not its newline has arrived. Capping it here is what keeps a
          # record — and every preview and event derived from one — bounded by configuration
          # rather than by what the runtime happened to print.
          break unless newline || @buffer.bytesize > @max_record_bytes

          frame = take(@max_record_bytes, truncated: true, emit: emit)
          frames << frame if frame
          @truncating = true
        end
        frames
      end

      # Consumes `length` bytes as one record and advances the stream position past them.
      def take(length, truncated:, emit: true)
        raw = @buffer.byteslice(0, length)
        @buffer = @buffer.byteslice(length, @buffer.bytesize - length) || binary(nil)
        start_offset = @byte_offset
        @byte_offset += raw.bytesize
        index = @record_index
        @record_index += 1
        return nil unless emit

        text, encoding = transcode(raw)
        {
          "index" => index,
          "start_offset" => start_offset,
          "end_offset" => @byte_offset,
          "text" => text,
          "encoding" => encoding,
          "truncated" => truncated
        }
      end

      # A record's text excludes its own line ending; CRLF is a terminator, not content.
      def transcode(raw)
        body = raw.end_with?(NEWLINE) ? raw.byteslice(0, raw.bytesize - 1) : raw
        body = body.byteslice(0, body.bytesize - 1) if body.end_with?("\r".b)
        text = body.dup.force_encoding(Encoding::UTF_8)
        return [text, "utf-8"] if text.valid_encoding?

        [text.scrub("�"), "replaced"]
      end

      # Discards the remainder of an oversized record. Returns false when no newline has arrived
      # yet, having dropped what it has so nothing accumulates while waiting.
      def skip_to_newline
        newline = @buffer.index(NEWLINE)
        if newline.nil?
          @byte_offset += @buffer.bytesize
          @buffer = binary(nil)
          return false
        end
        @byte_offset += newline + 1
        @buffer = @buffer.byteslice(newline + 1, @buffer.bytesize - newline - 1) || binary(nil)
        @truncating = false
        true
      end

      def binary(value)
        value.to_s.dup.force_encoding(Encoding::BINARY)
      end
    end
  end
end
