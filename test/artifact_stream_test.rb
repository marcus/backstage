# frozen_string_literal: true

require_relative "test_helper"

# The durability half of runtime capture. A chunk is written to `<index>.log.part`, fsynced,
# renamed, and only then described to a caller that may commit an event referencing it. What that
# buys is the property proven here: at every moment a chunk file is either complete under a name
# something can reference, or an orphan `.part` nothing refers to and a cleaner may remove.
class ArtifactStreamTest < Minitest::Test
  Guard = Backstage::Support::SecretGuard

  def open_stream(directory, secrets: [], **overrides)
    store = Backstage::ArtifactStore.new(File.join(directory, "artifacts"),
                                         secret_guard: Guard.new(secret_values: secrets))
    handle = store.open_stream(
      work_item_id: "work-1", run_id: "run-a1b2", stream_id: "run-a1b2:1:2:harness",
      kind: "runtime_output", provenance: { "adapter" => "test" }, **overrides
    )
    [store, handle]
  end

  def test_chunks_land_under_a_stream_directory_named_for_index_and_step
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)

      assert_equal "2-harness", File.basename(handle.directory)
      assert_equal %w[artifacts work-1 run-a1b2 streams 2-harness],
                   handle.directory.sub("#{directory}/", "").split("/")
    end
  end

  def test_append_returns_a_complete_durable_chunk_with_contiguous_offsets
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)

      first = handle.append("hello\n", index: 0)
      second = handle.append("world\n", index: 1)

      assert_equal [0, 6], [first.fetch("start_offset"), first.fetch("end_offset")]
      assert_equal [6, 12], [second.fetch("start_offset"), second.fetch("end_offset")]
      assert_equal "hello\n", File.binread(first.fetch("path"))
      assert_equal Digest::SHA256.hexdigest("world\n"), second.fetch("sha256")
      # Nothing is left behind under the incomplete name.
      assert_empty Dir[File.join(handle.directory, "*.part")]
      assert_equal %w[0.log 1.log], Dir.children(handle.directory).sort
      refute_equal first.fetch("chain_sha256"), second.fetch("chain_sha256")
    end
  end

  def test_appending_the_same_chunk_twice_is_the_same_chunk
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)
      handle.append("first\n", index: 0)

      once = handle.append("retry\n", index: 1)
      twice = handle.append("retry\n", index: 1)

      # A caller whose commit failed retries the whole flush. The offsets it was handed must not
      # shift underneath it, or the retry would describe different bytes than the first attempt.
      assert_equal once, twice
      assert_equal 12, handle.offset
      assert_equal 2, handle.chunk_count
    end
  end

  def test_reusing_an_index_for_different_bytes_is_refused
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)
      handle.append("original\n", index: 0)

      error = assert_raises(Backstage::CaptureError) { handle.append("different\n", index: 0) }

      assert_includes error.message, "already written with different bytes"
      assert_equal "run-a1b2:1:2:harness", error.stream_id
    end
  end

  # The case a handle cannot see in its own memory: a *second* handle over the same stream, which
  # is what a re-open produces. An acknowledged event already names 0.log and its sha, so the bytes
  # under that name are not this handle's to replace.
  def test_a_fresh_handle_refuses_to_overwrite_a_chunk_an_event_may_already_name
    in_tmpdir do |directory|
      _store, first = open_stream(directory)
      acknowledged = first.append("a" * 32, index: 0)

      _store, second = open_stream(directory, committed_chunks: 1)
      error = assert_raises(Backstage::CaptureError) { second.append("b" * 32, index: 0) }

      assert_includes error.message, "already durable with different bytes"
      assert_equal "a" * 32, File.binread(acknowledged.fetch("path"))
      assert_equal acknowledged.fetch("sha256"),
                   Digest::SHA256.hexdigest(File.binread(acknowledged.fetch("path")))
    end
  end

  # The other side of that line, and the reason it has to be a line rather than a rule about files:
  # a crash between a chunk's rename and its commit leaves a durable file nothing references, at
  # exactly the index a resumed writer starts from. It rebuilds that chunk from wherever its own
  # reads fall, which is not the same byte range unless the flush was a size flush. Refusing there
  # would make a designed-for crash unrecoverable.
  def test_an_orphan_chunk_no_event_names_is_rebuilt_rather_than_refused
    in_tmpdir do |directory|
      _store, crashed = open_stream(directory)
      crashed.append("acknowledged\n", index: 0)
      crashed.append("committed to disk, never committed to history\n", index: 1)

      # One chunk was acknowledged; the second is the orphan.
      _store, resumed = open_stream(directory, start_offset: 13, committed_chunks: 1)
      rebuilt = resumed.append("a different split of the same output\n", index: 1)

      assert_equal "a different split of the same output\n", File.binread(rebuilt.fetch("path"))
      assert_equal 13, rebuilt.fetch("start_offset")
      assert_equal "acknowledged\n", File.binread(File.join(resumed.directory, "0.log"))
      assert_empty Dir[File.join(resumed.directory, "*.part")]
    end
  end

  def test_a_fresh_handle_appending_identical_bytes_is_the_same_chunk
    in_tmpdir do |directory|
      _store, first = open_stream(directory)
      once = first.append("same bytes\n", index: 0)

      _store, second = open_stream(directory)
      twice = second.append("same bytes\n", index: 0)

      assert_equal once, twice
      assert_equal %w[0.log], Dir.children(second.directory).sort
    end
  end

  # A chunk is BINARY and may hold invalid UTF-8; a configured secret may hold non-ASCII bytes.
  # Comparing those in Ruby raises rather than answering, which would fail open exactly when a
  # secret is present.
  def test_a_non_ascii_secret_is_caught_in_a_binary_chunk
    in_tmpdir do |directory|
      secret = "clé-très-secrète-ünïcödé"
      _store, handle = open_stream(directory, secrets: [secret])
      chunk = "broken \xFF\xFE bytes token=#{secret}\n".b

      assert_raises(Backstage::ContractError) { handle.append(chunk, index: 0) }
      assert_empty Dir.children(handle.directory)

      # And the same bytes without the secret are still ordinary output.
      handle.append("broken \xFF\xFE bytes\n".b, index: 0)

      assert_equal %w[0.log], Dir.children(handle.directory)
    end
  end

  def test_a_torn_part_file_is_invisible_and_cleanup_eligible
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)
      handle.append("acknowledged\n", index: 0)

      # What a crash between write and rename leaves behind.
      File.binwrite(File.join(handle.directory, "1.log.part"), "half a chunk")

      assert_path_exists File.join(handle.directory, "0.log")
      refute_path_exists File.join(handle.directory, "1.log")
      # The manifest indexes only complete chunks, so nothing references the torn bytes.
      artifact = handle.finalize("chunks" => 1, "bytes" => 13, "coverage" => "complete")
      manifest = JSON.parse(File.read(artifact.fetch("path")))

      assert_equal 1, manifest.fetch("chunks")
      refute_includes File.read(artifact.fetch("path")), "1.log.part"
      assert_equal ["1.log.part"], Dir.children(handle.directory).grep(/\.part\z/)
    end
  end

  def test_finalize_writes_a_manifest_artifact_with_a_deterministic_id
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)
      chunk = handle.append("line\n", index: 0)

      artifact = handle.finalize("stream_id" => handle.stream_id, "chunks" => 1,
                                 "artifact_ids" => [handle.class.chunk_artifact_id(handle.stream_id, 0)])

      assert_equal "stream.json", File.basename(artifact.fetch("path"))
      assert_equal "runtime_output_manifest", artifact.fetch("kind")
      assert_equal handle.class.manifest_artifact_id(handle.stream_id), artifact.fetch("id")
      assert_equal Digest::SHA256.hexdigest(File.read(artifact.fetch("path"))), artifact.fetch("sha256")

      manifest = JSON.parse(File.read(artifact.fetch("path")))

      assert_equal chunk.fetch("chain_sha256"), manifest.fetch("chain_sha256")
      assert_empty Dir[File.join(handle.directory, "*.part")]
    end
  end

  def test_a_chunk_artifact_record_is_derived_from_the_stream_and_index
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)
      chunk = handle.append("payload\n", index: 3)
      artifact = handle.chunk_artifact(chunk)

      assert_equal Backstage::Adapters::LocalFiles::ArtifactStore::StreamHandle
                     .chunk_artifact_id("run-a1b2:1:2:harness", 3), artifact.fetch("id")
      assert_equal "run-a1b2:1:2:harness", artifact.fetch("stream_id")
      assert_equal 3, artifact.fetch("chunk_index")
      assert_equal chunk.fetch("sha256"), artifact.fetch("sha256")
      assert_equal 3, artifact.dig("provenance", "chunk_index")
    end
  end

  def test_resuming_continues_the_same_offsets_and_hash_chain
    in_tmpdir do |directory|
      _store, handle = open_stream(directory)
      first = handle.append("aaaa\n", index: 0)

      _store, resumed = open_stream(directory, start_offset: first.fetch("end_offset"),
                                               chain_sha256: first.fetch("chain_sha256"))
      second = resumed.append("bbbb\n", index: 1)

      assert_equal 5, second.fetch("start_offset")
      assert_equal 10, second.fetch("end_offset")
      assert_equal handle.append("bbbb\n", index: 1).fetch("chain_sha256"), second.fetch("chain_sha256")
    end
  end

  def test_a_secret_reaching_the_chunk_writer_stops_the_write
    in_tmpdir do |directory|
      _store, handle = open_stream(directory, secrets: ["canary-supersecret"])

      assert_raises(Backstage::ContractError) { handle.append("token=canary-supersecret\n", index: 0) }

      # Nothing durable, under either name: a chunk that should not exist never gets referenced.
      assert_empty Dir.children(handle.directory)
    end
  end
end
