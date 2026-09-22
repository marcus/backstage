# frozen_string_literal: true

require "delegate"
require_relative "test_helper"

# Runtime capture, proven against the properties that make it worth having: bytes become durable
# chunks before any event claims them, a secret split across a chunk boundary never lands anywhere,
# a replayed stream contributes nothing already recorded, a failed append is loud rather than
# silently in-memory, and a noisy process cannot grow the buffers without bound.
class RuntimeCaptureTest < Minitest::Test
  Capture = Backstage::Application::RuntimeCapture
  Sink = Backstage::Application::CaptureSink
  Guard = Backstage::Support::SecretGuard
  Recorder = Backstage::Application::ActivityRecorder
  STREAMS = Backstage::Ports::RuntimeCapture::STREAM_COLLECTION

  RUN = { "id" => "run-a1b2", "phase" => "implementation" }.freeze
  ATTEMPT = { "id" => "attempt-1", "number" => 1 }.freeze

  # Time is injected, so a 250 ms flush boundary is exact rather than a sleep and a hope.
  class FakeClock < Backstage::Ports::Clock
    attr_reader :now

    def initialize(start = Time.utc(2026, 9, 9, 12, 0, 0))
      @now = start
    end

    def advance(seconds)
      @now += seconds
      self
    end

    def wait(seconds, interrupt: nil)
      advance(seconds)
      true
    end
  end

  # A deliberately trivial interpreter: enough to prove semantic flushing, sentinels and the
  # malformed path without importing a provider's format. The real ones are Pi's and the runners'.
  class ToyInterpreter < Backstage::Ports::StreamInterpreter
    def self.restore(state)
      new(seen: (state || {})["seen"].to_i)
    end

    def initialize(seen: 0)
      @seen = seen
    end

    def state
      { "seen" => @seen }
    end

    def observe(frame)
      text = frame.fetch("text")
      return [] unless text.start_with?("{")

      row = begin
        JSON.parse(text)
      rescue JSON::ParserError
        return [{ "malformed" => true }]
      end
      @seen += 1
      case row["kind"]
      when "tool"
        [{ "type" => "agent.tool_observed", "summary" => "tool #{row["name"]}",
           "data" => { "tool_name" => row["name"] }, "provenance" => "agent_reported",
           "provider_session_id" => row["session"] }.compact]
      when "session"
        # Metadata, not history: it names the session and is no event of its own.
        [{ "provider_session_id" => row["session"] }]
      when "sentinel"
        [{ "type" => "artifact.available", "summary" => "sentinel #{row["name"]}",
           "data" => { "name" => row["name"] }, "provenance" => "runtime_reported",
           "sentinel" => { "name" => row["name"], "payload" => row["payload"] } }]
      else
        []
      end
    end
  end

  # A stream whose records are a machine protocol, so the capture gives it the larger record bound.
  class ProtocolInterpreter < Backstage::Ports::StreamInterpreter
    def protocol? = true
  end

  # A store whose commits stop working partway through, which is what a full disk looks like from
  # here. Deployment identity is minted before wrapping so the failure lands on a capture commit.
  class BreakingStore < SimpleDelegator
    def initialize(store, after:)
      super(store)
      @after = after
      @count = 0
    end

    def commit(writes, expect: [], activity: [])
      @count += 1
      raise Errno::EIO, "state log is gone" if @count > @after

      __getobj__.commit(writes, expect: expect, activity: activity)
    end
  end

  # A store that applies a commit and then loses the answer, the way a process killed between
  # fsync and return does. The caller has no way to know its write landed except by retrying.
  class LosingStore < SimpleDelegator
    def initialize(store, on:)
      super(store)
      @on = on
      @count = 0
    end

    def commit(writes, expect: [], activity: [])
      @count += 1
      result = __getobj__.commit(writes, expect: expect, activity: activity)
      raise Errno::EIO, "acknowledgement lost" if @count == @on

      result
    end
  end

  def build(directory, secrets: [], clock: FakeClock.new, store: nil, **options)
    guard = Guard.new(secret_values: secrets)
    store ||= Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
    artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
    sink = Sink::Durable.new(store: store, artifact_store: artifacts, work_item_id: "work-1",
                             run: RUN, attempt: ATTEMPT)
    capture = Capture.new(sink: sink, clock: clock, run: RUN, attempt: ATTEMPT,
                          secret_guard: guard, **options)
    [store, artifacts, capture]
  end

  def events(store, **filters)
    store.read_activity(filters: filters, limit: 1000).fetch("events")
  end

  def observed(store) = events(store).select { |event| event.fetch("type") == "runtime.observed" }

  # The coalesced chunk events, without the dedicated record-fault reports that share their type.
  def coalesced(store)
    observed(store).reject do |event|
      event.dig("data", "malformed") == true || event.dig("data", "truncated") == true
    end
  end

  def chunk_files(directory)
    Dir[File.join(directory, "artifacts", "work-1", "run-a1b2", "streams", "*", "*.log")].sort
  end

  def stream_directory(directory)
    Dir[File.join(directory, "artifacts", "work-1", "run-a1b2", "streams", "*")].first
  end

  # --- the basic contract -----------------------------------------------------------------------

  def test_a_stream_registers_a_checkpoint_before_any_byte_is_captured
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory)
      writer = capture.open(step: "harness")

      row = store.fetch(STREAMS, writer.id)

      assert_equal "run-a1b2:1:0:harness", writer.id
      assert_equal 0, row.fetch("revision")
      assert_equal 0, row.fetch("last_offset")
      assert_nil row.fetch("closed_at")
      assert_equal "implementation", row.fetch("phase")
      assert_equal "work-1", row.fetch("work_item_id")
      assert_equal "attempt-1", row.fetch("attempt_id")
    end
  end

  def test_every_acknowledged_event_points_at_a_complete_chunk_file
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 64)
      writer = capture.open(step: "harness")
      writer.write("#{"a" * 63}\n#{"b" * 63}\n")
      summary = writer.close

      assert_equal "complete", summary.fetch("coverage")
      assert_equal 2, summary.fetch("chunks")
      assert_equal 128, summary.fetch("bytes")
      assert_equal 2, summary.fetch("records")

      refute_empty observed(store)
      observed(store).each do |event|
        artifact = store.fetch("artifacts", event.fetch("artifact_refs").first)

        assert_path_exists artifact.fetch("path")
        assert_equal artifact.fetch("sha256"), Digest::SHA256.hexdigest(File.binread(artifact.fetch("path")))
        assert_equal event.dig("data", "sha256"), artifact.fetch("sha256")
      end
      assert_empty Dir[File.join(stream_directory(directory), "*.part")]
    end
  end

  def test_the_checkpoint_and_close_summary_describe_the_same_stream
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 64)
      writer = capture.open(step: "harness")
      writer.write("#{"c" * 63}\n")
      summary = writer.close(reason: "close")

      row = store.fetch(STREAMS, writer.id)

      assert_equal summary.fetch("last_offset"), row.fetch("last_offset")
      assert_equal summary.fetch("records"), row.fetch("records")
      assert_equal summary.fetch("chunks"), row.fetch("chunks")
      assert_equal "close", row.fetch("close_reason")
      refute_nil row.fetch("closed_at")
      assert_includes summary.fetch("artifact_ids"), row_manifest_id(writer.id)
    end
  end

  def row_manifest_id(stream_id)
    Backstage::Adapters::LocalFiles::ArtifactStore::StreamHandle.manifest_artifact_id(stream_id)
  end

  # --- secrets ----------------------------------------------------------------------------------

  def test_a_secret_split_across_a_chunk_boundary_never_reaches_a_file_a_record_or_an_event
    in_tmpdir do |directory|
      secret = "canary-averylongsecretvalue1234567890"
      store, _artifacts, capture = build(directory, secrets: [secret], flush_bytes: 64)
      writer = capture.open(step: "harness")

      # The secret is written in two halves, each in its own call, and the flush size is small
      # enough that a naive implementation would have already persisted the first half.
      writer.write("#{"p" * 60}\ntoken=#{secret[0, 12]}")
      writer.write("#{secret[12..]} done\n#{"q" * 70}\n")
      summary = writer.close

      assert_operator summary.fetch("chunks"), :>, 1
      persisted = chunk_files(directory).map { |path| File.binread(path) }.join

      refute_includes persisted, secret
      assert_includes persisted, "token=[REDACTED] done"
      refute_includes File.read(store.path), secret
      refute_includes File.read(store.path), secret[12..]
      events(store).each { |event| refute_includes JSON.generate(event), secret }
      # Offsets index the persisted bytes, so the last chunk ends exactly at the file total.
      assert_equal persisted.bytesize, summary.fetch("last_offset")
    end
  end

  # --- crash and failure semantics --------------------------------------------------------------

  def test_a_torn_part_file_is_referenced_by_nothing_and_is_cleanup_eligible
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 32)
      writer = capture.open(step: "harness")
      writer.write("#{"m" * 31}\n")
      writer.close

      torn = File.join(stream_directory(directory), "9.log.part")
      File.binwrite(torn, "bytes a crash left behind")

      referenced = store.list("artifacts").map { |artifact| artifact.fetch("path") }

      refute_includes referenced, torn
      events(store).flat_map { |event| Array(event["artifact_refs"]) }.each do |id|
        refute_equal torn, store.fetch("artifacts", id)&.fetch("path")
      end
      assert_path_exists torn
      refute_path_exists File.join(stream_directory(directory), "9.log")
    end
  end

  def test_a_failed_commit_raises_capture_error_and_leaves_the_chunk_unreferenced
    in_tmpdir do |directory|
      guard = Guard.new
      inner = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      inner.deployment_id # mint before the breakage, so the failure lands on a capture commit
      store = BreakingStore.new(inner, after: 1) # 1 = the stream's registration
      _store, _artifacts, capture = build(directory, store: store, flush_bytes: 32)
      writer = capture.open(step: "harness")

      error = assert_raises(Backstage::CaptureError) { writer.write("#{"n" * 31}\n") }

      assert_equal writer.id, error.stream_id
      assert_equal 0, error.offset
      assert_equal "Errno::EIO", error.cause_class
      assert_equal "failed", error.to_h.fetch("status")

      # The bytes are durable; nothing claims them. That is the correct order to fail in.
      assert_equal 1, chunk_files(directory).length
      assert_empty inner.list("artifacts")
      assert_empty observed(inner)
      assert writer.failed?

      # A failed writer will not quietly keep going, and its summary says so.
      assert_raises(Backstage::CaptureError) { writer.write("more\n") }
      assert_equal "failed", writer.close.fetch("coverage")
    end
  end

  # --- deduplication ----------------------------------------------------------------------------

  def test_a_resumed_stream_skips_bytes_already_durable_and_continues
    in_tmpdir do |directory|
      guard = Guard.new
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      script = 4.times.map { |i| "#{("a".ord + i).chr * 31}\n" }

      _store, _artifacts, capture = build(directory, store: store, flush_bytes: 32)
      first = capture.open(step: "harness")
      first.write(script[0])
      first.write(script[1])
      # No close: this is a process that died with two chunks acknowledged.
      committed = observed(store).length
      checkpoint = store.fetch(STREAMS, first.id)

      assert_equal 64, checkpoint.fetch("last_offset")

      _store, _artifacts, resumed_capture = build(directory, store: store, flush_bytes: 32)
      resumed = resumed_capture.open(step: "harness", resume: true)

      assert_equal first.id, resumed.id

      # The provider replays its whole stream from byte zero, then continues.
      script.each { |line| resumed.write(line) }
      summary = resumed.close

      assert_equal 4, summary.fetch("chunks")
      assert_equal 128, summary.fetch("last_offset")
      assert_equal 4, chunk_files(directory).length
      assert_equal committed + 2, observed(store).length
      assert_equal (0..3).map { |i| ("a".ord + i).chr * 31 }.join("\n") + "\n",
                   chunk_files(directory).map { |path| File.binread(path) }.join
    end
  end

  # --- checkpoint and offset consistency --------------------------------------------------------

  # Irregular on purpose. Lines that do not divide the flush size are the ordinary case, and they
  # are what makes a chunk boundary fall inside a record — where a checkpoint that describes more
  # bytes than it acknowledges renumbers every record after it.
  def crash_fixture
    lines = [
      "starting up",
      JSON.generate("kind" => "tool", "name" => "read_file", "session" => "sess-1"),
      "a" * 3,
      "",
      "b" * 71,
      JSON.generate("kind" => "noise"),
      "{not json at all",
      "c" * 17,
      JSON.generate("kind" => "sentinel", "name" => "repository_prepared", "payload" => { "branch" => "topic" }),
      "d" * 45,
      JSON.generate("kind" => "tool", "name" => "write_file"),
      "trailing without a newline"
    ]
    "#{lines[0..-2].join("\n")}\n#{lines.last}"
  end

  # Delivered in sizes that have nothing to do with the flush size or the line lengths, so writes,
  # records and chunks all disagree about where a boundary is.
  def irregular_writes(fixture, sizes: [7, 40, 3, 96, 13, 61, 5])
    writes = []
    position = 0
    index = 0
    while position < fixture.bytesize
      size = sizes[index % sizes.length]
      writes << fixture.byteslice(position, size)
      position += size
      index += 1
    end
    writes
  end

  # Everything a consumer would read as "what happened", in the order it happened. Store markers
  # are excluded: their ids are minted, not derived, so they cannot be equal across two stores.
  def told_history(store)
    events(store).reject { |event| event.dig("source", "provenance") == "store" }
                 .map { |event| [event.fetch("type"), event.dig("data", "record_index"), event.fetch("event_id")] }
  end

  def capture_run(directory, store, writes, resume: false, **options)
    guard = Guard.new
    artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
    sink = Sink::Durable.new(store: store, artifact_store: artifacts, work_item_id: "work-1",
                             run: RUN, attempt: ATTEMPT)
    capture = Capture.new(sink: sink, clock: FakeClock.new, run: RUN, attempt: ATTEMPT,
                          secret_guard: guard, **options)
    writer = capture.open(step: "harness", interpreter: ToyInterpreter.new, resume: resume)
    writes.each { |bytes| writer.write(bytes) }
    writer.close
  end

  # A store that counts what reached it, to find every boundary a crash could fall on.
  class CountingStore < SimpleDelegator
    attr_reader :count

    def initialize(store)
      super
      @count = 0
    end

    def commit(writes, expect: [], activity: [])
      @count += 1
      __getobj__.commit(writes, expect: expect, activity: activity)
    end
  end

  # The property the whole checkpoint exists for: where the crash fell must not be visible in the
  # history. For every commit an uninterrupted run makes, a second run is killed immediately after
  # that commit lands, resumed from its checkpoint, replayed from byte zero by the provider, and
  # closed — and the events it ends up with must be the events the uninterrupted run produced.
  # Same records under the same indices, same ids, nothing told twice, and no ActivityConflictError
  # from an id reused for a different fact.
  def test_a_crash_at_any_flush_boundary_resumes_to_the_identical_history
    fixture = crash_fixture
    writes = irregular_writes(fixture)
    baseline = nil
    commits = nil

    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      counter = CountingStore.new(store)
      summary = capture_run(directory, counter, writes)
      baseline = told_history(store)
      commits = counter.count

      assert_equal fixture.bytesize, summary.fetch("last_offset")
      assert_operator commits, :>, 4, "the fixture must cross several flush boundaries"
      refute_empty baseline.select { |type, _index, _id| type == "agent.tool_observed" }
    end

    (2..commits).each do |crash_at|
      in_tmpdir do |directory|
        store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
        store.deployment_id
        begin
          capture_run(directory, LosingStore.new(store, on: crash_at), writes)
        rescue Backstage::CaptureError
          nil # the acknowledgement was lost and the process died with it
        end
        checkpoint = store.fetch(STREAMS, "run-a1b2:1:0:harness")
        framer = checkpoint.fetch("framer_state") || {}

        # The invariant the reviewer named: the checkpoint describes exactly the durable bytes.
        assert_equal checkpoint.fetch("last_offset"),
                     framer.fetch("byte_offset") + Base64.strict_decode64(framer.fetch("partial_b64")).bytesize,
                     "checkpoint after commit #{crash_at} describes bytes it never acknowledged"

        capture_run(directory, store, writes, resume: true)

        assert_equal baseline, told_history(store), "resumed after commit #{crash_at}"
        assert_equal told_history(store).map(&:last).uniq.length, told_history(store).length
        assert_equal fixture, chunk_files(directory).map { |path| File.binread(path) }.join
      end
    end
  end

  def framed_prefix(row)
    state = row.fetch("framer_state")
    state.fetch("byte_offset") + Base64.strict_decode64(state.fetch("partial_b64")).bytesize
  end

  # The reviewer's numbers exactly: a 32-byte flush and one 50-byte write of ten 5-byte lines. The
  # checkpoint stored beside `last_offset` must describe the stream as it stands at `last_offset` —
  # six complete records and two bytes of the seventh — and not the ten records the writer has
  # already framed out of bytes it has not acknowledged.
  def test_a_checkpoint_describes_the_bytes_it_acknowledges_and_no_more
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 32)
      writer = capture.open(step: "harness")
      writer.write((0...10).map { |index| "#{index}###\n" }.join)

      row = store.fetch(STREAMS, writer.id)

      assert_equal 32, row.fetch("last_offset")
      assert_equal 32, framed_prefix(row)
      assert_equal 6, row.fetch("framer_state").fetch("record_index")
      assert_equal 6, row.fetch("record_index")
      assert_equal 6, row.fetch("records")

      summary = writer.close

      assert_equal 50, summary.fetch("last_offset")
      assert_equal 10, summary.fetch("records")
    end
  end

  # Past a byte limit the framer keeps consuming bytes that will never be persisted, which is the
  # design: records and decisions outlive the byte budget. The checkpoint must not follow it there.
  def test_a_checkpoint_past_a_byte_limit_still_describes_only_the_durable_bytes
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 32, max_stream_bytes: 64)
      writer = capture.open(step: "harness")
      40.times { |index| writer.write("#{index % 10}###\n") }
      writer.close

      row = store.fetch(STREAMS, writer.id)

      assert_equal "truncated", row.fetch("coverage")
      assert_equal 64, row.fetch("last_offset")
      assert_equal 64, framed_prefix(row), "the checkpoint framer ran past the durable bytes"
      assert_equal 200 - 64, row.fetch("bytes_dropped")
      # Records went on being counted and named even though their bytes did not land.
      assert_equal 40, row.fetch("records")
      assert_operator row.fetch("framed_offset"), :>, row.fetch("last_offset")
    end
  end

  # Records commit past a limit while their bytes do not, so a resumed writer replays bytes whose
  # records are already history. It must renumber none of them and tell none of them twice.
  def test_resuming_a_truncated_stream_tells_no_record_twice
    fixture = 40.times.map { |index| "#{index % 10}###\n" }.join
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      store.deployment_id
      begin
        capture_run(directory, LosingStore.new(store, on: 4), [fixture], flush_bytes: 32,
                                                                         max_stream_bytes: 64)
      rescue Backstage::CaptureError
        nil
      end
      row = store.fetch(STREAMS, "run-a1b2:1:0:harness")

      assert_equal "truncated", row.fetch("coverage")
      assert_equal 64, framed_prefix(row)

      before = told_history(store)
      capture_run(directory, store, [fixture], resume: true, flush_bytes: 32, max_stream_bytes: 64)
      after = told_history(store)

      assert_equal before, after.first(before.length), "history was rewritten, not continued"
      assert_equal after.map(&:last).uniq.length, after.length
      records = after.filter_map { |_type, index, _id| index }

      assert_equal records.uniq, records
      assert_equal 64, store.fetch(STREAMS, "run-a1b2:1:0:harness").fetch("last_offset")
      assert_equal 64, chunk_files(directory).sum { |path| File.size(path) }
    end
  end

  # --- the crash window between rename and commit -------------------------------------------------

  # The design's own crash: the chunk file is fsynced and renamed, and the commit that would have
  # referenced it never lands. The file is durable, unreferenced, and sits at exactly the index a
  # resumed writer starts from — and the resumed run rebuilds that chunk from wherever its own
  # reads fall. Only a size flush is a pure function of last_offset; a semantic flush cuts where
  # the reader's read sizes put the record, so the rebuilt chunk holds different bytes and must be
  # allowed to replace an orphan that no event names.
  def test_an_orphan_chunk_does_not_block_a_resume_that_flushes_elsewhere
    record = "#{JSON.generate("kind" => "tool", "name" => "bash")}\n"
    tail = "trailing output line\n"
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      store.deployment_id
      # One read: the semantic flush cuts after both lines.
      assert_raises(Backstage::CaptureError) do
        capture_run(directory, BreakingStore.new(store, after: 1), [record + tail], flush_bytes: 4096)
      end
      orphan = File.join(stream_directory(directory), "0.log")

      assert_path_exists orphan
      assert_equal (record + tail).bytesize, File.size(orphan)
      assert_empty observed(store)
      assert_equal 0, store.fetch(STREAMS, "run-a1b2:1:0:harness").fetch("chunk_index")

      # The restarted provider delivers the same bytes as two reads, so the flush lands elsewhere.
      summary = capture_run(directory, store, [record, tail], resume: true, flush_bytes: 4096)

      assert_equal "complete", summary.fetch("coverage")
      assert_equal record.bytesize + tail.bytesize, summary.fetch("last_offset")
      assert_equal record + tail, chunk_files(directory).map { |path| File.binread(path) }.join
      assert_equal ["bash"], events(store, type: "agent.tool_observed").map { |event| event.dig("data", "tool_name") }
    end
  end

  # The same crash with no interpreter at all — a plain shell step — where the only thing cutting a
  # chunk is the drain loop's poll. Two runs never poll in the same place.
  def test_an_orphan_chunk_from_a_time_flush_does_not_block_resume
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      store.deployment_id
      clock = FakeClock.new
      _store, _artifacts, capture = build(directory, store: BreakingStore.new(store, after: 1),
                                                     clock: clock, flush_bytes: 4096)
      writer = capture.open(step: "harness")
      writer.write("x" * 300)
      clock.advance(0.3)

      assert_raises(Backstage::CaptureError) { writer.tick }
      assert_equal 300, File.size(File.join(stream_directory(directory), "0.log"))
      assert_empty observed(store)

      restarted = FakeClock.new
      _store, _artifacts, again = build(directory, store: store, clock: restarted, flush_bytes: 4096)
      resumed = again.open(step: "harness", resume: true)
      resumed.write("x" * 200)
      restarted.advance(0.3)
      resumed.tick
      resumed.write("#{"x" * 100}\n")
      summary = resumed.close

      assert_equal "complete", summary.fetch("coverage")
      assert_equal 301, summary.fetch("last_offset")
      assert_equal "#{"x" * 300}\n", chunk_files(directory).map { |path| File.binread(path) }.join
      assert_equal 301, observed(store).sum { |event| event.dig("data", "bytes") }
    end
  end

  def random_writes(fixture, seed)
    random = Random.new(seed)
    writes = []
    position = 0
    while position < fixture.bytesize
      slice = fixture.byteslice(position, random.rand(1..37))
      writes << slice
      position += slice.bytesize
    end
    writes
  end

  # The sweep the reviewer ran: crash between rename and commit at every commit, resume with a
  # different read sequence than the run that crashed. What must survive a different chunking is
  # the record stream — the semantic events, their indices and their ids — and the bytes.
  def test_a_resume_with_different_read_sizes_reproduces_the_same_records
    fixture = crash_fixture
    baseline = nil
    commits = nil

    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      counter = CountingStore.new(store)
      capture_run(directory, counter, random_writes(fixture, 11), flush_bytes: 64)
      baseline = told_history(store).reject { |type, _index, _id| type == "runtime.observed" }
      commits = counter.count

      refute_empty baseline
    end

    (1...commits).each do |crash_at|
      in_tmpdir do |directory|
        store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
        store.deployment_id
        begin
          capture_run(directory, BreakingStore.new(store, after: crash_at),
                      random_writes(fixture, 11), flush_bytes: 64)
        rescue Backstage::CaptureError
          nil # the chunk is durable, the commit never landed
        end

        summary = capture_run(directory, store, random_writes(fixture, 29), resume: true,
                                                                            flush_bytes: 64)
        history = told_history(store)

        assert_equal "complete", summary.fetch("coverage"), "resumed after commit #{crash_at}"
        assert_equal baseline, history.reject { |type, _index, _id| type == "runtime.observed" },
                     "records after a crash at commit #{crash_at}"
        assert_equal history.map(&:last).uniq.length, history.length
        assert_equal fixture, chunk_files(directory).map { |path| File.binread(path) }.join
        assert_equal summary.fetch("records"),
                     coalesced(store).sum { |event| event.dig("data", "records") }
      end
    end
  end

  def test_replaying_a_closed_stream_from_the_beginning_appends_no_history
    in_tmpdir do |directory|
      guard = Guard.new
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      clock = FakeClock.new
      payload = "#{"r" * 31}\n#{"s" * 31}\n"

      _store, _artifacts, capture = build(directory, store: store, clock: clock, flush_bytes: 32)
      capture.open(step: "harness").tap { |writer| writer.write(payload) }.close

      before = events(store).length
      files = chunk_files(directory).to_h { |path| [path, Digest::SHA256.hexdigest(File.binread(path))] }

      _store, _artifacts, again = build(directory, store: store, clock: clock, flush_bytes: 32)
      resumed = again.open(step: "harness", resume: true)
      resumed.write(payload)
      summary = resumed.close

      # Everything replayed was already durable, so the stream's totals are unchanged.
      assert_equal 2, summary.fetch("chunks")
      assert_equal 64, summary.fetch("last_offset")
      assert_equal before, events(store).length
      assert_equal files, chunk_files(directory).to_h { |path| [path, Digest::SHA256.hexdigest(File.binread(path))] }
    end
  end

  def test_retrying_a_commit_that_actually_landed_reconciles_instead_of_growing_history
    in_tmpdir do |directory|
      guard = Guard.new
      inner = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      inner.deployment_id
      artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
      # The failure a retry actually has to survive: the write landed, the acknowledgement did not.
      store = LosingStore.new(inner, on: 2)
      sink = Sink::Durable.new(store: store, artifact_store: artifacts, work_item_id: "work-1",
                               run: RUN, attempt: ATTEMPT)
      stream = sink.open(stream_id: "run-a1b2:1:0:harness", step: "harness", kind: "runtime_output",
                         opened_at: "2026-09-09T12:00:00.000000Z")

      assert_raises(Errno::EIO) { stream.commit(chunk_payload) }

      after_failure = events(inner).length
      retried = stream.commit(chunk_payload)

      assert retried.fetch("reconciled")
      assert_equal Backstage::Adapters::LocalFiles::ArtifactStore::StreamHandle
                     .chunk_artifact_id("run-a1b2:1:0:harness", 0), retried.fetch("artifact_id")
      assert_equal after_failure, events(inner).length
      assert_equal 1, chunk_files(directory).length
      assert_equal 1, inner.fetch(STREAMS, "run-a1b2:1:0:harness").fetch("revision")
      assert_equal 6, inner.fetch(STREAMS, "run-a1b2:1:0:harness").fetch("last_offset")
    end
  end

  def chunk_payload
    { "chunk_index" => 0, "payload" => "hello\n".b, "start_offset" => 0, "end_offset" => 6,
      "records" => 1, "record_index" => 1, "framed_offset" => 6, "malformed" => 0,
      "truncated_records" => 0,
      "reason" => "close", "coverage" => "complete", "bytes_dropped" => 0,
      "observations" => [], "framer_state" => { "record_index" => 1 },
      "occurred_at" => "2026-09-09T12:00:00.000000Z" }
  end

  # --- flushing ---------------------------------------------------------------------------------

  def test_a_quiet_stream_flushes_on_the_injected_clock_not_before
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, clock: (clock = FakeClock.new),
                                                    flush_bytes: 4096, flush_millis: 250)
      writer = capture.open(step: "harness")
      writer.write("a trickle\n")

      assert_empty observed(store)

      clock.advance(0.249)
      writer.tick

      assert_empty observed(store)

      clock.advance(0.001)
      writer.tick

      assert_equal 1, observed(store).length
      assert_equal "time", observed(store).fetch(0).dig("data", "reason")
      assert_equal 0, writer.buffered_bytes
    end
  end

  def test_a_semantic_observation_flushes_immediately_and_commits_its_event
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 1024 * 1024)
      writer = capture.open(step: "harness", interpreter: ToyInterpreter.new)
      writer.write("#{JSON.generate("kind" => "tool", "name" => "bash", "session" => "sess-9")}\n")

      tool = events(store, type: "agent.tool_observed")

      assert_equal 1, tool.length
      assert_equal "bash", tool.fetch(0).dig("data", "tool_name")
      assert_equal "sess-9", tool.fetch(0).fetch("provider_session_id")
      assert_equal "agent_reported", tool.fetch(0).dig("source", "provenance")
      assert_equal Recorder.event_id("agent.tool_observed", writer.id, 0), tool.fetch(0).fetch("event_id")
      assert_equal "semantic", observed(store).fetch(0).dig("data", "reason")
      # The bytes the observation came from are durable in the same commit.
      assert_equal tool.fetch(0).fetch("artifact_refs"), observed(store).fetch(0).fetch("artifact_refs")

      writer.write("#{JSON.generate("kind" => "sentinel", "name" => "repository_prepared", "payload" => { "branch" => "topic" })}\n")
      summary = writer.close

      assert_equal({ "branch" => "topic" }, summary.dig("sentinels", "repository_prepared"))
      assert_equal "sess-9", summary.fetch("provider_session_id")
      assert_equal "sess-9", store.fetch(STREAMS, writer.id).fetch("provider_session_id")
    end
  end

  # A provider's session line names the session every later event belongs to and is not itself a
  # fact worth a place in history. The sink must take it as metadata rather than reaching for a
  # type that is not there.
  def test_an_observation_with_no_type_is_metadata_and_not_an_event
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 1024)
      writer = capture.open(step: "harness", interpreter: ToyInterpreter.new)
      writer.write("#{JSON.generate("kind" => "session", "session" => "sess-42")}\n")

      assert_equal "sess-42", writer.close.fetch("provider_session_id")
      assert_equal "sess-42", store.fetch(STREAMS, writer.id).fetch("provider_session_id")
      assert_empty events(store, type: "agent.tool_observed")
      assert_equal 1, store.fetch(STREAMS, writer.id).fetch("records")
      # One chunk event for the bytes, the manifest event for the close, and nothing else.
      assert_equal %w[artifact.available runtime.observed],
                   told_history(store).map(&:first).uniq.sort
    end
  end

  # --- malformed records ------------------------------------------------------------------------

  def test_malformed_records_are_reported_in_a_bounded_way_and_never_dropped
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 1024 * 1024)
      writer = capture.open(step: "harness", interpreter: ToyInterpreter.new)
      writer.write("{not json at all #{"z" * 500}\n{\"kind\":\"noise\"}\nbroken \xFF\xFE bytes\n".b)
      summary = writer.close

      assert_equal 3, summary.fetch("records")
      assert_equal 2, summary.fetch("malformed")

      # A chunk event carries `malformed` as a count; the dedicated one carries it as `true`.
      malformed = observed(store).select { |event| event.dig("data", "malformed") == true }

      assert_equal 1, malformed.length
      assert_equal 2, malformed.fetch(0).dig("data", "malformed_records")
      assert_equal 0, malformed.fetch(0).dig("data", "offset")
      assert_operator malformed.fetch(0).dig("data", "preview").bytesize, :<=, 200
      # The bytes themselves are still in the chunk, so nothing was actually lost.
      persisted = chunk_files(directory).map { |path| File.binread(path) }.join

      assert_includes persisted, "z" * 500
      assert_includes persisted, "broken".b
    end
  end

  # --- limits -----------------------------------------------------------------------------------

  def test_a_stream_limit_stops_bytes_but_not_decisions
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 64, max_stream_bytes: 128)
      writer = capture.open(step: "harness", interpreter: ToyInterpreter.new)
      4.times { writer.write("#{"w" * 63}\n") }
      writer.write("#{JSON.generate("kind" => "tool", "name" => "after_limit")}\n")
      summary = writer.close

      assert_equal "truncated", summary.fetch("coverage")
      assert_equal 128, summary.fetch("last_offset")
      assert_operator summary.fetch("bytes_dropped"), :>, 0
      assert_equal 128, chunk_files(directory).sum { |path| File.size(path) }

      limits = observed(store).select { |event| event.dig("data", "reason") == "limit" }

      assert_equal 1, limits.length
      assert_equal 128, limits.fetch(0).dig("data", "truncated_from_offset")

      # A tool call after the limit is still history. Only the bytes were rationed.
      tool = events(store, type: "agent.tool_observed")

      assert_equal 1, tool.length
      assert_equal "after_limit", tool.fetch(0).dig("data", "tool_name")
      assert_equal "truncated", store.fetch(STREAMS, writer.id).fetch("coverage")
    end
  end

  def test_the_run_budget_is_shared_across_streams
    in_tmpdir do |directory|
      _store, _artifacts, capture = build(directory, flush_bytes: 64, max_run_bytes: 128)
      first = capture.open(step: "context")
      second = capture.open(step: "harness")
      2.times { first.write("#{"x" * 63}\n") }
      second.write("#{"y" * 63}\n")

      assert_equal "complete", first.close.fetch("coverage")
      assert_equal "truncated", second.close.fetch("coverage")
      assert_equal 128, capture.run_bytes
    end
  end

  # --- bounds -----------------------------------------------------------------------------------

  def test_eight_mebibytes_of_output_stays_bounded_and_chunks_exactly
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory) # 64 KiB flush, clock never advances
      writer = capture.open(step: "harness")
      line = "#{"d" * 4095}\n"
      total = 8 * 1024 * 1024
      writes = total / line.bytesize

      writes.times do
        writer.write(line)

        assert_operator writer.buffered_bytes, :<=, 64 * 1024
        assert_operator writer.pending_bytes, :<=, 64 * 1024
      end
      summary = writer.close

      assert_equal total, summary.fetch("bytes")
      assert_equal writes, summary.fetch("records")
      assert_equal total / (64 * 1024), summary.fetch("chunks")
      assert_equal summary.fetch("chunks"), chunk_files(directory).length
      assert_equal "complete", summary.fetch("coverage")
      assert_equal summary.fetch("chunks"), observed(store).length
      assert_equal 0, writer.buffered_bytes
    end
  end

  # A runtime that keeps printing after its byte budget is spent is the ordinary case, not an
  # attack: nothing is persisted, so nothing fills the buffer, nothing starts the flush timer, and
  # the in-flight record list is the only thing left that can grow. Eight mebibytes past the limit
  # with no interpreter at all — a plain shell step — must leave it bounded by configuration.
  def test_output_after_a_byte_limit_cannot_grow_the_in_flight_records
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 64 * 1024,
                                                    max_stream_bytes: 64 * 1024)
      writer = capture.open(step: "harness")
      line = "#{"d" * 1023}\n"
      writes = (8 * 1024 * 1024) / line.bytesize
      peak_frames = 0
      peak_retained = 0

      writes.times do
        writer.write(line)
        peak_frames = [peak_frames, writer.retained_frames].max
        peak_retained = [peak_retained, writer.retained_bytes].max
      end
      summary = writer.close

      assert_operator peak_frames, :<=, Capture::FLUSH_RECORDS
      assert_operator peak_retained, :<=, (64 * 1024) + Backstage::Support::RecordFramer::DEFAULT_MAX_RECORD_BYTES + line.bytesize
      assert_equal 0, writer.buffered_bytes
      assert_equal 0, writer.retained_frames
      assert_equal "truncated", summary.fetch("coverage")
      assert_equal 64 * 1024, summary.fetch("bytes")
      assert_equal writes, summary.fetch("records")
      assert_equal (8 * 1024 * 1024) - (64 * 1024), summary.fetch("bytes_dropped")

      # Finding 6: every record counted is named by an event, bytes or no bytes.
      assert_equal writes, observed(store).sum { |event| event.dig("data", "records") }
      byteless = observed(store).select { |event| event.dig("data", "bytes").zero? }

      refute_empty byteless
      assert_empty byteless.map { |event| event.dig("data", "reason") }.uniq - %w[close limit records]
      assert_includes byteless.map { |event| event.dig("data", "reason") }, "records"
      assert_equal 64 * 1024, byteless.first.dig("data", "truncated_from_offset")
      assert_equal byteless.map { |event| event.fetch("event_id") }.uniq.length, byteless.length
    end
  end

  # The reviewer's shape: a hundred small records with room for three. The 97 that never reached a
  # chunk are still counted in the checkpoint, so something has to name them.
  def test_records_past_a_limit_are_named_by_a_byteless_observed_event
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 64, max_stream_bytes: 192)
      writer = capture.open(step: "harness")
      100.times { |index| writer.write("#{format("%03d", index)}#{"e" * 60}\n") }
      summary = writer.close

      assert_equal 100, summary.fetch("records")
      assert_equal 100, observed(store).sum { |event| event.dig("data", "records") }
      last = observed(store).last

      assert_equal 0, last.dig("data", "bytes")
      assert_operator last.dig("data", "records"), :>, 0
      assert_equal 192, last.dig("data", "end_offset")
      assert_equal 100, last.dig("data", "record_index")
      assert_equal 192, last.dig("data", "truncated_from_offset")
    end
  end

  # --- the record bound ---------------------------------------------------------------------------

  # The defect, at the shipped defaults and with nothing but a null sink: a record over
  # `max_record_bytes` whose cut lands on a chunk boundary was framed *after* the flush that
  # committed its bytes, so `record`'s resume-dedupe guard read it as already told and dropped it.
  # It was counted in neither `records` nor `truncated_records` nor `malformed`, named by no event,
  # and the stream still reported `complete`.
  def test_an_oversized_record_ending_on_a_chunk_boundary_is_counted_not_dropped
    capture = Capture.new(sink: Sink::Null.new, clock: FakeClock.new, run: "run-oversized", attempt: 1)
    writer = capture.open(step: "harness")
    bound = Backstage::Support::RecordFramer::DEFAULT_MAX_RECORD_BYTES
    data = ("oversized #{"Z" * (bound + 4096)}\n" + "next line\n").b
    position = 0
    while position < data.bytesize
      writer.write(data.byteslice(position, 8192))
      position += 8192
    end
    summary = writer.close(reason: "close")

    assert_equal data.bytesize, summary.fetch("bytes"), "every byte is durable, which was never in doubt"
    assert_equal 2, summary.fetch("records"), "the truncated record is a record and must be counted"
    assert_equal 1, summary.fetch("truncated_records")
    assert_equal 0, summary.fetch("truncated_record_offset")
    assert_equal "truncated", summary.fetch("coverage"), "coverage must reflect what was cut"
    assert_equal bound, summary.fetch("max_record_bytes")
  end

  # The property, over the whole matrix the reviewer found it on: whatever the flush size and the
  # record bound, an oversized record spanning a chunk boundary is counted exactly once, named by
  # exactly one event, and reflected in coverage. 64/64 is the shipped default; 1 MiB/64 KiB is the
  # combination that happened to work and so hid the rest.
  def test_an_oversized_record_is_counted_once_at_every_flush_and_record_size
    [[1024 * 1024, 65_536], [65_536, 65_536], [32_768, 65_536], [8192, 65_536], [16_384, 16_384]].each do |flush, bound|
      in_tmpdir do |directory|
        store, _artifacts, capture = build(directory, flush_bytes: flush, max_record_bytes: bound)
        writer = capture.open(step: "harness", interpreter: ToyInterpreter.new)
        oversized = "{\"kind\":\"tool\",\"name\":\"#{"Z" * (bound + 4096)}\"}\n"
        writer.write("#{oversized}tail\n".b)
        summary = writer.close
        label = "flush #{flush} / record #{bound}"

        assert_equal 2, summary.fetch("records"), label
        assert_equal 1, summary.fetch("truncated_records"), label
        assert_equal 1, summary.fetch("malformed"), "#{label}: a cut JSON record no longer parses"
        assert_equal 0, summary.fetch("truncated_record_offset"), label
        assert_equal "truncated", summary.fetch("coverage"), label

        # Exactly one event names the record itself, however many chunks its bytes were split over.
        faults = observed(store).select { |event| event.dig("data", "truncated") == true }

        assert_equal 1, faults.length, label
        assert_equal 1, faults.fetch(0).dig("data", "truncated_records"), label
        assert_equal 0, faults.fetch(0).dig("data", "truncated_record_offset"), label
        assert_equal true, faults.fetch(0).dig("data", "malformed"), label
        # And the record range of the chunk events covers both records exactly once.
        assert_equal 2, coalesced(store).sum { |event| event.dig("data", "records") }, label
        assert_equal "truncated", store.fetch(STREAMS, writer.id).fetch("coverage"), label
        assert_equal 0, store.fetch(STREAMS, writer.id).fetch("truncated_record_offset"), label
      end
    end
  end

  # A record bound is a memory bound, not a semantic one, so a stream whose records are a protocol
  # asks for the generous one. Nothing about persistence changes: a stream truncated by the *record*
  # bound has every byte durable, which is what lets a resumed writer keep persisting.
  def test_a_protocol_stream_gets_the_larger_record_bound_and_a_resume_keeps_persisting
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 4096, max_record_bytes: 256,
                                         max_protocol_record_bytes: 64 * 1024)
      plain = capture.open(step: "context")
      protocol = capture.open(step: "harness", interpreter: ProtocolInterpreter.new)
      line = "#{"y" * 5000}\n"
      plain.write(line.b)
      protocol.write(line.b)
      # No close on `plain`: this is a process that died having cut one record.
      checkpoint = store.fetch(STREAMS, plain.id)
      finished = protocol.close

      assert_equal 0, finished.fetch("truncated_records")
      assert_equal "complete", finished.fetch("coverage")
      assert_equal 64 * 1024, finished.fetch("max_record_bytes")
      assert_equal "truncated", checkpoint.fetch("coverage")
      assert_equal 0, checkpoint.fetch("bytes_dropped")
      assert_equal 0, checkpoint.fetch("truncated_record_offset")

      # A stream cut by the record bound resumes and still writes bytes. Reading persistence off
      # the coverage word alone — `truncated` also being what a byte limit reports — would have
      # stopped this stream from ever persisting another byte.
      _store, _artifacts, again = build(directory, store: store, flush_bytes: 4096, max_record_bytes: 256)
      resumed = again.open(step: "context", resume: true)
      resumed.write("#{line}second\n".b)
      summary = resumed.close

      assert_equal "truncated", summary.fetch("coverage")
      assert_equal 0, summary.fetch("bytes_dropped")
      assert_equal line.bytesize + 7, summary.fetch("last_offset"), "the resumed bytes are still persisted"
    end
  end

  # --- reopening a stream -------------------------------------------------------------------------

  # A stream id is a durable position. Opening one that already has bytes without asking to resume
  # would hand out a writer at offset zero over chunk files an acknowledged event already names.
  def test_opening_a_stream_that_already_has_bytes_without_resume_is_refused
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory, flush_bytes: 32)
      writer = capture.open(step: "harness")
      writer.write("#{"a" * 31}\n")
      durable = chunk_files(directory).to_h { |path| [path, File.binread(path)] }

      refute_empty durable
      _store, _artifacts, again = build(directory, store: store, flush_bytes: 32)
      error = assert_raises(Backstage::CaptureError) { again.open(step: "harness") }

      assert_equal writer.id, error.stream_id
      assert_equal 32, error.offset
      assert_equal durable, chunk_files(directory).to_h { |path| [path, File.binread(path)] }

      # Asking to resume is the supported way to reach the same stream, and it continues.
      _store, _artifacts, third = build(directory, store: store, flush_bytes: 32)
      resumed = third.open(step: "harness", resume: true)
      resumed.write("#{"a" * 31}\n#{"b" * 31}\n")
      summary = resumed.close

      assert_equal 64, summary.fetch("last_offset")
      assert_equal durable.values.first, File.binread(durable.keys.first)
    end
  end

  # --- conflicts --------------------------------------------------------------------------------

  def tool_chunk(name)
    chunk_payload.merge(
      "observations" => [{ "type" => "agent.tool_observed", "record_index" => 0,
                           "summary" => "tool #{name}", "provenance" => "agent_reported",
                           "data" => { "tool_name" => name } }]
    )
  end

  # The store checks record expectations before activity fingerprints, so an event id reused for a
  # different fact reaches a caller as a plain ConflictError from the stale guard. The checkpoint
  # row is derived from the chunk and so is equal either way; believing it would drop every event
  # in the commit and report success.
  def test_a_reconcile_is_refused_when_the_stored_events_say_something_else
    in_tmpdir do |directory|
      guard = Guard.new
      inner = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      inner.deployment_id
      artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
      sink = Sink::Durable.new(store: LosingStore.new(inner, on: 2), artifact_store: artifacts,
                               work_item_id: "work-1", run: RUN, attempt: ATTEMPT)
      stream = sink.open(stream_id: "run-a1b2:1:0:harness", step: "harness", kind: "runtime_output",
                         opened_at: "2026-09-09T12:00:00.000000Z")

      assert_raises(Errno::EIO) { stream.commit(tool_chunk("read_only")) }

      error = assert_raises(Backstage::ActivityConflictError) do
        stream.commit(tool_chunk("deploy_production"))
      end

      assert_equal Recorder.event_id("agent.tool_observed", "run-a1b2:1:0:harness", 0), error.event_id
      tools = events(inner, type: "agent.tool_observed")

      assert_equal ["read_only"], tools.map { |event| event.dig("data", "tool_name") }
    end
  end

  def test_an_honest_retry_of_the_same_chunk_still_reconciles
    in_tmpdir do |directory|
      guard = Guard.new
      inner = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      inner.deployment_id
      artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
      sink = Sink::Durable.new(store: LosingStore.new(inner, on: 2), artifact_store: artifacts,
                               work_item_id: "work-1", run: RUN, attempt: ATTEMPT)
      stream = sink.open(stream_id: "run-a1b2:1:0:harness", step: "harness", kind: "runtime_output",
                         opened_at: "2026-09-09T12:00:00.000000Z")

      assert_raises(Errno::EIO) { stream.commit(tool_chunk("read_only")) }

      before = events(inner).length

      assert stream.commit(tool_chunk("read_only")).fetch("reconciled")
      assert_equal before, events(inner).length
    end
  end

  # --- divergent replay -------------------------------------------------------------------------

  # Offset skip assumes the bytes being discarded are the bytes already recorded. A provider that
  # comes back with different output would otherwise be spliced onto this stream's offsets, and the
  # artifact would read as one contiguous stream no process ever produced.
  def test_a_replay_that_diverges_from_the_durable_bytes_is_refused
    in_tmpdir do |directory|
      guard = Guard.new
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
      _store, _artifacts, capture = build(directory, store: store, flush_bytes: 32)
      capture.open(step: "harness").write("#{"a" * 31}\n")
      durable = chunk_files(directory).map { |path| File.binread(path) }.join

      _store, _artifacts, again = build(directory, store: store, flush_bytes: 32)
      resumed = again.open(step: "harness", resume: true)
      error = assert_raises(Backstage::CaptureError) { resumed.write("#{"Z" * 31}\n") }

      assert_equal "run-a1b2:1:0:harness", error.stream_id
      assert_includes error.message, "diverge"
      assert resumed.failed?
      assert_equal durable, chunk_files(directory).map { |path| File.binread(path) }.join
      assert_equal "failed", resumed.close.fetch("coverage")
    end
  end

  # --- the interfaces the wiring task calls -----------------------------------------------------

  def test_a_phase_runner_can_name_the_phase_per_stream
    in_tmpdir do |directory|
      store, _artifacts, capture = build(directory)
      writer = capture.open(step: "context", phase: "finalize")
      writer.write("one line\n")
      summary = writer.close

      assert_equal "finalize", summary.fetch("phase")
      assert_equal "finalize", store.fetch(STREAMS, writer.id).fetch("phase")
      assert_equal "finalize", observed(store).first.dig("data", "phase")
      # Without one, the capture's own phase still applies.
      assert_equal "implementation", capture.open(step: "harness").phase
    end
  end

  def test_a_close_summary_names_its_chunks_and_its_manifest
    in_tmpdir do |directory|
      _store, _artifacts, capture = build(directory, flush_bytes: 32)
      writer = capture.open(step: "harness")
      writer.write("#{"f" * 31}\n#{"g" * 31}\n")
      summary = writer.close

      assert_equal row_manifest_id(writer.id), summary.fetch("manifest_artifact_id")
      assert_equal 2, summary.fetch("chunk_artifact_ids").length
      assert_equal summary.fetch("chunk_artifact_ids") + [summary.fetch("manifest_artifact_id")],
                   summary.fetch("artifact_ids")
    end
  end

  def test_a_stream_that_was_never_closed_is_still_in_the_run_summaries
    in_tmpdir do |directory|
      _store, _artifacts, capture = build(directory, flush_bytes: 32)
      closed = capture.open(step: "context")
      closed.write("done\n")
      closed.close
      live = capture.open(step: "harness")
      live.write("#{"h" * 31}\n")

      summaries = capture.summaries

      assert_equal 2, summaries.length
      assert_equal %w[complete open], summaries.map { |row| row.fetch("coverage") }
      open_row = summaries.last

      assert_equal live.id, open_row.fetch("stream_id")
      assert_equal 32, open_row.fetch("last_offset")
      assert_equal "open", open_row.fetch("reason")
      refute open_row.key?("closed_at")
    end
  end

  def test_writing_after_close_is_a_capture_error
    in_tmpdir do |directory|
      _store, _artifacts, capture = build(directory, flush_bytes: 32)
      writer = capture.open(step: "harness")
      writer.write("first\n")
      writer.close

      error = assert_raises(Backstage::CaptureError) { writer.write("late\n") }

      assert_equal writer.id, error.stream_id
      assert_equal 6, error.offset
    end
  end

  # A secret with non-ASCII bytes against a stream that is binary by nature. Comparing those as
  # text raises rather than answering, which would let the secret through every check downstream.
  def test_a_non_ascii_secret_is_redacted_from_binary_output
    in_tmpdir do |directory|
      secret = "clé-très-secrète-ünïcödé-0123456789"
      store, _artifacts, capture = build(directory, secrets: [secret], flush_bytes: 64)
      writer = capture.open(step: "harness")
      writer.write("broken \xFF\xFE bytes token=#{secret[0, 9]}".b)
      writer.write("#{secret[9..]} done\n#{"q" * 70}\n".b)
      summary = writer.close

      persisted = chunk_files(directory).map { |path| File.binread(path) }.join

      refute_includes persisted, secret.b
      assert_includes persisted, "token=[REDACTED] done"
      refute_includes File.read(store.path).b, secret.b
      assert_includes persisted, "broken \xFF\xFE bytes".b
      assert_equal persisted.bytesize, summary.fetch("last_offset")
    end
  end

  # --- the null sink ----------------------------------------------------------------------------

  def test_the_null_sink_frames_and_interprets_without_persisting_anything
    in_tmpdir do |directory|
      capture = Capture.new(sink: Sink::Null.new, clock: FakeClock.new, run: RUN, attempt: ATTEMPT,
                            flush_bytes: 64)
      writer = capture.open(step: "harness", interpreter: ToyInterpreter.new)
      writer.write("#{JSON.generate("kind" => "tool", "name" => "ls")}\n#{"e" * 200}\n")
      summary = writer.close

      assert_operator summary.fetch("records"), :>=, 2
      assert_equal "complete", summary.fetch("coverage")
      assert_empty summary.fetch("artifact_ids")
      assert_empty Dir[File.join(directory, "**", "*.log")]
    end
  end
end
