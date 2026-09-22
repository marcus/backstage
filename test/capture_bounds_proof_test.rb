# frozen_string_literal: true

require_relative "test_helper"

# Large output, and what it is allowed to cost.
#
# A real child process (the same Ruby stub-docker pattern as docker_capture_test) streams a
# mixed-content fixture — irregular line lengths, multibyte characters, invalid UTF-8, one line
# larger than the record limit, and secrets — through the Docker runtime into a durable sink. The
# assertions are structural: what may be held in memory at any moment, how many chunks a byte
# count implies, and how big the diagnostic tail may get. `ObjectSpace.memsize_of_all` is too
# unstable on macOS for a hard threshold, so the memory claim is a live-slot delta with a generous
# bound plus the three in-flight readers the port bounds by contract.
#
# The default run uses a 4 MiB fixture so the suite stays fast. `BACKSTAGE_CAPTURE_PROOF=1` runs
# the full 64 MiB fixture the plan calls for. Both record the same measurements; the thresholds in
# docs/plans/active/activity-and-effects/activity.md are derived from them.
class CaptureBoundsProofTest < Minitest::Test
  RuntimeCapture = Backstage::Application::RuntimeCapture
  CaptureSink = Backstage::Application::CaptureSink
  FLUSH_BYTES = RuntimeCapture::FLUSH_BYTES
  MAX_RECORD_BYTES = Backstage::Support::RecordFramer::DEFAULT_MAX_RECORD_BYTES
  FLUSH_RECORDS = RuntimeCapture::FLUSH_RECORDS
  SECRETS = ["fixture-secret-value-0123456789", "second-secret-abcdefghijklmnop"].freeze

  def full_proof? = ENV["BACKSTAGE_CAPTURE_PROOF"] == "1"

  def fixture_bytes = full_proof? ? 64 * 1024 * 1024 : 4 * 1024 * 1024

  # One megabyte of deliberately awkward output. Line lengths are irregular so flush boundaries
  # fall inside records; multibyte characters and invalid bytes are placed so a chunk boundary can
  # split them; secrets appear so redaction has something to catch on the way to a chunk file.
  def block(seed)
    random = Random.new(seed)
    out = +"".b
    index = 0
    while out.bytesize < 1024 * 1024
      length = random.rand(8..3000)
      line = case index % 11
             when 0 then "réussi ✅ #{"é" * (length / 2)}"
             when 3 then "invalid \xC3\x28\xA0\xA1 bytes #{"q" * length}".b
             when 7 then "before #{SECRETS[index % SECRETS.length]} after #{"m" * length}"
             else "line #{index} #{"n" * length}"
             end
      out << line.b << "\n".b
      index += 1
    end
    out
  end

  # Writes the fixture to disk once and returns [path, byte size]. The oversized line lands in the
  # first block so the framer's truncation path runs whatever the total size is.
  def write_fixture(directory, total)
    path = File.join(directory, "fixture.bin")
    File.open(path, "wb") do |file|
      # The short preamble is load-bearing: it stops the oversized record from ending exactly on a
      # chunk boundary, which is the condition that makes the writer discard it (see
      # `test_an_oversized_record_ending_on_a_chunk_boundary_is_silently_discarded`). Without it
      # this fixture would quietly prove less than it claims to.
      file.write("preamble #{"p" * 27}\n")
      file.write("oversized #{"Z" * (MAX_RECORD_BYTES + 4096)}\n")
      seed = 0
      file.write(block(seed += 1)) while file.size < total
    end
    [path, File.size(path)]
  end

  def stub_docker(directory, fixture_path)
    path = File.join(directory, "docker-stub")
    File.write(path, <<~SCRIPT)
      #!#{RbConfig.ruby}
      exit 0 unless ARGV.first == "run"
      STDIN.read
      STDOUT.binmode
      STDOUT.sync = true
      File.open(#{fixture_path.inspect}, "rb") { |source| IO.copy_stream(source, STDOUT) }
    SCRIPT
    FileUtils.chmod(0o755, path)
    path
  end

  def durable_capture(directory, guard, **options)
    run = { "id" => "run-bounds", "phase" => "implementation" }
    store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
    artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
    sink = CaptureSink::Durable.new(store: store, artifact_store: artifacts, work_item_id: "work-bounds",
                                    run: run, attempt: 1)
    [store, RuntimeCapture.new(sink: sink, clock: Backstage::Adapters::Fake::Clock.new, run: run,
                               attempt: 1, secret_guard: guard, **options)]
  end

  def job_bundle
    JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__))).tap do |value|
      value["execution"]["command"] = %w[noop]
      value["execution"]["credential_refs"] = []
    end
  end

  # Runs the fixture through the runtime, sampling the three in-flight readers on the controller
  # thread every time the drain reports progress. Returns everything the assertions and the
  # measurement table need.
  def stream_fixture(directory, capture_options: {}, interpreter: nil)
    guard = Backstage::SecretGuard.new(secret_values: SECRETS)
    fixture_path, fixture_size = write_fixture(directory, fixture_bytes)
    docker = stub_docker(directory, fixture_path)
    store, capture = durable_capture(directory, guard, **capture_options)
    writer = capture.open(step: "harness", interpreter: interpreter)
    peak = { buffered: 0, pending: 0, frames: 0, retained: 0 }

    GC.start
    before_slots = GC.stat(:heap_live_slots)
    before_allocated = GC.stat(:total_allocated_objects)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    outcome = Backstage::DockerRuntime.new(docker: docker, secret_guard: guard,
                                           read_bytes: 64 * 1024, queue_limit: 8)
                                      .run(bundle: job_bundle, capture: writer) do |event|
      next unless event["type"] == "runtime_progress"

      peak[:buffered] = [peak[:buffered], writer.buffered_bytes].max
      peak[:pending] = [peak[:pending], writer.pending_bytes].max
      peak[:frames] = [peak[:frames], writer.retained_frames].max
      peak[:retained] = [peak[:retained], writer.retained_bytes].max
    end
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    allocated = GC.stat(:total_allocated_objects) - before_allocated
    GC.start
    live_slots = GC.stat(:heap_live_slots) - before_slots

    # The runtime already closed the stream; `close` is idempotent and hands back that summary,
    # which carries the counters the outcome's bounded projection does not.
    { outcome: outcome, capture: capture, writer: writer, store: store, peak: peak,
      summary: writer.close(reason: "close"),
      elapsed: elapsed, allocated: allocated, live_slots: live_slots,
      fixture_size: fixture_size, directory: directory }
  end

  def chunk_files(directory)
    Dir.glob(File.join(directory, "artifacts", "**", "*.log"))
  end

  def report(label, result, extra = {})
    summary = result.fetch(:outcome).dig("capture", "streams", 0) || {}
    numbers = {
      "fixture_bytes" => result.fetch(:fixture_size),
      "captured_bytes" => summary["bytes"],
      "records" => summary["records"],
      "chunks" => summary["chunks"],
      "elapsed_s" => result.fetch(:elapsed).round(3),
      "allocated_objects" => result.fetch(:allocated),
      "live_slot_delta" => result.fetch(:live_slots),
      "peak_buffered_bytes" => result.dig(:peak, :buffered),
      "peak_pending_bytes" => result.dig(:peak, :pending),
      "peak_retained_frames" => result.dig(:peak, :frames),
      "peak_retained_bytes" => result.dig(:peak, :retained),
      "state_jsonl_bytes" => File.size(File.join(result.fetch(:directory), "state.jsonl")),
      "chunk_files" => chunk_files(result.fetch(:directory)).length
    }.merge(extra)
    puts "MEASURED #{label}: #{JSON.generate(numbers)}" if ENV["BACKSTAGE_CAPTURE_REPORT"] == "1" || full_proof?
    numbers
  end

  def test_a_large_mixed_fixture_stays_within_every_stated_bound
    in_tmpdir do |directory|
      # The fixture is bigger than the default per-stream limit, so both limits are raised here:
      # this test is about the bounds that hold when nothing is being dropped. The limit behaviour
      # has its own test below.
      result = stream_fixture(directory, capture_options: {
                                max_stream_bytes: 128 * 1024 * 1024, max_run_bytes: 128 * 1024 * 1024
                              })
      outcome = result.fetch(:outcome)
      writer = result.fetch(:writer)
      stream = outcome.dig("capture", "streams", 0)

      assert_equal "succeeded", outcome.fetch("status")
      # Every byte is durable, but the fixture carries one record over `max_record_bytes`, and a
      # record the interpreter could not read whole is reported as truncated coverage (td-bce761)
      # rather than hidden behind "complete". The byte trail and the counts below are unaffected.
      assert_equal "truncated", outcome.dig("capture", "status")
      assert_equal "truncated", stream.fetch("coverage")
      assert_equal 1, result.fetch(:summary).fetch("truncated_records"), "the oversized record is reported, not dropped"
      assert_kind_of Integer, result.fetch(:summary).fetch("truncated_record_offset")

      # 1. Nothing unbounded is held while the bytes go past.
      assert_operator result.dig(:peak, :buffered), :<=, FLUSH_BYTES, "buffered bytes are bounded by one flush"
      assert_operator result.dig(:peak, :pending), :<=, MAX_RECORD_BYTES, "a record cannot grow past the record limit"
      assert_operator result.dig(:peak, :frames), :<=, FLUSH_RECORDS, "in-flight records are bounded by flush_records"
      assert_operator result.dig(:peak, :retained), :<=, FLUSH_BYTES + MAX_RECORD_BYTES + MAX_RECORD_BYTES,
                      "retained record text is bounded by one flush plus one record"
      assert_operator writer.buffered_bytes, :<=, FLUSH_BYTES
      assert_operator writer.pending_bytes, :<=, MAX_RECORD_BYTES
      assert_operator writer.retained_frames, :<=, FLUSH_RECORDS

      # 2. The diagnostic tail is a tail, not an accumulator.
      #
      # Measured deviation, recorded rather than papered over: `Docker::Runtime::Tail` bounds its
      # binary buffer at exactly `log_tail_bytes`, but `#text` scrubs it to UTF-8 afterwards and
      # every invalid byte becomes a 3-byte U+FFFD. A tail whose last 64 KiB contains invalid
      # bytes — this fixture's does — therefore comes back slightly larger than `log_tail_bytes`
      # says (65,560 bytes here), and in the worst case up to 3x it. It is still bounded, which is
      # what the memory claim needs; it is the *stated* number that is imprecise. Asserting the
      # real bound here so a regression that removes the trim is still caught.
      assert_equal 64 * 1024, outcome.fetch("log_tail_bytes")
      assert_operator outcome.fetch("logs").bytesize, :<=, 3 * outcome.fetch("log_tail_bytes")
      assert_equal true, outcome.fetch("logs_truncated")

      # 3. Every byte is accounted for, in exactly the number of chunks a 64 KiB flush implies.
      captured = stream.fetch("bytes")
      assert_operator captured, :>, (result.fetch(:fixture_size) * 0.95).to_i,
                      "redaction shortens the stream; it does not lose most of it"
      assert_operator captured, :<=, result.fetch(:fixture_size)
      expected_chunks = (captured.to_f / FLUSH_BYTES).ceil
      assert_equal expected_chunks, stream.fetch("chunks")
      assert_equal expected_chunks, chunk_files(directory).length
      assert_equal captured, chunk_files(directory).sum { |path| File.size(path) }
      assert_operator result.fetch(:summary).fetch("truncated_records"), :>=, 1,
                      "the oversized line was truncated, not buffered"
      assert_equal 0, result.fetch(:summary).fetch("bytes_dropped"), "nothing was dropped under raised limits"
      assert_operator result.fetch(:summary).fetch("malformed"), :>=, 1, "invalid bytes were reported, not dropped"

      # 4. Redaction held across every chunk boundary the fixture crosses.
      SECRETS.each do |secret|
        refute_includes outcome.fetch("logs"), secret
        assert(chunk_files(directory).none? { |path| File.binread(path).include?(secret) },
               "#{secret} reached a chunk file")
      end

      # 5. Memory: a generous ceiling, because a live-slot delta is noisy, but a leak of the
      # transcript would be orders of magnitude past it.
      assert_operator result.fetch(:live_slots), :<, 200_000,
                      "live objects after the run should not scale with the output"

      numbers = report("bounded", result)
      assert_operator numbers.fetch("state_jsonl_bytes"), :<, result.fetch(:fixture_size),
                      "the state log holds references, not the transcript"
    end
  end

  def test_activity_queries_stay_bounded_at_the_history_the_fixture_produced
    in_tmpdir do |directory|
      result = stream_fixture(directory, capture_options: {
                                max_stream_bytes: 128 * 1024 * 1024, max_run_bytes: 128 * 1024 * 1024
                              })
      store = result.fetch(:store)
      state_path = File.join(directory, "state.jsonl")
      total = count_activity(store)
      assert_operator total, :>, result.fetch(:summary).fetch("chunks")

      base = ["--state", state_path, "--artifacts", File.join(directory, "artifacts"), "--pack", PACK]
      list_seconds = timed { cli(base + ["--json", "activity", "list", "--limit", "100"]) }
      follow_seconds = timed do
        cli(base + ["--json", "activity", "follow", "--limit", "100", "--max-passes", "3", "--interval", "0"])
      end

      # The plan's Proof section records the acceptance thresholds at 2x the measured baseline.
      # The assertion here is deliberately looser than that: a wall-clock bound at 2x is flaky on a
      # machine running anything else, and what this must actually catch is a page that starts
      # materializing the transcript — which at this history size is orders of magnitude away, not
      # a factor of two. Measured on an Apple M4 Pro against the 64 MiB fixture: 0.015 s and
      # 0.042 s, against a 5.4 MiB state log and 2,051 events.
      assert_operator list_seconds, :<, 1.0, "a bounded page must not scale with the transcript"
      assert_operator follow_seconds, :<, 2.0, "three follow passes stay bounded too"

      report("queries", result,
             "activity_events" => total,
             "list_100_seconds" => list_seconds.round(3),
             "follow_3_passes_seconds" => follow_seconds.round(3))
    end
  end

  def test_output_past_an_explicit_limit_is_truncated_and_still_says_what_the_agent_did
    in_tmpdir do |directory|
      # A limit far below the fixture: byte persistence stops, framing and interpretation do not,
      # and the decisions the agent reported still reach history.
      guard = Backstage::SecretGuard.new(secret_values: [])
      fixture_path = File.join(directory, "fixture.bin")
      File.open(fixture_path, "wb") do |file|
        200.times do |index|
          file.write(JSON.generate({ "type" => "tool_execution_start", "toolCallId" => "tool-#{index}",
                                     "toolName" => "bash", "args" => { "command" => "echo #{index}" },
                                     "timestamp" => "2026-01-01T00:00:00Z" }) + "\n")
          file.write("noise #{"w" * 4000}\n")
        end
      end
      docker = stub_docker(directory, fixture_path)
      _store, capture = durable_capture(directory, guard, max_stream_bytes: 64 * 1024,
                                                          max_run_bytes: 64 * 1024)
      writer = capture.open(step: "harness",
                            interpreter: Backstage::Adapters::Pi::StreamInterpreter.new(model: "m"))
      outcome = Backstage::DockerRuntime.new(docker: docker, read_bytes: 16 * 1024, queue_limit: 4)
                                        .run(bundle: job_bundle, capture: writer)
      summary = writer.close(reason: "close")

      assert_equal "truncated", summary.fetch("coverage")
      assert_operator summary.fetch("bytes_dropped"), :>, 0
      assert_operator summary.fetch("bytes"), :<=, 64 * 1024
      assert_operator summary.fetch("records"), :>=, 400, "records keep being framed past the byte limit"
      assert_operator writer.retained_frames, :<=, FLUSH_RECORDS
      assert_equal "truncated", outcome.dig("capture", "status")

      events = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
                                    .read_activity(limit: 1000).fetch("events")
      tools = events.select { |event| event.fetch("type") == "agent.tool_observed" }
      assert_equal 200, tools.length, "a byte limit discards debug noise, never decisions"
      # And the records past the limit are named by an event even though they have no byte range.
      observed = events.select { |event| event.fetch("type") == "runtime.observed" }
      assert_equal summary.fetch("records"), observed.sum { |event| event.dig("data", "records").to_i }
      assert_includes observed.map { |event| event.dig("data", "reason") }, "limit"
    end
  end

  # Regression for a defect found at 4f7bb0f and fixed in td-bce761 (4817176).
  #
  # `RecordFramer` emits an oversized record only once one byte *past* `max_record_bytes` arrives.
  # `StreamWriter#absorb` appends to the chunk buffer before it frames, so when that record's end
  # offset falls exactly on a chunk boundary the chunk covering its bytes is emitted first;
  # `build_chunk` sets `framed_offset = max(chunk_end, …)`, and the frame that arrives one byte
  # later is dropped by `record`'s `end_offset <= @framed_offset` guard — the resume-dedupe guard,
  # firing on a record that was never told.
  #
  # The bytes stay durable, but the record is invisible: it is not counted in `records` or
  # `truncated_records`, no `runtime.observed` names it, and the interpreter never observes it — so
  # a pi `message_end` or tool result longer than 64 KiB would produce no `agent.*` event at all.
  # The port says "Nothing is dropped" and "Every record counted in a checkpoint is named by one of
  # the two", so this is a contract violation, not a tuning choice.
  #
  # It is not a corner case under the defaults: `flush_bytes` and `max_record_bytes` are both
  # 64 KiB, so an oversized record that begins on a chunk boundary always ends on the next one.
  # Measured before the fix: prefix "" -> records 1, truncated_records 0 (should be 2 and 1); a
  # 37-byte preamble, or flush_bytes 100_000, let the same record survive.
  def test_an_oversized_record_ending_on_a_chunk_boundary_is_counted_and_reported
    capture = RuntimeCapture.new(sink: CaptureSink::Null.new,
                                 clock: Backstage::Adapters::Fake::Clock.new,
                                 run: "run-oversized", attempt: 1)
    writer = capture.open(step: "harness")
    data = ("oversized #{"Z" * (MAX_RECORD_BYTES + 4096)}\n" + "next line\n").b
    position = 0
    while position < data.bytesize
      writer.write(data.byteslice(position, 8192))
      position += 8192
    end
    summary = writer.close(reason: "close")

    assert_equal data.bytesize, summary.fetch("bytes"), "every byte is durable, which is not in doubt"
    assert_equal 2, summary.fetch("records"), "the truncated record is a record and must be counted"
    assert_equal 1, summary.fetch("truncated_records"), "and must be reported as truncated"
  end

  private

  # The whole history, paged the way any consumer reads it, so the recorded number is the real one
  # rather than whatever a single page happened to hold.
  def count_activity(store)
    cursor = nil
    total = 0
    loop do
      page = store.read_activity(after: cursor, limit: 1000)
      total += page.fetch("events").length
      break total if page.fetch("next_cursor") == cursor

      cursor = page.fetch("next_cursor")
    end
  end

  def timed
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    yield
    Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
  end

  def cli(argv)
    out = StringIO.new
    err = StringIO.new
    code = Backstage::CLI.new(argv, out: out, err: err, env: {}).call
    assert_equal 0, code, err.string
    out.string
  end
end
