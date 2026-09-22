# frozen_string_literal: true

require_relative "test_helper"

# Replaying the same output must not become a second history.
#
# The unit-level version of this already exists in `runtime_capture_test.rb`
# (`test_a_crash_at_any_flush_boundary_resumes_to_the_identical_history`,
# `test_replaying_a_closed_stream_from_the_beginning_appends_no_history`,
# `test_retrying_a_commit_that_actually_landed_reconciles_instead_of_growing_history`,
# `test_a_replay_that_diverges_from_the_durable_bytes_is_refused`) and drives the writer directly.
# What is proven here is the same claim through a *runtime*: `Adapters::Fake::Runtime` scripted
# against `Adapters::Fake::Clock`, so the bytes arrive the way a worker delivers them, the 250 ms
# flush boundary falls exactly where the script says, and the comparison is between two whole runs
# rather than two sequences of `write` calls.
#
# Partial and multibyte reconstruction is covered by `record_framer_test.rb`
# (`test_a_multibyte_character_split_across_reads_is_reconstructed`,
# `test_a_line_split_at_every_byte_still_frames_once`, `test_crlf_is_a_terminator_not_content`,
# `test_a_trailing_line_without_a_newline_arrives_on_finish`), by `docker_capture_test.rb`
# (`test_a_multibyte_character_split_across_pipe_reads_is_captured_intact`,
# `test_a_secret_split_across_pipe_reads_reaches_no_chunk_file_and_no_tail`) and by
# `pi_stream_interpreter_test.rb` (`test_a_multibyte_character_split_across_writes_reaches_the_event_intact`).
# The one case none of those covers is a *chunk* boundary — a persisted, checkpointed boundary
# rather than a read or a write boundary — cutting a multibyte character and a secret at once, and
# then surviving a resume. That is the last test here.
class CaptureReplayDedupeTest < Minitest::Test
  Capture = Backstage::Application::RuntimeCapture
  Sink = Backstage::Application::CaptureSink
  STREAMS = Backstage::Ports::RuntimeCapture::STREAM_COLLECTION
  RUN = { "id" => "run-replay", "phase" => "implementation" }.freeze
  ATTEMPT = { "id" => "attempt-1", "number" => 1 }.freeze
  SECRET = "replay-secret-value-0123456789"

  # A tiny interpreter so the comparison covers semantic events, not only coalesced chunks. Its
  # observations key on the record's own content, so a record framed twice at the same index is
  # the same observation.
  class ToyInterpreter < Backstage::Ports::StreamInterpreter
    def self.restore(state) = new(seen: (state || {})["seen"].to_i)
    def initialize(seen: 0) = @seen = seen
    def state = { "seen" => @seen }

    def observe(frame)
      text = frame.fetch("text")
      return [] unless text.start_with?("{")

      row = JSON.parse(text)
      @seen += 1
      [{ "type" => "agent.tool_observed", "summary" => "tool #{row["name"]}",
         "data" => { "tool_name" => row["name"] }, "provenance" => "agent_reported" }]
    rescue JSON::ParserError
      [{ "malformed" => true }]
    end
  end

  # Records enough of an event to say "this is the same history", and nothing that a second run is
  # allowed to differ on. `record_index` comes off the semantic events; a coalesced chunk event
  # carries its record range instead, which is the same claim about the same records.
  def signature(store)
    store.read_activity(limit: 1000).fetch("events")
         .reject { |event| event.fetch("type") == "activity.stream_started" }
         .map do |event|
      [event.fetch("type"), event.dig("data", "record_index") || event.dig("data", "records"),
       event.fetch("event_id")]
    end
  end

  # `secrets` defaults to none because the streaming redactor withholds `longest_secret - 1` bytes
  # to catch a secret split across a boundary, which would starve these deliberately small scripts.
  # The one test that is about a split secret configures it.
  def chunk_bytes(directory)
    Dir[File.join(directory, "artifacts", "**", "*.log")]
      .sort_by { |path| File.basename(path).to_i }
      .map { |path| File.binread(path) }.join
  end

  def build(directory, secrets: [], clock: nil, **options)
    guard = Backstage::SecretGuard.new(secret_values: secrets)
    store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard)
    artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard)
    sink = Sink::Durable.new(store: store, artifact_store: artifacts, work_item_id: "work-replay",
                             run: RUN, attempt: ATTEMPT)
    capture = Capture.new(sink: sink, clock: clock || Backstage::Adapters::Fake::Clock.new,
                          run: RUN, attempt: ATTEMPT, secret_guard: guard, **options)
    [store, capture]
  end

  # Four records over 0.9 s, so both a size flush and a time flush happen, with a tool line the
  # interpreter turns into a semantic event.
  SCRIPT = [
    [0.0, %({"name":"bash","args":"#{"a" * 60}"}\n)],
    [0.3, "plain line with réussi ✅ and #{"b" * 60}\n"],
    [0.6, %({"name":"grep","args":"#{"c" * 60}"}\n)],
    [0.9, "trailing line without a newline #{"d" * 60}"]
  ].freeze

  def run_script(capture, script, clock:, step: "fake", resume: false)
    writer = capture.open(step: step, interpreter: ToyInterpreter.new, resume: resume)
    outcome = Backstage::Adapters::Fake::Runtime.new(script: script, clock: clock)
                                                .run(bundle: { "id" => RUN.fetch("id") }, capture: writer)
    [writer, outcome]
  end

  # The same delivery `Fake::Runtime` performs — advance the clock to the script's offset, tick,
  # then write — but without the close. `Fake::Runtime` always closes its stream, and the state a
  # crash leaves behind is precisely the one where nothing closed it, so the interrupted pass is
  # driven by hand here and only the *replay* goes through the runtime.
  def crash_after(capture, script, clock:, step: "fake")
    writer = capture.open(step: step, interpreter: ToyInterpreter.new)
    elapsed = 0.0
    script.each do |offset, bytes|
      clock.advance(offset.to_f - elapsed)
      elapsed = offset.to_f
      writer.tick
      writer.write(bytes.dup.force_encoding(Encoding::BINARY))
    end
    writer
  end

  def test_a_full_replay_of_the_same_output_appends_no_history_and_repeats_no_identity
    baseline = nil
    in_tmpdir do |directory|
      clock = Backstage::Adapters::Fake::Clock.new
      store, capture = build(directory, clock: clock, flush_bytes: 32)
      run_script(capture, SCRIPT, clock: clock)
      baseline = signature(store)
      refute_empty baseline
      assert_includes baseline.map(&:first), "agent.tool_observed"
    end

    in_tmpdir do |directory|
      # A run that is interrupted after two records: the checkpoint is durable, the stream was
      # never closed. This is the state a killed worker leaves.
      clock = Backstage::Adapters::Fake::Clock.new
      store, capture = build(directory, clock: clock, flush_bytes: 32)
      first = crash_after(capture, SCRIPT.first(2), clock: clock)
      row = store.fetch!(STREAMS, first.id)
      assert_nil row["closed_at"], "a crash leaves the stream open, which is what a resume resumes"
      interrupted = signature(store)
      refute_empty interrupted

      # A new process resumes and the provider replays the whole stream from byte zero — the case
      # the port names as the cross-process mechanism. Nothing already recorded may be told twice.
      resumed_clock = Backstage::Adapters::Fake::Clock.new
      _store2, resumed_capture = build(directory, clock: resumed_clock, flush_bytes: 32)
      writer, outcome = run_script(resumed_capture, SCRIPT, clock: resumed_clock, resume: true)

      assert_equal "succeeded", outcome.fetch("status")
      assert_equal "complete", outcome.dig("capture", "status")
      final = signature(store)
      assert_equal baseline, final,
                   "a resumed stream that replays every byte must reconstruct the identical history"
      assert_equal interrupted, final.first(interrupted.length),
                   "and must leave what was already acknowledged exactly as it was"
      assert_operator interrupted.length, :<, final.length,
                      "the resume has to have added the records the crash never reached"
      assert_operator final.count { |type, _, _| type == "agent.tool_observed" }, :>=, 2
      assert_equal final.map(&:last).uniq.length, final.length, "no event identity appears twice"
      assert_equal writer.id, first.id, "the same stream, not a second one"
    end
  end

  def test_a_replay_that_says_something_else_is_refused_rather_than_spliced
    in_tmpdir do |directory|
      clock = Backstage::Adapters::Fake::Clock.new
      store, capture = build(directory, clock: clock, flush_bytes: 32)
      first = crash_after(capture, SCRIPT.first(2), clock: clock)
      before = signature(store)
      durable = chunk_bytes(directory)
      refute_empty durable

      resumed_clock = Backstage::Adapters::Fake::Clock.new
      _store2, resumed_capture = build(directory, clock: resumed_clock, flush_bytes: 32)
      writer = resumed_capture.open(step: "fake", interpreter: ToyInterpreter.new, resume: true)

      error = assert_raises(Backstage::CaptureError) do
        writer.write("a completely different run's output, from byte zero\n" * 4)
      end
      assert_equal first.id, error.stream_id
      assert_equal true, writer.failed?
      assert_equal before, signature(store), "a refused replay writes no history"
      assert_equal durable, chunk_bytes(directory),
                   "and does not touch the bytes that were already acknowledged"
    end
  end

  def test_a_runtime_whose_replay_diverges_reports_a_failed_capture_rather_than_succeeding
    in_tmpdir do |directory|
      clock = Backstage::Adapters::Fake::Clock.new
      _store, capture = build(directory, clock: clock, flush_bytes: 32)
      crash_after(capture, SCRIPT.first(2), clock: clock)

      resumed_clock = Backstage::Adapters::Fake::Clock.new
      _store2, resumed_capture = build(directory, clock: resumed_clock, flush_bytes: 32)
      divergent = [[0.0, "another run entirely\n"], [0.3, "and more of it\n"]]
      _writer, outcome = run_script(resumed_capture, divergent, clock: resumed_clock, resume: true)

      assert_equal "failed", outcome.fetch("status")
      assert_equal "failed", outcome.dig("capture", "status")
      assert_match(/diverge/, outcome.dig("capture", "error"))
      assert_match(/could not capture/, outcome.fetch("summary"))
    end
  end

  def test_a_multibyte_character_and_a_secret_cut_by_a_chunk_boundary_survive_a_resume
    in_tmpdir do |directory|
      clock = Backstage::Adapters::Fake::Clock.new
      # A flush size chosen so the first chunk ends inside the multibyte run and inside the secret:
      # both straddle a persisted, checkpointed boundary rather than a read boundary.
      store, capture = build(directory, secrets: [SECRET], clock: clock, flush_bytes: 24)
      line = "réussi ✅ #{SECRET} tail #{"t" * 80}\n"
      script = [[0.0, line.byteslice(0, 12)], [0.05, line.byteslice(12, line.bytesize - 12)]]
      writer, = run_script(capture, script, clock: clock)

      chunks = Dir[File.join(directory, "artifacts", "**", "*.log")].sort_by { |path| File.basename(path).to_i }
      assert_operator chunks.length, :>=, 2, "the line was cut by at least one chunk boundary"
      joined = chunks.map { |path| File.binread(path) }.join
      refute_includes joined, SECRET, "a secret split by a chunk boundary reaches no chunk file"
      assert_includes joined.force_encoding(Encoding::UTF_8), "réussi ✅",
                      "and the multibyte character it was cut next to is intact"
      assert_includes joined.force_encoding(Encoding::UTF_8), "tail"
      store.read_activity(limit: 1000).fetch("events").each do |event|
        refute_includes JSON.generate(event), SECRET
      end
      assert_equal 1, writer.close(reason: "close").fetch("records")

      # And a resumed writer replaying the same bytes reads the same boundary the same way.
      resumed_clock = Backstage::Adapters::Fake::Clock.new
      _store2, resumed = build(directory, secrets: [SECRET], clock: resumed_clock, flush_bytes: 24)
      before = signature(store)
      run_script(resumed, script, clock: resumed_clock, resume: true)

      assert_equal before, signature(store), "replaying the split bytes tells nothing twice"
    end
  end
end
