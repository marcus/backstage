# frozen_string_literal: true

require_relative "test_helper"

# What pi's output means, end to end through the real framer and the real capture.
#
# Most of these go through `Application::RuntimeCapture` rather than calling `observe` directly,
# because the thing worth proving is a property of the path: a record split across writes still
# parses, an oversized argument is bounded before it can reach an event, and a line nobody can read
# is counted rather than dropped.
class PiStreamInterpreterTest < Minitest::Test
  Interpreter = Backstage::Adapters::Pi::StreamInterpreter
  RuntimeCapture = Backstage::Application::RuntimeCapture

  # A null sink that keeps the chunks it was handed. Every observation in them is one the durable
  # sink would have committed as an event, so asserting on these asserts on history.
  class RecordingSink < Backstage::Application::CaptureSink::Null
    attr_reader :streams

    def initialize
      @streams = []
    end

    def open(stream_id:, step:, kind:, phase: nil, opened_at: nil, resume: false)
      super.tap { |stream| @streams << stream }
    end

    def observations
      @streams.flat_map { |stream| stream.chunks }.flat_map { |chunk| Array(chunk["observations"]) }
    end
  end

  def fixture
    File.binread(File.expand_path("fixtures/pi_success.jsonl", __dir__))
  end

  def test_the_success_fixture_produces_two_tool_observations_and_one_message
    interpreter, sink, summary = stream(fixture)

    # The leading nil is the session line's metadata observation: applied to the stream, committed
    # as nothing.
    assert_equal [nil, "agent.tool_observed", "agent.tool_observed", "agent.message_observed"],
                 sink.observations.map { |row| row["type"] }
    assert_equal 7, summary.fetch("records")
    assert_equal 0, summary.fetch("malformed")
    assert_equal "complete", summary.fetch("coverage")
    assert_equal "pi-session-1", summary["provider_session_id"],
                 "the session line sets the stream's provider session even though it emits no event"
    assert_equal "Implemented and tested.", interpreter.parser.outcome(succeeded).fetch("assistant_text")
  end

  def test_a_message_event_carries_digests_and_a_bounded_preview_not_the_text
    long = "x" * 2000
    line = JSON.generate("type" => "message_end", "timestamp" => "2026-08-29T18:00:04Z",
                         "message" => { "role" => "assistant", "stopReason" => "stop",
                                        "usage" => { "input" => 3, "output" => 4, "totalTokens" => 7, "cost" => { "total" => 0.5 } },
                                        "content" => [{ "type" => "text", "text" => long }] })
    _interpreter, sink, = stream("#{line}\n")
    observation = sink.observations.fetch(0)

    assert_equal "agent.message_observed", observation.fetch("type")
    assert_equal "agent_reported", observation.fetch("provenance")
    assert_equal "2026-08-29T18:00:04Z", observation.fetch("occurred_at")
    assert_equal 2000, observation.dig("data", "text_bytes")
    assert_equal Digest::SHA256.hexdigest(long), observation.dig("data", "text_sha256")
    assert_equal true, observation.dig("data", "preview_truncated")
    assert_equal 512, observation.dig("data", "preview").bytesize
    assert_equal 200, observation.fetch("summary").length
    assert_equal({ "input" => 3, "output" => 4, "total_tokens" => 7, "cost" => 0.5 },
                 observation.dig("data", "usage"))
  end

  def test_oversized_tool_arguments_and_results_are_truncated_with_the_truncation_stated
    args = { "command" => "y" * 4000 }
    line = JSON.generate("type" => "tool_execution_end", "toolCallId" => "tool-9", "toolName" => "bash",
                         "args" => args, "result" => { "content" => "z" * 4000 }, "isError" => true)
    _interpreter, sink, = stream("#{line}\n", max_record_bytes: 32 * 1024)
    observation = sink.observations.fetch(0)

    assert_equal "agent.tool_observed", observation.fetch("type")
    assert_equal "end", observation.dig("data", "phase")
    assert_equal "tool-9", observation.dig("data", "tool_call_id")
    assert_equal true, observation.dig("data", "is_error")
    assert_equal 512, observation.dig("data", "args_preview").bytesize
    assert_equal 512, observation.dig("data", "result_preview").bytesize
    assert_equal({ "args" => true, "result" => true }, observation.dig("data", "truncation"))
    assert_operator observation.dig("data", "args_bytes"), :>, 4000
    assert_equal Digest::SHA256.hexdigest(JSON.generate(args)), observation.dig("data", "args_sha256")
    # The bound that decides whether this can ever be committed is the store's payload limit.
    assert_operator JSON.generate(observation.fetch("data")).bytesize, :<,
                    Backstage::Domain::Activity::DATA_BYTE_LIMIT
  end

  def test_an_unreadable_line_is_reported_not_dropped
    _interpreter, sink, summary = stream("this is not json at all\n")
    observation = sink.observations.fetch(0)

    assert_equal true, observation.fetch("malformed")
    refute_nil observation["detail"]
    assert_nil observation["type"], "a malformed record has no activity type to commit"
    assert_equal 1, summary.fetch("malformed"), "the stream counts it rather than losing it"
    assert_equal 1, summary.fetch("records")
  end

  def test_a_json_line_that_is_not_an_object_is_malformed_rather_than_ignored
    _interpreter, sink, = stream("[1,2,3]\n")

    assert_equal true, sink.observations.fetch(0).fetch("malformed")
  end

  def test_events_that_only_bracket_other_events_produce_nothing
    interpreter = Interpreter.new(model: "m")
    %w[agent_start agent_end tool_execution_update agent_settled].each do |type|
      assert_empty interpreter.observe(frame(JSON.generate("type" => type))), type
    end

    assert_equal true, interpreter.parser.state.fetch("settled")
  end

  def test_a_session_line_names_the_stream_and_commits_nothing
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      artifacts = Backstage::ArtifactStore.new(File.join(directory, "artifacts"))
      run = { "id" => "run-session", "phase" => "implementation" }
      capture = RuntimeCapture.new(
        sink: Backstage::Application::CaptureSink::Durable.new(
          store: store, artifact_store: artifacts, work_item_id: "work-1", run: run, attempt: 1
        ),
        clock: Backstage::Adapters::Fake::Clock.new, run: run, attempt: 1
      )
      writer = capture.open(step: "harness", interpreter: Interpreter.new(model: "m"))
      writer.write(%({"type":"session","version":3,"id":"pi-session-1"}\n))
      summary = writer.close(reason: "close")

      assert_equal "pi-session-1", summary.fetch("provider_session_id"),
                   "a stream whose only line is a session line still learns the session id"
      assert_equal "pi-session-1", store.fetch!("runtime_streams", writer.id).fetch("provider_session_id")
      semantic = store.read_activity(limit: 100).fetch("events")
                      .select { |event| event.fetch("type").start_with?("agent.") }
      assert_empty semantic, "identity is not history: a session line commits no event of its own"
      assert_equal 1, summary.fetch("records")
      assert_equal 0, summary.fetch("malformed")
    end
  end

  def test_the_fixture_parses_identically_however_the_bytes_are_split
    # 7 bytes is deliberately hostile: it lands inside JSON tokens and never on a record boundary.
    assert_equal summarize(fixture), summarize(fixture, chunk_size: 7)
    assert_equal summarize(fixture), summarize(fixture, chunk_size: 1)
  end

  def test_a_multibyte_character_split_across_writes_reaches_the_event_intact
    text = "réussi ✅"
    line = JSON.generate("type" => "message_end",
                         "message" => { "role" => "assistant", "stopReason" => "stop",
                                        "content" => [{ "type" => "text", "text" => text }] })
    _interpreter, sink, = stream("#{line}\n", chunk_size: 1)

    assert_equal text, sink.observations.fetch(0).dig("data", "preview")
  end

  private

  def succeeded
    { "status" => "succeeded", "process" => { "exit_code" => 0, "signal" => nil } }
  end

  def frame(text, index: 0)
    { "index" => index, "start_offset" => 0, "end_offset" => text.bytesize + 1,
      "text" => text, "encoding" => "utf-8", "truncated" => false }
  end

  # Feeds `bytes` through a real capture and returns what came out of it.
  def stream(bytes, chunk_size: nil, **options)
    interpreter = Interpreter.new(model: "z-ai/glm-5.3-flash")
    sink = RecordingSink.new
    capture = RuntimeCapture.new(sink: sink, clock: Backstage::Adapters::Fake::Clock.new,
                                 run: "run-pi", attempt: 1, **options)
    writer = capture.open(step: "harness", interpreter: interpreter)
    if chunk_size
      offset = 0
      while offset < bytes.bytesize
        writer.write(bytes.byteslice(offset, chunk_size))
        offset += chunk_size
      end
    else
      writer.write(bytes)
    end
    [interpreter, sink, writer.close(reason: "close")]
  end

  def summarize(bytes, chunk_size: nil)
    interpreter, sink, summary = stream(bytes, chunk_size: chunk_size)
    {
      "records" => summary.fetch("records"),
      "malformed" => summary.fetch("malformed"),
      "session" => summary["provider_session_id"],
      "observations" => sink.observations.map { |row| [row["type"], row["summary"], row.dig("data", "text_sha256")] },
      "outcome" => interpreter.parser.outcome(succeeded).slice("status", "assistant_text", "stop_reason", "settled")
    }
  end
end
