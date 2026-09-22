# frozen_string_literal: true

require_relative "test_helper"

class PiHarnessTest < Minitest::Test
  class FakeRuntime
    attr_reader :bundle, :secrets

    def initialize(outcome)
      @outcome = outcome
    end

    # Output reaches the harness only through the capture stream now, so this double writes its
    # fixture bytes there the way a container's reader thread would.
    def run(bundle:, secrets:, cancellation:, capture: nil)
      @bundle = bundle
      @secrets = secrets
      capture&.write(@outcome.fetch("logs", ""))
      capture&.close(reason: @outcome.fetch("status") == "cancelled" ? "cancelled" : "close")
      @outcome
    end
  end

  def bundle
    JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__))).tap do |value|
      value["harness"] = {
        "adapter" => "pi",
        "provider" => "openrouter",
        "model" => "z-ai/glm-5.3-flash",
        "prompt" => "sealed prompt",
        "credential_ref" => "openrouter",
        "options" => {}
      }
      value["execution"]["credential_refs"] = ["openrouter"]
    end
  end

  def broker
    example_broker( { "OPENROUTER_API_KEY" => "secret-openrouter" })
  end

  def runtime_outcome(logs:, status: "succeeded", exit_code: 0)
    {
      "schema_version" => 1,
      "status" => status,
      "summary" => "container #{status}",
      "process" => { "exit_code" => exit_code, "signal" => nil },
      "cancellation" => { "requested" => status == "cancelled", "timed_out" => false },
      "logs" => logs
    }
  end

  def test_exact_invocation_uses_stdin_and_environment_credential
    runtime = FakeRuntime.new(runtime_outcome(logs: File.read(File.expand_path("fixtures/pi_success.jsonl", __dir__))))
    harness = Backstage::PiHarness.new(runtime: runtime, credential_broker: broker)
    normalized = []

    outcome = harness.run(bundle: bundle) { |event| normalized << event }

    assert_equal ["pi", "--mode", "json", "--provider", "openrouter", "--model", "z-ai/glm-5.3-flash", "--no-session", "--no-extensions", "--no-skills", "--no-context-files", "--no-approve"], runtime.bundle.dig("execution", "command")
    assert_equal "sealed prompt", runtime.bundle.dig("harness", "prompt")
    assert_equal({ "OPENROUTER_API_KEY" => "secret-openrouter" }, runtime.secrets)
    assert_equal "succeeded", outcome["status"]
    assert_equal "Implemented and tested.", outcome["assistant_text"]
    assert_nil outcome.dig("usage", "cost")
    assert_equal false, outcome.dig("usage", "cost_trusted")
    assert normalized.any? { |event| event["type"] == "tool_finished" }
    # The transcript is no longer inlined: the outcome names the stream that holds it.
    refute outcome.dig("raw").key?("events"), "v2 outcomes must not carry an inlined transcript"
    assert_equal 1, outcome.dig("raw", "stream_refs").length
    assert_equal "complete", outcome.dig("raw", "stream_refs", 0, "coverage")
    assert_equal 7, outcome.dig("raw", "stream_refs", 0, "records")
    assert_equal 2, outcome.fetch("schema_version")
  end

  def test_error_stop_reason_fails_even_when_json_mode_process_exits_zero
    logs = <<~JSONL
      {"type":"message_end","message":{"role":"assistant","content":[],"usage":{},"stopReason":"error","errorMessage":"provider failed"}}
      {"type":"agent_settled"}
    JSONL
    parser = Backstage::PiEventParser.new(model: "known-model")
    logs.each_line { |line| parser.feed(line) }

    outcome = parser.outcome(runtime_outcome(logs: logs))

    assert_equal "failed", outcome["status"]
    assert_equal "provider failed", outcome["summary"]
  end

  def test_controller_cancellation_wins_without_terminal_pi_json
    parser = Backstage::PiEventParser.new(model: "z-ai/glm-5.3-flash")
    parser.feed(%({"type":"session","id":"partial"}\n))

    outcome = parser.outcome(runtime_outcome(logs: "", status: "cancelled", exit_code: 143))

    assert_equal "cancelled", outcome["status"]
    assert_equal false, outcome["settled"]
  end

  def test_unsettled_stop_is_not_reported_as_success
    parser = Backstage::PiEventParser.new(model: "z-ai/glm-5.3-flash")
    parser.feed(%({"type":"message_end","message":{"role":"assistant","content":[],"usage":{},"stopReason":"stop"}}\n))

    assert_equal "failed", parser.outcome(runtime_outcome(logs: ""))["status"]
  end

  # --- the record bound ---------------------------------------------------------------------------

  # An 80 KiB final message and a 200 KiB tool result. Both are ordinary for a coding agent reading
  # files, and both are over the 64 KiB bound that plain output gets. Under that bound the records
  # were cut, stopped parsing, never reached the parser, and a run pi completed was reported "pi run
  # failed" with an empty message, no usage and no stop reason.
  def big_transcript(message_bytes: 80 * 1024, result_bytes: 200 * 1024)
    [
      JSON.generate("type" => "session", "id" => "session-big"),
      JSON.generate("type" => "tool_execution_end", "toolCallId" => "call-1", "toolName" => "read_file",
                    "args" => { "path" => "big.txt" }, "result" => "r" * result_bytes),
      JSON.generate("type" => "message_end",
                    "message" => { "role" => "assistant", "stopReason" => "stop",
                                   "content" => [{ "type" => "text", "text" => "m" * message_bytes }],
                                   "usage" => { "input" => 1, "output" => 2, "totalTokens" => 3,
                                                "cost" => { "total" => 0.5 } } }),
      JSON.generate("type" => "agent_settled")
    ].join("\n") + "\n"
  end

  def test_records_far_over_the_plain_output_bound_still_make_a_successful_run
    runtime = FakeRuntime.new(runtime_outcome(logs: big_transcript))
    harness = Backstage::PiHarness.new(runtime: runtime, credential_broker: broker)

    outcome = harness.run(bundle: bundle)

    assert_equal "succeeded", outcome.fetch("status")
    assert_equal 80 * 1024, outcome.fetch("assistant_text").bytesize
    assert_equal "stop", outcome.fetch("stop_reason")
    assert_equal 3, outcome.dig("usage", "total_tokens")
    assert_equal({ "events" => 4, "messages" => 1, "tools" => 1 }, outcome.dig("raw", "counts"))
    assert_equal 0, outcome.dig("raw", "stream_refs", 0, "truncated_records")
    assert_equal 8 * 1024 * 1024, outcome.dig("raw", "stream_refs", 0, "max_record_bytes")
    # The previews stay small however large the record they came from.
    assert_nil outcome["incomplete_reason"]
  end

  # And past even the generous bound the answer is an explicit one. A cut record is not a failed
  # run with empty fields: the status says incomplete, the reason names the truncation, and the
  # summary and the stream ref carry the offset a reader has to go and look at.
  def test_a_record_past_the_protocol_bound_is_reported_as_a_truncation_not_a_bare_failure
    runtime = FakeRuntime.new(runtime_outcome(logs: big_transcript))
    harness = Backstage::PiHarness.new(runtime: runtime, credential_broker: broker)
    capture = Backstage::Application::CaptureDefaults.null(run: "run-cut", max_protocol_record_bytes: 4096)

    outcome = harness.run(bundle: bundle, capture: capture)

    assert_equal "incomplete", outcome.fetch("status")
    assert_equal "record_truncated", outcome.fetch("incomplete_reason")
    assert_includes outcome.fetch("summary"), "exceeded the 4096-byte capture record limit"
    assert_includes outcome.fetch("summary"), "first at offset"
    ref = outcome.dig("raw", "stream_refs", 0)

    assert_equal 2, ref.fetch("truncated_records"), "the tool result and the message were both cut"
    assert_equal 4096, ref.fetch("max_record_bytes")
    assert_equal "truncated", ref.fetch("coverage")
    assert_operator ref.fetch("records"), :>, 0, "the cut records are still counted"
  end

  # The counts and the coverage are what a reader is left with, so they have to be right even when
  # the parse failed: nothing is quietly zeroed.
  def test_a_truncated_run_still_reports_what_it_did_read
    runtime = FakeRuntime.new(runtime_outcome(logs: big_transcript))
    harness = Backstage::PiHarness.new(runtime: runtime, credential_broker: broker)
    capture = Backstage::Application::CaptureDefaults.null(run: "run-cut", max_protocol_record_bytes: 4096)

    outcome = harness.run(bundle: bundle, capture: capture)

    assert_equal "session-big", outcome.dig("raw", "provider_session_id"), "the readable records still counted"
    assert_equal 2, outcome.dig("raw", "stream_refs", 0, "malformed")
    assert_operator outcome.dig("raw", "stream_refs", 0, "truncated_record_offset"), :>=, 0
  end
end
