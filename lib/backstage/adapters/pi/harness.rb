# frozen_string_literal: true

require "json"

module Backstage::Adapters::Pi
  # The pi harness: it composes the invocation, runs it on a runtime, and turns what pi said into
  # a structured outcome.
  #
  # It no longer reads the runtime's output itself. It used to receive `runtime_log` events, split
  # their text on `each_line`, and — if nothing had streamed — replay the runtime's whole `logs`
  # string a second time. That made the harness a line-splitter with a fallback re-parse, and the
  # transcript it accumulated rode inside the outcome. Now it opens one capture stream with a
  # `StreamInterpreter`, hands the writer down to the runtime, and lets the capture path frame the
  # records once, on redacted bytes, across chunk boundaries. There is no fallback: the stream is
  # the source, and a runtime that returned only `logs` returned nothing this harness can read.
  class Harness
    ContractError = Backstage::ContractError
    ContractValidator = Backstage::Contracts::Validator
    CaptureDefaults = Backstage::Application::CaptureDefaults
    Outcome = Backstage::Domain::Outcome
    COMMAND_PREFIX = ["pi", "--mode", "json", "--provider"].freeze
    COMMAND_SUFFIX = ["--no-session", "--no-extensions", "--no-skills", "--no-context-files", "--no-approve"].freeze
    STEP = "harness"

    def initialize(runtime:, credential_broker:, validator: ContractValidator.new)
      @runtime = runtime
      @credential_broker = credential_broker
      @validator = validator
    end

    def runtime_identity_before_launch?
      @runtime.respond_to?(:runtime_identity_before_launch?) && @runtime.runtime_identity_before_launch?
    end

    def invocation(bundle)
      harness = bundle.fetch("harness")
      raise ContractError, "pi harness adapter required" unless harness.fetch("adapter") == "pi"

      {
        "command" => COMMAND_PREFIX + [harness.fetch("provider"), "--model", harness.fetch("model")] + COMMAND_SUFFIX,
        "stdin" => harness.fetch("prompt"),
        "credential_refs" => [harness.fetch("credential_ref")]
      }
    end

    # `capture` is a `Ports::RuntimeCapture` — the component, not a stream. The harness is the only
    # object that knows this run's output is pi's JSON-lines protocol, so it is the one that opens
    # the stream and chooses the interpreter. Without one it composes a null-sink capture: framing
    # and interpretation still happen, nothing is written down.
    def run(bundle:, secrets: nil, cancellation: nil, capture: nil)
      invocation = invocation(bundle)
      runtime_env = secrets || @credential_broker.runtime_environment(invocation.fetch("credential_refs"))
      runtime_bundle = JSON.parse(JSON.generate(bundle))
      runtime_bundle["execution"]["command"] = invocation.fetch("command")
      runtime_bundle["execution"]["credential_refs"] = runtime_env.keys
      interpreter = StreamInterpreter.new(model: bundle.dig("harness", "model"))
      opener = capture || CaptureDefaults.null(run: bundle.fetch("id", "local"))
      # No `phase:` override: the harness knows which step it is, not which phase of the run it
      # belongs to, and the capture already carries the run's phase.
      writer = opener.open(step: STEP, interpreter: interpreter)

      runtime_outcome = nil
      begin
        runtime_outcome = @runtime.run(bundle: runtime_bundle, secrets: runtime_env,
                                       cancellation: cancellation, capture: writer) do |event|
          # Runtime identity, heartbeats and progress belong to the controller; the semantic events
          # pi produced belong to whoever asked for live visibility. Draining first means a caller
          # sees what the agent did before it sees the tick that revealed it.
          forward(interpreter) { |normalized| yield(normalized) if block_given? }
          yield(event) if block_given?
        end
      ensure
        # Closing flushes the trailing record, so the last observations exist only after this.
        summary = close(writer, runtime_outcome)
      end
      forward(interpreter) { |normalized| yield(normalized) if block_given? }

      Outcome.validate!(interpreter.parser.outcome(runtime_outcome, stream: summary))
    end

    private

    def forward(interpreter)
      interpreter.drain_normalized.each do |normalized|
        @validator.validate!("harness-event-v1.json", normalized)
        yield(normalized)
      end
    end

    # The runtime already closed the stream with the reason it ended for; `close` is idempotent, so
    # this returns that same summary. It matters when the runtime raised before closing.
    def close(writer, runtime_outcome)
      status = runtime_outcome && runtime_outcome["status"]
      reason = %w[cancelled timed_out].include?(status) ? status : (runtime_outcome ? "close" : "failed")
      writer.close(reason: reason)
    rescue Backstage::CaptureError
      nil
    end
  end

  # Vendor normalization and the fields the outcome is built from. It keeps counters, the settled
  # flag and the one authoritative assistant message — not the transcript. `@raw_events` used to
  # hold every event pi emitted and ride into `outcome.raw.events`; the transcript now lives in the
  # captured stream artifacts, which `raw.stream_refs` names.
  class EventParser
    Records = Backstage::Domain::Records
    TYPE_MAP = {
      "session" => "session",
      "agent_start" => "agent_started",
      "message_end" => "message",
      "tool_execution_start" => "tool_started",
      "tool_execution_update" => "tool_updated",
      "tool_execution_end" => "tool_finished",
      "agent_end" => "agent_finished",
      "agent_settled" => "settled"
    }.freeze

    attr_reader :counts, :provider_session_id

    def initialize(model:, authoritative_message: nil, settled: false, counts: nil,
                   provider_session_id: nil)
      @model = model
      @authoritative_message = authoritative_message
      @settled = settled ? true : false
      @counts = counts.is_a?(Hash) ? counts.dup : { "events" => 0, "messages" => 0, "tools" => 0 }
      @provider_session_id = provider_session_id
    end

    def self.restore(state)
      row = state || {}
      new(model: row["model"], authoritative_message: row["authoritative_message"],
          settled: row["settled"], counts: row["counts"], provider_session_id: row["provider_session_id"])
    end

    # Bounded and JSON-safe. The authoritative message is the one piece of vendor text here, and it
    # is kept deliberately: a resumed stream that could not rebuild `assistant_text` would produce a
    # different outcome for the same run.
    def state
      { "model" => @model, "settled" => @settled, "counts" => @counts,
        "provider_session_id" => @provider_session_id,
        "authoritative_message" => @authoritative_message }.compact
    end

    # One parsed pi event. Returns the normalized harness event, or nil for a type with no mapping.
    def absorb(event)
      return nil unless event.is_a?(Hash) && event["type"]

      @counts["events"] = @counts.fetch("events", 0) + 1
      case event["type"]
      when "agent_settled" then @settled = true
      when "session" then @provider_session_id = (event["id"] || event["sessionId"])&.to_s
      when "tool_execution_end" then @counts["tools"] = @counts.fetch("tools", 0) + 1
      when "message_end"
        if event.dig("message", "role") == "assistant"
          @authoritative_message = event.fetch("message")
          @counts["messages"] = @counts.fetch("messages", 0) + 1
        end
      end
      normalize(event)
    end

    # One raw line. Kept for callers holding a line rather than a framed record; the capture path
    # goes through StreamInterpreter, which parses once and hands the event straight to `absorb`.
    def feed(line)
      absorb(JSON.parse(line))
    rescue JSON::ParserError
      nil
    end

    # `stream` is the capture close summary for this run's harness stream, or nil.
    def outcome(runtime_outcome, stream: nil)
      runtime_outcome ||= { "status" => "failed", "process" => { "exit_code" => nil, "signal" => nil } }
      runtime_status = runtime_outcome.fetch("status")
      stop_reason = @authoritative_message&.fetch("stopReason", nil)
      cut = truncation(stream)
      status = classify(runtime_status, stop_reason, cut)
      usage = @authoritative_message&.fetch("usage", {}) || {}
      {
        "schema_version" => 2,
        "status" => status,
        "summary" => summary(status, cut),
        "incomplete_reason" => (cut && status == "incomplete" ? "record_truncated" : nil),
        "assistant_text" => assistant_text,
        "stop_reason" => stop_reason,
        "settled" => @settled,
        "process" => runtime_outcome.fetch("process"),
        "cancellation" => runtime_outcome["cancellation"],
        "logs" => runtime_outcome["logs"],
        "logs_truncated" => runtime_outcome["logs_truncated"],
        "log_tail_bytes" => runtime_outcome["log_tail_bytes"],
        "capture" => runtime_outcome["capture"],
        "usage" => {
          "input_tokens" => usage["input"],
          "output_tokens" => usage["output"],
          "total_tokens" => usage["totalTokens"],
          "cost" => trusted_cost? ? usage.dig("cost", "total") : nil,
          "cost_trusted" => trusted_cost?
        },
        "raw" => {
          "vendor" => "pi",
          "model" => @model,
          "authoritative_message" => @authoritative_message,
          "provider_session_id" => @provider_session_id,
          "counts" => @counts,
          "stream_refs" => stream_refs(stream)
        }
      }.compact
    end

    private

    # What the capture had to cut, or nil. A record over the stream's record bound is the one thing
    # that can make a run pi actually completed unreadable to this parser: the truncated line does
    # not parse, so the event it carried never reaches `absorb`. That must not surface as a bare
    # "pi run failed" with an empty message and no usage — it is a capture bound taking effect, and
    # the outcome says so, with the offset to go and look at.
    def truncation(stream)
      return nil unless stream && stream["truncated_records"].to_i.positive?

      { "records" => stream["truncated_records"].to_i,
        "offset" => stream["truncated_record_offset"],
        "max_record_bytes" => stream["max_record_bytes"] }.compact
    end

    # Where the transcript actually is. One entry, naming the stream, its manifest artifact and how
    # much of it was captured — enough for a reader to fetch the records or to know they cannot.
    def stream_refs(stream)
      return [] unless stream

      [{
        "stream_id" => stream.fetch("stream_id"),
        "artifact_id" => stream["manifest_artifact_id"],
        "records" => stream["records"],
        "bytes" => stream["bytes"],
        "malformed" => stream["malformed"],
        "truncated_records" => stream["truncated_records"],
        "truncated_record_offset" => stream["truncated_record_offset"],
        "max_record_bytes" => stream["max_record_bytes"],
        "coverage" => stream["coverage"]
      }.compact]
    end

    def normalize(event)
      mapped = TYPE_MAP[event["type"]]
      return nil unless mapped

      {
        "schema_version" => 1,
        "type" => mapped,
        "timestamp" => event["timestamp"] || Records.timestamp,
        "harness" => "pi",
        "run_id" => event["id"] || event["sessionId"],
        "message" => event["message"],
        "tool" => normalize_tool(event),
        "usage" => event["usage"] || event.dig("message", "usage"),
        "raw" => event
      }
    end

    def normalize_tool(event)
      return nil unless event["type"].start_with?("tool_execution_")

      {
        "id" => event["toolCallId"],
        "name" => event["toolName"],
        "arguments" => event["args"],
        "result" => event["result"] || event["partialResult"],
        "is_error" => event["isError"]
      }.compact
    end

    def classify(runtime_status, stop_reason, cut = nil)
      return "cancelled" if runtime_status == "cancelled" || stop_reason == "aborted"
      return "timed_out" if runtime_status == "timed_out"
      return "failed" unless runtime_status == "succeeded"
      # The process ran to completion but part of its protocol was cut, so what pi said is not
      # fully known. `incomplete` is the honest word for that, and it is never `succeeded`: a
      # caller must not act on a verdict or a result read out of a record with a hole in it.
      return "incomplete" if cut
      return "succeeded" if stop_reason == "stop" && @settled
      return "incomplete" if %w[length deferred toolUse].include?(stop_reason)

      "failed"
    end

    def assistant_text
      Array(@authoritative_message&.fetch("content", nil)).filter_map do |block|
        block["text"] if block.is_a?(Hash) && block["type"] == "text"
      end.join("\n")
    end

    def trusted_cost?
      @model != "z-ai/glm-5.3-flash"
    end

    def summary(status, cut = nil)
      return @authoritative_message["errorMessage"] if status == "failed" && @authoritative_message&.fetch("errorMessage", nil)
      if cut && status == "incomplete"
        return "pi run incomplete: #{cut.fetch("records")} record(s) exceeded the #{cut["max_record_bytes"]}-byte " \
               "capture record limit, first at offset #{cut["offset"]}; the harness protocol could not be read past it"
      end

      "pi run #{status}"
    end
  end
end

Backstage::PiHarness = Backstage::Adapters::Pi::Harness unless defined?(Backstage::PiHarness)
Backstage::PiEventParser = Backstage::Adapters::Pi::EventParser unless defined?(Backstage::PiEventParser)
