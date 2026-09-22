# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Adapters::Pi
  # Reads pi's JSON-lines output as meaning, one framed record at a time.
  #
  # This is the only place pi's format is parsed. The harness used to split the runtime's log text
  # into lines itself and hand each one to an EventParser; now the capture path frames the records
  # (across chunk and multibyte boundaries, on redacted bytes) and this interpreter says what each
  # one means. The parser it wraps still owns vendor normalization and the fields the outcome needs.
  #
  # What becomes an event and what does not is a judgment about evidence, not about volume:
  #
  # - `message_end` from the assistant is what the agent decided to say, so it commits as
  #   `agent.message_observed` — with digests, sizes and a bounded preview, never the whole text.
  # - `tool_execution_start` / `tool_execution_end` are what the agent did, so each commits as
  #   `agent.tool_observed`. Arguments and results are the parts most likely to be enormous and
  #   most likely to matter, so they are previewed under a hard cap with the truncation stated.
  # - `session` is identity, not an event: it returns a metadata observation, which the capture
  #   applies to the stream record and commits as nothing.
  # - `agent_start`, `agent_end`, `tool_execution_update` and `agent_settled` produce nothing.
  #   Start and end bracket what the surrounding execution events already say; an update is a
  #   partial result the `end` supersedes; settled and the stop reason surface on
  #   `runtime.reported_completion`. Committing them would be noise with a provenance stamp.
  #
  # A line that is not JSON is *not* dropped, which is what the old parser did on
  # `JSON::ParserError`. It returns a malformed observation, gets counted, and a bounded preview of
  # it reaches history: a harness printing something Backstage cannot read is evidence about the
  # harness.
  class StreamInterpreter < Backstage::Ports::StreamInterpreter
    PREVIEW_BYTES = 512
    SUMMARY_CHARS = 200
    SILENT = %w[agent_start agent_end tool_execution_update agent_settled].freeze

    attr_reader :parser

    def initialize(model: nil, parser: nil)
      @parser = parser || EventParser.new(model: model)
      @normalized = []
    end

    def self.restore(state)
      row = state || {}
      new(parser: EventParser.restore(row["parser"]))
    end

    # pi's output is a JSON-lines protocol: one record is one complete statement, and a tool result
    # of several hundred kilobytes is an ordinary morning's work for an agent reading files. Cutting
    # such a record at the plain-output bound does not shorten it, it makes it unparseable — which
    # turned a successful run into a failure with an empty message and no usage. See
    # Ports::StreamInterpreter#protocol?.
    def protocol?
      true
    end

    # Normalized harness events the parser has produced since the last drain. They are the live
    # progress signal the harness yields to its caller; they are deliberately *not* part of
    # `state`, because they are in flight rather than resumable position.
    def drain_normalized
      drained = @normalized
      @normalized = []
      drained
    end

    def observe(frame)
      text = frame.fetch("text").to_s
      return [] if text.strip.empty?

      begin
        event = JSON.parse(text)
      rescue JSON::ParserError => error
        return [malformed(error.message)]
      end
      return [malformed("record is not a JSON object")] unless event.is_a?(Hash) && event["type"]

      normalized = @parser.absorb(event)
      @normalized << normalized if normalized
      observations(event)
    end

    # Bounded and JSON-safe: counters, the session id, and the one message the outcome is built
    # from. The message is vendor text, so it is the only unbounded-ish thing here — it is kept
    # because a resumed stream that could not rebuild `assistant_text` would produce a different
    # outcome for the same run, which is worse than the bytes it costs.
    def state
      { "parser" => @parser.state }
    end

    private

    def observations(event)
      case event.fetch("type")
      when *SILENT then []
      when "session" then [session_observation(event)]
      when "message_end" then message_observations(event)
      when "tool_execution_start" then [tool_observation(event, "start")]
      when "tool_execution_end" then [tool_observation(event, "end")]
      else []
      end
    end

    # Identity, not history. A metadata observation — one with no `type` — is applied to the stream
    # and committed as no event at all, which is exactly right for an id: the capture stamps
    # `provider_session_id` on the stream record and on every event that follows, so a reader can
    # join Backstage's run to the vendor's session without an event whose only content is an id.
    def session_observation(event)
      { "provider_session_id" => (event["id"] || event["sessionId"]).to_s }
    end

    def session_id(event)
      event["sessionId"] || @parser.provider_session_id
    end

    def message_observations(event)
      message = event["message"]
      return [] unless message.is_a?(Hash) && message["role"] == "assistant"

      text = message_text(message)
      usage = message["usage"]
      [{
        "type" => "agent.message_observed",
        "provenance" => "agent_reported",
        "occurred_at" => provider_time(event),
        "provider_event_id" => event["id"],
        "provider_session_id" => session_id(event),
        "summary" => summary_of(text, "assistant message"),
        "data" => {
          "role" => message["role"],
          "stop_reason" => message["stopReason"],
          "text_bytes" => text.bytesize,
          "text_sha256" => Digest::SHA256.hexdigest(text),
          "usage" => bounded_usage(usage),
          "preview_truncated" => text.bytesize > PREVIEW_BYTES,
          "preview" => preview(text)
        }.compact
      }]
    end

    def tool_observation(event, phase)
      args = serialize(event["args"])
      result = serialize(event["result"] || event["partialResult"])
      name = event["toolName"].to_s
      {
        "type" => "agent.tool_observed",
        "provenance" => "agent_reported",
        "occurred_at" => provider_time(event),
        "provider_event_id" => event["toolCallId"] || event["id"],
        "provider_session_id" => session_id(event),
        "summary" => "tool #{name.empty? ? "(unnamed)" : name} #{phase}" \
                     "#{event["isError"] ? " with an error" : ""}",
        "data" => {
          "tool_name" => name,
          "tool_call_id" => event["toolCallId"],
          "phase" => phase,
          "is_error" => event["isError"] == true,
          "args_bytes" => args.bytesize,
          "args_sha256" => args.empty? ? nil : Digest::SHA256.hexdigest(args),
          "args_preview" => preview(args),
          "result_bytes" => result.bytesize,
          "result_sha256" => result.empty? ? nil : Digest::SHA256.hexdigest(result),
          "result_preview" => preview(result),
          "truncation" => {
            "args" => args.bytesize > PREVIEW_BYTES,
            "result" => result.bytesize > PREVIEW_BYTES
          }
        }.compact
      }
    end

    def malformed(detail)
      { "malformed" => true, "detail" => detail.to_s[0, SUMMARY_CHARS] }
    end

    def message_text(message)
      Array(message["content"]).filter_map do |block|
        block["text"] if block.is_a?(Hash) && block["type"] == "text"
      end.join("\n")
    end

    def summary_of(text, fallback)
      clipped = text.to_s.strip.gsub(/\s+/, " ")
      return fallback if clipped.empty?

      clipped[0, SUMMARY_CHARS]
    end

    # A preview is cut at a byte bound, so it can land mid-character; scrubbing keeps the event
    # valid JSON without pretending the cut did not happen.
    def preview(text)
      return nil if text.nil? || text.empty?

      text.byteslice(0, PREVIEW_BYTES).to_s.scrub("\u{FFFD}")
    end

    # Counts only. A vendor is free to invent usage fields; carrying them verbatim into bounded
    # activity data is how a payload limit gets discovered in production.
    def bounded_usage(usage)
      return nil unless usage.is_a?(Hash)

      {
        "input" => usage["input"], "output" => usage["output"],
        "total_tokens" => usage["totalTokens"], "cost" => usage.dig("cost", "total")
      }.compact
    end

    def serialize(value)
      case value
      when nil then ""
      when String then value
      else JSON.generate(value)
      end
    rescue JSON::GeneratorError
      value.to_s
    end

    # Provider time is used only when it is a string an envelope can carry. A vendor's epoch
    # integer is not silently reformatted into an ISO timestamp Backstage did not observe.
    def provider_time(event)
      value = event["timestamp"]
      value.is_a?(String) && !value.empty? && value.length <= 64 ? value : nil
    end
  end
end
