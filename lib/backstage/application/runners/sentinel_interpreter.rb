# frozen_string_literal: true

require "json"

module Backstage::Application::Runners
  # Reads the sealed worker's repository steps.
  #
  # `backstage-container-repository` announces what it did on a handful of JSON lines mixed into
  # ordinary log text: `repository_prepared`, `context_materialized`, `repository_materialized`,
  # `repository_published`. `ContainerPhaseRunner` used to find them by re-scanning the whole `logs`
  # string after each step — a second log-parsing path, with its own idea of what a line is, running
  # over an unbounded accumulator. This interpreter is the one path: the capture frames the records
  # once, this says which of them are sentinels, and the close summary hands the parsed payloads
  # back so the runner reads a value instead of re-reading a log.
  #
  # A line that is not JSON is not malformed here, and that is the difference between this stream
  # and pi's. A repository step's output is a *log* that happens to contain sentinels; pi's output
  # is a JSON-lines protocol. Reporting every human-readable progress line as an unreadable record
  # would bury the real ones.
  class SentinelInterpreter < Backstage::Ports::StreamInterpreter
    # The four announcements the runner acts on. Anything else the worker prints is output.
    SENTINELS = %w[repository_prepared context_materialized repository_materialized
                   repository_published].freeze

    # Fields worth putting in bounded activity data. Everything else stays in the payload the
    # runner reads and in the chunk artifact; an event is not the place for a whole announcement.
    REPORTED = %w[name kind mount read_only branch base base_revision resolved_revision
                  patch_size patch_sha256 number url repository draft reconciled].freeze
    VALUE_CHARS = 200

    def initialize(counts: nil)
      @counts = counts.is_a?(Hash) ? counts.dup : {}
    end

    def self.restore(state)
      new(counts: (state || {})["counts"])
    end

    def observe(frame)
      text = frame.fetch("text").to_s
      return [] unless text.lstrip.start_with?("{")

      event = begin
        JSON.parse(text)
      rescue JSON::ParserError
        nil
      end
      return [] unless event.is_a?(Hash)

      name = event["type"]
      return [] unless SENTINELS.include?(name)

      @counts[name] = @counts.fetch(name, 0) + 1
      [observation(name, event)]
    end

    def state
      { "counts" => @counts }
    end

    private

    def observation(name, event)
      payload = event.reject { |key, _| key == "type" }
      {
        "type" => "artifact.available",
        # The worker is claiming this, not Backstage. Validation of the materialized change
        # happens afterwards in the runner, against the files on disk.
        "provenance" => "runtime_reported",
        # When the worker says it did it. A published pull request is a real external effect, and
        # stamping it with the moment Backstage flushed a chunk both misdates it and makes the fact
        # depend on chunking. Absent a usable worker time the capture stamps the record's framing
        # time, which is at least a function of the bytes rather than of the buffer.
        "occurred_at" => announced_at(event),
        "summary" => "worker reported #{name.tr("_", " ")}",
        "data" => { "sentinel" => name }.merge(reported(payload)),
        "sentinel" => { "name" => name, "payload" => payload }
      }
    end

    # The worker's own time, used only when it is a string an envelope can carry. An epoch integer
    # is not silently reformatted into an ISO timestamp Backstage did not observe.
    def announced_at(event)
      value = event["timestamp"] || event["at"]
      value.is_a?(String) && !value.empty? && value.length <= 64 ? value : nil
    end

    def reported(payload)
      REPORTED.each_with_object({}) do |key, row|
        next unless payload.key?(key)

        value = payload.fetch(key)
        row[key] = value.is_a?(String) ? value[0, VALUE_CHARS] : value
      end
    end
  end
end
