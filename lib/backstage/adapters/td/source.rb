# frozen_string_literal: true

require "json"

module Backstage::Adapters::Td
  class Source < Backstage::Ports::WorkSource
    CAPABILITIES = %w[record_result request_review].freeze

    def initialize(client:)
      @client = client
    end

    def snapshot(ref)
      issue = @client.show(native_ref(ref))
      {
        "ref" => native_ref(issue.fetch("id")),
        "title" => issue.fetch("title"),
        "content" => JSON.generate(issue),
        "media_type" => "application/json",
        "version" => issue["updated_at"]
      }.compact
    end

    def discover
      @client.ready_issues.filter_map do |issue|
        native_ref(issue.fetch("id")) if issue["status"] == "open" && Array(issue["labels"]).include?("agent-ready")
      end.uniq
    end

    def capabilities = CAPABILITIES

    def prepare(operation:, ref:, result:, operation_id:)
      check_operation!(operation)
      native_ref(ref)
      case operation
      when "record_result"
        {
          "done" => result.fetch("summary"),
          "remaining" => "Human disposition of the independently reviewed draft.",
          "decisions" => [
            "Independent review: #{result.fetch("review").fetch("verdict")}; #{result.fetch("review").fetch("summary")}",
            "Candidate: #{result.fetch("candidate").fetch("sha256")}",
            marker(operation_id)
          ]
        }
      when "request_review"
        { "reason" => "Backstage independently reviewed draft is ready for human disposition. #{marker(operation_id)}" }
      end
    end

    def reconcile(operation:, ref:, payload:, operation_id:, attempted:)
      check_operation!(operation)
      issue = @client.show(native_ref(ref))
      case operation
      when "record_result"
        handoff = issue["handoff"]
        # A later handoff can replace the visible one. Absence here cannot prove an attempted
        # handoff did not happen, so an operator must resolve that ambiguity.
        applied = handoff.is_a?(Hash) && Array(handoff["decisions"]).include?(marker(operation_id))
        { "status" => applied ? "applied" : (attempted ? "unknown" : "not_applied"),
          "receipt" => { "ref" => native_ref(ref), "operation_marker_present" => applied, "native_scope" => "issue_and_td_hierarchy" } }
      when "request_review"
        status = issue["status"]
        observed = if %w[in_review closed].include?(status)
                     "applied"
                   else
                     attempted ? "unknown" : "not_applied"
                   end
        { "status" => observed, "receipt" => { "ref" => native_ref(ref), "status" => status, "native_scope" => "issue_and_td_hierarchy" } }
      end
    end

    def execute(operation:, ref:, payload:, operation_id:)
      check_operation!(operation)
      id = native_ref(ref)
      case operation
      when "record_result"
        @client.handoff(id, done: payload.fetch("done"), remaining: payload.fetch("remaining"), decisions: payload.fetch("decisions"))
      when "request_review"
        @client.review(id, reason: payload.fetch("reason"))
      end
      reconcile(operation: operation, ref: id, payload: payload, operation_id: operation_id, attempted: true).then do |observation|
        # A successful command without visible evidence is still ambiguous.
        observation.merge("status" => observation["status"] == "applied" ? "applied" : "unknown")
      end
    end

    private

    def native_ref(ref)
      value = ref.to_s.downcase
      raise Backstage::ContractError, "invalid td issue reference" unless value.match?(/\Atd-[a-z0-9]+\z/)

      value
    end

    def marker(operation_id) = "Backstage operation: #{operation_id}"

    def check_operation!(operation)
      raise Backstage::ContractError, "unsupported td operation: #{operation}" unless CAPABILITIES.include?(operation)
    end
  end
end
