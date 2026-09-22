# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Adapters::Td
  class Trigger
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    ActivityRecorder = Backstage::Application::ActivityRecorder
    APPROVED = %w[approved approved_by_parent_cascade].freeze
    # The latest poll receipt per source. One row, rewritten only when what the source says
    # actually changes, so an operator can always read what the last check found.
    CHECKS = "source_checks"

    def initialize(client:, store:, source_instance:)
      @client = client
      @store = store
      @source_instance = source_instance
      raise ContractError, "td source_instance is required" if @source_instance.to_s.empty?
    end

    # A poll is a health check, and a worker polls forever, so this deliberately does not record one
    # event per check. It records a receipt — and the `source.checked` event that explains it — only
    # when the answer changes: new triggers appeared, a failure started, or a failing source
    # recovered. An unchanged poll writes nothing at all, which is what keeps history readable and
    # the log from growing with the poll interval rather than with what happened.
    def poll
      triggers = begin
        authorized = @client.ready_issues.filter_map { |issue| authorized_trigger(issue) }
        approvals = @client.approval_candidates.filter_map do |candidate|
          issue = @client.show(candidate.fetch("id"))
          approval_trigger(issue)
        end
        authorized + approvals
      rescue StandardError => error
        record_check(status: "failed", triggers: [], error: error)
        raise
      end
      record_check(status: "ok", triggers: triggers)
      triggers
    end

    def manual(issue_id)
      authorized_trigger(@client.show(issue_id), require_ready: false)
    end

    def work_key(issue_id)
      "work:v1:td:#{@source_instance}:#{issue_id.to_s.downcase}"
    end

    private

    def authorized_trigger(issue, require_ready: true)
      if require_ready
        return nil unless issue["status"] == "open" && Array(issue["labels"]).include?("agent-ready")
      end
      key = work_key(issue.fetch("id"))
      persist_trigger(
        "id" => "trigger:v1:#{key}:authorized",
        "idempotency_key" => "trigger:v1:#{key}:authorized",
        "kind" => "authorized",
        "work_key" => key,
        "source" => "td",
        "source_instance" => @source_instance,
        "source_ref" => issue.fetch("id").downcase,
        "observed_at" => Records.timestamp,
        "payload" => public_issue(issue)
      )
    end

    def approval_trigger(issue)
      row = Array(issue["review_history"]).reject { |review| review["superseded"] }.select { |review| APPROVED.include?(review["decision"]) }.max_by { |review| review["created_at"].to_s }
      return nil unless row && row["id"].to_s.start_with?("rv-")

      key = work_key(issue.fetch("id"))
      persist_trigger(
        "id" => "trigger:v1:#{key}:approval:#{row.fetch("id")}",
        "idempotency_key" => "trigger:v1:#{key}:approval:#{row.fetch("id")}",
        "kind" => "approval",
        "work_key" => key,
        "source" => "td",
        "source_instance" => @source_instance,
        "source_ref" => issue.fetch("id").downcase,
        "observed_at" => Records.timestamp,
        "payload" => { "review" => row.reject { |field, _| field == "summary" }, "issue_status" => issue["status"] }
      )
    end

    def check_id
      "td:#{@source_instance}"
    end

    def record_check(status:, triggers:, error: nil)
      keys = triggers.map { |trigger| trigger.fetch("idempotency_key") }.sort
      # The class, not the message: a failure message is unbounded text from another system, and
      # the receipt only has to say what kind of failure this is and when it started.
      failure = error && error.class.name
      fingerprint = Digest::SHA256.hexdigest(JSON.generate([status, keys, failure]))
      existing = @store.fetch(CHECKS, check_id)
      return nil if existing && existing["fingerprint"] == fingerprint

      now = Records.timestamp
      # The fingerprint says *what* the source answered; it repeats whenever the answer comes back
      # around (ok -> failed -> ok is two different "ok"s). `check_sequence` counts *occurrences* of
      # the answer changing, so the event that explains a recovery has an identity of its own
      # instead of colliding with the event that recorded the state it recovered to.
      sequence = existing["check_sequence"].to_i + 1 if existing
      sequence ||= 1
      receipt = {
        "schema_version" => 1,
        "id" => check_id,
        "source" => "td",
        "source_instance" => @source_instance,
        "status" => status,
        "observed_at" => now,
        "check_sequence" => sequence,
        "new_triggers" => keys.length,
        "fingerprint" => fingerprint,
        "error" => failure
      }.compact
      # Guarding on the sequence as well as the fingerprint is what makes two pollers observing the
      # same change race for one occurrence rather than both claiming it. A receipt written before
      # this field existed has no `check_sequence`, and a nil guard matches that absence.
      expect = if existing
                 [{ collection: CHECKS, id: check_id,
                    fields: { fingerprint: existing["fingerprint"], check_sequence: existing["check_sequence"] } }]
               else
                 [{ collection: CHECKS, id: check_id, revision: nil }]
               end
      @store.commit([[CHECKS, receipt]], expect: expect, activity: [check_event(receipt, keys)])
      receipt
    rescue Backstage::ActivityConflictError
      # Not another poller: this occurrence's event id already names a different fact, which means
      # the identity above is wrong. Swallowing it is how a receipt gets stuck at a stale status
      # with no history explaining why, so it is raised rather than reported as a lost race.
      raise
    rescue Backstage::ConflictError
      # Another poller recorded this check first. There is one receipt per source; adopt theirs.
      nil
    end

    def check_event(receipt, keys)
      recorder.event(
        type: "source.checked",
        # Occurrence, not state: the sequence is what distinguishes a recovery from the first time
        # this source was healthy. The fingerprint stays in the id so a retry of the *same*
        # occurrence still reconciles instead of appending a second telling.
        event_id: ActivityRecorder.event_id("source.checked", check_id,
                                            receipt.fetch("check_sequence"), receipt.fetch("fingerprint")),
        provenance: "external_observation",
        offset: "#{receipt.fetch("check_sequence")}:#{receipt.fetch("fingerprint")}",
        occurred_at: receipt.fetch("observed_at"),
        correlation_id: check_id,
        summary: "td source #{@source_instance} checked: #{receipt.fetch("status")}, #{keys.length} new trigger(s)",
        data: receipt.slice("source", "source_instance", "status", "check_sequence", "new_triggers", "error")
      )
    end

    def recorder
      @recorder ||= ActivityRecorder.new(store: @store, adapter: "backstage.adapters.td.trigger")
    end

    def persist_trigger(trigger)
      existing = @store.find("triggers", idempotency_key: trigger.fetch("idempotency_key"))
      return nil if existing

      @store.save("triggers", trigger)
    end

    def public_issue(issue)
      issue.slice("id", "title", "description", "acceptance", "status", "type", "priority", "labels", "created_at", "updated_at")
    end
  end
end

Backstage::TdTrigger = Backstage::Adapters::Td::Trigger unless defined?(Backstage::TdTrigger)
