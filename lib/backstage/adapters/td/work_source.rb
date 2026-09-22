# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Adapters::Td
  class WorkSource
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    def initialize(client:, store:, engine:, source_instance:, workflow:, target_name: nil, source_identity: nil)
      @client = client
      @store = store
      @engine = engine
      @source_instance = source_instance
      @target_name = target_name
      @source_identity = source_identity || client.workspace
      @workflow = workflow
    end

    def reconcile(issue_id)
      issue = @client.show(issue_id)
      @engine.submit(
        idempotency_key: work_key(issue.fetch("id")),
        title: issue.fetch("title"),
        description: [issue["description"], issue["acceptance"]].compact.reject(&:empty?).join("\n\nAcceptance:\n"),
        source: "td",
        source_ref: { "issue_id" => issue.fetch("id").downcase, "source_instance" => @source_instance },
        workflow: @workflow,
        target: @target_name,
        source_instance: @source_instance,
        source_identity: File.expand_path(@source_identity)
      )
    end

    def post_handoff(work_item:, done:, remaining:, decisions:)
      issue_id = issue_id(work_item)
      canonical = { "done" => done, "remaining" => remaining, "decisions" => Array(decisions) }
      key = "td-handoff:v1:#{work_item.fetch("idempotency_key")}:#{Digest::SHA256.hexdigest(JSON.generate(canonical))}"
      return completed_action(key) if completed_action(key)

      issue = @client.show(issue_id)
      current = issue["handoff"]
      if current && current["done"] == done && current["remaining"] == remaining && Array(current["decisions"]) == Array(decisions)
        return record_action(work_item, "td_handoff", key, current)
      end

      pending_action(work_item, "td_handoff", key)
      response = @client.handoff(issue_id, done: done, remaining: remaining, decisions: decisions)
      record_action(work_item, "td_handoff", key, response)
    end

    def request_review(work_item:, reason:)
      issue_id = issue_id(work_item)
      key = "td-review:v1:#{work_item.fetch("idempotency_key")}"
      return completed_action(key) if completed_action(key)

      issue = @client.show(issue_id)
      return record_action(work_item, "td_review", key, { "status" => issue["status"], "reconciled" => true }) if %w[in_review closed].include?(issue["status"])

      pending_action(work_item, "td_review", key)
      response = @client.review(issue_id, reason: reason)
      fetched = @client.show(issue_id)
      raise ContractError, "td review did not reach in_review" unless %w[in_review closed].include?(fetched["status"])

      record_action(work_item, "td_review", key, response.merge("observed_status" => fetched["status"]))
    rescue ContractError
      fetched = @client.show(issue_id)
      return record_action(work_item, "td_review", key, { "status" => fetched["status"], "reconciled_after_error" => true }) if %w[in_review closed].include?(fetched["status"])

      raise
    end

    private

    def work_key(issue_id)
      "work:v1:td:#{@source_instance}:#{issue_id.to_s.downcase}"
    end

    def issue_id(work_item)
      ref = work_item.fetch("source_ref")
      ref.is_a?(Hash) ? ref.fetch("issue_id") : ref
    end

    def completed_action(key)
      action = @store.find("external_actions", idempotency_key: key)
      action && action["status"] == "succeeded" ? action["response"] : nil
    end

    def pending_action(work_item, kind, key)
      existing = @store.find("external_actions", idempotency_key: key)
      return existing if existing

      @store.save("external_actions", Records.external_action(work_item_id: work_item.fetch("id"), kind: kind, idempotency_key: key, status: "pending"))
    end

    def record_action(work_item, kind, key, response)
      action = @store.find("external_actions", idempotency_key: key) || Records.external_action(work_item_id: work_item.fetch("id"), kind: kind, idempotency_key: key, status: "pending")
      @store.save("external_actions", action.merge("status" => "succeeded", "response" => response, "updated_at" => Records.timestamp))
      response
    end
  end
end

Backstage::TdWorkSource = Backstage::Adapters::Td::WorkSource unless defined?(Backstage::TdWorkSource)
