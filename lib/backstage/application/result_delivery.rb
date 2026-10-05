# frozen_string_literal: true

require "digest"
require "json"
require "thread"

module Backstage::Application
  # Governed host-side source effects. Nothing in a native task document selects authority,
  # transport or operations. A trusted completion resolver proves independent candidate-bound
  # approval; the configured source adapter owns native formatting and credentials.
  class ResultDelivery
    Records = Backstage::Domain::Records
    COLLECTION = "external_actions"
    RECEIPT_BYTES = 4096

    def initialize(engine:, configuration:, adapter_factory:, completion_result:, ownership:)
      @store = engine.store
      @configuration = configuration
      @adapter_factory = adapter_factory
      @completion_result = completion_result
      @ownership = ownership
      @mutex = Mutex.new
    end

    def deliver(work_item_id:, operations:)
      with_ownership do
        work = @store.fetch!("work_items", work_item_id)
        source = work["source"]
        raise Backstage::ContractError, "manual work has no source for result delivery" unless source.is_a?(Hash)

        @configuration.validate_source_binding(source, target: work.fetch("target"))
        result = @completion_result.call(work)
        validate_completion!(result, work)
        adapter = @adapter_factory.call(source.fetch("connection"))
        requested = validate_operations!(operations, source, adapter)
        with_reservation(work) do |reserved|
          actions = []
          requested.each do |operation|
            action = deliver_operation(reserved, source, result, operation, adapter)
            actions << action
            break unless action["status"] == "succeeded"
          end
          status = if actions.all? { |action| action["status"] == "succeeded" }
                     "succeeded"
                   elsif actions.any? { |action| action["status"] == "succeeded" }
                     "partial"
                   else
                     "blocked"
                   end
          { "work_item_id" => work_item_id, "status" => status, "actions" => actions }
        end
      end
    end

    def describe(action_id)
      action = @store.fetch!(COLLECTION, action_id)
      raise Backstage::ContractError, "action is not source delivery" unless action["kind"] == "source_write"

      action
    end

    # This API is exposed only through the trusted host/operator entry point. It records a human
    # assertion rather than pretending a provider supplied evidence. A reason is mandatory.
    def resolve(action_id, applied:, reason:)
      raise Backstage::ContractError, "resolution reason is required" if reason.to_s.strip.empty?
      raise Backstage::ContractError, "resolution applied must be boolean" unless [true, false].include?(applied)

      with_ownership do
        action = describe(action_id)
        raise Backstage::ContractError, "only an ambiguous attempted action can be resolved" unless action["status"] == "unknown" && action["attempted"]

        resolutions = Array(action["resolutions"]) + [{
          "applied" => applied, "reason" => reason, "recorded_at" => Records.timestamp,
          "actor" => { "role" => "human", "entry" => "operator_cli", "id" => "host_operator" }
        }]
        save(action.merge("status" => applied ? "succeeded" : "pending",
                          "attempted" => applied, "resolutions" => resolutions,
                          "response" => { "operator_resolution" => applied ? "applied" : "not_applied" }))
      end
    end

    private

    def with_ownership
      raise Backstage::ConflictError, "source delivery is already owned" unless @mutex.try_lock

      acquired = false
      begin
        # held? matters when another component incorrectly shares this ownership object: acquiring
        # an already-held lease must not let us release that component's lock.
        raise Backstage::ConflictError, "source delivery is already owned" if @ownership.held?

        acquired = !!@ownership.acquire("purpose" => "source_delivery")
        raise Backstage::ConflictError, "source delivery is already owned" unless acquired

        yield
      ensure
        @ownership.release if acquired
        @mutex.unlock
      end
    end

    # The file lock proves any old marker is abandoned. The work-row guard fences concurrent
    # refresh/transitions between the completion read and reservation; their own commits require
    # an absent owner. Reservation does not create a workflow transition or bump its revision.
    def with_reservation(work)
      raise Backstage::ConflictError, "work item has been refreshed" if work["refreshed_to"]

      owner = Records.id("delivery")
      reserved = work.merge("source_delivery_owner" => owner, "updated_at" => Records.timestamp)
      @store.commit([["work_items", reserved]], expect: [{ collection: "work_items", id: work.fetch("id"),
        revision: work.fetch("revision"), fields: { refreshed_to: nil, source_delivery_owner: work["source_delivery_owner"] } }])
      begin
        yield reserved
      ensure
        release_reservation(work.fetch("id"), owner)
      end
    end

    def release_reservation(work_item_id, owner)
      # Never restore the pre-delivery row: a concurrent record update must survive cleanup.
      3.times do
        current = @store.fetch!("work_items", work_item_id)
        return unless current["source_delivery_owner"] == owner

        begin
          @store.commit([["work_items", current.merge("source_delivery_owner" => nil)]], expect: [{
            collection: "work_items", id: work_item_id, revision: current.fetch("revision"),
            fields: { source_delivery_owner: owner }
          }])
          return
        rescue Backstage::ConflictError
          # A guarded retry adopts the current row without changing its workflow position.
        end
      end
      raise Backstage::ConflictError, "could not release source delivery reservation"
    end

    def validate_current_completion!(reserved, result)
      current = @store.fetch!("work_items", reserved.fetch("id"))
      unless !current["refreshed_to"] && current["revision"] == reserved["revision"] &&
             current["source_delivery_owner"] == reserved["source_delivery_owner"]
        raise Backstage::ConflictError, "source delivery completion changed"
      end
      current_result = @completion_result.call(current)
      validate_completion!(current_result, current)
      unless current_result == result
        raise Backstage::ConflictError, "source delivery completion changed"
      end
    end

    def validate_completion!(result, work)
      raise Backstage::ConflictError, "work item has been refreshed" if work["refreshed_to"]

      valid = result.is_a?(Hash) && result["review"].is_a?(Hash) && result["candidate"].is_a?(Hash) &&
              result["work_revision"] == work["revision"] &&
              result.dig("review", "verdict") == "approved" &&
              !result.dig("review", "reviewer_session_id").to_s.empty? &&
              result.dig("candidate", "sha256").to_s.match?(/\A[0-9a-f]{64}\z/) &&
              !result.dig("candidate", "artifact_id").to_s.empty?
      raise Backstage::ContractError, "result delivery requires completed independent candidate-bound approval" unless valid
    end

    def validate_operations!(operations, source, adapter)
      requested = Array(operations).uniq
      raise Backstage::ContractError, "result delivery needs explicit operation names" if requested.empty? || requested.any? { |item| !item.is_a?(String) || item.empty? }

      permitted = @configuration.sources.fetch(source.fetch("connection")).fetch("operations")
      requested.each do |operation|
        raise Backstage::ContractError, "source operation is not permitted: #{operation}" unless permitted.include?(operation)
        raise Backstage::ContractError, "source operation is unsupported: #{operation}" unless adapter.capabilities.include?(operation)
      end
      requested
    end

    def deliver_operation(work, source, result, operation, adapter)
      validate_current_completion!(work, result)
      key = "source-write:v1:#{Digest::SHA256.hexdigest(JSON.generate([source, work.fetch("id"), result.fetch("candidate"), result.fetch("work_revision"), operation]))}"
      action = @store.find(COLLECTION, idempotency_key: key)
      unless action
        payload = immutable(adapter.prepare(operation: operation, ref: source.fetch("ref"), result: immutable(result), operation_id: key))
        action = Records.external_action(work_item_id: work.fetch("id"), kind: "source_write", idempotency_key: key, status: "pending").merge(
          "source" => immutable(source), "operation" => operation, "operation_id" => key,
          "candidate" => immutable(result.fetch("candidate")), "work_revision" => work.fetch("revision"),
          "payload" => payload, "payload_sha256" => Digest::SHA256.hexdigest(JSON.generate(payload)), "attempted" => false
        )
        action = save(action)
      end
      return action if action["status"] == "succeeded"

      args = { operation: action.fetch("operation"), ref: action.fetch("source").fetch("ref"),
               payload: immutable(action.fetch("payload")), operation_id: action.fetch("operation_id") }
      begin
        observation = adapter.reconcile(**args, attempted: action.fetch("attempted"))
        status, receipt = observation!(observation, allowed: %w[applied not_applied unknown])
        validate_current_completion!(work, result)
        case status
        when "applied"
          return save(action.merge("status" => "succeeded", "response" => receipt))
        when "unknown"
          return save(action.merge("status" => "unknown", "response" => receipt))
        end
        # Persist uncertainty before crossing the effect boundary. A crash, timeout or exception
        # from here on must reconcile; the next invocation cannot silently try again.
        action = save(action.merge("status" => "pending", "attempted" => true, "response" => receipt))
        validate_current_completion!(work, result)
        status, receipt = observation!(adapter.execute(**args), allowed: %w[applied unknown])
        save(action.merge("status" => status == "applied" ? "succeeded" : "unknown", "response" => receipt))
      rescue StandardError => error
        # Transport error text may contain native content or credentials. The class is enough for
        # a durable bounded failure record; use source-specific host diagnostics for detail.
        save(action.merge("status" => action["attempted"] ? "unknown" : "pending",
                          "response" => { "error_class" => error.class.name }))
      end
    end

    def observation!(observation, allowed:)
      unless observation.is_a?(Hash) && allowed.include?(observation["status"])
        raise Backstage::ContractError, "invalid source operation observation"
      end
      raw_receipt = observation.fetch("receipt", {})
      raise Backstage::ContractError, "source operation receipt must be an object" unless raw_receipt.is_a?(Hash)

      receipt = immutable(raw_receipt)
      raise Backstage::ContractError, "source operation receipt exceeds #{RECEIPT_BYTES} bytes" if JSON.generate(receipt).bytesize > RECEIPT_BYTES

      [observation.fetch("status"), receipt]
    end

    def save(action)
      @store.save(COLLECTION, action.merge("updated_at" => Records.timestamp))
    end

    def immutable(value)
      JSON.parse(JSON.generate(value)).then { |copy| freeze_tree(copy) }
    end

    def freeze_tree(value)
      value.each { |key, child| key.freeze; freeze_tree(child) } if value.is_a?(Hash)
      value.each { |child| freeze_tree(child) } if value.is_a?(Array)
      value.freeze
    end
  end
end
