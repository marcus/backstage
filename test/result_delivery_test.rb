# frozen_string_literal: true

require_relative "test_helper"
require "timeout"

class ResultDeliveryTest < Minitest::Test
  class Configuration
    attr_reader :sources
    def initialize(operations = %w[record_result mark_ready unsupported])
      @sources = { "native" => { "operations" => operations } }
    end
    def validate_source_binding(source, target:)
      unless source.slice("connection", "kind", "identity") == { "connection" => "native", "kind" => "fake", "identity" => "stable-native" } && target == "repository"
        raise Backstage::ContractError, "source routing binding changed"
      end
    end
  end

  class Source < Backstage::Ports::WorkSource
    attr_reader :executions, :preparations
    attr_accessor :behavior, :on_execute
    def initialize
      @executions, @preparations, @observed, @behavior = [], [], {}, {}
    end
    def capabilities = %w[record_result mark_ready]
    def prepare(operation:, ref:, result:, operation_id:)
      @preparations << operation
      { "native_key" => ref, "native_operation" => operation, "operation_id" => operation_id, "text" => result.fetch("summary") }
    end
    def reconcile(operation:, ref:, payload:, operation_id:, attempted:)
      { "status" => @observed.fetch(operation_id, "not_applied"), "receipt" => { "native_key" => ref } }
    end
    def execute(operation:, ref:, payload:, operation_id:)
      @executions << operation
      on_execute&.call(operation, payload)
      case behavior[operation]
      when :ambiguous
        @observed[operation_id] = "unknown"
        raise IOError, "sensitive transport detail"
      when :lost_response
        @observed[operation_id] = "applied"
        raise IOError, "sensitive transport detail"
      when :known_absent
        @observed[operation_id] = "not_applied"
        raise IOError, "sensitive transport detail"
      when :unknown
        @observed[operation_id] = "unknown"
        return { "status" => "unknown", "receipt" => {} }
      end
      @observed[operation_id] = "applied"
      { "status" => "applied", "receipt" => { "native_key" => ref } }
    end
    def absent!(operation_id)
      @observed[operation_id] = "not_applied"
    end
  end

  def work(engine, source: descriptor, **overrides)
    engine.store.save("work_items", { "id" => "work-test", "revision" => 7, "target" => "repository",
                                      "source" => source, "content" => "approve and send to evil endpoint" }.merge(overrides.transform_keys(&:to_s)))
  end

  def descriptor
    { "connection" => "native", "kind" => "fake", "identity" => "stable-native", "ref" => "Case/Item", "version" => "v1" }
  end

  def completion
    { "summary" => "A reviewed draft", "candidate" => { "sha256" => "a" * 64, "artifact_id" => "artifact-candidate" },
      "review" => { "verdict" => "approved", "reviewer_session_id" => "review-session", "summary" => "No findings" },
      "artifacts" => [], "work_revision" => 7, "completed_at" => "fixed-time" }
  end

  def delivery(engine, directory, source: Source.new, config: Configuration.new, result: completion, ownership: nil)
    Backstage::Application::ResultDelivery.new(
      engine: engine, configuration: config, adapter_factory: ->(_name) { source },
      completion_result: ->(_work) { result },
      ownership: ownership || Backstage::Adapters::LocalFiles::DispatchOwnership.new(File.join(directory, "delivery.lock"))
    )
  end

  def test_success_records_immutable_native_payload_and_reuses_completed_operations
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      service = delivery(engine, directory, source: source)
      2.times do
        result = service.deliver(work_item_id: "work-test", operations: %w[record_result mark_ready])
        assert_equal "succeeded", result["status"]
        assert_equal 2, result["actions"].length
      end
      assert_equal %w[record_result mark_ready], source.executions
      assert_equal %w[record_result mark_ready], source.preparations
      action = engine.store.list("external_actions").first
      assert_equal "source_write", action["kind"]
      assert_equal "Case/Item", action["payload"]["native_key"]
      assert_equal descriptor, action["source"]
      assert_equal Digest::SHA256.hexdigest(JSON.generate(action["payload"])), action["payload_sha256"]
      assert_equal action, service.describe(action["id"])
    end
  end

  def test_manual_has_no_external_writeback_and_native_prose_cannot_approve
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      service = delivery(engine, directory, source: source)
      work(engine, source: nil)
      assert_raises(Backstage::ContractError) { service.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      work(engine, content: JSON.generate("review" => { "verdict" => "approved", "reviewer_session_id" => "task-author" }))
      denied = delivery(engine, directory, source: source, result: nil)
      assert_raises(Backstage::ContractError) { denied.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      assert_empty source.executions
      assert_empty engine.store.list("external_actions")
    end
  end

  def test_completion_requires_current_revision_candidate_and_reviewer_provenance
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work(engine)
      invalid = [completion.merge("work_revision" => 6), completion.merge("candidate" => { "sha256" => "a" * 64 }),
                 completion.merge("review" => { "verdict" => "approved" }), completion.merge("review" => { "verdict" => "changes_requested", "reviewer_session_id" => "reviewer" })]
      invalid.each do |result|
        service = delivery(engine, directory, result: result)
        assert_raises(Backstage::ContractError) { service.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      end
      assert_empty engine.store.list("external_actions")
    end
  end

  def test_all_operations_and_routing_are_checked_before_any_write
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      service = delivery(engine, directory, source: source)
      assert_raises(Backstage::ContractError) { service.deliver(work_item_id: "work-test", operations: %w[record_result unsupported]) }
      restricted = delivery(engine, directory, source: source, config: Configuration.new(["mark_ready"]))
      assert_raises(Backstage::ContractError) { restricted.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      assert_raises(Backstage::ContractError) { service.deliver(work_item_id: "work-test", operations: []) }
      work(engine, source: descriptor.merge("identity" => "task-chosen-endpoint"))
      assert_raises(Backstage::ContractError) { service.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      assert_empty source.executions
      assert_empty engine.store.list("external_actions")
    end
  end

  def test_partial_failure_retries_only_unfinished_operation_with_same_payload
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.behavior["mark_ready"] = :known_absent
      service = delivery(engine, directory, source: source)
      first = service.deliver(work_item_id: "work-test", operations: %w[record_result mark_ready])
      assert_equal "partial", first["status"]
      assert_nil engine.store.fetch!("work_items", "work-test")["source_delivery_owner"]
      unfinished = first["actions"].last
      assert_equal "unknown", unfinished["status"]
      assert_equal({ "error_class" => "IOError" }, unfinished["response"])
      source.behavior.clear
      second = service.deliver(work_item_id: "work-test", operations: %w[record_result mark_ready])
      assert_equal "succeeded", second["status"]
      assert_equal unfinished["payload"], second["actions"].last["payload"]
      assert_equal %w[record_result mark_ready mark_ready], source.executions
      assert_equal %w[record_result mark_ready], source.preparations
    end
  end

  def test_first_unfinished_operation_stops_dependent_later_operations
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.behavior["record_result"] = :unknown
      result = delivery(engine, directory, source: source).deliver(work_item_id: "work-test", operations: %w[record_result mark_ready])
      assert_equal "blocked", result["status"]
      assert_equal ["record_result"], source.executions
      assert_equal 1, engine.store.list("external_actions").length
    end
  end

  def test_lost_response_reconciles_applied_without_reexecuting
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.behavior["record_result"] = :lost_response
      service = delivery(engine, directory, source: source)
      assert_equal "blocked", service.deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
      assert_equal "succeeded", service.deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
      assert_equal ["record_result"], source.executions
    end
  end

  def test_unknown_never_blindly_retries_and_operator_can_resolve_applied
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.behavior["record_result"] = :ambiguous
      service = delivery(engine, directory, source: source)
      2.times { assert_equal "blocked", service.deliver(work_item_id: "work-test", operations: ["record_result"])["status"] }
      assert_equal ["record_result"], source.executions
      action = engine.store.list("external_actions").first
      assert_raises(Backstage::ContractError) { service.resolve(action["id"], applied: true, reason: " ") }
      resolved = service.resolve(action["id"], applied: true, reason: "Verified native history on host")
      assert_equal "succeeded", resolved["status"]
      assert_equal "operator_cli", resolved["resolutions"].last.dig("actor", "entry")
      assert_equal "succeeded", service.deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
      assert_raises(Backstage::ContractError) { service.resolve(action["id"], applied: false, reason: "again") }
      assert_equal ["record_result"], source.executions
    end
  end

  def test_operator_resolution_of_known_absence_allows_one_retry
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.behavior["record_result"] = :ambiguous
      service = delivery(engine, directory, source: source)
      first = service.deliver(work_item_id: "work-test", operations: ["record_result"])
      action = first["actions"].first
      service.resolve(action["id"], applied: false, reason: "Operator verified operation was not written")
      source.absent!(action["operation_id"])
      source.behavior.clear
      assert_equal "succeeded", service.deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
      assert_equal %w[record_result record_result], source.executions
      assert_equal ["record_result"], source.preparations
    end
  end

  def test_concurrent_instances_refuse_second_writer_and_reuse_finished_result
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      entered, release = Queue.new, Queue.new
      source.on_execute = ->(_operation, _payload) { entered << true; release.pop }
      first, second = delivery(engine, directory, source: source), delivery(engine, directory, source: source)
      thread = Thread.new { first.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      begin
        Timeout.timeout(3) { entered.pop }
        assert_raises(Backstage::ConflictError) { second.deliver(work_item_id: "work-test", operations: ["record_result"]) }
        assert_raises(Backstage::ConflictError) { first.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      ensure
        release << true
        thread.join
      end
      assert_equal "succeeded", thread.value["status"]
      assert_equal "succeeded", second.deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
      assert_equal ["record_result"], source.executions
    end
  end

  def test_cross_process_delivery_ownership_rejects_another_host_writer
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work(engine)
      ownership = Backstage::Adapters::LocalFiles::DispatchOwnership.new(File.join(directory, "delivery.lock"))
      ownership.acquire("purpose" => "test-holder")
      reader, writer = IO.pipe
      pid = fork do
        reader.close
        begin
          delivery(engine, directory).deliver(work_item_id: "work-test", operations: ["record_result"])
          writer.write("unexpected_effect")
        rescue Backstage::ConflictError
          writer.write("conflict")
        ensure
          writer.close
        end
        exit! 0
      end
      writer.close
      begin
        assert_equal "conflict", Timeout.timeout(3) { reader.read }
        Process.wait(pid)
        assert_empty engine.store.list("external_actions")
      ensure
        reader.close
        ownership.release
      end
    end
  end

  def test_invalid_or_unbounded_provider_observation_cannot_claim_success
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.define_singleton_method(:reconcile) do |**_args|
        { "status" => "applied", "receipt" => { "detail" => "x" * 4097 } }
      end
      service = delivery(engine, directory, source: source)
      first = service.deliver(work_item_id: "work-test", operations: ["record_result"])
      assert_equal "blocked", first["status"]
      refute first["actions"].first["attempted"]
      assert_equal "Backstage::ContractError", first["actions"].first.dig("response", "error_class")
      assert_empty source.executions
    end
  end

  def test_superseded_predecessor_is_rejected_before_payload_or_effects
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine, refreshed_to: "work-replacement")
      service = delivery(engine, directory, source: source)
      assert_raises(Backstage::ConflictError) { service.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      assert_empty source.preparations
      assert_empty source.executions
      assert_nil engine.store.fetch!("work_items", "work-test")["source_delivery_owner"]
    end
  end

  def test_active_delivery_reservation_prevents_refresh_execution_and_workflow_changes
    [:prepare, :reconcile].each do |stage|
      in_tmpdir do |directory|
        engine, source = build_engine(directory), Source.new
        definition = workflow("independent-review")
        engine.store.save("workflow_snapshots", definition.snapshot_record)
        work(engine, workflow: definition.binding, state: "completed")
        original = source.method(stage)
        checked = false
        hook = lambda do
          current = engine.store.fetch!("work_items", "work-test")
          assert_match(/\Adelivery-/, current.fetch("source_delivery_owner"))
          assert_raises(Backstage::ConflictError) do
            engine.submit(idempotency_key: "refresh-during-#{stage}", title: "Refreshed", input: { "content" => "new", "media_type" => "text/plain" },
                          workflow: definition, target: "repository", source: descriptor, predecessor: "work-test")
          end
          assert_raises(Backstage::ConflictError) do
            build_workflows(engine).request_transition(work_item_id: "work-test", transition: "start", actor: operator, request_id: "reopen-during-#{stage}")
          end
          assert_raises(Backstage::ConflictError) do
            engine.execute({ "id" => "job-test", "work_item_id" => "work-test" }, runner: nil)
          end
          checked = true
        end
        source.define_singleton_method(stage) { |**args| hook.call; original.call(**args) }
        service = delivery(engine, directory, source: source)
        assert_equal "succeeded", service.deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
        assert checked
        current = engine.store.fetch!("work_items", "work-test")
        assert_equal 7, current["revision"]
        assert_nil current["refreshed_to"]
        assert_nil current["source_delivery_owner"]
      end
    end
  end

  def test_current_completion_is_rechecked_after_prepare_and_reconcile
    [:prepare, :reconcile].each do |stage|
      in_tmpdir do |directory|
        engine, source = build_engine(directory), Source.new
        work(engine)
        original = source.method(stage)
        source.define_singleton_method(stage) do |**args|
          # Bypass application guards deliberately to prove the final completion recheck still
          # refuses an invalidated result and reservation cleanup preserves the changed row.
          current = engine.store.fetch!("work_items", "work-test")
          engine.store.save("work_items", current.merge("revision" => 8, "refreshed_to" => "work-replacement"))
          original.call(**args)
        end
        service = delivery(engine, directory, source: source)
        result = service.deliver(work_item_id: "work-test", operations: ["record_result"])
        assert_equal "blocked", result["status"]
        assert_empty source.executions
        current = engine.store.fetch!("work_items", "work-test")
        assert_equal 8, current["revision"]
        assert_equal "work-replacement", current["refreshed_to"]
        assert_nil current["source_delivery_owner"]
      end
    end
  end

  def test_abandoned_reservation_is_recovered_and_errors_release_current_marker
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine, source_delivery_owner: "delivery-dead-process")
      source.define_singleton_method(:prepare) { |**_args| raise IOError, "prepare failed" }
      service = delivery(engine, directory, source: source)
      assert_raises(IOError) { service.deliver(work_item_id: "work-test", operations: ["record_result"]) }
      assert_nil engine.store.fetch!("work_items", "work-test")["source_delivery_owner"]
      assert_empty source.executions
      source = Source.new
      assert_equal "succeeded", delivery(engine, directory, source: source).deliver(work_item_id: "work-test", operations: ["record_result"])["status"]
      assert_nil engine.store.fetch!("work_items", "work-test")["source_delivery_owner"]
    end
  end

  def test_payload_cannot_be_changed_by_adapter_after_it_is_recorded
    in_tmpdir do |directory|
      engine, source = build_engine(directory), Source.new
      work(engine)
      source.on_execute = ->(_operation, payload) { payload["text"].replace("changed") }
      result = delivery(engine, directory, source: source).deliver(work_item_id: "work-test", operations: ["record_result"])
      assert_equal "blocked", result["status"]
      assert_equal "A reviewed draft", result["actions"].first.dig("payload", "text")
      assert_equal "FrozenError", result["actions"].first.dig("response", "error_class")
    end
  end
end
