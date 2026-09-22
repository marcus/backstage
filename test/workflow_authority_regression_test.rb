# frozen_string_literal: true
require_relative "test_helper"

class WorkflowAuthorityRegressionTest < Minitest::Test
  def live_run(engine, work)
    job = dispatch(engine, work, request_id: "start:#{work.fetch('id')}")
    run = Backstage::Domain::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), phase: "implementation", work_revision: job.fetch("work_revision")).merge("status" => "running")
    engine.store.save("runs", run)
    engine.store.save("jobs", job.merge("status" => "running", "run_id" => run.fetch("id")))
    run
  end

  def progress(service, work, run, **overrides)
    service.request_transition(**{
      work_item_id: work.fetch("id"), transition: "report_progress", request_id: "progress:#{run.fetch('id')}",
      actor: { "role" => "agent", "entry" => "worker_channel", "id" => run.fetch("id") }, run_id: run.fetch("id"), reason: "investigating"
    }.merge(overrides))
  end

  def test_request_identity_rejects_another_work_or_changed_payload
    in_tmpdir do |directory|
      engine = build_engine(directory)
      one = submit_work(engine, key: "one")
      two = submit_work(engine, key: "two")
      service = build_workflows(engine)
      args = { work_item_id: one.fetch("id"), transition: "start", actor: operator("system"), request_id: "shared", reason: "begin" }
      original = service.request_transition(**args)
      assert service.request_transition(**args).fetch("deduplicated")
      [args.merge(work_item_id: two.fetch("id")), args.merge(reason: "different"), args.merge(actor: operator)].each do |changed|
        assert_raises(Backstage::ContractError) { service.request_transition(**changed) }
      end
      assert_equal "ready", engine.show_work(two.fetch("id")).fetch("state")
      assert_equal original.dig("job", "id"), engine.store.list("jobs").first.fetch("id")
    end
  end

  def test_concurrent_same_request_has_one_transition_and_one_job
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      args = { work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "same-request" }
      results = 8.times.map { Thread.new { build_workflows(engine).request_transition(**args) } }.map(&:value)
      assert_equal 1, results.count { |row| !row.fetch("deduplicated") }
      assert_equal 1, engine.store.list("jobs").length
      assert_equal 1, engine.store.list("work_transitions").length
    end
  end

  def test_cancelled_run_cannot_report_progress_and_wrong_run_cannot_borrow_authority
    in_tmpdir do |directory|
      engine = build_engine(directory)
      one = submit_work(engine, key: "one")
      two = submit_work(engine, key: "two")
      run = live_run(engine, one)
      other = live_run(engine, two)
      service = build_workflows(engine)
      assert_raises(Backstage::AuthorityError) { progress(service, one, other) }
      engine.request_cancel(run.fetch("id"))
      assert_raises(Backstage::ConflictError) { progress(service, one, run) }
      assert_equal 1, service.history(one.fetch("id")).length
    end
  end

  def test_cancel_reactivate_without_new_dispatch_still_fences_old_run
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      run = live_run(engine, work)
      service = build_workflows(engine)
      service.request_transition(work_item_id: work.fetch("id"), transition: "cancel", actor: operator, request_id: "cancel")
      service.request_transition(work_item_id: work.fetch("id"), transition: "reactivate", actor: operator, request_id: "reactivate")
      assert_raises(Backstage::ConflictError) { progress(service, work, run) }
      assert_equal "ready", engine.show_work(work.fetch("id")).fetch("state")
    end
  end

  def test_human_decision_cannot_be_answered_by_system_even_if_cancel_is_requested
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      live_run(engine, work)
      service = build_workflows(engine)
      raised = service.request_transition(work_item_id: work.fetch("id"), transition: "escalate", actor: operator("system"), request_id: "ask")
      assert_raises(Backstage::AuthorityError) do
        service.request_transition(work_item_id: work.fetch("id"), transition: "cancel", actor: operator("system"), request_id: "answer", decision_id: raised.dig("decision", "id"))
      end
      assert_equal "open", engine.store.list("decisions").first.fetch("status")
    end
  end

  def test_snapshot_tampering_is_refused
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      snapshot = engine.store.fetch("workflow_snapshots", work.dig("workflow", "digest"))
      snapshot["definition"]["description"] = "tampered"
      engine.store.save("workflow_snapshots", snapshot)
      assert_raises(Backstage::ContractError) { build_workflows(engine).workflow_for(work) }
    end
  end
end
