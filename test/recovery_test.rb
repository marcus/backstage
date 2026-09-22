# frozen_string_literal: true

require_relative "test_helper"

# Recovery is only allowed to act on what was persisted and what is provably alive or gone.
class RecoveryTest < Minitest::Test
  class StubPresence < Backstage::Ports::RuntimePresence
    def initialize(answer) = @answer = answer
    def status(_runtime) = @answer
  end

  def setup_work(directory, workflow_name: "independent-review", presence: StubPresence.new("unknown"))
    engine = build_engine(directory)
    workflows = build_workflows(engine)
    work = submit_work(engine, workflow_name: workflow_name)
    recovery = Backstage::Application::Recovery.new(engine: engine, workflows: workflows, presence: presence)
    [engine, workflows, work, recovery]
  end

  # Dispatches implementation and puts a run in flight without finishing it.
  def in_flight(engine, workflows, work, runtime: nil, owner_pid: Process.pid)
    job = workflows.request_transition(
      work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1"
    ).fetch("job")
    job = engine.store.save("jobs", job.merge("status" => "running"))
    run = engine.store.save("runs", Backstage::Domain::Records.run(job_id: job.fetch("id"), phase: "implementation", work_item_id: work.fetch("id"), work_revision: job.fetch("work_revision")).merge(
      "status" => "running", "owner_pid" => owner_pid, "runtime" => runtime
    ).compact)
    [job, run]
  end

  def findings(report) = report.fetch("work_items").flat_map { |row| row.fetch("findings") }

  def test_a_live_worker_is_left_alone
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory, presence: StubPresence.new("alive"))
      _job, run = in_flight(engine, workflows, work, runtime: { "container_name" => "backstage-x" })

      finding = findings(recovery.reconcile).find { |row| row.fetch("kind") == "worker_active" }

      assert_equal run.fetch("id"), finding.fetch("run_id")
      assert_equal "running", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
      assert_equal "running", engine.store.fetch("runs", run.fetch("id")).fetch("status")
      assert_equal 1, engine.store.list("runs").length, "no replacement was launched"
    end
  end

  def test_unknown_runtime_status_stays_unknown
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory, presence: StubPresence.new("unknown"))
      in_flight(engine, workflows, work, runtime: { "container_name" => "backstage-x" })

      finding = findings(recovery.reconcile).find { |row| row.fetch("kind") == "runtime_unknown" }

      refute_nil finding
      assert_match(/resolve it explicitly/, finding.fetch("detail"))
      assert_equal "running", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
    end
  end

  def test_a_dead_worker_without_an_outcome_is_recorded_as_interrupted_and_stays_continuable
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory, presence: StubPresence.new("gone"))
      _job, run = in_flight(engine, workflows, work, runtime: { "container_name" => "backstage-x" }, owner_pid: 99999999)
      artifact = write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch", run_id: run.fetch("id"))

      finding = findings(recovery.reconcile).find { |row| row.fetch("kind") == "worker_interrupted" }

      assert_equal "transitioned", finding.fetch("action")
      assert_equal "execution_failed", finding.fetch("transition")
      assert_equal "ready", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
      assert_equal "interrupted", engine.store.fetch("runs", run.fetch("id")).fetch("status")
      assert engine.store.fetch("artifacts", artifact.fetch("id")), "artifacts are preserved"
      refute engine.store.fetch("runs", run.fetch("id")).dig("outcome", "status") == "succeeded"
    end
  end

  def test_an_outcome_persisted_before_its_transition_is_reconciled_not_invented
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory)
      _job, run = in_flight(engine, workflows, work)
      artifact = write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch", run_id: run.fetch("id"))
      engine.store.save("runs", engine.store.fetch("runs", run.fetch("id")).merge(
        "status" => "succeeded",
        "outcome" => { "schema_version" => 1, "status" => "succeeded", "summary" => "finished before the crash", "process" => { "exit_code" => 0, "signal" => nil } }
      ))

      finding = findings(recovery.reconcile).find { |row| row.fetch("kind") == "outcome_recorded" }

      assert_equal "transitioned", finding.fetch("action")
      assert_equal "submit_for_review", finding.fetch("transition")
      assert_equal "awaiting_review", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
      recorded = workflows.history(work.fetch("id")).last
      assert_equal "agent", recorded.dig("actor", "role")
      assert_equal [artifact.fetch("id")], recorded.fetch("evidence")
    end
  end

  def test_reconciliation_is_idempotent
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory)
      _job, run = in_flight(engine, workflows, work)
      write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch", run_id: run.fetch("id"))
      engine.store.save("runs", engine.store.fetch("runs", run.fetch("id")).merge(
        "status" => "succeeded",
        "outcome" => { "schema_version" => 1, "status" => "succeeded", "summary" => "done", "process" => { "exit_code" => 0, "signal" => nil } }
      ))

      recovery.reconcile
      before = workflows.history(work.fetch("id")).length
      recovery.reconcile

      assert_equal before, workflows.history(work.fetch("id")).length
      assert_equal "awaiting_review", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
    end
  end

  def test_a_replaced_run_is_fenced_and_cannot_overwrite_the_later_dispatch
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory, presence: StubPresence.new("gone"))
      _stale_job, stale_run = in_flight(engine, workflows, work, runtime: { "container_name" => "old" })
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "execution_failed", actor: operator("system"), request_id: "fail-1")
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-2")
      engine.store.save("runs", engine.store.fetch("runs", stale_run.fetch("id")).merge(
        "status" => "succeeded",
        "outcome" => { "schema_version" => 1, "status" => "succeeded", "summary" => "late result from a replaced run", "process" => { "exit_code" => 0, "signal" => nil } }
      ))

      fenced = findings(recovery.reconcile).find { |row| row["action"] == "fenced" }

      assert_match(/was superseded by/, fenced.fetch("detail"))
      assert_equal "running", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
    end
  end

  def test_recovery_never_clears_a_wait_on_a_human
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory, workflow_name: "human-gated-change", presence: StubPresence.new("gone"))
      _job, run = in_flight(engine, workflows, work, runtime: { "container_name" => "gone" })
      patch = write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch", run_id: run.fetch("id"))
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "request_approval",
        actor: { "role" => "agent", "id" => run.fetch("id"), "entry" => "worker_channel" }, run_id: run.fetch("id"),
        request_id: "ask-1", evidence: [patch.fetch("id")]
      )

      report = recovery.reconcile.fetch("work_items").first

      assert_equal "awaiting_approval", report.fetch("state")
      assert_includes report.fetch("findings").map { |row| row.fetch("kind") }, "awaiting_decision"
      assert_equal "awaiting_approval", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
      assert_equal "open", engine.store.list("decisions").last.fetch("status")
    end
  end

  def test_a_human_wait_with_no_worker_reports_only_the_wait
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory)
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1")
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "escalate", actor: operator("system"), request_id: "escalate-1",
        decision: { "question" => "what now?" }
      )
      engine.store.list("jobs").each { |job| engine.store.save("jobs", job.merge("status" => "cancelled")) }

      report = recovery.reconcile(work.fetch("id")).fetch("work_items").first

      assert_equal %w[awaiting_decision], report.fetch("findings").map { |row| row.fetch("kind") }
      assert_equal "needs_decision", report.fetch("state")
    end
  end

  def test_a_queued_dispatch_is_reported_rather_than_launched
    in_tmpdir do |directory|
      engine, workflows, work, recovery = setup_work(directory)
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1")

      finding = findings(recovery.reconcile).find { |row| row.fetch("kind") == "dispatch_pending" }

      refute_nil finding
      assert_empty engine.store.list("runs")
    end
  end

  def test_terminal_work_is_not_reported
    in_tmpdir do |directory|
      _engine, workflows, work, recovery = setup_work(directory)
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "cancel", actor: operator, request_id: "cancel-1")

      assert_empty recovery.reconcile.fetch("work_items")
    end
  end
end
