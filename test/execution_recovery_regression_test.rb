# frozen_string_literal: true

require_relative "test_helper"

class ExecutionRecoveryRegressionTest < Minitest::Test
  class Runner
    def initialize(&action) = @action = action
    def run(bundle:, secrets: {}, cancellation: nil, capture: nil, &events) = @action.call(cancellation, events)
    def adapter_identifier = "Test::RecoveryRunner"
  end

  class Configuration
    def compile(work_item:)
      { "id" => Backstage::Records.id("bundle"), "work_item" => work_item,
        "repository" => { "branch" => "backstage/test", "revision" => "main" },
        "harness" => { "prompt" => "Implement the assignment" }, "policy" => {} }
    end
  end

  class Presence < Backstage::Ports::RuntimePresence
    def status(_runtime) = GONE
  end

  def outcome(status = "succeeded")
    { "schema_version" => 1, "status" => status, "summary" => "recorded result",
      "process" => { "exit_code" => 0, "signal" => nil } }
  end

  def controller(engine, &factory)
    Backstage::Application::Controller.new(engine: engine, workflows: build_workflows(engine),
      configuration: Configuration.new, runner_factory: factory, mode: "fake")
  end

  def recovery(engine)
    Backstage::Application::Recovery.new(engine: engine, workflows: build_workflows(engine), presence: Presence.new)
  end

  def test_a_dispatch_is_claimed_once_even_when_two_controllers_race
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      job = dispatch(engine, work)
      entered = Queue.new
      release = Queue.new
      first = Thread.new do
        engine.execute(job, runner: Runner.new { entered << true; release.pop; outcome })
      end
      entered.pop
      begin
        assert_raises(Backstage::ConflictError) do
          engine.execute(job, runner: Runner.new { flunk "duplicate dispatch ran" })
        end
        assert_equal 1, engine.store.list("runs").length
        assert_equal 1, engine.store.list("attempts").length
        assert_equal "running", engine.store.fetch("jobs", job.fetch("id")).fetch("status")
        result = controller(engine) { flunk "active work dispatched again" }.process(work_item_id: work.fetch("id"))
        assert_equal "execution_active", result.fetch("halt_reason")
      ensure
        release << true
        first.value
      end
    end
  end

  def test_recovery_applies_a_terminal_job_outcome_once_with_controller_identity
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      job = dispatch(engine, work)
      executed = engine.execute(job, runner: Runner.new { outcome })
      assert_equal "succeeded", executed.dig("job", "status")
      recovery(engine).reconcile(work.fetch("id"))
      assert_equal "done", engine.show_work(work.fetch("id")).fetch("state")
      history = build_workflows(engine).history(work.fetch("id"))
      assert_equal "job:#{job.fetch("id")}:finish", history.last.fetch("request_id")
      replay = build_workflows(engine).request_transition(work_item_id: work.fetch("id"), transition: "finish",
        actor: build_workflows(engine).outcome_actor(executed.fetch("run"), outcome),
        request_id: "job:#{job.fetch("id")}:finish", reason: outcome.fetch("summary"), evidence: [], run_id: executed.dig("run", "id"))
      assert_equal history.last.fetch("id"), replay.dig("transition", "id")
      assert_equal 2, build_workflows(engine).history(work.fetch("id")).length
    end
  end

  def test_runner_exceptions_are_recoverable_and_do_not_retry_in_the_same_process
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      calls = 0
      result = controller(engine) { calls += 1; Runner.new { raise "worker lost" } }.process(work_item_id: work.fetch("id"))
      assert_equal 1, calls
      assert_equal "new", result.fetch("state")
      assert_equal "execution_stopped", result.fetch("halt_reason")
      assert_equal "failed", engine.store.list("runs").last.fetch("status")
      assert_match(/worker lost/, engine.store.list("runs").last.dig("outcome", "summary"))
    end
  end

  def test_cancellation_survives_runtime_observation_and_a_reported_success
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      calls = 0
      result = controller(engine) do
        calls += 1
        Runner.new do |cancelled, events|
          run = engine.store.list("runs").last
          engine.request_cancel(run.fetch("id"))
          events.call("type" => "runtime_started", "container_name" => "test-container")
          assert cancelled.call
          outcome
        end
      end.process(work_item_id: work.fetch("id"))
      assert_equal 1, calls
      assert_equal "new", result.fetch("state")
      run = engine.store.list("runs").last
      assert run["cancellation_requested_at"]
      assert_equal "cancelled", run.fetch("status")
      assert_equal "cancelled", engine.store.list("attempts").last.fetch("status")
    end
  end

  def test_a_review_verdict_is_bound_to_the_candidate_at_claim_time
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      dispatch(engine, work)
      implementation = engine.store.list("jobs").last
      completed = engine.execute(implementation, runner: Runner.new { outcome })
      patch = write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch", run_id: completed.dig("run", "id"))
      result = build_workflows(engine).request_transition(work_item_id: work.fetch("id"), transition: "submit_for_review",
        actor: operator("system"), request_id: "candidate", evidence: [patch.fetch("id")])
      candidate = result.dig("work_item", "candidate")
      reviewed = engine.execute(result.fetch("job"), runner: Runner.new do
        current = engine.store.fetch!("work_items", work.fetch("id"))
        engine.store.save("work_items", current.merge("candidate" => candidate.merge("sha256" => "different")))
        outcome.merge("review" => { "verdict" => "approved", "summary" => "approved", "independent" => true, "reviewer_session_id" => "reviewer-test" })
      end)
      assert_equal candidate, reviewed.dig("run", "reviewed_candidate")
      assert_equal candidate.fetch("sha256"), reviewed.dig("review_verdict_artifact", "provenance", "candidate_sha256")
    end
  end

  def test_missing_container_with_live_controller_and_missing_identity_with_dead_controller_stay_unknown
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      job = dispatch(engine, work)
      run = Backstage::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), work_revision: job.fetch("work_revision"))
        .merge("status" => "running", "runtime" => { "container_name" => "missing" })
      engine.store.save("runs", run)
      engine.store.save("jobs", job.merge("status" => "running", "run_id" => run.fetch("id")))
      assert_equal "runtime_unknown", recovery(engine).reconcile.dig("work_items", 0, "findings", 0, "kind")
      engine.store.save("runs", run.except("runtime").merge("owner_pid" => 2_000_000_000))
      assert_equal "runtime_unknown", recovery(engine).reconcile.dig("work_items", 0, "findings", 0, "kind")
      assert_equal "running", engine.store.fetch!("runs", run.fetch("id")).fetch("status")
    end
  end

  def test_confirmed_interruption_finishes_attempt_and_is_recoverable
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      job = dispatch(engine, work)
      run = Backstage::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), work_revision: job.fetch("work_revision"))
        .merge("status" => "running", "owner_pid" => 2_000_000_000, "runtime" => { "container_name" => "missing" })
      attempt = Backstage::Records.attempt(run_id: run.fetch("id"), number: 1)
      engine.store.commit([["runs", run], ["jobs", job.merge("status" => "running", "run_id" => run.fetch("id"))], ["attempts", attempt]])
      report = recovery(engine).reconcile
      assert_equal "transitioned", report.dig("work_items", 0, "findings", 0, "action")
      assert_equal "interrupted", engine.store.fetch!("attempts", attempt.fetch("id")).fetch("status")
      assert_equal "new", engine.store.fetch!("work_items", work.fetch("id")).fetch("state")
    end
  end

  def test_cancelled_work_still_reconciles_a_provably_gone_worker
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      job = dispatch(engine, work)
      run = Backstage::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), work_revision: job.fetch("work_revision"))
        .merge("status" => "running", "owner_pid" => 2_000_000_000, "runtime" => { "container_name" => "missing" })
      engine.store.commit([["runs", run], ["jobs", job.merge("status" => "running", "run_id" => run.fetch("id"))]])
      build_workflows(engine).request_transition(work_item_id: work.fetch("id"), transition: "cancel", actor: operator, request_id: "cancel")
      report = recovery(engine).reconcile
      assert_equal "cancelled", report.dig("work_items", 0, "state")
      assert_equal "interrupted", engine.store.fetch!("runs", run.fetch("id")).fetch("status")
      refute engine.active_execution?(work.fetch("id"))
      assert_empty recovery(engine).reconcile.fetch("work_items")
    end
  end

  def test_dead_controller_before_guaranteed_runtime_identity_is_recoverable
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      job = dispatch(engine, work)
      run = Backstage::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), work_revision: job.fetch("work_revision"))
        .merge("status" => "running", "owner_pid" => 2_000_000_000, "runtime_identity_before_launch" => true)
      engine.store.commit([["runs", run], ["jobs", job.merge("status" => "running", "run_id" => run.fetch("id"))]])
      report = recovery(engine).reconcile
      assert_equal "new", report.dig("work_items", 0, "state")
      assert_equal "interrupted", engine.store.fetch!("runs", run.fetch("id")).fetch("status")
      refute engine.active_execution?(work.fetch("id"))
    end
  end

  def test_container_runner_records_prelaunch_guarantee_before_a_controller_crash
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: "minimal")
      job = dispatch(engine, work)
      crash = Class.new(Exception)
      broker = Object.new
      broker.define_singleton_method(:runtime_environment) { |_refs| raise crash, "controller died before launch" }
      runtime = Backstage::Adapters::Docker::Runtime.new(docker: "/must-not-launch")
      harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
      runner = Backstage::Application::Runners::ContainerPhaseRunner.new(phase: "implementation", runtime: runtime,
        harness: harness, credential_broker: broker, workspace_root: directory)
      assert runner.runtime_identity_before_launch?
      assert_raises(crash) { engine.execute(job, runner: runner, bundle: { "id" => "prelaunch", "execution" => {} }) }
      run = engine.store.list("runs").first
      assert run.fetch("runtime_identity_before_launch")
      assert_nil run["runtime"]
      engine.store.save("runs", run.merge("owner_pid" => 2_000_000_000))
      assert_equal "new", recovery(engine).reconcile.dig("work_items", 0, "state")
      refute engine.active_execution?(work.fetch("id"))
    end
  end

  def test_exhausted_continue_budget_does_not_fall_back_to_start
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      engine.store.save("work_items", work.merge("state" => "changes_requested", "revisions_used" => 2))
      result = controller(engine) { flunk "exhausted work dispatched" }.process(work_item_id: work.fetch("id"))
      assert_equal "revision_budget_exhausted", result.fetch("halt_reason")
      assert_empty result.fetch("steps")
    end
  end

  def test_a_fresh_implementation_receives_the_human_question_context_and_answer
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      workflows = build_workflows(engine)
      dispatch(engine, work)
      raised = workflows.request_transition(work_item_id: work.fetch("id"), transition: "escalate", actor: operator("system"), request_id: "ask",
        decision: { "question" => "Which protocol?", "context" => "Two clients still use v1" })
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "resume_implementation", actor: operator,
        request_id: "answer", decision_id: raised.dig("decision", "id"), reason: "Keep v1 compatible")
      prompt = nil
      controller(engine) do |_phase, bundle, _work|
        prompt = bundle.dig("harness", "prompt")
        Runner.new { outcome("failed") }
      end.process(work_item_id: work.fetch("id"))
      assert_includes prompt, "Which protocol?"
      assert_includes prompt, "Two clients still use v1"
      assert_includes prompt, "Keep v1 compatible"
    end
  end
end
