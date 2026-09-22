# frozen_string_literal: true

require_relative "controller_test"

# Durable acceptance and dispatch. Every test here restarts the dispatcher — a new object over the
# same store — wherever a real operator would have restarted a process, because "what survives a
# restart" is the whole point of the component.
class DispatcherTest < Minitest::Test
  Dispatcher = Backstage::Application::Dispatcher
  Records = Backstage::Domain::Records

  # Scheduling time is injected so due times and retry delays are proven without sleeping.
  class TestClock < Backstage::Ports::Clock
    attr_accessor :now
    attr_reader :waits

    def initialize(now = Time.utc(2026, 1, 1))
      @now = now
      @waits = []
    end

    def wait(seconds, interrupt: nil)
      @waits << seconds
      @now += seconds
      true
    end

    def advance(seconds)
      @now += seconds
      self
    end
  end

  class StubPresence < Backstage::Ports::RuntimePresence
    def initialize(answer) = @answer = answer
    def status(_runtime) = @answer
  end

  # Runs whatever the test scripted, and counts every launch so "did it run again?" is answerable.
  class CountingRunner
    def initialize(outcomes)
      @outcomes = outcomes
      @launches = []
    end

    attr_reader :launches

    def runtime_identity_before_launch? = true

    def run(bundle:, secrets: {}, cancellation: nil, capture: nil)
      @launches << bundle
      yield({ "type" => "run_started" }) if block_given?
      outcome = @outcomes.length > 1 ? @outcomes.shift : @outcomes.first
      outcome.respond_to?(:call) ? outcome.call(bundle) : outcome
    end

    def adapter_identifier = "Test::CountingRunner"
  end

  # A dispatcher assembled the way bootstrap assembles it, with the injectable parts stubbed.
  class Harness
    attr_reader :engine, :workflows, :clock, :runner, :modes

    def initialize(directory, workflow_name: "minimal", workflow: nil, outcomes: nil, presence: "unknown", clock: nil, test: nil)
      @directory = directory
      @test = test
      @engine = test.build_engine(directory)
      @work = if workflow
                @engine.submit(idempotency_key: "dispatcher-custom", title: "custom", description: "", workflow: workflow)
              else
                test.submit_work(@engine, workflow_name: workflow_name, key: "dispatcher-#{workflow_name}")
              end
      @workflows = Backstage::Application::WorkflowService.new(store: @engine.store)
      @clock = clock || TestClock.new
      @presence = presence
      @runner = CountingRunner.new(outcomes || [{ "schema_version" => 1, "status" => "succeeded", "summary" => "ran", "process" => { "exit_code" => 0, "signal" => nil } }])
      @modes = []
    end

    def work = @engine.store.fetch!("work_items", @work.fetch("id"))
    def work_id = @work.fetch("id")
    def launches = @runner.launches.length
    def runs = @engine.store.list("runs")

    # A fresh dispatcher over the same store: the restart every durability claim depends on.
    def dispatcher(authorized_mode: "fake", recovery: nil)
      engine = @test.build_engine(@directory)
      workflows = Backstage::Application::WorkflowService.new(store: engine.store)
      recovery ||= Backstage::Application::Recovery.new(engine: engine, workflows: workflows, presence: StubPresence.new(@presence))
      Dispatcher.new(
        engine: engine, workflows: workflows, recovery: recovery, clock: @clock,
        authorized_mode: authorized_mode,
        controller_factory: lambda do |mode|
          @modes << mode
          Backstage::Application::Controller.new(
            engine: engine, workflows: workflows,
            configuration: ControllerTest::FakeConfiguration.new(@work),
            runner_factory: ->(_phase, _bundle, _work) { @runner },
            mode: mode
          )
        end
      )
    end
  end

  def harness(directory, **options) = Harness.new(directory, test: self, **options)

  def implementation(directory, summary: "implemented", status: "succeeded")
    outcome = { "schema_version" => 1, "status" => status, "summary" => summary, "process" => { "exit_code" => 0, "signal" => nil } }
    return outcome unless status == "succeeded"

    path = File.join(directory, "patches", "#{summary.gsub(/\W+/, "-")}-#{Records.id("p")}.patch")
    FileUtils.mkdir_p(File.dirname(path))
    content = "diff for #{summary}\n"
    File.binwrite(path, content)
    outcome.merge("change_artifact" => {
      "source_path" => path, "branch" => "backstage/x", "base_revision" => "abc",
      "patch_sha256" => Digest::SHA256.hexdigest(content), "patch_size" => content.bytesize
    })
  end

  def review(verdict, reviewer: "reviewer-#{verdict}")
    {
      "schema_version" => 1, "status" => "succeeded", "summary" => "review #{verdict}",
      "process" => { "exit_code" => 0, "signal" => nil },
      "review" => { "verdict" => verdict, "summary" => "review #{verdict}", "independent" => true, "reviewer_session_id" => reviewer }
    }
  end

  def failure(summary: "runner failed")
    { "schema_version" => 1, "status" => "failed", "summary" => summary, "process" => { "exit_code" => 1, "signal" => nil } }
  end

  def only(report) = report.fetch("intents").fetch(0)

  # --- acceptance -------------------------------------------------------------------------------

  def test_accepted_work_completes_once_across_a_restart_before_dispatch
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      # The accepting process exits here. A different dispatcher picks the work up from the store.
      report = test.dispatcher.pass

      assert_equal "completed", only(report).fetch("status")
      assert_equal "done", test.work.fetch("state")
      assert_equal 1, test.launches
      assert_equal 1, test.runs.length

      again = test.dispatcher.pass
      assert_empty again.fetch("intents"), "a completed intent is no longer considered"
      assert_equal 1, test.launches, "restarting must not run the work a second time"
    end
  end

  def test_repeated_acceptance_deduplicates_and_a_conflicting_payload_is_refused
    in_tmpdir do |directory|
      test = harness(directory)
      first = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      repeat = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      assert_equal first.fetch("id"), repeat.fetch("id")
      assert_equal 1, test.engine.store.list("execution_intents").length

      error = assert_raises(Backstage::ContractError) do
        test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 3 })
      end
      assert_match(/different request payload/, error.message)
    end
  end

  def test_a_second_acceptance_needs_supersede_and_fences_the_earlier_generation
    in_tmpdir do |directory|
      test = harness(directory)
      first = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      conflict = assert_raises(Backstage::ConflictError) do
        test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-2")
      end
      assert_match(/already accepted/, conflict.message)

      second = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-2", supersede: true)
      fenced = test.engine.store.fetch("execution_intents", first.fetch("id"))

      assert_equal 2, second.fetch("generation")
      assert_equal "cancelled", fenced.fetch("status")
      assert_match(/superseded by #{second.fetch("id")}/, fenced.fetch("stop_reason"))
      assert_equal [second.fetch("id")], test.dispatcher.intents.map { |row| row.fetch("id") }
    end
  end

  def test_acceptance_refuses_terminal_work_and_unknown_start_transitions
    in_tmpdir do |directory|
      test = harness(directory)
      assert_raises(Backstage::ContractError) do
        test.dispatcher.accept(work_item_id: test.work_id, request_id: "bad-start", start_transition: "nonexistent")
      end
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      test.dispatcher.pass

      error = assert_raises(Backstage::ContractError) do
        test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-late")
      end
      assert_match(/terminal state done/, error.message)
    end
  end

  def test_nothing_is_dispatched_without_an_accepted_intent
    in_tmpdir do |directory|
      test = harness(directory)

      report = test.dispatcher.pass

      assert_empty report.fetch("intents"), "unaccepted work is never adopted"
      assert_equal 0, test.launches
      assert_equal "new", test.work.fetch("state")
    end
  end

  def test_concurrent_acceptances_of_the_same_work_cannot_both_win
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "operator-a")

      # A second operator whose reads happened before the first acceptance committed: it sees no
      # acceptance pointer and no active intent, so only the store guard can stop it.
      second = test.dispatcher
      stale = second.store
      stale.define_singleton_method(:fetch) do |collection, id|
        collection.to_s == "execution_acceptances" ? nil : super(collection, id)
      end
      stale.define_singleton_method(:list) do |collection|
        collection.to_s == "execution_intents" ? [] : super(collection)
      end

      assert_raises(Backstage::ConflictError) do
        second.accept(work_item_id: test.work_id, request_id: "operator-b")
      end

      intents = test.engine.store.list("execution_intents")
      assert_equal 1, intents.length, "only one acceptance was recorded"
      assert_equal "operator-a", intents.first.fetch("request_id")
    end
  end

  def test_one_unusable_acceptance_does_not_stop_the_rest_of_a_pass
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      other = test.engine.submit(idempotency_key: "second", title: "second", description: "", workflow: workflow("minimal"))
      broken = test.dispatcher.accept(work_item_id: other.fetch("id"), request_id: "accept-2")
      # Its workflow snapshot is unreadable, so classifying it raises.
      test.engine.store.save("work_items", other.merge("workflow" => other.fetch("workflow").merge("digest" => "missing")))

      report = test.dispatcher.pass

      broken_report = report.fetch("intents").find { |row| row.fetch("intent_id") == broken.fetch("id") }
      good_report = report.fetch("intents").find { |row| row.fetch("intent_id") != broken.fetch("id") }
      assert_equal "blocked", broken_report.fetch("status")
      assert_match(/NotFound/, broken_report.fetch("detail"))
      assert_equal "completed", good_report.fetch("status"), "the healthy acceptance still ran"
      assert_equal 1, test.launches
    end
  end

  # --- reconciliation ---------------------------------------------------------------------------

  def test_a_live_execution_is_left_alone_and_no_replacement_is_launched
    in_tmpdir do |directory|
      test = harness(directory, presence: "alive")
      dispatcher = test.dispatcher
      intent = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-live"))
      test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-live", "status" => "running", "runtime" => { "container_name" => "backstage-live" }
      ))

      report = only(test.dispatcher.pass)

      assert_equal "running", report.fetch("status")
      assert_equal "observed", report.fetch("action")
      assert_equal 0, test.launches, "a live run is never duplicated"
      assert_equal "running", test.engine.store.fetch("runs", "run-live").fetch("status")
      assert_equal intent.fetch("id"), report.fetch("intent_id")
    end
  end

  def test_unknown_runtime_blocks_visibly_and_is_never_retried
    in_tmpdir do |directory|
      test = harness(directory, presence: "unknown")
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 3 })
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-unknown"))
      test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-unknown", "status" => "running", "owner_pid" => 999_999_999, "runtime" => { "container_name" => "backstage-x" }
      ))

      report = only(test.dispatcher.pass)

      assert_equal "uncertain", report.fetch("status")
      assert_equal 0, test.launches
      assert_match(/runtime status could not be established/, test.dispatcher.describe(test.work_id).fetch("blocked_reason"))
      assert_includes test.dispatcher.describe(test.work_id).fetch("actions"), "backstage recover #{test.work_id}"
    end
  end

  def test_dead_runtime_is_reconciled_conservatively_without_relaunching
    in_tmpdir do |directory|
      test = harness(directory, presence: "gone")
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-dead"))
      test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-dead", "status" => "running", "owner_pid" => 999_999_999, "runtime" => { "container_name" => "backstage-dead" }
      ))

      report = only(test.dispatcher.pass)

      run = test.engine.store.fetch("runs", "run-dead")
      assert_equal "interrupted", run.fetch("status")
      assert_equal true, run.dig("outcome", "interrupted")
      assert_equal "dispatched", report.fetch("action")
      assert_equal 1, report.fetch("attempts_used"), "the relaunch is this intent's first attempt, not a duplicate of the dead run"
      assert_equal 2, test.runs.length
    end
  end

  def test_a_recorded_outcome_is_applied_without_executing_again
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      outcome = { "schema_version" => 1, "status" => "succeeded", "summary" => "finished before the crash", "process" => { "exit_code" => 0, "signal" => nil } }
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-done"))
      test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-done", "status" => "running", "outcome" => outcome
      ))

      report = only(test.dispatcher.pass)

      assert_equal "completed", report.fetch("status")
      assert_equal "done", test.work.fetch("state")
      assert_equal 0, test.launches, "a persisted result is applied, never re-run"
      assert_equal 1, test.runs.length
    end
  end

  # --- human waits ------------------------------------------------------------------------------

  def test_a_human_wait_survives_restarts_and_resumes_exactly_once
    in_tmpdir do |directory|
      test = harness(directory, workflow_name: "independent-review",
        outcomes: [implementation(directory), review("blocked"), implementation(directory, summary: "second"), review("approved")])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      assert_equal "waiting", only(test.dispatcher.pass).fetch("status")
      assert_equal "needs_decision", test.work.fetch("state")
      launches_at_wait = test.launches

      3.times { assert_equal "waiting", only(test.dispatcher.pass).fetch("status") }
      assert_equal launches_at_wait, test.launches, "waiting for a human runs nothing"
      decision = test.engine.store.list("decisions").find { |row| row.fetch("status") == "open" }
      refute_nil decision, "the wait is a recorded decision, not an implicit pause"
      assert_equal decision.fetch("id"), test.dispatcher.describe(test.work_id).dig("open_decision", "id")

      test.workflows.request_transition(
        work_item_id: test.work_id, transition: "resume_implementation",
        actor: operator("human"), request_id: "decide-1", decision_id: decision.fetch("id")
      )

      report = only(test.dispatcher.pass)
      assert_equal "completed", report.fetch("status")
      assert_equal launches_at_wait + 2, test.launches, "exactly one implementation and its review continued"
      assert_equal "answered", test.engine.store.fetch("decisions", decision.fetch("id")).fetch("status")
      assert_empty test.dispatcher.pass.fetch("intents")
    end
  end

  def test_a_dispatcher_pass_never_answers_a_decision_itself
    in_tmpdir do |directory|
      test = harness(directory, workflow_name: "independent-review",
        outcomes: [implementation(directory), review("blocked")])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      test.dispatcher.pass

      5.times { test.dispatcher.pass }

      decision = test.engine.store.list("decisions").find { |row| row.fetch("status") == "open" }
      assert_equal "open", decision.fetch("status")
      assert_equal "needs_decision", test.work.fetch("state")
      assert_equal 2, test.launches
    end
  end

  # --- retries ----------------------------------------------------------------------------------

  def test_a_failure_defaults_to_no_retry_and_stops_with_a_reason
    in_tmpdir do |directory|
      test = harness(directory, outcomes: [failure])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      report = only(test.dispatcher.pass)

      assert_equal "exhausted", report.fetch("status")
      assert_equal 1, test.launches
      described = test.dispatcher.describe(test.work_id)
      assert_equal "no automatic retries are configured", described.fetch("stop_reason")
      assert_match(/runner failed/, described.fetch("last_error"))
      assert_equal 0, test.dispatcher.pass.fetch("dispatched"), "an exhausted intent is not retried"
      assert_equal 1, test.launches
    end
  end

  def test_a_bounded_retry_is_delayed_survives_restart_and_exhausts_stably
    in_tmpdir do |directory|
      clock = TestClock.new
      test = harness(directory, outcomes: [failure], clock: clock)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1",
        retry_policy: { "max_retries" => 1, "delay_seconds" => 60 })

      first = only(test.dispatcher.pass)
      assert_equal "delayed", first.fetch("status")
      assert_equal 1, test.launches
      due = first.fetch("due_at")
      assert_equal (clock.now + 60).utc.iso8601(6), due

      # A restart before the due time changes nothing: the deadline is persisted, not remembered.
      2.times { assert_equal "delayed", only(test.dispatcher.pass).fetch("status") }
      assert_equal 1, test.launches, "a delayed retry never runs early"
      assert_equal due, test.dispatcher.describe(test.work_id).fetch("next_wake_up")
      assert_equal 0, test.dispatcher.describe(test.work_id).fetch("retries_remaining")

      clock.advance(61)
      second = only(test.dispatcher.pass)
      assert_equal 2, test.launches, "the retry ran once its due time passed"
      assert_equal "exhausted", second.fetch("status")
      assert_equal "retry budget of 1 is spent", test.dispatcher.describe(test.work_id).fetch("stop_reason")

      clock.advance(3600)
      assert_equal 0, test.dispatcher.pass.fetch("dispatched")
      assert_equal 2, test.launches, "exhaustion is stable across restarts and time"
    end
  end

  def test_a_dispatcher_that_died_before_producing_a_run_requeues_the_work
    in_tmpdir do |directory|
      test = harness(directory)
      dispatcher = test.dispatcher
      intent = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      claimed = crashed_claim(test, dispatcher, intent)

      requeued = only(test.dispatcher.pass)

      # Nothing failed, so nothing may be charged to the failure budget — and the work is not lost.
      assert_equal "queued", requeued.fetch("status")
      assert_match(/ended before producing a run/, requeued.fetch("detail"))
      assert_equal 0, test.dispatcher.resolve(test.work_id).fetch("retries_used")
      assert_operator claimed.fetch("attempts_used"), :>=, 1, "the interrupted attempt is still recorded"

      assert_equal "completed", only(test.dispatcher.pass).fetch("status"), "the next pass runs it"
      assert_equal 1, test.launches
      assert_equal "done", test.work.fetch("state")
    end
  end

  def test_a_dispatcher_that_keeps_dying_before_producing_a_run_is_stopped
    in_tmpdir do |directory|
      test = harness(directory)
      dispatcher = test.dispatcher
      intent = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      report = nil
      Dispatcher::EMPTY_CLAIM_LIMIT.times do
        intent = test.dispatcher.resolve(test.work_id)
        crashed_claim(test, dispatcher, intent)
        report = only(test.dispatcher.pass(limit: 0))
      end

      assert_equal "blocked", report.fetch("status")
      assert_match(/dispatch attempts ended before producing a run/, test.dispatcher.describe(test.work_id).fetch("blocked_reason"))
      assert_equal 0, test.launches
      assert_equal 0, test.dispatcher.resolve(test.work_id).fetch("retries_used"), "a crash loop never spends a failure retry"
    end
  end

  # Claims an attempt and then makes the claiming process look dead, which is exactly what a killed
  # dispatcher leaves behind: a running intent, no run, and a pid nobody is using.
  def crashed_claim(test, dispatcher, intent)
    claimed = dispatcher.send(:claim, intent)
    test.engine.store.save("execution_intents", claimed.merge("dispatcher_pid" => 999_999_999, "revision" => claimed.fetch("revision") + 1))
    claimed
  end

  def test_a_crash_around_retry_accounting_never_reclaims_budget
    in_tmpdir do |directory|
      clock = TestClock.new
      test = harness(directory, outcomes: [failure], clock: clock)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1",
        retry_policy: { "max_retries" => 2, "delay_seconds" => 30, "backoff" => "exponential" })
      test.dispatcher.pass

      # Crash: a brand new dispatcher reads only what was persisted.
      restarted = test.dispatcher
      intent = restarted.resolve(test.work_id)
      assert_equal 1, intent.fetch("retries_used")
      assert_equal 1, intent.fetch("attempts_used")

      clock.advance(31)
      restarted.pass
      after_second = test.dispatcher.resolve(test.work_id)
      assert_equal 2, after_second.fetch("retries_used")
      assert_equal 2, after_second.fetch("attempts_used")
      assert_equal "delayed", after_second.fetch("status")
      assert_equal (clock.now + 60).utc.iso8601(6), after_second.fetch("due_at"), "exponential backoff doubled the delay"

      clock.advance(61)
      test.dispatcher.pass
      assert_equal 3, test.launches
      assert_equal "exhausted", test.dispatcher.resolve(test.work_id).fetch("status")
      assert_equal 2, test.dispatcher.resolve(test.work_id).fetch("retries_used")
    end
  end

  def test_a_cancelled_execution_is_never_retried_automatically
    in_tmpdir do |directory|
      cancelled = { "schema_version" => 1, "status" => "cancelled", "summary" => "operator cancelled", "process" => { "exit_code" => nil, "signal" => nil } }
      test = harness(directory, outcomes: [cancelled])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 5 })

      report = only(test.dispatcher.pass)

      assert_equal "blocked", report.fetch("status")
      assert_match(/cancellations are never retried/, test.dispatcher.describe(test.work_id).fetch("blocked_reason"))
      assert_equal 1, test.launches
    end
  end

  def test_a_block_is_a_durable_gate_that_later_passes_may_not_step_over
    in_tmpdir do |directory|
      cancelled = { "schema_version" => 1, "status" => "cancelled", "summary" => "operator cancelled", "process" => { "exit_code" => nil, "signal" => nil } }
      test = harness(directory, outcomes: [cancelled])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 5 })
      assert_equal "blocked", only(test.dispatcher.pass).fetch("status")
      assert_equal "new", test.work.fetch("state"), "the workflow's failure transition made the work eligible again"

      # The work item is dispatchable again, so only the durable block stops the next pass.
      3.times do
        report = only(test.dispatcher.pass)
        assert_equal "blocked", report.fetch("status")
        assert_equal "skipped", report.fetch("action")
        assert_match(/cancellations are never retried/, report.fetch("detail"))
      end
      assert_equal 1, test.launches, "a blocked acceptance is never quietly resumed"

      # Superseding is the operator control that clears it.
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-2", supersede: true)
      assert_equal "dispatched", only(test.dispatcher.pass).fetch("action")
      assert_equal 2, test.launches
    end
  end

  def test_a_terminal_work_item_keeps_its_acceptance_until_the_execution_resolves
    in_tmpdir do |directory|
      test = harness(directory, workflow_name: "independent-review", presence: "alive")
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-live"))
      test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-live", "status" => "running", "runtime" => { "container_name" => "backstage-live" }
      ))
      test.workflows.request_transition(work_item_id: test.work_id, transition: "cancel", actor: operator("human"), request_id: "stop-it")

      report = only(test.dispatcher.pass)

      assert_equal "cancelled", test.work.fetch("state")
      assert_equal "running", report.fetch("status"), "the acceptance still owns reconciling the live run"
      assert_match(/terminal but an execution is still resolving/, report.fetch("detail"))

      test.engine.store.save("runs", test.engine.store.fetch("runs", "run-live").merge("status" => "cancelled", "finished_at" => Records.timestamp))
      assert_equal "completed", only(test.dispatcher.pass).fetch("status")
    end
  end

  def test_an_unresolved_external_effect_blocks_a_retry
    in_tmpdir do |directory|
      test = harness(directory, outcomes: [failure])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 2 })
      test.engine.store.save("external_actions", Records.external_action(
        work_item_id: test.work_id, kind: "github-pr", idempotency_key: "github-pr:v1:#{test.work_id}", status: "pending"
      ))

      report = only(test.dispatcher.pass)

      assert_equal "blocked", report.fetch("status")
      assert_match(/unresolved external effect github-pr/, test.dispatcher.describe(test.work_id).fetch("blocked_reason"))
      assert_equal 1, test.launches
    end
  end

  def test_a_failure_the_workflow_cannot_redispatch_is_blocked_rather_than_retried
    in_tmpdir do |directory|
      # A workflow whose failure path lands somewhere no automatic dispatch can start from.
      definition = workflow("minimal").to_h
      definition["states"]["stalled"] = { "description" => "Failed and waiting for a human." }
      definition["transitions"]["reset"]["to"] = "stalled"
      definition["transitions"]["restart"] = { "from" => ["stalled"], "to" => "new", "actors" => ["human"] }
      test = harness(directory, workflow: Backstage::Domain::Workflow.compile(definition), outcomes: [failure])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 2 })

      report = only(test.dispatcher.pass)

      assert_equal "blocked", report.fetch("status")
      assert_equal "stalled", test.work.fetch("state")
      assert_match(/offers no new dispatch from stalled/, test.dispatcher.describe(test.work_id).fetch("blocked_reason"))
      assert_equal 1, test.launches
      assert_equal 0, test.dispatcher.resolve(test.work_id).fetch("retries_used"), "a blocked failure never spends retry budget"
    end
  end

  # --- ownership, fencing and modes ---------------------------------------------------------------

  def test_a_claimed_execution_cannot_be_dispatched_twice
    in_tmpdir do |directory|
      test = harness(directory)
      first = test.dispatcher
      intent = first.accept(work_item_id: test.work_id, request_id: "accept-1")
      claimed = first.send(:claim, intent)

      report = only(test.dispatcher.pass)

      assert_equal 0, test.launches, "a claimed attempt is not started by a second caller"
      assert_equal "running", claimed.fetch("status")
      assert_equal "running", report.fetch("status")
      assert_match(/claimed by dispatcher pid #{Process.pid}/, report.fetch("detail"))

      stale = assert_raises(Backstage::ConflictError) { first.send(:claim, intent) }
      assert_match(/execution_intents/, stale.message)
    end
  end

  def test_a_stale_dispatcher_is_fenced_by_a_cancellation
    in_tmpdir do |directory|
      test = harness(directory)
      dispatcher = test.dispatcher
      intent = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      dispatcher.cancel(test.work_id, reason: "operator stopped it")

      report = dispatcher.send(:dispatch, intent)

      assert_equal "fenced", report.fetch("action")
      assert_equal "cancelled", report.fetch("status")
      assert_equal 0, test.launches
      assert_equal "new", test.work.fetch("state")
    end
  end

  def test_a_superseding_generation_waits_for_the_old_execution_and_fences_it
    in_tmpdir do |directory|
      test = harness(directory, presence: "alive")
      dispatcher = test.dispatcher
      first = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-old"))
      test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-old", "status" => "running", "runtime" => { "container_name" => "backstage-old" }
      ))

      second = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-2", supersede: true)

      assert_equal "cancelled", test.engine.store.fetch("execution_intents", first.fetch("id")).fetch("status")
      refute_nil test.engine.store.fetch("runs", "run-old").fetch("cancellation_requested_at"),
        "the superseded generation's run is told to stop"

      report = only(test.dispatcher.pass)

      assert_equal second.fetch("id"), report.fetch("intent_id")
      assert_equal "running", report.fetch("status")
      assert_match(/accepted before this generation \(run-old\) is still resolving/, report.fetch("detail"))
      assert_equal 0, test.launches, "the new generation does not launch beside an unresolved execution"
    end
  end

  def test_every_status_a_pass_can_record_is_explained
    assert_equal Dispatcher::STATUSES.sort, Dispatcher::EXPLANATIONS.keys.sort
  end

  def test_a_cancelled_intent_does_not_wake_and_its_wait_stays_answered_by_a_human
    in_tmpdir do |directory|
      test = harness(directory, workflow_name: "independent-review",
        outcomes: [implementation(directory), review("blocked")])
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      test.dispatcher.pass
      launches = test.launches

      cancelled = test.dispatcher.cancel(test.work_id, reason: "no longer wanted")

      assert_equal "cancelled", cancelled.fetch("status")
      assert_equal 0, test.dispatcher.pass.fetch("considered"), "a cancelled wait is not considered again"
      assert_equal launches, test.launches
      assert_equal "open", test.engine.store.list("decisions").find { |row| row["status"] == "open" }.fetch("status")
    end
  end

  def test_a_fake_dispatcher_will_not_consume_work_accepted_for_real_execution
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-real", mode: "publish_draft")

      report = only(test.dispatcher(authorized_mode: "fake").pass)

      assert_equal "skipped", report.fetch("action")
      assert_equal "queued", report.fetch("status")
      assert_match(/requires publish_draft authorization/, report.fetch("detail"))
      assert_equal 0, test.launches
      assert_empty test.modes, "no controller was built for unauthorized work"
    end
  end

  def test_a_real_dispatcher_still_runs_fake_work_as_fake
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-fake", mode: "fake")

      report = only(test.dispatcher(authorized_mode: "publish_draft").pass)

      assert_equal "fake", report.fetch("mode")
      assert_equal ["fake"], test.modes, "a worker flag never upgrades an accepted fake intent"
      assert_equal "fake", report.dig("process", "mode")
    end
  end

  def test_direct_processing_may_not_bypass_an_active_intent
    in_tmpdir do |directory|
      test = harness(directory)
      dispatcher = test.dispatcher
      dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      error = assert_raises(Backstage::ConflictError) { dispatcher.guard_direct_processing!(test.work_id) }
      assert_match(/cancel or supersede/, error.message)

      dispatcher.cancel(test.work_id)
      assert_nil dispatcher.guard_direct_processing!(test.work_id)
    end
  end

  def test_re_accepting_after_a_cancellation_starts_a_new_generation
    in_tmpdir do |directory|
      test = harness(directory)
      first = test.dispatcher.accept(work_item_id: test.work_id)
      test.dispatcher.cancel(test.work_id, reason: "changed my mind")

      second = test.dispatcher.accept(work_item_id: test.work_id)

      refute_equal first.fetch("id"), second.fetch("id"), "a cancelled acceptance is not resurrected"
      assert_equal 2, second.fetch("generation")
      assert_equal false, second.fetch("deduplicated")
      assert_equal "completed", only(test.dispatcher.pass).fetch("status")
      assert_equal 1, test.launches
    end
  end

  def test_the_documented_control_actually_clears_a_block
    in_tmpdir do |directory|
      cancelled = { "schema_version" => 1, "status" => "cancelled", "summary" => "operator cancelled", "process" => { "exit_code" => nil, "signal" => nil } }
      test = harness(directory, outcomes: [cancelled])
      test.dispatcher.accept(work_item_id: test.work_id)
      assert_equal "blocked", only(test.dispatcher.pass).fetch("status")
      described = test.dispatcher.describe(test.work_id)

      # Exactly the command the CLI tells the operator to run, with no explicit identity.
      assert_includes described.fetch("actions"), "backstage dispatch accept #{test.work_id} --supersede"
      accepted = test.dispatcher.accept(work_item_id: test.work_id, supersede: true)

      assert_equal 2, accepted.fetch("generation")
      assert_equal "queued", accepted.fetch("status")
      assert_equal "dispatched", only(test.dispatcher.pass).fetch("action")
      assert_equal 2, test.launches
    end
  end

  def test_a_repeat_acceptance_says_it_deduplicated_and_supersede_needs_a_new_identity
    in_tmpdir do |directory|
      test = harness(directory)
      first = test.dispatcher.accept(work_item_id: test.work_id)
      repeat = test.dispatcher.accept(work_item_id: test.work_id)

      assert_equal first.fetch("id"), repeat.fetch("id")
      assert_equal true, repeat.fetch("deduplicated")

      # Superseding under an identity that was recorded without it is refused loudly, rather than
      # quietly returning the very acceptance it was meant to replace.
      error = assert_raises(Backstage::ContractError) do
        test.dispatcher.accept(work_item_id: test.work_id, request_id: first.fetch("request_id"), supersede: true)
      end
      assert_match(/different request payload/, error.message)
      assert_equal 1, test.engine.store.list("execution_intents").length

      # A caller retrying its own supersede after a timeout must not cancel the generation it just
      # created, so the identical request deduplicates onto it.
      superseded = test.dispatcher.accept(work_item_id: test.work_id, request_id: "take-two", supersede: true)
      retried = test.dispatcher.accept(work_item_id: test.work_id, request_id: "take-two", supersede: true)

      assert_equal 2, superseded.fetch("generation")
      assert_equal superseded.fetch("id"), retried.fetch("id")
      assert_equal true, retried.fetch("deduplicated")
      assert_equal 1, test.dispatcher.intents.length, "a retried supersede does not burn another generation"

      # An identity that names a closed generation cannot record a new one.
      test.dispatcher.cancel(test.work_id)
      closed = assert_raises(Backstage::ContractError) do
        test.dispatcher.accept(work_item_id: test.work_id, request_id: "take-two", supersede: true)
      end
      assert_match(/supersede needs a new acceptance identity/, closed.message)
    end
  end

  def test_a_block_lifts_when_the_work_item_actually_moves
    in_tmpdir do |directory|
      cancelled = { "schema_version" => 1, "status" => "cancelled", "summary" => "operator cancelled", "process" => { "exit_code" => nil, "signal" => nil } }
      succeeded = { "schema_version" => 1, "status" => "succeeded", "summary" => "second attempt", "process" => { "exit_code" => 0, "signal" => nil } }
      test = harness(directory, outcomes: [cancelled, succeeded])
      test.dispatcher.accept(work_item_id: test.work_id)
      assert_equal "blocked", only(test.dispatcher.pass).fetch("status")
      assert_equal "blocked", only(test.dispatcher.pass).fetch("status"), "nothing automatic lifts it"

      # An operator moves the work item themselves.
      test.workflows.request_transition(work_item_id: test.work_id, transition: "start",
        actor: operator("human"), request_id: "operator-start")

      report = only(test.dispatcher.pass)

      assert_equal "completed", report.fetch("status")
      assert_equal 2, test.launches, "the operator's own transition is what resumed it"
    end
  end

  def test_automatic_progress_never_lifts_a_block
    in_tmpdir do |directory|
      cancelled = { "schema_version" => 1, "status" => "cancelled", "summary" => "operator cancelled", "process" => { "exit_code" => nil, "signal" => nil } }
      test = harness(directory, outcomes: [cancelled])
      test.dispatcher.accept(work_item_id: test.work_id)
      assert_equal "blocked", only(test.dispatcher.pass).fetch("status")

      # The system moves the work item itself. That is Backstage talking to itself, not authority.
      test.workflows.request_transition(work_item_id: test.work_id, transition: "start",
        actor: { "role" => "system", "id" => "controller", "entry" => "controller" }, request_id: "system-start")

      assert_equal "blocked", only(test.dispatcher.pass).fetch("status")
      assert_equal 1, test.launches, "a system transition does not clear an operator-visible stop"
    end
  end

  def test_an_unresolved_external_effect_keeps_a_block_even_after_a_human_moves_the_work
    in_tmpdir do |directory|
      test = harness(directory, outcomes: [failure])
      test.dispatcher.accept(work_item_id: test.work_id, retry_policy: { "max_retries" => 3 })
      test.engine.store.save("external_actions", Records.external_action(
        work_item_id: test.work_id, kind: "github-pr", idempotency_key: "github-pr:v1:#{test.work_id}", status: "pending"
      ))
      assert_equal "blocked", only(test.dispatcher.pass).fetch("status")

      test.workflows.request_transition(work_item_id: test.work_id, transition: "start",
        actor: operator("human"), request_id: "operator-start")

      report = only(test.dispatcher.pass)
      assert_equal "blocked", report.fetch("status")
      assert_match(/unresolved external effect github-pr/, report.fetch("detail"))
      assert_equal 1, test.launches, "an unresolved effect is not cleared by workflow progress"

      # Resolving the effect is what makes the human's transition count.
      action = test.engine.store.list("external_actions").first
      test.engine.store.save("external_actions", action.merge("status" => "succeeded"))
      assert_equal "dispatched", only(test.dispatcher.pass).fetch("action")
    end
  end

  def test_a_non_object_retry_policy_is_a_contract_error
    in_tmpdir do |directory|
      test = harness(directory)

      error = assert_raises(Backstage::ContractError) do
        test.dispatcher.accept(work_item_id: test.work_id, retry_policy: "3")
      end

      assert_match(/retry policy must be an object/, error.message)
    end
  end

  def test_a_stopping_dispatcher_starts_nothing_further_inside_a_pass
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id)
      2.times do |index|
        other = test.engine.submit(idempotency_key: "extra-#{index}", title: "extra", description: "", workflow: workflow("minimal"))
        test.dispatcher.accept(work_item_id: other.fetch("id"))
      end
      stopping = false

      report = test.dispatcher.pass(stop: -> { stopping }.tap { stopping = true })

      assert_equal 0, report.fetch("dispatched")
      assert_equal 3, report.fetch("deferred")
      assert_equal 0, test.launches, "a signal stops new dispatches within the pass, not only between passes"
      assert_equal %w[deferred], report.fetch("intents").map { |row| row.fetch("action") }.uniq
      assert_match(/the dispatcher is stopping/, report.fetch("intents").first.fetch("detail"))
    end
  end

  def test_work_the_pass_declined_is_reported_as_deferred_not_dispatched
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id)
      other = test.engine.submit(idempotency_key: "extra", title: "extra", description: "", workflow: workflow("minimal"))
      test.dispatcher.accept(work_item_id: other.fetch("id"))

      report = test.dispatcher.pass(limit: 1)

      actions = report.fetch("intents").map { |row| row.fetch("action") }
      assert_equal 1, report.fetch("dispatched")
      assert_equal 1, report.fetch("deferred")
      assert_includes actions, "deferred"
      refute_includes actions, "dispatch", "no report names an action that did not happen"
      assert_equal 1, test.launches
    end
  end

  def test_the_next_wake_up_ignores_work_this_dispatcher_cannot_consume
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id, mode: "publish_draft")

      fake = test.dispatcher(authorized_mode: "fake")
      assert_nil fake.pass.fetch("next_wake_up"),
        "a permanently past deadline for work it may not run would tell the worker to wake forever"
      refute_nil test.dispatcher(authorized_mode: "publish_draft").next_wake_up
    end
  end

  def test_a_lost_race_during_a_pass_fences_instead_of_blocking
    in_tmpdir do |directory|
      test = harness(directory)
      test.dispatcher.accept(work_item_id: test.work_id)
      racing = Object.new
      racing.define_singleton_method(:reconcile) { |_id = nil| raise Backstage::ConflictError, "work moved during recovery" }

      report = only(test.dispatcher(recovery: racing).pass)

      assert_equal "fenced", report.fetch("action")
      assert_equal "queued", report.fetch("status"), "a lost optimistic race is not a durable block"
      assert_equal "completed", only(test.dispatcher.pass).fetch("status"), "the next pass carries on"
    end
  end

  def test_a_superseded_generations_outcome_cannot_move_the_work_item
    in_tmpdir do |directory|
      test = harness(directory, presence: "alive")
      dispatcher = test.dispatcher
      dispatcher.accept(work_item_id: test.work_id)
      job = dispatch(test.engine, test.work, transition: "start", request_id: "manual-start")
      test.engine.store.save("jobs", job.merge("status" => "running", "run_id" => "run-old"))
      run = test.engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: test.work_id, work_revision: job.fetch("work_revision")).merge(
        "id" => "run-old", "status" => "running", "runtime" => { "container_name" => "backstage-old" }
      ))
      dispatcher.accept(work_item_id: test.work_id, supersede: true)

      # The old run finishes and tries to speak for the work item it was dispatched for.
      test.engine.store.save("runs", test.engine.store.fetch("runs", "run-old").merge(
        "status" => "succeeded", "outcome" => { "schema_version" => 1, "status" => "succeeded", "summary" => "late" }
      ))
      error = assert_raises(Backstage::Error) do
        test.workflows.request_transition(
          work_item_id: test.work_id, transition: "finish",
          actor: test.workflows.outcome_actor(run.merge("phase" => "implementation"), { "status" => "succeeded" }),
          request_id: "late-outcome", run_id: "run-old"
        )
      end
      assert_match(/cancel|superseded|no uncancelled/, error.message)
      assert_equal "in_progress", test.work.fetch("state"), "the late outcome moved nothing"
    end
  end

  # --- inspection -------------------------------------------------------------------------------

  def test_inspection_explains_queued_delayed_and_completed_work
    in_tmpdir do |directory|
      clock = TestClock.new
      test = harness(directory, outcomes: [failure], clock: clock)
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1", retry_policy: { "max_retries" => 1 })

      queued = test.dispatcher.queue_status
      assert_equal 1, queued.fetch("counts").fetch("queued")
      assert_equal 1, queued.fetch("active")
      assert_equal "accepted and eligible; the next dispatcher pass will run it", test.dispatcher.describe(test.work_id).fetch("explanation")

      test.dispatcher.pass
      delayed = test.dispatcher.queue_status
      assert_equal 1, delayed.fetch("counts").fetch("delayed")
      refute_nil delayed.fetch("next_wake_up")
      described = test.dispatcher.describe(test.work_id)
      assert_equal "a bounded retry is scheduled and will not run before its due time", described.fetch("explanation")
      assert_equal "failed: runner failed", described.fetch("last_error")
      assert_equal "implementation", described.dig("last_execution", "phase")
      assert_equal Backstage::Application::Dispatcher::STATUSES.sort, delayed.fetch("counts").keys.sort
    end
  end
end
