# frozen_string_literal: true

require "delegate"
require_relative "test_helper"
require_relative "dispatcher_test"

# The application half of durable activity: every acknowledged lifecycle change commits its event
# in the same store commit as the state change it explains. What that buys is proven three ways for
# each emitter — the record and the event are in one transaction, a guard conflict commits neither,
# and an idempotent replay returns what was recorded without appending history.
class ActivityEmittersTest < Minitest::Test
  Activity = Backstage::Domain::Activity
  Recorder = Backstage::Application::ActivityRecorder
  Records = Backstage::Domain::Records

  # Every committed transaction line, so "did these land together?" is answerable directly rather
  # than inferred from sequence numbers.
  def transactions(store)
    File.readlines(store.path).map { |line| JSON.parse(line) }
  end

  # The one transaction that wrote this record, with the events it carried.
  def transaction_for(store, collection, id, state: nil)
    lines = transactions(store).select do |line|
      line.fetch("events").any? do |write|
        write["collection"] == collection && write.dig("record", "id") == id &&
          (state.nil? || write.dig("record", "status") == state || write.dig("record", "state") == state)
      end
    end
    refute_empty lines, "#{collection} #{id} was never written"
    lines.last
  end

  def event_types(line) = Array(line["activity"]).map { |event| event.fetch("type") }

  def stream(store, **filters)
    store.read_activity(filters: filters, limit: 500).fetch("events")
  end

  def types(store, **filters) = stream(store, **filters).map { |event| event.fetch("type") }

  def event_of(store, type, **filters)
    stream(store, **filters).select { |event| event.fetch("type") == type }
  end

  # A store that lets another writer in just before the guarded commit, which is how a real caller
  # loses a race: its expectation was true when it read, and stale by the time it committed.
  class RacingStore < SimpleDelegator
    def initialize(store, &interference)
      super(store)
      @interference = interference
    end

    def commit(writes, expect: [], activity: [])
      @interference.call(__getobj__)
      __getobj__.commit(writes, expect: expect, activity: activity)
    end
  end

  # --- work admission ---------------------------------------------------------------------------

  def test_admitted_work_and_its_event_are_one_commit_and_a_repeat_appends_nothing
    in_tmpdir do |directory|
      engine = build_engine(directory)

      work = submit_work(engine, key: "admit-1", target: "tasks")

      line = transaction_for(engine.store, "work_items", work.fetch("id"))
      assert_includes event_types(line), "work.admitted"
      assert_includes line.fetch("events").map { |write| write.fetch("collection") }, "workflow_snapshots"

      admitted = event_of(engine.store, "work.admitted").fetch(0)
      assert_equal work.fetch("id"), admitted.fetch("work_item_id")
      assert_equal work.fetch("id"), admitted.fetch("correlation_id")
      assert_equal "tasks", admitted.fetch("target_id")
      assert_equal "ready", admitted.dig("data", "state")
      assert_equal "independent-review", admitted.dig("data", "workflow")
      assert_equal "core", admitted.dig("source", "provenance")
      # An idempotency key is built from the source ref and the slugified title, so history keeps
      # only its digest: enough to recognize the same admission, not a second copy of source text.
      refute admitted.fetch("data").key?("idempotency_key"), "history must not carry the slugified title"
      assert_equal Digest::SHA256.hexdigest("admit-1"), admitted.dig("data", "idempotency_key_digest")
      refute_nil admitted.fetch("sequence")
      refute_nil admitted.fetch("recorded_at")
      refute_includes admitted.fetch("summary"), "Test work", "a summary is built from ids and states, not source text"

      before = transactions(engine.store).length
      again = submit_work(engine, key: "admit-1", target: "tasks")

      assert_equal work.fetch("id"), again.fetch("id")
      assert_equal before, transactions(engine.store).length, "an idempotent admission writes nothing at all"
      assert_equal 1, event_of(engine.store, "work.admitted").length
    end
  end

  # --- execution --------------------------------------------------------------------------------

  def test_a_claim_and_its_completion_each_land_with_their_own_records
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      job = dispatch(engine, work)

      executed = engine.execute(job, runner: Backstage::Adapters::Fake::Runner.new)
      run_id = executed.fetch("run").fetch("id")

      claim = transaction_for(engine.store, "runs", run_id, state: "running")
      assert_equal ["execution.started"], event_types(claim)
      finish = transaction_for(engine.store, "runs", run_id, state: "succeeded")
      assert_equal %w[runtime.reported_completion execution.verified_completion], event_types(finish)

      started = event_of(engine.store, "execution.started").fetch(0)
      assert_equal run_id, started.fetch("run_id")
      assert_equal job.fetch("id"), started.fetch("job_id")
      assert_equal work.fetch("id"), started.fetch("correlation_id")
      assert_equal engine.store.list("attempts").fetch(0).fetch("id"), started.fetch("attempt_id")
      assert_equal "implementation", started.dig("data", "phase")
      assert_equal Recorder.event_id("work.transition_applied", job.fetch("transition_id")),
                   started.fetch("causation_event_id"), "the dispatching transition is the cause"

      reported = event_of(engine.store, "runtime.reported_completion").fetch(0)
      verified = event_of(engine.store, "execution.verified_completion").fetch(0)
      assert_equal "runtime_reported", reported.dig("source", "provenance")
      assert_equal "Backstage::FakeRunner", reported.dig("source", "adapter")
      assert_equal "core", verified.dig("source", "provenance")
      assert_equal started.fetch("event_id"), reported.fetch("causation_event_id")
      assert_equal reported.fetch("event_id"), verified.fetch("causation_event_id")
      assert_equal "succeeded", verified.dig("data", "status")
      assert_equal false, verified.dig("data", "overrode_report")
    end
  end

  # A worker reporting success while it was being stopped is not a success, and the two facts stay
  # separately readable: what the runtime said, and what the core recorded.
  def test_a_cancelled_execution_records_the_runtime_claim_apart_from_the_verified_result
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      job = dispatch(engine, work)
      runner = CancellingRunner.new(engine)

      executed = engine.execute(job, runner: runner)
      run_id = executed.fetch("run").fetch("id")

      assert_equal "cancelled", executed.fetch("outcome").fetch("status")
      reported = event_of(engine.store, "runtime.reported_completion").fetch(0)
      verified = event_of(engine.store, "execution.verified_completion").fetch(0)
      assert_equal "succeeded", reported.dig("data", "reported_status")
      assert_equal "cancelled", verified.dig("data", "status")
      assert_equal true, verified.dig("data", "overrode_report")
      assert_equal true, verified.dig("data", "cancellation_requested")

      requested = event_of(engine.store, "execution.cancellation_requested")
      assert_equal 1, requested.length
      assert_equal run_id, requested.fetch(0).fetch("run_id")
      assert_equal ["execution.cancellation_requested"],
                   event_types(transaction_for(engine.store, "runs", run_id, state: "running"))
    end
  end

  # Asks for its own cancellation mid-run and then reports success anyway.
  class CancellingRunner
    def initialize(engine) = @engine = engine

    def runtime_identity_before_launch? = true
    def adapter_identifier = "Test::CancellingRunner"

    def run(bundle:, secrets: {}, cancellation: nil, capture: nil)
      run = @engine.store.list("runs").find { |row| row.fetch("status") == "running" }
      @engine.request_cancel(run.fetch("id"))
      { "schema_version" => 1, "status" => "succeeded", "summary" => "worker reported success",
        "process" => { "exit_code" => 0, "signal" => nil } }
    end
  end

  def test_a_repeated_cancellation_request_keeps_the_first_and_appends_no_history
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      job = dispatch(engine, work)
      run = engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id")).merge("status" => "running"))

      first = engine.request_cancel(run.fetch("id"))
      lines = transactions(engine.store).length
      second = engine.request_cancel(run.fetch("id"))

      assert_equal first.fetch("cancellation_requested_at"), second.fetch("cancellation_requested_at")
      assert_equal lines, transactions(engine.store).length, "a repeated request writes nothing"
      assert_equal 1, event_of(engine.store, "execution.cancellation_requested").length
    end
  end

  def test_a_lost_claim_race_commits_neither_the_run_nor_its_event
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      job = dispatch(engine, work)
      before = stream(engine.store).length
      racing = Backstage::Engine.new(
        store: RacingStore.new(engine.store) { |store| store.save("work_items", store.fetch!("work_items", work.fetch("id")).merge("revision" => 99)) },
        artifact_store: engine.artifact_store
      )

      assert_raises(Backstage::ConflictError) { racing.execute(job, runner: Backstage::Adapters::Fake::Runner.new) }

      assert_empty engine.store.list("runs"), "the claim did not land"
      assert_equal before, stream(engine.store).length, "and neither did its history"
      assert_empty event_of(engine.store, "execution.started")
    end
  end

  # --- transitions, decisions ---------------------------------------------------------------------

  def test_a_transition_and_its_event_are_one_commit_with_ownership_from_the_entry_context
    in_tmpdir do |directory|
      engine = build_engine(directory)
      workflows = build_workflows(engine)
      work = submit_work(engine, target: "tasks")

      result = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1"
      )
      transition = result.fetch("transition")

      line = transaction_for(engine.store, "work_transitions", transition.fetch("id"))
      assert_equal ["work.transition_applied"], event_types(line)
      assert_includes line.fetch("events").map { |write| write.fetch("collection") }, "work_items"
      assert_includes line.fetch("events").map { |write| write.fetch("collection") }, "jobs"

      applied = event_of(engine.store, "work.transition_applied").fetch(0)
      assert_equal transition.fetch("id"), applied.fetch("transition_id")
      assert_equal "start-1", applied.fetch("request_id")
      assert_equal result.fetch("job").fetch("id"), applied.fetch("job_id")
      assert_equal work.fetch("id"), applied.fetch("work_item_id")
      assert_equal "tasks", applied.fetch("target_id")
      assert_equal({ "role" => "system", "entry" => "operator_cli", "id" => "tester" }, applied.dig("data", "actor"))
      assert_equal "running", applied.dig("data", "to")
      assert_equal 1, applied.dig("data", "revision")
      assert_equal "core", applied.dig("source", "provenance"), "the core checked this authority and applied it"
    end
  end

  def test_replaying_a_transition_request_returns_the_recorded_result_and_appends_nothing
    in_tmpdir do |directory|
      engine = build_engine(directory)
      workflows = build_workflows(engine)
      work = submit_work(engine)
      args = { work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1" }

      first = workflows.request_transition(**args)
      lines = transactions(engine.store).length
      second = workflows.request_transition(**args)

      assert_equal true, second.fetch("deduplicated")
      assert_equal first.fetch("transition").fetch("id"), second.fetch("transition").fetch("id")
      assert_equal lines, transactions(engine.store).length
      assert_equal 1, event_of(engine.store, "work.transition_applied").length
    end
  end

  def test_a_guard_conflict_commits_neither_the_transition_nor_its_event
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      before = stream(engine.store).length
      racing = Backstage::Application::WorkflowService.new(
        store: RacingStore.new(engine.store) { |store| store.save("work_items", store.fetch!("work_items", work.fetch("id")).merge("revision" => 7)) }
      )

      assert_raises(Backstage::ConflictError) do
        racing.request_transition(work_item_id: work.fetch("id"), transition: "start",
                                  actor: operator("system"), request_id: "start-1")
      end

      assert_empty engine.store.list("work_transitions")
      assert_empty engine.store.list("jobs"), "no execution was queued for a transition that never happened"
      assert_equal before, stream(engine.store).length, "and no history claims one was"
    end
  end

  def test_a_decision_is_raised_and_answered_inside_the_transition_that_did_it
    in_tmpdir do |directory|
      engine = build_engine(directory)
      workflows = build_workflows(engine)
      work = submit_work(engine, workflow_name: "human-gated-change", key: "gated-1")
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "start",
                                   actor: operator("system"), request_id: "start-1")
      candidate = write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch")

      raised = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "request_approval", actor: operator("system"),
        request_id: "ask-1", evidence: [candidate.fetch("id")]
      )
      decision = raised.fetch("decision")

      line = transaction_for(engine.store, "decisions", decision.fetch("id"), state: nil)
      assert_equal %w[work.transition_applied decision.raised], event_types(line)
      event = event_of(engine.store, "decision.raised").fetch(0)
      assert_equal decision.fetch("id"), event.fetch("decision_id")
      assert_equal raised.fetch("transition").fetch("id"), event.fetch("transition_id")
      assert_equal %w[approve decline cancel], event.dig("data", "choices")
      assert_equal Recorder.event_id("work.transition_applied", raised.fetch("transition").fetch("id")),
                   event.fetch("causation_event_id")

      answered = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "approve", actor: operator,
        request_id: "approve-1", decision_id: decision.fetch("id")
      )

      closing = transaction_for(engine.store, "work_transitions", answered.fetch("transition").fetch("id"))
      assert_equal %w[work.transition_applied decision.answered], event_types(closing)
      answer = event_of(engine.store, "decision.answered").fetch(0)
      assert_equal "approve", answer.dig("data", "choice")
      assert_equal "human", answer.dig("data", "actor", "role")
      assert_equal "operator_cli", answer.dig("data", "actor", "entry")
    end
  end

  # --- conflicts a retry cannot fix ---------------------------------------------------------------

  # A store in which some other writer always got to this event id first, with different content.
  # It bounds itself so the pre-fix behaviour — `rescue ConflictError; next` looping forever on an
  # identity that will never stop clashing — surfaces as a failure instead of a hung test.
  class PoisonedIdentityStore < SimpleDelegator
    class Spun < StandardError; end

    ATTEMPT_LIMIT = 5

    attr_reader :attempts

    def initialize(store, type:)
      super(store)
      @type = type
      @attempts = 0
    end

    def commit(writes, expect: [], activity: [])
      poison = activity.find { |event| event["type"] == @type }
      if poison
        @attempts += 1
        raise Spun, "the retry loop spun #{@attempts} times on an event id that can never stop clashing" if @attempts > ATTEMPT_LIMIT

        __getobj__.commit([], activity: [poison.merge("summary" => "a different story under the same id")])
      end
      __getobj__.commit(writes, expect: expect, activity: activity)
    end
  end

  def test_an_engine_retry_loop_raises_an_event_identity_conflict_instead_of_retrying_it_forever
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      engine.store.save("runs", { "id" => "run-poisoned", "status" => "running", "phase" => "implement",
                                  "work_item_id" => work.fetch("id"), "job_id" => "job-1" })
      poisoned = PoisonedIdentityStore.new(engine.store, type: "execution.cancellation_requested")
      fenced = Backstage::Engine.new(store: poisoned, artifact_store: engine.artifact_store)

      assert_raises(Backstage::ActivityConflictError) { fenced.request_cancel("run-poisoned") }

      assert_equal 1, poisoned.attempts, "a clashing identity is answered once, not re-derived on every pass"
      assert_nil engine.store.fetch!("runs", "run-poisoned")["cancellation_requested_at"],
                 "the refused commit took the state change with it"
    end
  end

  # --- acceptance and the dispatcher --------------------------------------------------------------

  def test_an_acceptance_records_one_event_and_a_repeat_records_none
    in_tmpdir do |directory|
      test = DispatcherTest::Harness.new(directory, test: self)
      dispatcher = test.dispatcher
      store = test.engine.store

      intent = dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      line = transaction_for(store, "execution_intents", intent.fetch("id"))
      assert_equal ["execution.accepted"], event_types(line)
      accepted = event_of(store, "execution.accepted").fetch(0)
      assert_equal "operator", accepted.dig("source", "provenance")
      assert_equal test.work_id, accepted.fetch("work_item_id")
      assert_equal "accept-1", accepted.fetch("request_id")
      assert_equal intent.fetch("id"), accepted.dig("data", "intent_id")
      assert_equal 1, accepted.dig("data", "generation")

      # The ids an operator would follow this acceptance by are the ones the store indexes.
      assert_equal ["execution.accepted"], types(store, related_id: intent.fetch("id"))
      assert_equal ["work.admitted", "execution.accepted"], types(store, work_item_id: test.work_id)

      lines = transactions(store).length
      assert_equal true, dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1").fetch("deduplicated")
      assert_equal lines, transactions(store).length
      assert_equal 1, event_of(store, "execution.accepted").length
    end
  end

  def test_superseding_records_the_new_acceptance_and_the_old_one_ending_together
    in_tmpdir do |directory|
      test = DispatcherTest::Harness.new(directory, test: self)
      store = test.engine.store
      first = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")

      second = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-2", supersede: true)

      line = transaction_for(store, "execution_intents", second.fetch("id"))
      assert_equal %w[execution.accepted execution.finished], event_types(line)
      finished = event_of(store, "execution.finished").fetch(0)
      assert_equal first.fetch("id"), finished.dig("data", "intent_id")
      assert_equal "cancelled", finished.dig("data", "status")
      assert_match(/superseded by/, finished.dig("data", "stop_reason"))
      assert_equal Recorder.event_id("execution.accepted", second.fetch("id")), finished.fetch("causation_event_id")
    end
  end

  # `--reason` is the one place unbounded operator text reaches an event payload. The record keeps
  # what was typed; history, which is append-only and fsynced beside every state change, does not.
  def test_an_operators_cancellation_reason_is_bounded_before_it_becomes_history
    in_tmpdir do |directory|
      test = DispatcherTest::Harness.new(directory, test: self)
      store = test.engine.store
      test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1")
      reason = "because " * 500

      cancelled = test.dispatcher.cancel(test.work_id, reason: reason)

      assert_equal reason, cancelled.fetch("stop_reason"), "the record still holds what the operator typed"
      recorded = event_of(store, "execution.finished").fetch(0).dig("data", "stop_reason")
      assert_equal Activity::REASON_LIMIT, recorded.length
      assert recorded.end_with?("..."), "a truncated reason says so rather than reading as complete"
      assert reason.start_with?(recorded[0...-3])
    end
  end

  def test_a_scheduled_retry_and_the_end_of_an_acceptance_are_each_recorded_once
    in_tmpdir do |directory|
      clock = DispatcherTest::TestClock.new
      failure = { "schema_version" => 1, "status" => "failed", "summary" => "runner failed",
                  "process" => { "exit_code" => 1, "signal" => nil } }
      test = DispatcherTest::Harness.new(directory, test: self, outcomes: [failure], clock: clock)
      store = test.engine.store
      intent = test.dispatcher.accept(work_item_id: test.work_id, request_id: "accept-1",
                                      retry_policy: { "max_retries" => 1, "delay_seconds" => 60 })

      test.dispatcher.pass

      scheduled = event_of(store, "execution.retry_scheduled")
      assert_equal 1, scheduled.length
      retry_event = scheduled.fetch(0)
      assert_equal (clock.now + 60).utc.iso8601(6), retry_event.dig("data", "due_at")
      assert_equal 1, retry_event.dig("data", "retries_used")
      assert_equal 1, retry_event.dig("data", "max_retries")
      assert_equal 1, retry_event.dig("data", "attempts_used")
      run_id = store.list("runs").last.fetch("id")
      assert_equal Recorder.event_id("execution.verified_completion", run_id), retry_event.fetch("causation_event_id")
      assert_includes event_types(transaction_for(store, "execution_intents", intent.fetch("id"), state: "delayed")),
                      "execution.retry_scheduled"

      # A worker polls this intent until its due time arrives. Observing a delayed acceptance again
      # is not a lifecycle change and adds no history, however many passes look at it.
      recorded = stream(store).length
      3.times { test.dispatcher.pass }
      assert_equal recorded, stream(store).length, "idle passes over an unchanged intent record nothing"

      clock.advance(61)
      test.dispatcher.pass

      assert_equal 1, event_of(store, "execution.retry_scheduled").length, "the budget was spent, not rescheduled"
      finished = event_of(store, "execution.finished")
      assert_equal 1, finished.length
      assert_equal "exhausted", finished.fetch(0).dig("data", "status")
      clock.advance(3600)
      test.dispatcher.pass
      assert_equal 1, event_of(store, "execution.finished").length, "a terminal acceptance is recorded once"
    end
  end

  # --- recovery ---------------------------------------------------------------------------------

  def test_recovery_records_a_finding_with_the_repair_it_made
    in_tmpdir do |directory|
      engine = build_engine(directory)
      workflows = build_workflows(engine)
      work = submit_work(engine, workflow_name: "human-gated-change", key: "gated-2")
      job = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1"
      ).fetch("job")
      job = engine.store.save("jobs", job.merge("status" => "running"))
      run = engine.store.save("runs", Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"),
                                                  work_revision: job.fetch("work_revision"))
        .merge("status" => "running", "owner_pid" => 99_999_999, "runtime" => { "container_name" => "backstage-x" }))
      recovery = Backstage::Application::Recovery.new(engine: engine, workflows: workflows,
                                                     presence: GonePresence.new)

      report = recovery.reconcile(work.fetch("id"))

      assert_includes report.fetch("work_items").flat_map { |row| row.fetch("findings") }.map { |row| row.fetch("kind") },
                      "worker_interrupted"
      line = transaction_for(engine.store, "runs", run.fetch("id"), state: "interrupted")
      assert_equal ["reconciliation.finding"], event_types(line)
      finding = event_of(engine.store, "reconciliation.finding").fetch(0)
      assert_equal run.fetch("id"), finding.fetch("run_id")
      assert_equal work.fetch("id"), finding.fetch("work_item_id")
      assert_equal "worker_interrupted", finding.dig("data", "kind")
      assert_equal true, finding.dig("data", "repaired")

      # Reconciliation runs before every dispatch. Observing the same repaired state again must not
      # append a second telling of it.
      recovery.reconcile(work.fetch("id"))
      assert_equal 1, event_of(engine.store, "reconciliation.finding").length
    end
  end

  class GonePresence < Backstage::Ports::RuntimePresence
    def status(_runtime) = Backstage::Ports::RuntimePresence::GONE
  end

  # --- source checks ------------------------------------------------------------------------------

  def test_a_source_check_is_recorded_when_its_answer_changes_and_not_once_per_poll
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      client = PollingClient.new
      trigger = Backstage::TdTrigger.new(client: client, store: store, source_instance: "widgets-example")

      assert_empty trigger.poll
      assert_equal 1, event_of(store, "source.checked").length, "the first answer is a change"
      lines = transactions(store).length
      3.times { assert_empty trigger.poll }
      assert_equal lines, transactions(store).length, "an unchanged answer writes nothing"

      client.ready = [{ "id" => "td-abc123", "status" => "open", "labels" => ["agent-ready"], "title" => "Ready" }]
      found = trigger.poll

      assert_equal 1, found.length
      checks = event_of(store, "source.checked")
      assert_equal 2, checks.length
      latest = checks.last
      assert_equal "external_observation", latest.dig("source", "provenance")
      assert_equal 1, latest.dig("data", "new_triggers")
      assert_equal "td:widgets-example", latest.fetch("correlation_id")
      receipt = store.fetch("source_checks", "td:widgets-example")
      assert_equal "ok", receipt.fetch("status")
      assert_equal ["source.checked"], event_types(transaction_for(store, "source_checks", "td:widgets-example"))

      client.error = RuntimeError
      assert_raises(RuntimeError) { trigger.poll }
      failed = event_of(store, "source.checked").last
      assert_equal "failed", failed.dig("data", "status")
      assert_equal "RuntimeError", failed.dig("data", "error")
      lines = transactions(store).length
      assert_raises(RuntimeError) { trigger.poll }
      assert_equal lines, transactions(store).length, "a repeated failure is the same answer"
    end
  end

  # The defect this reproduces: the `source.checked` event id was keyed on a *state* fingerprint, so
  # a source that recovered produced the same "ok" id as the first time it was healthy — with a
  # different occurred_at. The store refused the reuse, `record_check` swallowed the refusal as a
  # lost race, and the receipt stayed at "failed" with no event explaining the recovery.
  def test_a_recovered_source_records_its_recovery_rather_than_colliding_with_the_first_ok
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      client = PollingClient.new
      trigger = Backstage::TdTrigger.new(client: client, store: store, source_instance: "widgets-example")

      trigger.poll
      client.error = RuntimeError
      assert_raises(RuntimeError) { trigger.poll }
      client.error = nil

      trigger.poll

      receipt = store.fetch("source_checks", "td:widgets-example")
      assert_equal "ok", receipt.fetch("status"), "the receipt must follow the source back to healthy"
      assert_nil receipt["error"]
      assert_equal 3, receipt.fetch("check_sequence")

      checks = event_of(store, "source.checked")
      assert_equal %w[ok failed ok], checks.map { |check| check.dig("data", "status") }
      assert_equal [1, 2, 3], checks.map { |check| check.dig("data", "check_sequence") }
      assert_equal 3, checks.map { |check| check.fetch("event_id") }.uniq.length,
                   "each occurrence needs its own identity, or the recovery has nowhere to land"
      # The recovery receipt and the event explaining it are still one transaction.
      assert_equal ["source.checked"], event_types(transaction_for(store, "source_checks", "td:widgets-example", state: "ok"))
    end
  end

  def test_repeating_one_occurrence_of_a_check_still_reconciles_instead_of_appending
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      client = PollingClient.new
      trigger = Backstage::TdTrigger.new(client: client, store: store, source_instance: "widgets-example")
      receipt = trigger.send(:record_check, status: "ok", triggers: [])
      lines = transactions(store).length

      # The same occurrence told twice — a retry after an interrupted acknowledgement — is one fact.
      assert_nil trigger.send(:record_check, status: "ok", triggers: [])

      assert_equal lines, transactions(store).length
      assert_equal 1, receipt.fetch("check_sequence")
      assert_equal 1, event_of(store, "source.checked").length
    end
  end

  # A store that forces the second `source.checked` event to reuse the first one's id. That is what
  # a mis-derived identity looks like from the trigger's side, and it must reach the caller rather
  # than be filed away as "another poller got there first".
  class CollidingStore < SimpleDelegator
    def initialize(store)
      super(store)
      @first = nil
    end

    def commit(writes, expect: [], activity: [])
      activity = activity.map do |event|
        next event unless event["type"] == "source.checked"

        @first ? event.merge("event_id" => @first) : (@first = event.fetch("event_id")) && event
      end
      __getobj__.commit(writes, expect: expect, activity: activity)
    end
  end

  def test_an_event_identity_conflict_on_a_check_is_raised_and_never_swallowed
    in_tmpdir do |directory|
      store = CollidingStore.new(Backstage::JsonlStore.new(File.join(directory, "state.jsonl")))
      client = PollingClient.new
      trigger = Backstage::TdTrigger.new(client: client, store: store, source_instance: "widgets-example")
      trigger.poll
      client.ready = [{ "id" => "td-abc123", "status" => "open", "labels" => ["agent-ready"], "title" => "Ready" }]

      error = assert_raises(Backstage::ActivityConflictError) { trigger.poll }

      assert_match(/already exists with different content/, error.message)
      assert_equal "ok", store.fetch("source_checks", "td:widgets-example").fetch("status")
      assert_equal 1, event_of(store, "source.checked").length, "the refused event was never written"
    end
  end

  class PollingClient
    attr_accessor :ready, :error

    def initialize
      @ready = []
      @error = nil
    end

    def ready_issues
      raise @error, "td is unreachable" if @error

      @ready
    end

    def approval_candidates = []
    def show(id) = @ready.find { |issue| issue.fetch("id") == id }
  end
end
