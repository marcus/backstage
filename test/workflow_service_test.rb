# frozen_string_literal: true

require_relative "test_helper"
require "delegate"

class WorkflowServiceTest < Minitest::Test
  def with_work(workflow_name: "independent-review")
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine, workflow_name: workflow_name)
      yield engine, build_workflows(engine), work
    end
  end

  def start(workflows, work, request_id: "start-1")
    workflows.request_transition(
      work_item_id: work.fetch("id"), transition: "start",
      actor: operator("system"), request_id: request_id
    )
  end

  def candidate(engine, work, content: "candidate one")
    write_artifact(engine, work_item_id: work.fetch("id"), kind: "binary_patch", content: content)
  end

  def verdict(engine, work, sha:, verdict: "approved", reviewer: "reviewer-1", independent: true)
    write_artifact(
      engine, work_item_id: work.fetch("id"), kind: "review_verdict",
      content: { "verdict" => verdict }.to_json,
      provenance: { "verdict" => verdict, "independent" => independent, "reviewer_session_id" => reviewer, "candidate_sha256" => sha }
    )
  end

  def reviewer_context(engine, work)
    job = engine.store.list("jobs").reverse.find { |row| row["work_item_id"] == work.fetch("id") && row["phase"] == "review" }
    run = Backstage::Domain::Records.run(job_id: job.fetch("id"), work_item_id: work.fetch("id"), phase: "review", work_revision: job.fetch("work_revision"))
    # All evidence in these fixture-only unit tests belongs to this current review run.
    run["id"] = "run-fixture"
    run["status"] = "succeeded"
    run["outcome"] = { "status" => "succeeded", "review" => { "reviewer_session_id" => "reviewer-1" } }
    engine.store.save("runs", run)
    engine.store.save("jobs", job.merge("status" => "succeeded", "run_id" => run.fetch("id")))
    { actor: { "role" => "reviewer", "id" => "reviewer-1", "entry" => "run_outcome" }, run_id: run.fetch("id") }
  end

  def test_a_transition_records_state_history_and_dispatch_together
    with_work do |engine, workflows, work|
      result = start(workflows, work)

      assert_equal "running", result.dig("work_item", "state")
      assert_equal 1, result.dig("work_item", "revision")
      assert_equal "start", result.dig("transition", "transition")
      assert_equal "system", result.dig("transition", "actor", "role")
      assert_equal "independent-review", result.dig("transition", "workflow_name")
      assert_equal "implementation", result.dig("job", "phase")
      assert_equal "queued", result.dig("job", "status")
      assert_equal "running", result.dig("job", "dispatch_state")
      assert_equal 1, workflows.history(work.fetch("id")).length
    end
  end

  def test_a_repeated_request_returns_the_recorded_result_and_launches_nothing_twice
    with_work do |engine, workflows, work|
      first = start(workflows, work)
      second = start(workflows, work)

      assert_equal false, first.fetch("deduplicated")
      assert_equal true, second.fetch("deduplicated")
      assert_equal first.dig("transition", "id"), second.dig("transition", "id")
      assert_equal 1, engine.store.list("jobs").length
      assert_equal 1, engine.store.fetch("work_items", work.fetch("id")).fetch("revision")
    end
  end

  def test_reusing_a_request_id_for_a_different_transition_is_refused
    with_work do |_engine, workflows, work|
      start(workflows, work)
      error = assert_raises(Backstage::ContractError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "cancel",
          actor: operator, request_id: "start-1"
        )
      end
      assert_match(/already recorded start, not cancel/, error.message)
    end
  end

  def test_a_stale_expectation_conflicts_without_writing
    with_work do |engine, workflows, work|
      start(workflows, work)
      assert_raises(Backstage::ConflictError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "cancel", actor: operator,
          request_id: "cancel-stale", expected_revision: 0
        )
      end
      assert_raises(Backstage::ConflictError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "cancel", actor: operator,
          request_id: "cancel-stale-state", expected_state: "ready"
        )
      end
      assert_equal "running", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
      assert_equal 1, workflows.history(work.fetch("id")).length
    end
  end

  def test_an_undefined_transition_names_what_is_available
    with_work do |_engine, workflows, work|
      error = assert_raises(Backstage::InvalidTransition) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "approve",
          actor: operator, request_id: "bad-1"
        )
      end
      assert_match(/no transition "approve" from ready/, error.message)
      assert_match(/available: cancel, start/, error.message)
    end
  end

  # A worker cannot promote itself, and an operator cannot impersonate one.
  def test_authority_comes_from_the_entry_context
    with_work do |engine, workflows, work|
      start(workflows, work)
      sha = candidate(engine, work).fetch("sha256")

      spoofed = assert_raises(Backstage::AuthorityError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "submit_for_review",
          actor: { "role" => "agent", "entry" => "operator_cli" }, request_id: "spoof-1",
          evidence: [candidate(engine, work, content: "another").fetch("id")]
        )
      end
      assert_match(/operator_cli requests may not act as agent/, spoofed.message)

      escalated = assert_raises(Backstage::AuthorityError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "cancel",
          actor: { "role" => "human", "entry" => "worker_channel" }, request_id: "spoof-2"
        )
      end
      assert_match(/worker_channel requests may not act as human/, escalated.message)
      assert_equal "running", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
      refute_nil sha
    end
  end

  def test_a_reviewer_only_transition_refuses_the_controller
    with_work do |engine, workflows, work|
      start(workflows, work)
      sha = candidate(engine, work).fetch("sha256")
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "submit_for_review",
        actor: operator("system"),
        request_id: "review-1", evidence: [engine.store.list("artifacts").first.fetch("id")]
      )
      error = assert_raises(Backstage::AuthorityError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "approve",
          actor: operator("system"), request_id: "approve-1",
          evidence: [verdict(engine, work, sha: sha).fetch("id")]
        )
      end
      assert_match(/actor system may not take approve; permitted: reviewer/, error.message)
    end
  end

  def test_missing_and_foreign_evidence_are_refused
    with_work do |engine, workflows, work|
      start(workflows, work)
      missing = assert_raises(Backstage::ContractError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "submit_for_review",
          actor: operator("system"), request_id: "no-evidence"
        )
      end
      assert_match(/requires change_candidate evidence/, missing.message)

      other = submit_work(engine, key: "other-work")
      foreign = write_artifact(engine, work_item_id: other.fetch("id"), kind: "binary_patch")
      error = assert_raises(Backstage::AuthorityError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "submit_for_review",
          actor: operator("system"),
          request_id: "foreign", evidence: [foreign.fetch("id")]
        )
      end
      assert_match(/belongs to #{other.fetch("id")}/, error.message)
    end
  end

  def test_an_approval_cannot_be_replayed_against_a_new_candidate
    with_work do |engine, workflows, work|
      start(workflows, work)
      first = candidate(engine, work, content: "candidate one")
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "submit_for_review",
        actor: operator("system"),
        request_id: "review-1", evidence: [first.fetch("id")]
      )
      approval = verdict(engine, work, sha: first.fetch("sha256"))
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "request_changes",
        **reviewer_context(engine, work),
        request_id: "changes-1",
        evidence: [verdict(engine, work, sha: first.fetch("sha256"), verdict: "changes_requested").fetch("id")]
      )
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "revise",
        actor: operator("system"), request_id: "revise-1"
      )
      second = candidate(engine, work, content: "candidate two")
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "submit_for_review",
        actor: operator("system"),
        request_id: "review-2", evidence: [second.fetch("id")]
      )

      error = assert_raises(Backstage::ConflictError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "approve",
          **reviewer_context(engine, work),
          request_id: "approve-stale", evidence: [approval.fetch("id")]
        )
      end
      assert_match(/reviewed a different candidate/, error.message)
      assert_equal "awaiting_review", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
    end
  end

  def test_a_verdict_must_be_independent_and_match_the_transition_it_authorizes
    with_work do |engine, workflows, work|
      start(workflows, work)
      patch = candidate(engine, work)
      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "submit_for_review",
        actor: operator("system"),
        request_id: "review-1", evidence: [patch.fetch("id")]
      )
      reviewer = reviewer_context(engine, work)

      not_independent = verdict(engine, work, sha: patch.fetch("sha256"), independent: false)
      error = assert_raises(Backstage::AuthorityError) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "approve", **reviewer, request_id: "a1", evidence: [not_independent.fetch("id")])
      end
      assert_match(/not marked independent/, error.message)

      wrong = verdict(engine, work, sha: patch.fetch("sha256"), verdict: "changes_requested")
      mismatch = assert_raises(Backstage::ContractError) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "approve", **reviewer, request_id: "a2", evidence: [wrong.fetch("id")])
      end
      assert_match(/approve needs a approved verdict/, mismatch.message)
    end
  end

  def test_the_revision_budget_bounds_automatic_cycles
    with_work do |engine, workflows, work|
      engine.store.save("work_items", engine.store.fetch("work_items", work.fetch("id")).merge("state" => "changes_requested", "revisions_used" => 2))
      allowed = workflows.allowed_transitions(work.fetch("id")).find { |row| row.fetch("name") == "revise" }

      assert_equal false, allowed.fetch("available")
      assert_match(/revision budget of 2 is exhausted/, allowed.fetch("blocked_reason"))
      assert workflows.revision_budget_exhausted?(engine.store.fetch("work_items", work.fetch("id")), "revise")
      error = assert_raises(Backstage::ContractError) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "revise", actor: operator("system"), request_id: "over-budget")
      end
      assert_match(/allows 2 revision\(s\)/, error.message)
    end
  end

  def test_a_decision_records_its_question_and_resumes_exactly_once
    with_work(workflow_name: "human-gated-change") do |engine, workflows, work|
      start(workflows, work)
      patch = candidate(engine, work)
      raised = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "request_approval",
        actor: operator("system"),
        request_id: "ask-1", evidence: [patch.fetch("id")]
      )
      decision = raised.fetch("decision")

      assert_equal "open", decision.fetch("status")
      assert_equal %w[approve decline cancel], decision.fetch("choices")
      assert_equal patch.fetch("sha256"), decision.fetch("candidate_sha256")
      assert_equal decision.fetch("id"), raised.dig("work_item", "open_decision_id")

      missing_id = assert_raises(Backstage::ContractError) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "approve", actor: operator, request_id: "answer-0")
      end
      assert_match(/requires its decision id/, missing_id.message)

      answered = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "approve", actor: operator,
        request_id: "answer-1", decision_id: decision.fetch("id"), reason: "looks right"
      )
      assert_equal "completed", answered.dig("work_item", "state")
      assert_equal "answered", answered.dig("decision", "status")
      assert_equal "approve", answered.dig("decision", "answer", "choice")
      refute answered.dig("work_item").key?("open_decision_id")

      replay = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "approve", actor: operator,
        request_id: "answer-1", decision_id: decision.fetch("id"), reason: "looks right"
      )
      assert_equal true, replay.fetch("deduplicated")
      assert_equal 1, engine.store.list("decisions").length
      assert_equal 3, workflows.history(work.fetch("id")).length
    end
  end

  def test_a_decision_cannot_authorize_a_candidate_it_never_saw
    with_work(workflow_name: "human-gated-change") do |engine, workflows, work|
      start(workflows, work)
      patch = candidate(engine, work)
      raised = workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "request_approval",
        actor: operator("system"),
        request_id: "ask-1", evidence: [patch.fetch("id")]
      )
      decision = raised.fetch("decision")
      stored = engine.store.fetch("work_items", work.fetch("id"))
      engine.store.save("work_items", stored.merge("candidate" => stored.fetch("candidate").merge("sha256" => "a-newer-candidate")))

      error = assert_raises(Backstage::ConflictError) do
        workflows.request_transition(
          work_item_id: work.fetch("id"), transition: "approve", actor: operator,
          request_id: "answer-1", decision_id: decision.fetch("id"), reason: "looks right"
        )
      end
      assert_match(/raised against a different candidate/, error.message)
      assert_equal "awaiting_approval", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
    end
  end

  # An edited pack must not reinterpret work already admitted under the old definition.
  def test_a_workflow_edit_leaves_work_in_flight_on_the_definition_it_was_admitted_with
    in_tmpdir do |directory|
      engine = build_engine(directory)
      original = workflow("minimal")
      work = submit_work(engine, workflow_name: "minimal", key: "pinned")
      workflows = build_workflows(engine)
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1")

      edited = Backstage::Domain::Workflow.compile(
        original.to_h.merge(
          "states" => original.to_h.fetch("states").merge("blocked" => { "description" => "new" }),
          "transitions" => original.to_h.fetch("transitions").merge("block" => { "from" => ["in_progress"], "to" => "blocked", "actors" => ["system"] }, "unblock" => { "from" => ["blocked"], "to" => "in_progress", "actors" => ["system"] })
        )
      )
      engine.store.save("workflow_snapshots", edited.snapshot_record)
      refute_equal original.digest, edited.digest

      assert_equal %w[finish reset], workflows.allowed_transitions(work.fetch("id")).map { |row| row.fetch("name") }.sort
      assert_raises(Backstage::InvalidTransition) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "block", actor: operator("system"), request_id: "new-1")
      end

      fresh = engine.submit(idempotency_key: "fresh", title: "Fresh", description: "", workflow: edited)
      assert_equal edited.digest, fresh.dig("workflow", "digest")
      assert_includes build_workflows(engine).workflow_for(fresh).transitions.keys, "block"
    end
  end

  # A crash between the state write, the history write, and the dispatch write must leave nothing
  # half-applied, and must not queue an execution the transition never actually took.
  def test_an_interrupted_commit_leaves_no_partial_transition
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      interrupted = InterruptingStore.new(engine.store)
      workflows = Backstage::Application::WorkflowService.new(store: interrupted)

      assert_raises(InterruptingStore::Crash) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1")
      end

      stored = engine.store.fetch("work_items", work.fetch("id"))
      assert_equal "ready", stored.fetch("state")
      assert_equal 0, stored.fetch("revision")
      assert_empty engine.store.list("work_transitions")
      assert_empty engine.store.list("jobs"), "no execution was queued for a transition that never happened"

      recovered = build_workflows(engine).request_transition(
        work_item_id: work.fetch("id"), transition: "start", actor: operator("system"), request_id: "start-1"
      )
      assert_equal "running", recovered.dig("work_item", "state")
      assert_equal 1, engine.store.list("jobs").length
    end
  end

  # Stands in for the process dying inside the store's commit.
  class InterruptingStore < SimpleDelegator
    class Crash < StandardError; end

    def commit(_writes, expect: [], activity: [])
      raise Crash, "the process died mid-commit"
    end
  end

  def test_a_superseded_dispatch_is_reported_so_a_late_run_cannot_write
    with_work do |engine, workflows, work|
      first = start(workflows, work).fetch("job")
      workflows.request_transition(work_item_id: work.fetch("id"), transition: "execution_failed", actor: operator("system"), request_id: "fail-1")
      second = start(workflows, work, request_id: "start-2").fetch("job")

      assert_nil workflows.superseded_dispatch?(work.fetch("id"), second)
      assert_match(/was superseded by/, workflows.superseded_dispatch?(work.fetch("id"), first))
    end
  end
end
