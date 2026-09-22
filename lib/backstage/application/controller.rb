# frozen_string_literal: true

require "json"

module Backstage::Application
  # Joins execution to workflow position.
  #
  # The controller advances a work item as far as its configured workflow allows without guessing:
  # it drains dispatched jobs, applies the transition the workflow's own dispatch block names for
  # the outcome, follows a state's declared `continue`, and stops at anything terminal, awaiting a
  # decision, or out of revision budget. Progress never depends on an agent remembering bookkeeping,
  # and agent requests still arrive on the same idempotent operation.
  class Controller
    AuthorityError = Backstage::AuthorityError
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    CONTROLLER = { "role" => "system", "id" => "controller", "entry" => "controller" }.freeze

    def initialize(engine:, workflows:, configuration:, runner_factory:, mode:, channel_factory: nil)
      raise ContractError, "unknown controller mode #{mode.inspect}" unless %w[fake publish_draft].include?(mode)

      @engine = engine
      @workflows = workflows
      @configuration = configuration
      @runner_factory = runner_factory
      @mode = mode
      @channel_factory = channel_factory
    end

    def process(work_item_id:, start_transition: nil, secrets: {})
      work = @engine.store.fetch!("work_items", work_item_id)
      workflow = @workflows.workflow_for(work)
      steps = []
      started = false
      halt_reason = "step_budget_exhausted"
      budget = 4 + (2 * (workflow.max_revisions + 1))

      budget.times do
        work = @engine.store.fetch!("work_items", work_item_id)
        if halted?(workflow, work)
          halt_reason = workflow.terminal?(work.fetch("state")) ? "terminal" : "awaiting_decision"
          break
        end
        if @engine.active_execution?(work_item_id)
          halt_reason = "execution_active"
          break
        end

        job = @engine.queued_job(work_item_id)
        if job
          started = true
          step = run_dispatched_job(work, workflow, job, secrets)
          steps << step
          if step["fenced"] || step.dig("outcome", "status") != "succeeded"
            halt_reason = step["fenced"] ? "dispatch_fenced" : "execution_stopped"
            break
          end
          next
        end

        follow = continue_transition(workflow, work)
        if follow.nil? && workflow.state(work.fetch("state")).continue
          halt_reason = "revision_budget_exhausted"
          break
        end
        if follow.nil? && !started
          started = true
          follow = resolve_start(workflow, work, start_transition)
        end
        unless follow
          halt_reason = "no_automatic_transition"
          break
        end

        result = @workflows.request_transition(
          work_item_id: work_item_id,
          transition: follow,
          actor: CONTROLLER,
          request_id: request_id(work, follow),
          expected_revision: work.fetch("revision"),
          reason: "controller advanced #{work.fetch("state")} via #{follow}"
        )
        steps << { "transition" => result.fetch("transition"), "decision" => result["decision"] }.compact
      end

      work = @engine.show_work(work_item_id)
      handoff = handoff_payload(work, workflow)
      {
        "work_item" => work,
        "mode" => @mode,
        "state" => work.fetch("state"),
        "steps" => steps,
        "halt_reason" => halt_reason,
        "handoff_allowed" => !handoff.nil?,
        "handoff" => handoff,
        "verdict" => latest_verdict(work)
      }.compact
    end

    # A handoff needs recorded authority for the completion, not a claim that one happened: an
    # independent approval bound to the candidate and the terminal transition that completed it.
    def handoff_payload(work, workflow = @workflows.workflow_for(work))
      state = work.fetch("state")
      return nil unless workflow.state?(state) && workflow.terminal?(state)

      completion = Array(work["transitions"]).max_by { |row| row.fetch("revision") }
      return nil unless completion && completion.fetch("to") == state

      definition = workflow.transition(completion.fetch("transition"))
      return nil unless definition.requires.include?("review_verdict")

      authority = completion_authority(work, completion)
      return nil unless authority

      {
        "done" => last_implementation_summary(work),
        "remaining" => "Human approval of the draft change",
        "decisions" => [authority]
      }
    end

    private

    def halted?(workflow, work)
      state = work.fetch("state")
      workflow.terminal?(state) || workflow.state(state).awaits_decision?
    end

    # A state's declared `continue` is the only automatic step, and an exhausted revision budget
    # stops it — the assignment then rests visibly in `changes_requested` with the findings on it.
    def continue_transition(workflow, work)
      state = workflow.state(work.fetch("state"))
      return nil unless state.continue
      return nil if @workflows.revision_budget_exhausted?(work, state.continue)

      state.continue
    end

    def resolve_start(workflow, work, start_transition)
      state = work.fetch("state")
      candidates = workflow.transitions_from(state).select { |candidate| candidate.dispatch && candidate.allows?("system") && candidate.requires.empty? && !@workflows.revision_budget_exhausted?(work, candidate.name) }
      if start_transition
        return start_transition.to_s if candidates.any? { |candidate| candidate.name == start_transition.to_s }

        raise ContractError, "transition #{start_transition.inspect} does not start work from #{state}"
      end
      return nil if candidates.empty?
      if candidates.length > 1
        raise ContractError, "state #{state} has several starting transitions (#{candidates.map(&:name).sort.join(", ")}); name one with --start"
      end

      candidates.first.name
    end

    def run_dispatched_job(work, workflow, job, secrets)
      stale = @workflows.superseded_dispatch?(work.fetch("id"), job)
      return { "job" => job, "fenced" => stale } if stale

      phase = job.fetch("phase")
      bundle = compile_bundle(work, phase)
      channel = @channel_factory&.call(bundle, phase)
      bundle = bundle.merge("agent_request_channel" => { "path" => channel.path }) if channel
      runner = @runner_factory.call(phase, bundle, work)

      begin
        executed = @engine.execute(job, runner: runner, bundle: bundle, secrets: secrets, request_channel: channel)
      rescue Backstage::ActivityConflictError
        raise
      rescue Backstage::ConflictError => error
        return { "job" => job, "fenced" => error.message }
      end
      outcome = executed.fetch("outcome")
      follow = @workflows.transition_for_outcome(job, outcome)
      step = { "phase" => phase, "job" => executed.fetch("job"), "run" => executed.fetch("run"),
               "outcome" => outcome, "capture" => executed["capture"] }.compact

      return step.merge("transition" => nil, "note" => "no configured transition for #{outcome.fetch("status")}") unless follow

      evidence = [executed.dig("change_artifact", "id"), executed.dig("review_verdict_artifact", "id")].compact
      stale = @workflows.superseded_dispatch?(work.fetch("id"), job)
      return step.merge("transition" => nil, "fenced" => stale) if stale

      # A success or verdict transition speaks for the run that produced it; a failure transition is
      # the controller's own judgment about execution.
      actor = outcome.fetch("status") == "succeeded" ? @workflows.outcome_actor(executed.fetch("run"), outcome) : CONTROLLER
      begin
        result = @workflows.request_transition(
          work_item_id: work.fetch("id"),
          transition: follow,
          actor: actor,
          request_id: "job:#{job.fetch("id")}:#{follow}",
          reason: outcome["summary"],
          evidence: evidence,
          run_id: executed.fetch("run").fetch("id")
        )
        step.merge("transition" => result.fetch("transition"), "decision" => result["decision"]).compact
      rescue Backstage::ActivityConflictError
        raise
      rescue Backstage::ConflictError, Backstage::InvalidTransition => error
        # The work item moved while this run was in flight. Its outcome stays on the run; a
        # cancelled or replaced run may not overwrite a later decision.
        step.merge("transition" => nil, "fenced" => error.message)
      end
    end

    def compile_bundle(work, phase)
      bundle = @configuration.compile(work_item: work_item_fields(work))
      prior_change = @engine.store.find("external_actions", idempotency_key: "github-pr:v1:#{work.fetch("id")}")
      if prior_change && prior_change["status"] == "succeeded"
        bundle["repository"]["revision"] = bundle["repository"]["branch"]
        bundle["repository"]["resume_existing_change"] = true
      end
      bundle = build_review_bundle(bundle, work) if phase == "review"
      workflow = @workflows.workflow_for(work)
      role = phase == "review" ? "reviewer" : "agent"
      bundle["workflow_context"] = {
        "binding" => workflow.binding, "state" => work.fetch("state"), "revision" => work.fetch("revision"),
        "role" => role,
        "transitions" => workflow.transitions.values.select { |row| row.allows?(role) }.map { |row| row.to_h.merge("name" => row.name) },
        "candidate" => work["candidate"]
      }.compact
      bundle["harness"]["prompt"] += <<~INSTRUCTIONS

        Your workflow position and permitted transitions are in workflow_context in the job bundle.
        For progress or a question, use backstage-agent-request TRANSITION REASON, or
        backstage-agent-request --json '{"transition":"NAME","request_id":"stable-id","decision":{"question":"..."}}'.
        Requests are checked by the controller; they never grant authority. Do not claim human decisions.
        The controller persists candidate/review evidence and performs required completion transitions
        after your structured outcome, so you do not need to invent artifact ids or mark yourself approved.
      INSTRUCTIONS
      decisions = @engine.store.list("decisions").select { |row| row["work_item_id"] == work.fetch("id") && row["status"] == "answered" }
      unless decisions.empty?
        context = decisions.map { |row| row.slice("id", "question", "context", "answer") }
        bundle["harness"]["prompt"] = "#{bundle.dig("harness", "prompt")}\n\nRecorded human decisions for this assignment:\n#{JSON.generate(context)}"
      end
      return bundle if phase == "review"

      findings = outstanding_findings(work)
      return bundle unless findings

      bundle.merge("harness" => bundle.fetch("harness").merge(
        "prompt" => "#{bundle.dig("harness", "prompt")}\n\nA previous independent review returned findings. Address them:\n#{findings}"
      ))
    end

    def work_item_fields(work)
      @engine.store.fetch!("work_items", work.fetch("id"))
        .slice("id", "idempotency_key", "title", "description", "source", "source_ref", "target", "source_instance", "source_identity")
    end

    def build_review_bundle(bundle, work)
      review = JSON.parse(JSON.generate(bundle))
      review["id"] = Records.id("bundle-review")
      review["repository"]["revision"] = review["repository"]["branch"]
      review["harness"]["prompt"] = <<~PROMPT.strip
        Independently review the implementation for work item #{work.fetch("id")}.
        Inspect the branch, run focused checks where needed, and return only JSON:
        {"verdict":"approved|changes_requested|blocked","summary":"concrete review result"}
        You may not modify, push, merge, or deploy anything.
        Implementation outcome: #{JSON.generate(last_implementation_outcome(work).slice("status", "summary", "assistant_text", "stop_reason"))}
      PROMPT
      review["policy"] = review.fetch("policy").merge(
        "role" => "independent_review",
        "allowed_actions" => ["clone"],
        "forbidden_actions" => %w[modify commit push_branch create_draft_pr push_default merge deploy]
      )
      review
    end

    def runs_for(work)
      jobs = @engine.store.list("jobs").select { |job| job["work_item_id"] == work.fetch("id") }
      ids = jobs.map { |job| job.fetch("id") }
      @engine.store.list("runs").select { |run| ids.include?(run["job_id"]) }
    end

    def last_run(work, phase)
      runs_for(work).select { |run| run["phase"] == phase && run["outcome"] }.max_by { |run| run["updated_at"].to_s }
    end

    def last_implementation_outcome(work)
      last_run(work, "implementation")&.fetch("outcome") || {}
    end

    def last_implementation_summary(work)
      last_implementation_outcome(work)["summary"] || "Implementation completed"
    end

    def outstanding_findings(work)
      review = last_run(work, "review")
      verdict = review&.dig("outcome", "review")
      return nil unless verdict && verdict["verdict"] != "approved"

      verdict["summary"]
    end

    def latest_verdict(work)
      runs_for(work).select { |run| run.dig("outcome", "review") }.max_by { |run| run["updated_at"].to_s }&.dig("outcome", "review", "verdict")
    end

    def completion_authority(work, completion)
      return nil unless completion.dig("actor", "role") == "reviewer"

      candidate = work.dig("candidate", "sha256")
      approval = Array(work["artifacts"]).select { |row| row["kind"] == "review_verdict" }.find do |row|
        Array(completion["evidence"]).include?(row.fetch("id")) &&
          row.dig("provenance", "verdict") == "approved" &&
          row.dig("provenance", "independent") == true &&
          !row.dig("provenance", "reviewer_session_id").to_s.empty? &&
          row.dig("provenance", "candidate_sha256") == candidate
      end
      return nil unless approval

      "Independent review approved by #{approval.dig("provenance", "reviewer_session_id")}: #{last_review_summary(work)}"
    end

    def last_review_summary(work)
      last_run(work, "review")&.dig("outcome", "review", "summary") || "approved"
    end

    def request_id(work, transition)
      "controller:#{work.fetch("id")}:#{work.fetch("revision")}:#{transition}"
    end
  end
end
