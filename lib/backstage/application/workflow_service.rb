# frozen_string_literal: true

require "json"
require "digest"

module Backstage::Application
  # The one transition operation. CLI requests, normalized agent requests, reviewer outcomes, and
  # human decisions all arrive here, are checked the same way, and are recorded in one store commit
  # together with any decision they raise or answer and any execution they request.
  class WorkflowService
    AuthorityError = Backstage::AuthorityError
    ConflictError = Backstage::ConflictError
    ContractError = Backstage::ContractError
    NotFound = Backstage::NotFound
    Records = Backstage::Domain::Records
    Workflow = Backstage::Domain::Workflow

    # Authority comes from the entry context, never from request text. A worker's own claim about
    # who it is carries no weight; the channel it arrived on decides what it may be.
    ENTRY_ROLES = {
      # A trusted local operator. May speak for a human or for the system, never for a worker.
      "operator_cli" => %w[human system],
      # Backstage's own judgment: advancing the graph, recording a failure, recovering.
      "controller" => %w[system],
      # A live request from a running worker, scoped to the run that owns its channel.
      "worker_channel" => %w[agent reviewer],
      # A role read off a completed run's persisted outcome, not off model text.
      "run_outcome" => %w[agent reviewer]
    }.freeze

    # Evidence kinds name the artifact kind that satisfies them.
    EVIDENCE_ARTIFACTS = { "change_candidate" => "binary_patch", "review_verdict" => "review_verdict" }.freeze

    attr_reader :store

    def initialize(store:)
      @store = store
    end

    def workflow_for(work)
      binding = work.fetch("workflow") { raise ContractError, "work item #{work["id"]} has no workflow binding" }
      digest = binding.fetch("digest")
      @compiled ||= {}
      @compiled[digest] ||= begin
        snapshot = store.fetch("workflow_snapshots", digest) ||
                   raise(NotFound, "workflow snapshot #{digest} for #{binding.fetch("name")} was not found")
        compiled = Workflow.compile(snapshot.fetch("definition"))
        unless compiled.binding == binding
          raise ContractError, "workflow snapshot does not match the work item binding"
        end
        compiled
      end
    end

    def allowed_transitions(work_item_id)
      work = fetch_work(work_item_id)
      workflow = workflow_for(work)
      decision = open_decision(work)
      workflow.transitions_from(work["state"]).map do |transition|
        blocked = block_reason(work, workflow, transition, decision)
        {
          "name" => transition.name,
          "description" => transition.description,
          "from" => work.fetch("state"),
          "to" => transition.to,
          "actors" => transition.actors,
          "requires" => transition.requires,
          "counts_revision" => transition.counts_revision?,
          "dispatches" => transition.dispatch&.phase,
          "answers_decision" => decision && decision.fetch("choices").include?(transition.name) ? decision.fetch("id") : nil,
          "available" => blocked.nil?,
          "blocked_reason" => blocked
        }.compact
      end
    end

    def history(work_item_id)
      fetch_work(work_item_id)
      store.list("work_transitions").select { |row| row["work_item_id"] == work_item_id }.sort_by { |row| row.fetch("revision") }
    end

    def decisions(work_item_id)
      store.list("decisions").select { |row| row["work_item_id"] == work_item_id }
    end

    def open_decision(work)
      decisions(work.fetch("id")).find { |row| row["status"] == "open" }
    end

    # Who a run's own outcome speaks for. The phase decides the role and the run supplies the
    # identity; nothing the model wrote is consulted.
    def outcome_actor(run, outcome)
      role = run.fetch("phase") == "review" ? "reviewer" : "agent"
      identity = outcome.dig("review", "reviewer_session_id") || run.fetch("id")
      { "role" => role, "id" => identity, "entry" => "run_outcome" }
    end

    # The workflow's dispatch block, carried on the job, is what maps an outcome to the next
    # transition. Controller and recovery read it the same way.
    def transition_for_outcome(job, outcome)
      if outcome["status"] == "succeeded"
        verdict = outcome.dig("review", "verdict")
        return verdict ? (job["on_verdict"] || {})[verdict] : job["on_success"]
      end

      job["on_failure"]
    end

    # True when a newer dispatch has been recorded for this work item, which is how a cancelled or
    # replaced run is stopped from writing over a later decision.
    def superseded_dispatch?(work_item_id, job)
      job = store.fetch("jobs", job.fetch("id")) || job
      latest = store.list("work_transitions")
                    .select { |row| row["work_item_id"] == work_item_id && row["job_id"] }
                    .max_by { |row| row.fetch("revision") }
      if latest && latest.fetch("id") != job["transition_id"]
        return "dispatch #{job["transition_id"]} was superseded by #{latest.fetch("id")}"
      end
      work = fetch_work(work_item_id)
      workflow = workflow_for(work)
      boundary = history(work_item_id).find do |row|
        next false unless row.fetch("revision") > job.fetch("work_revision", 0)
        next false if row.fetch("from") == row.fetch("to")
        progress = row["run_id"] && row["run_id"] == job["run_id"] &&
                   row.dig("actor", "entry") == "worker_channel" && !row["job_id"] &&
                   !row["decision_id"] && !workflow.terminal?(row.fetch("to"))
        !progress
      end
      "dispatch #{job["transition_id"]} was superseded by transition #{boundary.fetch("id")}" if boundary
    end

    def revision_budget_exhausted?(work, transition_name)
      workflow = workflow_for(work)
      return false unless workflow.transition?(transition_name)
      return false unless workflow.transition(transition_name).counts_revision?

      work.fetch("revisions_used", 0) >= workflow.max_revisions
    end

    # Records a transition. `request_id` makes the call idempotent: an identical repeat returns the
    # recorded result, and reuse for a different transition is refused.
    def request_transition(work_item_id:, transition:, actor:, request_id:,
                           expected_state: nil, expected_revision: nil, reason: nil,
                           evidence: [], decision: nil, decision_id: nil, run_id: nil,
                           job: nil)
      raise ContractError, "request_id is required" if request_id.to_s.empty?

      actor = normalize_actor(actor)
      request = {
        "work_item_id" => work_item_id, "transition" => transition.to_s, "actor" => actor,
        "reason" => reason, "evidence" => Array(evidence), "decision" => decision,
        "decision_id" => decision_id, "run_id" => run_id, "job" => job
      }
      fingerprint = Digest::SHA256.hexdigest(JSON.generate(canonical(request)))
      transition_id = "transition-#{Digest::SHA256.hexdigest(request_id)}"
      recorded = store.find("work_transitions", request_id: request_id)
      return replay(recorded, transition, fingerprint) if recorded

      work = fetch_work(work_item_id)
      workflow = workflow_for(work)
      from = work.fetch("state")
      check_expectations!(work, expected_state, expected_revision)
      run_guards = authorize_run!(work, actor, run_id)

      definition = workflow.transitions_from(from).find { |candidate| candidate.name == transition.to_s }
      unless definition
        available = workflow.transitions_from(from).map(&:name).sort
        raise Backstage::InvalidTransition,
              "workflow #{workflow.name} has no transition #{transition.inspect} from #{from}; available: #{available.join(", ")}"
      end
      unless definition.allows?(actor.fetch("role"))
        raise AuthorityError, "actor #{actor.fetch("role")} may not take #{definition.name}; permitted: #{definition.actors.join(", ")}"
      end
      if definition.counts_revision? && work.fetch("revisions_used", 0) >= workflow.max_revisions
        raise ContractError, "workflow #{workflow.name} allows #{workflow.max_revisions} revision(s) and #{definition.name} would exceed it"
      end

      answered = answer_for(work, workflow, definition, decision_id, actor)
      candidate = validate_evidence!(work, workflow, definition, evidence, run_id: run_id)

      revision = work.fetch("revision") + 1
      raised = raise_decision(work, workflow, definition, revision, candidate, decision)
      transition_record = Records.work_transition(
        work_item_id: work.fetch("id"),
        request_id: request_id,
        transition: definition.name,
        from: from,
        to: definition.to,
        workflow: work.fetch("workflow"),
        actor: actor,
        revision: revision,
        reason: reason,
        evidence: Array(evidence),
        decision_id: raised&.fetch("id") || answered&.fetch("id"),
        run_id: run_id
      )
      transition_record = transition_record.merge("id" => transition_id, "request_fingerprint" => fingerprint)
      dispatch_job = build_dispatch(work, definition, transition_record, request_id, job)
      transition_record = transition_record.merge("job_id" => dispatch_job&.fetch("id")).compact

      updated = work.merge(
        "state" => definition.to,
        "revision" => revision,
        "revisions_used" => work.fetch("revisions_used", 0) + (definition.counts_revision? ? 1 : 0),
        "updated_at" => Records.timestamp
      )
      updated = updated.merge("candidate" => candidate) if candidate
      updated = if raised
                  updated.merge("open_decision_id" => raised.fetch("id"))
                else
                  updated.except("open_decision_id")
                end

      writes = [["work_items", updated], ["work_transitions", transition_record]]
      writes << ["decisions", raised] if raised
      writes << ["decisions", answered.merge(
        "status" => "answered",
        "answer" => { "choice" => definition.name, "actor" => actor, "reason" => reason, "request_id" => request_id, "at" => Records.timestamp },
        "updated_at" => Records.timestamp
      )] if answered
      writes << ["jobs", dispatch_job] if dispatch_job
      store.commit(writes, expect: [
        { collection: "work_items", id: work.fetch("id"), revision: work.fetch("revision") },
        { collection: "work_transitions", id: transition_id, revision: nil },
        *run_guards
      ], activity: transition_events(updated, transition_record, definition, raised, answered))

      {
        "work_item" => updated,
        "transition" => transition_record,
        "decision" => raised || (answered && store.fetch("decisions", answered.fetch("id"))),
        "job" => dispatch_job,
        "deduplicated" => false
      }.compact
    rescue Backstage::ActivityConflictError
      # The replay path below answers "someone else already applied this request". An event id
      # reused for a different fact is not that: nothing was applied, and treating it as a replay
      # would return a transition record while its history is silently missing.
      raise
    rescue Backstage::Error
      # A concurrent caller may have applied this exact request between the identity check above and
      # here, which turns this caller's re-derivation into an error about a state that has already
      # moved — a lost guard, but equally a transition that is no longer offered from where the work
      # now is. Identity is bound to the request, so a recorded one is answered with what it
      # recorded; anything else is the caller's own error and is raised.
      recorded = store.find("work_transitions", request_id: request_id)
      return replay(recorded, transition, fingerprint) if recorded && fingerprint
      raise
    end

    private

    def recorder
      @recorder ||= ActivityRecorder.new(store: store, adapter: "backstage.application.workflow_service")
    end

    # The transition and the history that explains it are one commit, so the guard that fences a
    # stale caller fences both. Provenance stays `core` because this is the operation that checked
    # the actor's authority and applied the move; who asked for it is recorded from the entry
    # context in `data`, never from the request's own text. `reason` is left out on purpose — it is
    # often an agent's own summary, and a summary line is not where model text belongs.
    def transition_events(work, record, definition, raised, answered)
      transition_id = record.fetch("id")
      applied = recorder.event(
        type: "work.transition_applied",
        event_id: ActivityRecorder.event_id("work.transition_applied", transition_id),
        occurred_at: record["recorded_at"],
        work_item_id: work.fetch("id"),
        target_id: work["target"],
        request_id: record.fetch("request_id"),
        transition_id: transition_id,
        decision_id: record["decision_id"],
        job_id: record["job_id"],
        run_id: record["run_id"],
        summary: "#{record.fetch("transition")} moved #{work.fetch("id")} from #{record.fetch("from")} to #{record.fetch("to")} at revision #{record.fetch("revision")}",
        data: {
          "transition" => record.fetch("transition"),
          "from" => record.fetch("from"),
          "to" => record.fetch("to"),
          "revision" => record.fetch("revision"),
          "revisions_used" => work.fetch("revisions_used", 0),
          "workflow" => record.fetch("workflow_name"),
          "workflow_digest" => record.fetch("workflow_digest"),
          "actor" => ActivityRecorder.actor_data(record.fetch("actor")),
          "counts_revision" => definition.counts_revision?,
          "evidence" => Array(record["evidence"]).length,
          "dispatched_job" => record["job_id"]
        }
      )
      events = [applied]
      if raised
        events << recorder.event(
          type: "decision.raised",
          event_id: ActivityRecorder.event_id("decision.raised", transition_id, raised.fetch("id")),
          occurred_at: raised.fetch("created_at"),
          work_item_id: work.fetch("id"),
          target_id: work["target"],
          request_id: record.fetch("request_id"),
          transition_id: transition_id,
          decision_id: raised.fetch("id"),
          causation_event_id: applied.fetch("event_id"),
          summary: "decision #{raised.fetch("id")} raised in #{raised.fetch("state")} with #{raised.fetch("choices").length} choice(s)",
          data: { "state" => raised.fetch("state"), "choices" => raised.fetch("choices"),
                  "raised_by_transition" => raised.fetch("raised_by_transition"),
                  "work_revision" => raised.fetch("work_revision"),
                  "candidate_sha256" => raised["candidate_sha256"] }
        )
      end
      if answered
        events << recorder.event(
          type: "decision.answered",
          event_id: ActivityRecorder.event_id("decision.answered", transition_id, answered.fetch("id")),
          occurred_at: record["recorded_at"],
          work_item_id: work.fetch("id"),
          target_id: work["target"],
          request_id: record.fetch("request_id"),
          transition_id: transition_id,
          decision_id: answered.fetch("id"),
          causation_event_id: applied.fetch("event_id"),
          summary: "decision #{answered.fetch("id")} answered with #{record.fetch("transition")}",
          data: { "choice" => record.fetch("transition"),
                  "choices" => answered.fetch("choices"),
                  "actor" => ActivityRecorder.actor_data(record.fetch("actor")),
                  "work_revision" => answered.fetch("work_revision") }
        )
      end
      events
    end

    def fetch_work(work_item_id)
      store.fetch("work_items", work_item_id) || raise(NotFound, "work_items #{work_item_id} was not found")
    end

    def canonical(value)
      case value
      when Hash then value.keys.sort.to_h { |key| [key, canonical(value.fetch(key))] }
      when Array then value.map { |item| canonical(item) }
      else value
      end
    end

    def authorize_run!(work, actor, run_id)
      worker = %w[worker_channel run_outcome].include?(actor.fetch("entry"))
      raise AuthorityError, "worker authority requires a persisted run id" if worker && run_id.to_s.empty?
      return [] unless run_id

      run = store.fetch("runs", run_id) || raise(AuthorityError, "run #{run_id} is not recorded")
      job = store.fetch("jobs", run.fetch("job_id")) || raise(AuthorityError, "run job is not recorded")
      unless run["work_item_id"] == work.fetch("id") && job["work_item_id"] == work.fetch("id")
        raise AuthorityError, "run #{run_id} belongs to another work item"
      end
      superseded = superseded_dispatch?(work.fetch("id"), job)
      raise ConflictError, superseded if superseded
      if job["run_id"] && job["run_id"] != run_id
        raise ConflictError, "run #{run_id} has been replaced"
      end
      if actor.fetch("entry") == "worker_channel"
        unless run["status"] == "running" && !run["cancellation_requested_at"]
          raise ConflictError, "run #{run_id} is no longer accepting worker requests"
        end
      elsif actor.fetch("entry") == "run_outcome"
        unless run.dig("outcome", "status") == "succeeded" && !run["cancellation_requested_at"]
          raise AuthorityError, "run #{run_id} has no uncancelled successful outcome"
        end
      end
      if worker
        role = run.fetch("phase") == "review" ? "reviewer" : "agent"
        identity = actor.fetch("entry") == "worker_channel" ? run_id : outcome_actor(run, run.fetch("outcome")).fetch("id")
        unless actor["role"] == role && actor["id"] == identity
          raise AuthorityError, "actor does not match the persisted run authority"
        end
      end
      [
        { collection: "runs", id: run_id, fields: { status: run["status"], cancellation_requested_at: run["cancellation_requested_at"] } },
        { collection: "jobs", id: job.fetch("id"), fields: { status: job["status"], run_id: job["run_id"] } }
      ]
    end

    def normalize_actor(actor)
      actor = { "role" => actor.to_s } if actor.is_a?(String) || actor.is_a?(Symbol)
      raise ContractError, "actor must name a role and an entry context" unless actor.is_a?(Hash)

      normalized = { "role" => actor["role"].to_s, "id" => actor["id"], "entry" => actor["entry"].to_s }.compact
      permitted = ENTRY_ROLES.fetch(normalized["entry"]) do
        raise ContractError, "unknown actor entry context #{normalized["entry"].inspect}; expected #{ENTRY_ROLES.keys.join(", ")}"
      end
      unless permitted.include?(normalized.fetch("role"))
        raise AuthorityError, "#{normalized["entry"]} requests may not act as #{normalized.fetch("role")}; permitted: #{permitted.join(", ")}"
      end

      normalized
    end

    def check_expectations!(work, expected_state, expected_revision)
      if expected_state && work.fetch("state") != expected_state.to_s
        raise ConflictError, "work item #{work.fetch("id")} is in #{work.fetch("state")}, not #{expected_state}"
      end
      return unless expected_revision && work.fetch("revision") != expected_revision

      raise ConflictError, "work item #{work.fetch("id")} is at revision #{work.fetch("revision")}, not #{expected_revision}"
    end

    def replay(recorded, transition, fingerprint)
      unless recorded.fetch("transition") == transition.to_s
        raise ContractError,
              "request #{recorded.fetch("request_id")} already recorded #{recorded.fetch("transition")}, not #{transition}"
      end

      unless recorded["request_fingerprint"] == fingerprint
        raise ContractError, "request #{recorded.fetch("request_id")} already recorded a different request payload"
      end

      {
        "work_item" => store.fetch("work_items", recorded.fetch("work_item_id")),
        "transition" => recorded,
        "decision" => recorded["decision_id"] && store.fetch("decisions", recorded.fetch("decision_id")),
        "job" => recorded["job_id"] && store.fetch("jobs", recorded.fetch("job_id")),
        "deduplicated" => true
      }.compact
    end

    def block_reason(work, workflow, transition, decision)
      if transition.counts_revision? && work.fetch("revisions_used", 0) >= workflow.max_revisions
        return "revision budget of #{workflow.max_revisions} is exhausted"
      end
      if decision && !decision.fetch("choices").include?(transition.name)
        return "decision #{decision.fetch("id")} is open and does not offer this choice"
      end

      nil
    end

    def answer_for(work, workflow, definition, decision_id, actor)
      awaiting = workflow.state(work.fetch("state")).awaits_decision?
      unless awaiting
        raise ContractError, "work item #{work.fetch("id")} has no open decision to answer" if decision_id
        return nil
      end

      unless actor.fetch("role") == "human" && actor.fetch("entry") == "operator_cli"
        raise AuthorityError, "a human decision requires the trusted operator entry context"
      end
      decision = open_decision(work)
      raise ContractError, "work item #{work.fetch("id")} awaits a decision that is not recorded" unless decision
      raise ContractError, "answering #{decision.fetch("id")} requires its decision id" if decision_id.to_s.empty?
      unless decision_id.to_s == decision.fetch("id")
        raise ConflictError, "decision #{decision_id} is not the open decision #{decision.fetch("id")}"
      end
      unless decision.fetch("choices").include?(definition.name)
        raise ContractError, "decision #{decision.fetch("id")} offers #{decision.fetch("choices").join(", ")}, not #{definition.name}"
      end
      unless decision.fetch("work_revision") == work.fetch("revision")
        raise ConflictError, "decision #{decision.fetch("id")} was raised at a different work revision"
      end
      unless decision["candidate_sha256"] == work.dig("candidate", "sha256")
        raise ConflictError, "decision #{decision.fetch("id")} was raised against a different candidate and cannot authorize the current one"
      end

      decision
    end

    def raise_decision(work, workflow, definition, revision, candidate, supplied)
      return nil unless workflow.state(definition.to).awaits_decision?

      declared = definition.decision
      raise ContractError, "transition #{definition.name} enters #{definition.to} without a decision question" unless declared

      supplied ||= {}
      raise ContractError, "decision override must be an object" unless supplied.is_a?(Hash)

      choices = Array(supplied["choices"]).map(&:to_s)
      choices = declared.fetch("choices") if choices.empty?
      unknown = choices - declared.fetch("choices")
      raise ContractError, "decision choices #{unknown.join(", ")} are not offered by #{definition.name}" unless unknown.empty?

      Records.decision(
        work_item_id: work.fetch("id"),
        work_revision: revision,
        transition: definition.name,
        state: definition.to,
        question: supplied["question"].to_s.empty? ? declared.fetch("question") : supplied["question"].to_s,
        choices: choices,
        candidate_sha256: (candidate || work["candidate"])&.fetch("sha256", nil),
        context: supplied["context"]
      )
    end

    def validate_evidence!(work, workflow, definition, evidence, run_id: nil)
      artifacts = Array(evidence).map do |artifact_id|
        artifact = store.fetch("artifacts", artifact_id) || raise(NotFound, "artifacts #{artifact_id} was not found")
        unless artifact.fetch("work_item_id") == work.fetch("id")
          raise AuthorityError, "artifact #{artifact_id} belongs to #{artifact.fetch("work_item_id")}, not #{work.fetch("id")}"
        end

        artifact
      end

      candidate = nil
      definition.requires.each do |kind|
        wanted = EVIDENCE_ARTIFACTS.fetch(kind)
        artifact = artifacts.find { |row| row.fetch("kind") == wanted }
        raise ContractError, "transition #{definition.name} requires #{kind} evidence (artifact kind #{wanted})" unless artifact

        if run_id && artifact["run_id"] != run_id
          raise AuthorityError, "artifact #{artifact.fetch("id")} belongs to a different run"
        end
        case kind
        when "change_candidate"
          candidate = { "artifact_id" => artifact.fetch("id"), "sha256" => artifact.fetch("sha256"), "run_id" => artifact["run_id"] }
        when "review_verdict"
          validate_review_verdict!(work, workflow, definition, artifact)
        end
      end
      candidate
    end

    def validate_review_verdict!(work, workflow, definition, artifact)
      provenance = artifact["provenance"] || {}
      raise AuthorityError, "review verdict #{artifact.fetch("id")} is not marked independent" unless provenance["independent"] == true
      raise AuthorityError, "review verdict #{artifact.fetch("id")} has no reviewer session" if provenance["reviewer_session_id"].to_s.empty?

      current = work.dig("candidate", "sha256")
      raise ContractError, "work item #{work.fetch("id")} has no candidate to review" if current.to_s.empty?
      unless provenance["candidate_sha256"] == current
        raise ConflictError, "review verdict #{artifact.fetch("id")} reviewed a different candidate than the current one"
      end

      expected = expected_verdict(workflow, definition)
      raise ContractError, "review verdict requirement has no unambiguous expected verdict" unless expected

      unless provenance["verdict"] == expected
        raise ContractError, "transition #{definition.name} needs a #{expected} verdict, not #{provenance["verdict"].inspect}"
      end
    end

    # The workflow's own on_verdict map is what ties a verdict to a transition, so nothing here
    # hardcodes reviewer vocabulary.
    def expected_verdict(workflow, definition)
      workflow.transitions.each_value do |candidate|
        map = candidate.dispatch&.on_verdict
        next unless map

        verdict = map.key(definition.name)
        return verdict if verdict
      end
      nil
    end

    def build_dispatch(work, definition, transition_record, request_id, job_overrides)
      dispatch = definition.dispatch
      return nil unless dispatch

      Records.job(
        work_item_id: work.fetch("id"),
        policy_name: (job_overrides || {})["policy_name"] || work.dig("workflow", "name"),
        bundle: (job_overrides || {})["bundle"],
        phase: dispatch.phase,
        transition_id: transition_record.fetch("id"),
        request_id: "#{request_id}:dispatch"
      ).merge(
        "on_success" => dispatch.on_success,
        "on_verdict" => dispatch.on_verdict,
        "on_failure" => dispatch.on_failure,
        "dispatch_state" => transition_record.fetch("to"),
        "work_revision" => transition_record.fetch("revision")
      ).compact
    end
  end
end
