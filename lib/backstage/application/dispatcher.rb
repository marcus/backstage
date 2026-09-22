# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Application
  # Durable local execution of explicitly accepted work.
  #
  # The dispatcher is a small interpreter of what is already persisted. It never scans the store for
  # work to adopt: an operator accepts a work item, which records an execution intent, and only
  # intents are ever dispatched. Every pass reconciles before it acts, so a restart resumes from
  # what actually happened rather than from what a previous process remembered.
  #
  # Checkpoints are the store commits around each dispatch. Claiming an attempt, scheduling a retry,
  # and recording a result are separate durable writes guarded on the intent's revision, which is
  # what makes a crash lose at most the current attempt and never the accounting for it.
  class Dispatcher
    ConflictError = Backstage::ConflictError
    ActivityConflictError = Backstage::ActivityConflictError
    ContractError = Backstage::ContractError
    NotFound = Backstage::NotFound
    Records = Backstage::Domain::Records
    RetryPolicy = Backstage::Domain::RetryPolicy
    COLLECTION = "execution_intents"
    # One record per work item naming its current acceptance. It carries no lifecycle state of its
    # own; it exists so that two operators accepting the same work at the same time contend for one
    # store guard instead of both creating an intent.
    ACCEPTANCES = "execution_acceptances"
    MODES = %w[fake publish_draft].freeze
    TERMINAL_STATUSES = %w[completed cancelled exhausted].freeze
    ACTIVE_STATUSES = %w[queued running waiting delayed uncertain blocked].freeze
    STATUSES = (ACTIVE_STATUSES + TERMINAL_STATUSES).freeze
    # How many times an attempt may end before it produced a run before the dispatcher stops trying.
    # Bounding the repetition is what a crash loop needs; spending a failure retry it never used is not.
    EMPTY_CLAIM_LIMIT = 3
    EXPLANATIONS = {
      "queued" => "accepted and eligible; the next dispatcher pass will run it",
      "running" => "an execution is in flight and no replacement will be launched",
      "waiting" => "a human answer is required before anything else happens",
      "delayed" => "a bounded retry is scheduled and will not run before its due time",
      "uncertain" => "runtime status could not be established; resolve it explicitly",
      "blocked" => "the dispatcher stopped and will not act without an operator",
      "completed" => "the work item reached a terminal state in its workflow",
      "cancelled" => "the operator stopped this acceptance",
      "exhausted" => "no automatic attempts remain"
    }.freeze

    attr_reader :authorized_mode

    def initialize(engine:, workflows:, recovery:, controller_factory:, clock:, authorized_mode: "fake",
                   default_retry_policy: RetryPolicy::DEFAULT)
      raise ContractError, "unknown dispatcher mode #{authorized_mode.inspect}" unless MODES.include?(authorized_mode)

      @engine = engine
      @workflows = workflows
      @recovery = recovery
      @controller_factory = controller_factory
      @clock = clock
      @authorized_mode = authorized_mode
      @default_retry_policy = RetryPolicy.normalize(default_retry_policy)
    end

    def store
      @engine.store
    end

    # Durable acceptance. The intent id is derived from the acceptance request identity, so the
    # store's own absence guard makes deduplication atomic between concurrent operators.
    def accept(work_item_id:, request_id: nil, mode: "fake", start_transition: nil, retry_policy: {}, accepted_by: nil, supersede: false)
      raise ContractError, "unknown execution mode #{mode.inspect}" unless MODES.include?(mode)

      work = store.fetch("work_items", work_item_id) || raise(NotFound, "work_items #{work_item_id} was not found")
      # Without an explicit identity, a repeat of the same command deduplicates onto the acceptance
      # it already made, while asking for a new generation — after a cancellation, an exhaustion, or
      # with supersede — mints a new one. Getting this wrong makes the documented recovery command a
      # silent no-op, so the rule lives here rather than in a surface.
      request_id = default_request_id(work_item_id, mode, supersede) if request_id.to_s.empty?
      workflow = @workflows.workflow_for(work)
      if workflow.terminal?(work.fetch("state"))
        raise ContractError, "work item #{work_item_id} is already in terminal state #{work.fetch("state")}"
      end
      if start_transition && !workflow.transition?(start_transition.to_s)
        raise ContractError, "workflow #{workflow.name} has no transition #{start_transition.inspect}"
      end

      overrides = retry_policy || {}
      raise ContractError, "retry policy must be an object" unless overrides.is_a?(Hash)

      policy = RetryPolicy.normalize(@default_retry_policy.merge(RetryPolicy.stringify(overrides)))
      id = "intent-#{Digest::SHA256.hexdigest(request_id.to_s)}"
      fingerprint = fingerprint_for(work_item_id: work_item_id, mode: mode, start_transition: start_transition,
                                    policy: policy, supersede: supersede)
      existing = store.fetch(COLLECTION, id)
      return deduplicate(existing, fingerprint, supersede) if existing

      pointer = store.fetch(ACCEPTANCES, work_item_id)
      active = active_intent_for(work_item_id)
      if active && !supersede
        raise ConflictError,
              "work item #{work_item_id} is already accepted as #{active.fetch("id")} (#{active.fetch("status")}); " \
              "cancel it or accept with supersede to start a new generation"
      end

      now = @clock.timestamp
      intent = Records.execution_intent(
        id: id,
        work_item_id: work_item_id,
        request_id: request_id.to_s,
        request_fingerprint: fingerprint,
        mode: mode,
        generation: next_generation(work_item_id),
        retry_policy: policy,
        due_at: now,
        start_transition: start_transition&.to_s,
        accepted_by: accepted_by
      )
      writes = [
        [COLLECTION, intent],
        [ACCEPTANCES, { "schema_version" => 1, "id" => work_item_id, "work_item_id" => work_item_id,
                        "intent_id" => id, "generation" => intent.fetch("generation"), "updated_at" => now }]
      ]
      expect = [
        { collection: COLLECTION, id: id, revision: nil },
        # Whichever concurrent acceptance commits first owns the pointer; the other one conflicts
        # rather than producing a second active intent for the same work.
        if pointer
          { collection: ACCEPTANCES, id: work_item_id, fields: { intent_id: pointer.fetch("intent_id") } }
        else
          { collection: ACCEPTANCES, id: work_item_id, revision: nil }
        end
      ]
      events = [accepted_event(intent, work)]
      if active
        superseded = supersede_record(active, intent, now)
        writes << [COLLECTION, superseded]
        expect << { collection: COLLECTION, id: active.fetch("id"), revision: active.fetch("revision") }
        events << finished_event(superseded, active, events.first.fetch("event_id"))
      end
      store.commit(writes, expect: expect, activity: events)
      cancel_active_runs(active) if active
      intent.merge("deduplicated" => false)
    end

    def cancel(reference, reason: nil)
      intent = resolve(reference)
      return intent if TERMINAL_STATUSES.include?(intent.fetch("status"))

      cancelled = settle(intent, status: "cancelled",
        stop_reason: reason || "cancelled by operator", closed_at: @clock.timestamp)
      cancel_active_runs(intent)
      cancelled
    end

    def intents(all: false)
      rows = store.list(COLLECTION)
      rows = rows.reject { |row| TERMINAL_STATUSES.include?(row.fetch("status")) } unless all
      rows
    end

    def active_intent_for(work_item_id)
      store.list(COLLECTION).find do |row|
        row["work_item_id"] == work_item_id && !TERMINAL_STATUSES.include?(row.fetch("status"))
      end
    end

    def resolve(reference)
      reference = reference.to_s
      raise ContractError, "an intent or work item id is required" if reference.empty?

      direct = store.fetch(COLLECTION, reference)
      return direct if direct

      active_intent_for(reference) || latest_intent_for(reference) ||
        raise(NotFound, "no execution intent for #{reference}")
    end

    # Direct processing stays available, but it may not step past a dispatcher that owns this work.
    def guard_direct_processing!(work_item_id)
      active = active_intent_for(work_item_id)
      return nil unless active

      raise ConflictError,
            "work item #{work_item_id} is accepted as #{active.fetch("id")} (#{active.fetch("status")}); " \
            "let the dispatcher run it, or cancel or supersede the intent to take it back"
    end

    # One bounded pass: reconcile each accepted work item, then make progress where it is due.
    #
    # `stop` is asked before every dispatch, so a signal ends new launches within the pass rather
    # than only between passes. Work the pass declines to start is reported as deferred, never as
    # though something ran.
    def pass(limit: nil, work_item_id: nil, stop: nil)
      started_at = @clock.timestamp
      candidates = intents.select { |row| work_item_id.nil? || row["work_item_id"] == work_item_id }
      candidates = candidates.sort_by { |row| [row["due_at"].to_s, row.fetch("created_at")] }
      dispatched = 0
      deferred = 0
      deferred_by_limit = 0
      reports = candidates.map do |intent|
        stopping = stop&.call ? "the dispatcher is stopping" : nil
        at_limit = limit && dispatched >= limit ? "this pass already dispatched #{dispatched}" : nil
        report = begin
          advance(intent, defer_reason: stopping || at_limit)
        rescue ActivityConflictError
          # An event id reused for a different fact is a defect in this pass, not a fence. Reporting
          # it as "fenced" would let a supervised loop swallow it once per pass forever.
          raise
        rescue ConflictError => error
          # A lost optimistic race is not a verdict about the work; the next pass re-reads it.
          report(fetch_intent(intent.fetch("id")), action: "fenced", detail: error.message)
        rescue Backstage::Error => error
          # One unusable acceptance must not stop a supervised loop from serving the others.
          block(intent, error)
        end
        dispatched += 1 if report["action"] == "dispatched"
        if report["action"] == "deferred"
          deferred += 1
          deferred_by_limit += 1 if at_limit && !stopping
        end
        report
      end
      {
        "schema_version" => 1,
        "pass_started_at" => started_at,
        "pass_finished_at" => @clock.timestamp,
        "authorized_mode" => @authorized_mode,
        "considered" => reports.length,
        "dispatched" => dispatched,
        "deferred" => deferred,
        # Work this pass would have started had it not hit its own limit. The worker uses this to
        # tell "there is more to do right now" apart from "nothing here is actionable".
        "deferred_by_limit" => deferred_by_limit,
        "next_wake_up" => next_wake_up,
        "intents" => reports
      }
    end

    # The earliest time this dispatcher might actually have something to do. Work it cannot consume —
    # an acceptance that needs authorization this process was not started with — is left out, because
    # reporting its long-past due time would tell the worker to wake immediately, forever.
    def next_wake_up
      intents.filter_map do |row|
        next unless %w[queued delayed].include?(row.fetch("status"))
        next unless mode_authorized?(row)

        row["due_at"]
      end.min
    end

    def queue_status
      rows = store.list(COLLECTION)
      {
        "schema_version" => 1,
        "checked_at" => @clock.timestamp,
        "authorized_mode" => @authorized_mode,
        "counts" => STATUSES.to_h { |status| [status, rows.count { |row| row.fetch("status") == status }] },
        "active" => rows.count { |row| ACTIVE_STATUSES.include?(row.fetch("status")) },
        "next_wake_up" => next_wake_up,
        "intents" => intents.map { |row| summarize(row) }
      }
    end

    # Everything an operator needs to know why this piece of work is where it is, and what to do.
    def describe(reference)
      intent = resolve(reference)
      work = store.fetch!("work_items", intent.fetch("work_item_id"))
      workflow = @workflows.workflow_for(work)
      runs = runs_for(work.fetch("id"))
      current = runs.find { |run| %w[queued running].include?(run["status"]) }
      last = runs.max_by { |run| run["created_at"].to_s }
      status = intent.fetch("status")
      {
        "schema_version" => 1,
        "intent" => intent,
        "work_item_id" => work.fetch("id"),
        "title" => work["title"],
        "status" => status,
        "explanation" => EXPLANATIONS.fetch(status),
        "mode" => intent.fetch("mode"),
        "authorized_mode" => @authorized_mode,
        "workflow" => workflow.name,
        "work_state" => work.fetch("state"),
        "work_revision" => work.fetch("revision"),
        "terminal" => workflow.terminal?(work.fetch("state")),
        "awaiting_decision" => workflow.state(work.fetch("state")).awaits_decision?,
        "next_wake_up" => %w[queued delayed].include?(status) ? intent["due_at"] : nil,
        "attempts_used" => intent.fetch("attempts_used"),
        "retries_used" => intent.fetch("retries_used"),
        "retries_remaining" => RetryPolicy.retries_remaining(intent.fetch("retry_policy"), intent.fetch("retries_used")),
        "retry_policy" => intent.fetch("retry_policy"),
        "current_execution" => current && execution_summary(current),
        "last_execution" => last && execution_summary(last),
        "open_decision" => open_decision(work),
        "blocked_reason" => intent["blocked_reason"],
        "stop_reason" => intent["stop_reason"],
        "last_error" => intent["last_error"],
        "actions" => actions_for(intent, work, current)
      }.compact
    end

    private

    def recorder
      @recorder ||= ActivityRecorder.new(store: store, adapter: "backstage.application.dispatcher")
    end

    # An acceptance is a person authorizing work, so its provenance is the operator entry it came
    # through rather than the dispatcher that will consume it. The event id is derived from the
    # intent identity, which is itself derived from the acceptance request, so a retried accept
    # deduplicates onto the recorded intent and appends nothing.
    def accepted_event(intent, work)
      recorder.event(
        type: "execution.accepted",
        event_id: ActivityRecorder.event_id("execution.accepted", intent.fetch("id")),
        provenance: "operator",
        occurred_at: intent.fetch("accepted_at"),
        work_item_id: intent.fetch("work_item_id"),
        target_id: work["target"],
        request_id: intent.fetch("request_id"),
        links: [{ "type" => "execution_intent", "id" => intent.fetch("id") }],
        summary: "accepted #{intent.fetch("work_item_id")} as #{intent.fetch("id")} in #{intent.fetch("mode")} mode",
        data: {
          "intent_id" => intent.fetch("id"),
          "mode" => intent.fetch("mode"),
          "generation" => intent.fetch("generation"),
          "start_transition" => intent["start_transition"],
          "max_retries" => intent.fetch("retry_policy").fetch("max_retries"),
          "accepted_by" => intent["accepted_by"],
          "work_state" => work.fetch("state")
        }
      )
    end

    # The acceptance's own terminal fact: it completed, was cancelled, or ran out of attempts. The
    # run-level terminal record belongs to the engine (`execution.verified_completion`); this one
    # says the dispatcher will do nothing more for this generation.
    def finished_event(settled, previous, causation_event_id = nil)
      recorder.event(
        type: "execution.finished",
        event_id: ActivityRecorder.event_id("execution.finished", settled.fetch("id")),
        occurred_at: settled["closed_at"] || settled["updated_at"],
        work_item_id: settled.fetch("work_item_id"),
        request_id: settled.fetch("request_id"),
        run_id: settled["last_run_id"],
        causation_event_id: causation_event_id,
        links: [{ "type" => "execution_intent", "id" => settled.fetch("id") }],
        summary: "acceptance #{settled.fetch("id")} finished as #{settled.fetch("status")}",
        data: {
          "intent_id" => settled.fetch("id"),
          "status" => settled.fetch("status"),
          "previous_status" => previous.fetch("status"),
          "generation" => settled.fetch("generation"),
          "attempts_used" => settled.fetch("attempts_used"),
          "retries_used" => settled.fetch("retries_used"),
          # `stop_reason` is the only field here that is not an id, a state or a count: an operator's
          # `--reason` reaches it verbatim from the CLI. History is append-only, so it is bounded
          # before it lands. The store's secret guard still sees it, as it sees every event.
          "stop_reason" => settled["stop_reason"] && Backstage::Domain::Activity.bounded_text(settled["stop_reason"])
        }
      )
    end

    # What one write-back of a dispatcher decision changed, said as history. Only two of them are
    # lifecycle facts — a retry becoming due, and an acceptance reaching its end — and both are
    # keyed on identities a repeat produces again, so a re-decided pass reconciles instead of
    # appending. Everything else a pass observes is already in its report and writes no event: a
    # supervised loop polls forever, and history is not a log file.
    def applied_events(current, updated)
      events = []
      status = updated.fetch("status")
      if status == "delayed" && updated.fetch("retries_used") > current.fetch("retries_used")
        events << recorder.event(
          type: "execution.retry_scheduled",
          event_id: ActivityRecorder.event_id("execution.retry_scheduled", updated.fetch("id"), updated.fetch("retries_used")),
          occurred_at: updated["updated_at"],
          work_item_id: updated.fetch("work_item_id"),
          request_id: updated.fetch("request_id"),
          run_id: updated["last_accounted_run_id"],
          causation_event_id: updated["last_accounted_run_id"] &&
            ActivityRecorder.event_id("execution.verified_completion", updated.fetch("last_accounted_run_id")),
          links: [{ "type" => "execution_intent", "id" => updated.fetch("id") }],
          summary: "retry #{updated.fetch("retries_used")}/#{updated.fetch("retry_policy").fetch("max_retries")} " \
                   "scheduled for #{updated.fetch("id")} at #{updated["due_at"]}",
          data: {
            "intent_id" => updated.fetch("id"),
            "due_at" => updated["due_at"],
            "retries_used" => updated.fetch("retries_used"),
            "max_retries" => updated.fetch("retry_policy").fetch("max_retries"),
            "attempts_used" => updated.fetch("attempts_used")
          }
        )
      end
      if TERMINAL_STATUSES.include?(status) && !TERMINAL_STATUSES.include?(current.fetch("status"))
        events << finished_event(updated, current)
      end
      events
    end

    def advance(intent, defer_reason: nil)
      intent = fetch_intent(intent.fetch("id"))
      return report(intent, action: "skipped", detail: "intent is #{intent.fetch("status")}") if TERMINAL_STATUSES.include?(intent.fetch("status"))

      reconciliation = @recovery.reconcile(intent.fetch("work_item_id"))
      findings = reconciliation.fetch("work_items").flat_map { |row| row.fetch("findings") }
      decision = classify(intent, findings)
      if decision["action"] == "dispatch"
        return dispatch(intent) unless defer_reason

        settled = apply(intent, decision.except("action"))
        return report(settled, action: "deferred", detail: "eligible, not started: #{defer_reason}")
      end

      settled = apply(intent, decision)
      report(settled, action: decision.fetch("action", "observed"), detail: decision["detail"])
    end

    # Reads only what is persisted, in the order that keeps the conservative answer first: a live
    # execution and an unresolvable runtime outrank anything the dispatcher would rather do.
    def classify(intent, findings, ignore_claim: false)
      work_item_id = intent.fetch("work_item_id")
      work = store.fetch!("work_items", work_item_id)
      workflow = @workflows.workflow_for(work)
      state = work.fetch("state")

      # Unknown runtime outranks everything: a record nobody can confirm is not evidence of a live
      # worker, and it is never quietly treated as one — nor as finished work.
      if (unknown = findings.find { |row| row["kind"] == "runtime_unknown" })
        return { "status" => "uncertain", "action" => "observed", "blocked_reason" => unknown.fetch("detail") }
      end
      # An unresolved execution keeps this acceptance open even when the work item is already
      # terminal, so a run that is still stopping stays owned by the dispatcher that must reconcile it.
      if @engine.active_execution?(work_item_id)
        return { "status" => "running", "action" => "observed", "detail" => live_execution_detail(intent, work_item_id, workflow.terminal?(state)) }
      end
      if workflow.terminal?(state)
        return { "status" => "completed", "action" => "completed", "stop_reason" => "work item reached terminal state #{state}",
                 "closed_at" => @clock.timestamp }
      end
      # A claim held by a dispatcher that is still alive is honored even before it has produced a
      # run, so two callers racing the same accepted work cannot both start it.
      if !ignore_claim && intent.fetch("status") == "running" && (holder = claim_holder(intent))
        return { "status" => "running", "action" => "observed", "detail" => "claimed by dispatcher #{holder}" }
      end
      if workflow.state(state).awaits_decision?
        return { "status" => "waiting", "action" => "observed", "decision_id" => work["open_decision_id"],
                 "detail" => "waiting for a human answer" }
      end
      # A block is a durable gate, not a one-pass pause: no later pass may quietly step over what
      # stopped this acceptance. Only a person lifts it — a human decision or an operator transition
      # recorded against this work item, or cancelling or superseding the acceptance. Automatic
      # progress never does, and conditions that forbid work outright are re-checked before any lift,
      # so a workflow that moved on cannot clear an unresolved external effect or a crash loop.
      if intent.fetch("status") == "blocked" && (blocked = still_blocked(intent, work))
        return blocked
      end
      if (blocked = findings.find { |row| row["action"] == "blocked" })
        return { "status" => "blocked", "action" => "observed", "blocked_at_revision" => work.fetch("revision"),
                 "blocked_reason" => "recovery could not apply the recorded outcome: #{blocked["error"]}" }
      end

      # A dispatcher that died between claiming an attempt and producing a run left no outcome to
      # read, so the acceptance goes back on the queue rather than being charged for a failure that
      # did not happen. Only a claim that keeps ending this way is stopped.
      if intent.fetch("status") == "running" && claim_holder(intent).nil? && attempt_runs(intent).empty?
        return interrupted_claim(intent, work)
      end

      failure = unaccounted_failure(intent)
      return retry_decision(intent, work, workflow, failure) if failure

      due = intent["due_at"]
      if due && due > @clock.timestamp
        return { "status" => intent.fetch("status") == "delayed" ? "delayed" : intent.fetch("status"),
                 "action" => "observed", "detail" => "not due before #{due}" }
      end
      unless mode_authorized?(intent)
        return { "status" => "queued", "action" => "skipped",
                 "detail" => "intent requires #{intent.fetch("mode")} authorization; this dispatcher is #{@authorized_mode}" }
      end
      unless dispatchable?(work, workflow)
        return { "status" => "blocked", "action" => "observed", "blocked_at_revision" => work.fetch("revision"),
                 "blocked_reason" => "workflow #{workflow.name} offers no automatic dispatch from #{state}" }
      end

      { "action" => "dispatch", "status" => "queued" }
    end

    # A block holds unless a person has moved the work item since it was raised, and never while a
    # reason that forbids the work outright is still true.
    def still_blocked(intent, work)
      persistent = persistent_block_reason(intent, work)
      return nil if persistent.nil? && human_progress_since?(work, intent["blocked_at_revision"])

      reason = persistent || intent["blocked_reason"]
      # The revision stays as recorded when the block was raised, so a person's transition still
      # counts once whatever forbids the work is resolved.
      { "status" => "blocked", "action" => "skipped", "blocked_reason" => reason,
        "blocked_at_revision" => intent["blocked_at_revision"], "detail" => reason }.compact
    end

    # Reasons that forbid work no matter where the workflow has since travelled.
    def persistent_block_reason(intent, work)
      pending = unresolved_effects(work.fetch("id")).first
      if pending
        return "unresolved external effect #{pending.fetch("kind")} (#{pending.fetch("idempotency_key")})"
      end
      return nil if intent.fetch("empty_claims", 0) < EMPTY_CLAIM_LIMIT

      "#{intent.fetch("empty_claims")} dispatch attempts ended before producing a run; " \
        "the dispatcher is not getting far enough to record one"
    end

    # Only authority that came from a person counts as lifting a block. Recovery applying a recorded
    # outcome is the system talking to itself, and must not clear an operator's stop.
    def human_progress_since?(work, revision)
      return false if revision.nil?

      @workflows.history(work.fetch("id")).any? do |row|
        row.fetch("revision") > revision &&
          (row.dig("actor", "role") == "human" || row.dig("actor", "entry") == "operator_cli")
      end
    end

    # A dispatcher that died between claiming an attempt and launching anything produced no failure
    # to retry, so the acceptance goes back on the queue with the attempt recorded. Only a claim that
    # keeps ending this way is stopped, because that is a crash loop rather than a lost process.
    def interrupted_claim(intent, work)
      empty = intent.fetch("empty_claims", 0) + 1
      base = { "empty_claims" => empty,
               "last_error" => "interrupted: the dispatcher exited before its execution produced a run" }
      if empty >= EMPTY_CLAIM_LIMIT
        return base.merge("status" => "blocked", "action" => "observed",
                          "blocked_at_revision" => work.fetch("revision"),
                          "blocked_reason" => "#{empty} dispatch attempts ended before producing a run; " \
                                              "the dispatcher is not getting far enough to record one")
      end

      base.merge("status" => "queued", "action" => "observed", "due_at" => @clock.timestamp,
                 "detail" => "a dispatch attempt ended before producing a run; it is queued again")
    end

    # A failed attempt only ever becomes a retry here, and the budget is spent when the retry is
    # scheduled, so a crash between scheduling and running can lose an attempt but never reclaim one.
    def retry_decision(intent, work, workflow, failure)
      run = failure["run"]
      outcome = (run && run["outcome"]) || {}
      summary = outcome["summary"] || failure["summary"] || "execution did not succeed"
      status = outcome["status"] || (run && run["status"]) || failure.fetch("status")
      base = { "last_accounted_run_id" => run&.fetch("id"), "last_run_id" => run&.fetch("id"),
               "last_error" => "#{status}: #{summary}" }.compact

      base = base.merge("blocked_at_revision" => work.fetch("revision"))
      if status == "cancelled"
        return base.merge("status" => "blocked", "action" => "observed",
                          "blocked_reason" => "the last execution was cancelled; cancellations are never retried automatically")
      end
      if (pending = unresolved_effects(work.fetch("id"))).any?
        return base.merge("status" => "blocked", "action" => "observed",
                          "blocked_reason" => "unresolved external effect #{pending.first.fetch("kind")} (#{pending.first.fetch("idempotency_key")})")
      end
      unless dispatchable?(work, workflow)
        return base.merge("status" => "blocked", "action" => "observed",
                          "blocked_reason" => "workflow #{workflow.name} offers no new dispatch from #{work.fetch("state")} after this failure")
      end
      base = base.except("blocked_at_revision")

      policy = intent.fetch("retry_policy")
      used = intent.fetch("retries_used")
      if RetryPolicy.retries_remaining(policy, used).zero?
        reason = policy.fetch("max_retries").zero? ? "no automatic retries are configured" : "retry budget of #{policy.fetch("max_retries")} is spent"
        return base.merge("status" => "exhausted", "action" => "exhausted",
                          "stop_reason" => reason, "closed_at" => @clock.timestamp)
      end

      number = used + 1
      due = @clock.now + RetryPolicy.delay_for(policy, number)
      base.merge("status" => "delayed", "action" => "scheduled_retry", "retries_used" => number,
                 "due_at" => due.utc.iso8601(6), "detail" => "retry #{number}/#{policy.fetch("max_retries")} scheduled")
    end

    def dispatch(intent)
      begin
        claimed = claim(intent)
      rescue ActivityConflictError
        raise
      rescue ConflictError => error
        return report(fetch_intent(intent.fetch("id")), action: "fenced", detail: error.message)
      end

      controller = @controller_factory.call(claimed.fetch("mode"))
      result = begin
        controller.process(work_item_id: claimed.fetch("work_item_id"), start_transition: claimed["start_transition"])
      rescue ActivityConflictError
        raise
      rescue ConflictError => error
        # Something moved underneath this dispatch. That is a fence, not a verdict about the work:
        # re-read and let the next classification decide from what is actually recorded now.
        settled = apply(claimed, classify(fetch_intent(claimed.fetch("id")), [], ignore_claim: true))
        return report(settled, action: "fenced", detail: error.message)
      rescue Backstage::Error => error
        # The workflow refused this dispatch. That is a contract or authority answer, never a retry,
        # and it needs an operator rather than another pass.
        settled = apply(claimed, { "status" => "blocked", "action" => "observed",
                                   "blocked_at_revision" => store.fetch!("work_items", claimed.fetch("work_item_id")).fetch("revision"),
                                   "blocked_reason" => "#{error.class.name.split("::").last}: #{error.message}",
                                   "last_error" => error.message })
        return report(settled, action: "blocked", detail: error.message)
      end

      decision = classify(fetch_intent(claimed.fetch("id")), [], ignore_claim: true)
      decision = decision.merge("empty_claims" => 0) if attempt_runs(fetch_intent(claimed.fetch("id"))).any?
      settled = apply(claimed, decision)
      report(settled, action: "dispatched", detail: result.fetch("halt_reason"), process: {
        "mode" => result.fetch("mode"),
        "halt_reason" => result.fetch("halt_reason"),
        "state" => result.fetch("state"),
        "steps" => result.fetch("steps").length
      })
    end

    def block(intent, error)
      reason = "#{error.class.name.split("::").last}: #{error.message}"
      revision = store.fetch("work_items", intent.fetch("work_item_id"))&.fetch("revision")
      settled = begin
        apply(intent, { "status" => "blocked", "blocked_reason" => reason, "blocked_at_revision" => revision }.compact)
      rescue Backstage::Error
        intent
      end
      report(settled, action: "blocked", detail: reason)
    end

    def claim(intent)
      now = @clock.timestamp
      claimed = intent.merge(
        "status" => "running",
        "revision" => intent.fetch("revision") + 1,
        "attempts_used" => intent.fetch("attempts_used") + 1,
        # Recorded against the same wall clock the run records use, so an injected scheduling clock
        # cannot move the boundary between this attempt's runs and an earlier generation's.
        "dispatch_started_at" => Records.timestamp,
        "dispatcher_pid" => Process.pid,
        "dispatcher_host" => Records.host_name,
        "updated_at" => now
      ).except("blocked_reason", "detail")
      store.commit([[COLLECTION, claimed]], expect: [
        { collection: COLLECTION, id: intent.fetch("id"), fields: { revision: intent.fetch("revision"), status: intent.fetch("status") } }
      ])
      claimed
    end

    # Writes the decision back. A terminal intent is never reopened by a late pass: an operator's
    # cancellation outranks whatever the execution went on to report.
    def apply(intent, decision)
      changes = decision.slice("status", "blocked_reason", "blocked_at_revision", "stop_reason", "last_error", "due_at",
                               "retries_used", "empty_claims", "decision_id", "last_run_id", "last_accounted_run_id",
                               "closed_at", "detail")
      status = changes.fetch("status", intent.fetch("status"))
      3.times do
        current = fetch_intent(intent.fetch("id"))
        return current if TERMINAL_STATUSES.include?(current.fetch("status")) && current.fetch("status") != status

        cleared = { "blocked_reason" => nil, "blocked_at_revision" => nil, "detail" => nil, "decision_id" => nil }
        cleared["due_at"] = nil unless %w[queued delayed].include?(status)
        cleared["stop_reason"] = nil unless TERMINAL_STATUSES.include?(status)
        settled = current.merge(cleared).merge(changes).compact
        # An idle pass observing an unchanged intent writes nothing, so a long-running worker does
        # not grow the log by one line per poll.
        return current if settled == current

        updated = settled.merge("revision" => current.fetch("revision") + 1, "updated_at" => @clock.timestamp)
        begin
          return store.commit([[COLLECTION, updated]], expect: [
            { collection: COLLECTION, id: current.fetch("id"), revision: current.fetch("revision") }
          ], activity: applied_events(current, updated)).first
        rescue ActivityConflictError
          raise
        rescue ConflictError
          next
        end
      end
      fetch_intent(intent.fetch("id"))
    end

    def settle(intent, status:, **fields)
      apply(intent, fields.transform_keys(&:to_s).merge("status" => status))
    end

    # The runs this intent's current attempt produced. `dispatch_started_at` is the checkpoint that
    # separates them from anything an earlier generation or a direct caller left behind.
    def attempt_runs(intent)
      started = intent["dispatch_started_at"]
      return [] unless started && intent.fetch("attempts_used").positive?

      runs_for(intent.fetch("work_item_id")).select { |run| run["created_at"].to_s >= started }
    end

    def unaccounted_failure(intent)
      last = attempt_runs(intent).max_by { |run| run["created_at"].to_s }
      return nil unless last
      return nil if %w[queued running].include?(last["status"])
      return nil if last.fetch("id") == intent["last_accounted_run_id"]

      status = last.dig("outcome", "status") || last["status"]
      return nil if status == "succeeded"

      { "run" => last, "status" => status }
    end

    # A superseding generation waits here rather than launching beside an execution the previous
    # generation still owns. The old run is already cancellation-requested; when it resolves, its
    # outcome can only be recorded as cancelled, and this generation dispatches on the next pass.
    # Describes the process holding this claim while it is still alive. Same-process and unreadable
    # cases count as held, matching how recovery treats an owner it cannot prove is gone.
    def claim_holder(intent)
      pid = intent["dispatcher_pid"]
      host = intent["dispatcher_host"]
      return nil unless pid
      return "pid #{pid} on #{host}" unless host == Records.host_name

      begin
        Process.kill(0, pid)
        "pid #{pid} on #{host}"
      rescue Errno::ESRCH
        nil
      rescue Errno::EPERM
        "pid #{pid} on #{host}"
      end
    end

    def live_execution_detail(intent, work_item_id, work_terminal = false)
      earlier = runs_for(work_item_id).find do |run|
        %w[queued running].include?(run["status"]) && run["created_at"].to_s < intent.fetch("accepted_at").to_s
      end
      return "an execution accepted before this generation (#{earlier.fetch("id")}) is still resolving" if earlier
      return "the work item is terminal but an execution is still resolving" if work_terminal

      "execution is in flight"
    end

    def dispatchable?(work, workflow)
      return true if @engine.queued_job(work.fetch("id"))

      state = workflow.state(work.fetch("state"))
      return true if state.continue && !@workflows.revision_budget_exhausted?(work, state.continue)

      workflow.transitions_from(work.fetch("state")).any? do |transition|
        transition.dispatch && transition.allows?("system") && transition.requires.empty? &&
          !@workflows.revision_budget_exhausted?(work, transition.name)
      end
    end

    # A fake dispatcher may not consume real work, and a real dispatcher runs fake work as fake.
    def mode_authorized?(intent)
      intent.fetch("mode") == "fake" || @authorized_mode == "publish_draft"
    end

    def unresolved_effects(work_item_id)
      store.list("external_actions").select { |row| row["work_item_id"] == work_item_id && row["status"] == "pending" }
    end

    def runs_for(work_item_id)
      job_ids = store.list("jobs").select { |job| job["work_item_id"] == work_item_id }.map { |job| job.fetch("id") }
      store.list("runs").select { |run| run["work_item_id"] == work_item_id || job_ids.include?(run["job_id"]) }
    end

    def cancel_active_runs(intent)
      runs_for(intent.fetch("work_item_id")).each do |run|
        next unless %w[queued running].include?(run["status"])

        @engine.request_cancel(run.fetch("id"))
      end
    end

    def supersede_record(active, replacement, now)
      active.merge(
        "status" => "cancelled",
        "revision" => active.fetch("revision") + 1,
        "stop_reason" => "superseded by #{replacement.fetch("id")} (generation #{replacement.fetch("generation")})",
        "closed_at" => now,
        "updated_at" => now
      )
    end

    def next_generation(work_item_id)
      store.list(COLLECTION).select { |row| row["work_item_id"] == work_item_id }
           .map { |row| row.fetch("generation", 0) }.max.to_i + 1
    end

    # A repeat of a recorded acceptance returns it, including a repeated supersede whose new
    # generation is the one now active — a caller retrying after a timeout must not cancel the
    # generation it just created. A supersede naming an older, already closed generation is refused,
    # because that asks for something this identity cannot record.
    def deduplicate(existing, fingerprint, supersede = false)
      unless existing["request_fingerprint"] == fingerprint
        raise ContractError, "acceptance #{existing.fetch("request_id")} already recorded a different request payload"
      end
      if supersede && TERMINAL_STATUSES.include?(existing.fetch("status"))
        raise ContractError,
              "acceptance #{existing.fetch("request_id")} already recorded #{existing.fetch("id")} " \
              "(#{existing.fetch("status")}); supersede needs a new acceptance identity, so omit " \
              "--request-id or give a different one"
      end

      existing.merge("deduplicated" => true)
    end

    def default_request_id(work_item_id, mode, supersede)
      active = supersede ? nil : active_intent_for(work_item_id)
      generation = active ? active.fetch("generation") : next_generation(work_item_id)
      "accept:#{work_item_id}:#{mode}:#{generation}"
    end

    # Superseding is part of what was asked for, not a modifier on it. Including it means a repeat of
    # the same supersede deduplicates, while reusing an identity that was recorded without it fails
    # loudly instead of quietly returning the acceptance it was meant to replace.
    def fingerprint_for(work_item_id:, mode:, start_transition:, policy:, supersede: false)
      Digest::SHA256.hexdigest(JSON.generate([work_item_id, mode, start_transition&.to_s, policy.sort, supersede == true]))
    end

    def fetch_intent(id)
      store.fetch(COLLECTION, id) || raise(NotFound, "#{COLLECTION} #{id} was not found")
    end

    def latest_intent_for(work_item_id)
      store.list(COLLECTION).select { |row| row["work_item_id"] == work_item_id }.max_by { |row| row.fetch("generation", 0) }
    end

    def open_decision(work)
      store.list("decisions").find { |row| row["work_item_id"] == work.fetch("id") && row["status"] == "open" }
           &.slice("id", "question", "choices", "created_at")
    end

    def execution_summary(run)
      {
        "run_id" => run.fetch("id"),
        "phase" => run["phase"],
        "status" => run["status"],
        "summary" => run.dig("outcome", "summary"),
        "started_at" => run["created_at"],
        "finished_at" => run["finished_at"],
        "runtime" => run["runtime"]
      }.compact
    end

    def summarize(intent)
      {
        "id" => intent.fetch("id"),
        "work_item_id" => intent.fetch("work_item_id"),
        "generation" => intent.fetch("generation"),
        "mode" => intent.fetch("mode"),
        "status" => intent.fetch("status"),
        "due_at" => intent["due_at"],
        "attempts_used" => intent.fetch("attempts_used"),
        "retries_remaining" => RetryPolicy.retries_remaining(intent.fetch("retry_policy"), intent.fetch("retries_used")),
        "reason" => intent["blocked_reason"] || intent["stop_reason"] || intent["detail"]
      }.compact
    end

    def actions_for(intent, work, current_run)
      id = work.fetch("id")
      case intent.fetch("status")
      when "waiting" then ["backstage decide #{id} --choose TRANSITION"]
      when "uncertain" then ["backstage recover #{id}", current_run ? "backstage cancel #{current_run.fetch("id")}" : nil].compact
      when "blocked" then ["backstage show #{id}", "backstage dispatch accept #{id} --supersede"]
      when "exhausted" then ["backstage dispatch accept #{id} --supersede"]
      when "delayed" then ["backstage dispatch show #{id}"]
      when "running" then ["backstage dispatch show #{id}", current_run ? "backstage cancel #{current_run.fetch("id")}" : nil].compact
      when "queued" then ["backstage dispatch pass"]
      else ["backstage show #{id}"]
      end
    end

    def report(intent, action:, detail: nil, process: nil)
      {
        "intent_id" => intent.fetch("id"),
        "work_item_id" => intent.fetch("work_item_id"),
        "generation" => intent.fetch("generation"),
        "mode" => intent.fetch("mode"),
        "status" => intent.fetch("status"),
        "action" => action,
        "detail" => detail || intent["blocked_reason"] || intent["stop_reason"] || intent["detail"],
        "due_at" => intent["due_at"],
        "attempts_used" => intent.fetch("attempts_used"),
        "retries_remaining" => RetryPolicy.retries_remaining(intent.fetch("retry_policy"), intent.fetch("retries_used")),
        "process" => process
      }.compact
    end
  end
end
