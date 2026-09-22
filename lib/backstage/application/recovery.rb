# frozen_string_literal: true

module Backstage::Application
  # Explicit reconciliation of interrupted work.
  #
  # Recovery inspects what was actually persisted and what is actually alive before deciding
  # anything. It reconciles a trustworthy outcome that was recorded before its transition, leaves a
  # live worker alone, records an interruption when a worker is provably gone, and reports unknown
  # runtime status as unknown. It never clears a state that is waiting on a human, never fabricates
  # a completion, and never launches replacement work itself.
  class Recovery
    Presence = Backstage::Ports::RuntimePresence
    Records = Backstage::Domain::Records
    Outcome = Backstage::Domain::Outcome
    RECOVERY = Controller::CONTROLLER

    def initialize(engine:, workflows:, presence: Presence.new)
      @engine = engine
      @workflows = workflows
      @presence = presence
    end

    def reconcile(work_item_id = nil)
      items = work_item_id ? [@engine.store.fetch!("work_items", work_item_id)] : @engine.store.list("work_items")
      reports = items.filter_map { |work| report(work) }
      { "reconciled_at" => Records.timestamp, "work_items" => reports }
    end

    private

    def recorder
      @recorder ||= ActivityRecorder.new(store: @engine.store, adapter: "backstage.application.recovery")
    end

    # A finding becomes history when recovery *records* one — in the same commit as the repair it
    # made. Reconciliation runs before every dispatch, so the findings it only observes (a live
    # worker, a pending dispatch, an unresolvable runtime, a decision waiting on a person) write no
    # state and get no event; they are in the pass report, and one per poll would drown the stream.
    def finding_event(finding, work_item_id:, job_id: nil, run_id: nil)
      recorder.event(
        type: "reconciliation.finding",
        event_id: ActivityRecorder.event_id("reconciliation.finding", finding.fetch("kind"), run_id || job_id),
        work_item_id: work_item_id,
        job_id: job_id,
        run_id: run_id,
        summary: "reconciliation found #{finding.fetch("kind")} for #{run_id || job_id}",
        data: finding.merge("repaired" => true)
      )
    end

    def report(work)
      workflow = @workflows.workflow_for(work)
      state = work.fetch("state")
      jobs = open_jobs(work)
      return nil if workflow.terminal?(state) && jobs.empty?

      findings = []
      awaiting = workflow.state(state).awaits_decision?
      findings << { "kind" => "awaiting_decision", "detail" => "a human answer is required", "decision_id" => work["open_decision_id"] }.compact if awaiting

      jobs.each do |job|
        current = @engine.store.fetch!("work_items", work.fetch("id"))
        findings.concat(inspect_job(current, job, workflow.state(current.fetch("state")).awaits_decision?))
      end

      {
        "work_item_id" => work.fetch("id"),
        "state" => @engine.store.fetch!("work_items", work.fetch("id")).fetch("state"),
        "workflow" => work.dig("workflow", "name"),
        "findings" => findings
      }
    end

    def open_jobs(work)
      @engine.store.list("jobs").select do |job|
        next false unless job["work_item_id"] == work.fetch("id")
        next true if %w[queued running].include?(job["status"])
        run = latest_run(job)
        outcome = run && run["outcome"]
        next false unless outcome
        follow = @workflows.transition_for_outcome(job, outcome)
        follow && !@engine.store.find("work_transitions", request_id: "job:#{job.fetch("id")}:#{follow}") &&
          !@workflows.superseded_dispatch?(work.fetch("id"), job)
      end
    end

    def inspect_job(work, job, awaiting)
      if job.fetch("status") == "queued"
        if (stale = @workflows.superseded_dispatch?(work.fetch("id"), job))
          finding = { "kind" => "dispatch_cancelled", "job_id" => job.fetch("id"), "detail" => stale }
          @engine.store.commit([["jobs", job.merge("status" => "cancelled", "updated_at" => Records.timestamp)]],
            expect: [{ collection: "jobs", id: job.fetch("id"), fields: { status: "queued", run_id: nil } }],
            activity: [finding_event(finding, work_item_id: work.fetch("id"), job_id: job.fetch("id"))])
          return [finding]
        end
        return [{ "kind" => "dispatch_pending", "job_id" => job.fetch("id"), "detail" => "a requested execution has not started; run process to dispatch it" }]
      end

      run = latest_run(job)
      return [{ "kind" => "dispatch_pending", "job_id" => job.fetch("id"), "detail" => "job is marked running but has no run record" }] unless run

      outcome = run["outcome"]
      if outcome
        reconcile_execution_records(job, run, outcome)
        return reconcile_outcome(work, job, run, outcome, awaiting)
      end

      case liveness(run)
      when Presence::ALIVE
        [{ "kind" => "worker_active", "run_id" => run.fetch("id"), "detail" => "execution is still alive; no replacement was launched" }]
      when Presence::GONE
        interrupt(work, job, run, awaiting)
      else
        [{ "kind" => "runtime_unknown", "run_id" => run.fetch("id"), "detail" => "runtime status could not be established; resolve it explicitly before continuing" }]
      end
    end

    # Older/interrupted writers may have saved an outcome before closing the job and attempt.
    # Reconcile those records too, so execution visibility agrees with the persisted result.
    def reconcile_execution_records(job, run, outcome)
      status = run["status"] == "interrupted" ? "interrupted" : outcome.fetch("status")
      now = Records.timestamp
      writes = []
      writes << ["jobs", job.merge("status" => status, "run_id" => run.fetch("id"), "updated_at" => now)] if %w[queued running].include?(job["status"])
      writes << ["runs", run.merge("status" => status, "finished_at" => now, "updated_at" => now)] if %w[queued running].include?(run["status"])
      @engine.store.list("attempts").select { |attempt| attempt["run_id"] == run.fetch("id") && attempt["status"] == "running" }.each do |attempt|
        writes << ["attempts", attempt.merge("status" => %w[succeeded cancelled interrupted].include?(status) ? status : "failed",
          "outcome" => outcome, "finished_at" => now, "updated_at" => now)]
      end
      return if writes.empty?

      finding = { "kind" => "execution_records_reconciled", "run_id" => run.fetch("id"), "status" => status,
                  "detail" => "an outcome was persisted before its execution records were closed" }
      @engine.store.commit(writes, expect: [
        { collection: "jobs", id: job.fetch("id"), fields: job },
        { collection: "runs", id: run.fetch("id"), fields: run.merge("cancellation_requested_at" => run["cancellation_requested_at"]) }
      ], activity: [finding_event(finding, work_item_id: run["work_item_id"] || job["work_item_id"],
                                  job_id: job.fetch("id"), run_id: run.fetch("id"))])
    rescue Backstage::ActivityConflictError
      # Not the controller finishing first: an event id was reused for a different fact, and
      # adopting "the controller won" would file that defect away as a normal race.
      raise
    rescue Backstage::ConflictError
      # The controller completed concurrently. Its persisted outcome and the transition operation's
      # own run guards remain authoritative; do not overwrite it with the earlier snapshot.
      nil
    end

    # An outcome that was persisted before its transition is authoritative; applying it here is
    # reconciliation, not invention.
    def reconcile_outcome(work, job, run, outcome, awaiting)
      follow = @workflows.transition_for_outcome(job, outcome)
      finding = { "kind" => "outcome_recorded", "run_id" => run.fetch("id"), "status" => outcome["status"] }
      return [finding.merge("detail" => "no configured transition for this outcome")] unless follow

      actor = outcome["status"] == "succeeded" ? @workflows.outcome_actor(run, outcome) : RECOVERY
      apply(work, job, run, follow, outcome["summary"], finding, awaiting, actor)
    end

    def interrupt(work, job, run, awaiting)
      capture = interrupted_capture(run)
      failure = Outcome.failure(
        summary: "execution ended without a recorded outcome",
        interrupted: true,
        capture: capture,
        raw: { "stream_refs" => capture.fetch("streams").map { |stream| stream.slice("stream_id", "artifact_ids", "records", "bytes", "coverage") } }
      )
      now = Records.timestamp
      writes = [
        ["runs", run.merge("status" => "interrupted", "outcome" => failure, "finished_at" => now, "updated_at" => now)],
        ["jobs", job.merge("status" => "interrupted", "updated_at" => now)]
      ]
      @engine.store.list("attempts").select { |attempt| attempt["run_id"] == run.fetch("id") && attempt["status"] == "running" }.each do |attempt|
        writes << ["attempts", attempt.merge("status" => "interrupted", "outcome" => failure, "finished_at" => now, "updated_at" => now)]
      end
      finding = { "kind" => "worker_interrupted", "run_id" => run.fetch("id"),
                  "detail" => "execution ended without an outcome; artifacts were preserved",
                  "capture" => finding_capture(capture) }
      begin
        @engine.store.commit(writes, expect: [
          { collection: "runs", id: run.fetch("id"), fields: run.merge("outcome" => nil, "cancellation_requested_at" => run["cancellation_requested_at"]) },
          { collection: "jobs", id: job.fetch("id"), fields: job }
        ], activity: [finding_event(finding, work_item_id: work.fetch("id"), job_id: job.fetch("id"), run_id: run.fetch("id"))])
      rescue Backstage::ActivityConflictError
        raise
      rescue Backstage::ConflictError
        return [{ "kind" => "runtime_unknown", "run_id" => run.fetch("id"), "detail" => "execution changed during recovery; reconcile again" }]
      end
      follow = job["on_failure"]
      return [finding] unless follow

      apply(work, job, run, follow, failure["summary"], finding, awaiting, RECOVERY)
    end

    # What is actually known about an interrupted run's output.
    #
    # The run's own `capture` block is only as fresh as the last coverage it managed to write, and a
    # process that died mid-stream wrote nothing after it. So the checkpoint rows are authoritative
    # here: a `runtime_streams` row with `closed_at == nil` means bytes may have been produced past
    # `last_offset` that nothing acknowledged, and that is a `gap` — not a complete capture, and not
    # a failure either, because everything up to `last_offset` is durable and referenced.
    def interrupted_capture(run)
      run_id = run.fetch("id")
      recorded = run["capture"].is_a?(Hash) ? run["capture"] : nil
      rows = @engine.store.list(Backstage::Ports::RuntimeCapture::STREAM_COLLECTION)
                    .select { |row| row["run_id"] == run_id }
      streams = rows.map { |row| interrupted_stream(row) }
      streams = Array(recorded && recorded["streams"]) if streams.empty?
      Outcome.capture_summary(streams, limit_bytes: recorded && recorded["limit_bytes"],
                              updated_at: Records.timestamp)
    end

    def interrupted_stream(row)
      open = row["closed_at"].nil?
      {
        "stream_id" => row.fetch("id"), "step" => row["step"], "phase" => row["phase"],
        "bytes" => row["bytes"], "records" => row["records"], "chunks" => row["chunks"],
        "malformed" => row["malformed"],
        "coverage" => open ? "gap" : row.fetch("coverage", "complete"),
        "last_offset" => row["last_offset"],
        "artifact_ids" => artifact_ids_for(row.fetch("id")),
        "opened_at" => row["opened_at"],
        "closed_at" => row["closed_at"]
      }
    end

    # Artifacts name the stream they came from, so recovery finds them without knowing how the
    # artifact store lays its files out or how it derives an id.
    def artifact_ids_for(stream_id)
      @engine.store.list("artifacts").select { |artifact| artifact["stream_id"] == stream_id }
             .map { |artifact| artifact.fetch("id") }
    end

    # The bounded form that rides on the finding. It names the interval nobody observed rather than
    # restating the whole coverage block, because a finding is read to answer "what is missing".
    def finding_capture(capture)
      unobserved = capture.fetch("streams").select { |stream| stream["coverage"] == "gap" }.map do |stream|
        { "stream_id" => stream["stream_id"], "step" => stream["step"],
          "observed_through_offset" => stream["last_offset"].to_i,
          "detail" => "output after this offset, if any, was never acknowledged" }
      end
      { "status" => capture.fetch("status"), "bytes" => capture["bytes"],
        "streams" => capture.fetch("streams").length, "unobserved" => unobserved }
    end

    def apply(work, job, run, follow, reason, finding, awaiting, actor)
      return [finding.merge("action" => "none", "detail" => "work is waiting on a human decision")] if awaiting

      superseded = @workflows.superseded_dispatch?(work.fetch("id"), job)
      return [finding.merge("action" => "fenced", "detail" => superseded)] if superseded

      result = @workflows.request_transition(
        work_item_id: work.fetch("id"),
        transition: follow,
        actor: actor,
        request_id: "job:#{job.fetch("id")}:#{follow}",
        reason: reason,
        evidence: evidence_for(work, run, follow),
        run_id: run.fetch("id")
      )
      [finding.merge("action" => "transitioned", "transition" => result.fetch("transition").fetch("transition"), "to" => result.fetch("work_item").fetch("state"))]
    rescue Backstage::Error => error
      [finding.merge("action" => "blocked", "error" => "#{error.class.name.split("::").last}: #{error.message}")]
    end

    def evidence_for(work, run, follow)
      @engine.store.list("artifacts").select do |artifact|
        artifact["run_id"] == run.fetch("id") && %w[binary_patch review_verdict].include?(artifact["kind"])
      end.map { |artifact| artifact.fetch("id") }

    end

    def latest_run(job)
      @engine.store.list("runs").select { |run| run["job_id"] == job.fetch("id") }.max_by { |run| run["created_at"].to_s }
    end

    def liveness(run)
      runtime = run["runtime"]
      unless runtime
        return Presence::GONE if run["runtime_identity_before_launch"] == true && owner_gone?(run)
        return Presence::UNKNOWN
      end

      status = @presence.status(runtime)
      # A controller may be between worker containers or persisting the final outcome. Even a
      # missing container cannot establish interruption while its owning controller is alive.
      return Presence::UNKNOWN if status == Presence::GONE && !owner_gone?(run)

      status
    end

    def owner_gone?(run)
      return false unless run["owner_host"] == Records.host_name
      return false if run["owner_pid"].nil?
      return false if run.fetch("owner_pid") == Process.pid

      begin
        Process.kill(0, run.fetch("owner_pid"))
        false
      rescue Errno::ESRCH
        true
      rescue Errno::EPERM
        false
      end
    end
  end
end
