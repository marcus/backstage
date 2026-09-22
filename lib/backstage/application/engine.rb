# frozen_string_literal: true

require "digest"

module Backstage::Application
  # Execution coordination. The engine admits work, claims dispatched jobs, runs them, and records
  # what the run produced. It never decides where the work sits in its workflow — that belongs to
  # WorkflowService, and the controller joins the two.
  class Engine
    ConflictError = Backstage::ConflictError
    ActivityConflictError = Backstage::ActivityConflictError
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    Outcome = Backstage::Domain::Outcome
    COLLECTIONS = %w[work_items workflow_snapshots work_transitions decisions agent_requests jobs runs attempts artifacts external_actions triggers source_checks execution_intents execution_acceptances runtime_streams].freeze

    attr_reader :store, :artifact_store, :capture_options

    # `secret_guard` is the engine's, not just the store's: capture redacts the runtime's bytes on
    # the way to a chunk file, and the offsets an event refers to are offsets into the redacted
    # stream. An engine composed without one captures unredacted bytes, which the artifact store's
    # own check would then refuse — so the composition root passes the same guard everywhere.
    def initialize(store:, artifact_store:, validator: Backstage::Contracts::Validator.new,
                   secret_guard: Backstage::Support::SecretGuard.new, clock: nil, capture_options: {})
      @store = store
      @artifact_store = artifact_store
      @validator = validator
      @secret_guard = secret_guard
      # The composition root injects the deployment's clock; the fallback is the plain wall clock so
      # an engine built directly in a test still stamps real times without naming an adapter here.
      @clock = clock || CaptureDefaults::WallClock.new
      @capture_options = symbolize(capture_options)
    end

    def submit(idempotency_key:, title:, description:, workflow:, source: "manual", source_ref: nil, target: nil, source_instance: nil, source_identity: nil)
      existing = store.find("work_items", idempotency_key: idempotency_key)
      if existing
        requested_binding = { "target" => target, "source_instance" => source_instance, "source_identity" => source_identity }.compact
        unless requested_binding.all? { |key, value| existing[key] == value }
          raise ContractError, "work item routing binding is immutable"
        end

        return existing
      end

      work = Records.work_item(
        idempotency_key: idempotency_key,
        title: title,
        description: description,
        source: source,
        source_ref: source_ref,
        workflow: workflow,
        target: target,
        source_instance: source_instance,
        source_identity: source_identity
      )
      # The snapshot and the work item were two saves; they are one commit now so the admission and
      # the event that records it cannot be observed apart. Summary and payload carry ids and
      # states only — the title and description are source text and stay in the record.
      admitted = recorder.event(
        type: "work.admitted",
        event_id: ActivityRecorder.event_id("work.admitted", idempotency_key),
        work_item_id: work.fetch("id"),
        target_id: target,
        occurred_at: work["created_at"],
        summary: "admitted #{work.fetch("id")} into #{workflow.name} at #{work.fetch("state")}",
        data: {
          "state" => work.fetch("state"),
          "workflow" => workflow.name,
          "workflow_digest" => work.dig("workflow", "digest"),
          "source" => source,
          "source_instance" => source_instance,
          # The digest, not the key: an idempotency key is built from the source ref and the
          # slugified title, so recording it verbatim puts source text in history that the record
          # already holds. The digest still answers "is this the same admission?" for a reader
          # holding the key, and answers nothing to a reader who is not.
          "idempotency_key_digest" => Digest::SHA256.hexdigest(idempotency_key.to_s)
        }
      )
      store.commit([["workflow_snapshots", workflow.snapshot_record], ["work_items", work]], activity: [admitted]).last
    end

    def list_work
      store.list("work_items")
    end

    def show_work(id)
      work = store.fetch!("work_items", id)
      jobs = store.list("jobs").select { |job| job["work_item_id"] == id }
      job_ids = jobs.map { |job| job["id"] }
      runs = store.list("runs").select { |run| job_ids.include?(run["job_id"]) }
      run_ids = runs.map { |run| run["id"] }
      work.merge(
        "jobs" => jobs,
        "runs" => runs,
        "attempts" => store.list("attempts").select { |attempt| run_ids.include?(attempt["run_id"]) },
        "artifacts" => store.list("artifacts").select { |artifact| artifact["work_item_id"] == id },
        "external_actions" => store.list("external_actions").select { |action| action["work_item_id"] == id },
        "transitions" => store.list("work_transitions").select { |row| row["work_item_id"] == id }.sort_by { |row| row.fetch("revision") },
        "decisions" => store.list("decisions").select { |row| row["work_item_id"] == id },
        "agent_requests" => store.list("agent_requests").select { |row| row["work_item_id"] == id },
        # Coverage at the top level, not only buried on each run: "how much of what my worker
        # printed is actually accounted for" is a question about the work item, and an operator
        # should not have to know which run to open to ask it.
        "capture" => runs.filter_map { |run| run["capture"] && run.fetch("capture").merge("run_id" => run.fetch("id"), "phase" => run["phase"]) }
      )
    end

    def queued_job(work_item_id)
      workflows = workflow_service
      store.list("jobs").find do |job|
        job["work_item_id"] == work_item_id && job["status"] == "queued" && !workflows.superseded_dispatch?(work_item_id, job)
      end
    end

    # Runs one dispatched job. The work item's revision at dispatch time is carried on the run so a
    # late result can be fenced against a work item that has since moved on.
    def execute(job, runner:, bundle: nil, secrets: {}, request_channel: nil)
      work_item_id = job.fetch("work_item_id")
      phase = job.fetch("phase", "implementation")
      bundle ||= job["bundle"]
      # Claim all execution records together. A concurrent process either owns this dispatch or
      # observes it already claimed; it must never launch a second worker for the same job.
      work = store.fetch!("work_items", work_item_id)
      workflows = workflow_service
      stale = workflows.superseded_dispatch?(work_item_id, job)
      raise ConflictError, stale if stale
      raise ConflictError, "dispatch state has changed" unless job["dispatch_state"] == work.fetch("state")
      raise ConflictError, "work already has active execution" if active_execution?(work_item_id)

      run = Records.run(job_id: job.fetch("id"), phase: phase, work_item_id: work_item_id, work_revision: job["work_revision"]).merge(
        "status" => "running", "context_grants" => Array(bundle&.fetch("context_grants", []))
      )
      run["runtime_identity_before_launch"] = runner.respond_to?(:runtime_identity_before_launch?) && runner.runtime_identity_before_launch?
      run["reviewed_candidate"] = work["candidate"] if phase == "review"
      attempt = Records.attempt(run_id: run.fetch("id"), number: 1)
      job = job.merge("status" => "running", "run_id" => run.fetch("id"), "bundle" => bundle, "updated_at" => Records.timestamp).compact
      store.commit([["jobs", job], ["runs", run], ["attempts", attempt]], expect: [
        { collection: "work_items", id: work_item_id, revision: work.fetch("revision") },
        { collection: "jobs", id: job.fetch("id"), fields: { status: "queued", run_id: nil } }
      ], activity: [started_event(work, job, run, attempt, runner)])
      claimed = true
      # One capture component per run. It replaces the `events = []` accumulator that grew for the
      # whole run and that nothing read: what a runtime produces now reaches durable chunks and
      # committed events incrementally, and what the engine keeps is the bounded coverage summary.
      capture = Backstage::Application::RuntimeCapture.new(
        sink: CaptureSink::Durable.new(store: store, artifact_store: @artifact_store,
                                       work_item_id: work_item_id, run: run, attempt: attempt),
        clock: @clock, run: run, attempt: attempt, secret_guard: @secret_guard, **@capture_options
      )

      if bundle
        store.save("artifacts", @artifact_store.write(
          work_item_id: work_item_id,
          run_id: run.fetch("id"),
          name: "job-bundle.json",
          content: bundle,
          kind: "job_bundle",
          provenance: { "adapter" => "Backstage::Configuration", "captured_at" => Records.timestamp }
        ))
      end

      outcome = runner.run(bundle: bundle, secrets: secrets, capture: capture,
                           cancellation: -> { cancellation_requested?(run.fetch("id")) }) do |event|
        run = observe_event(run, event)
        run = record_capture(run, capture)
        request_channel&.drain(run: run)
      end
      request_channel&.drain(run: run)
      run = record_capture(run, capture, final: true)
      outcome = Outcome.normalize(outcome)
      capture_summary = capture_block(capture, outcome["capture"])
      outcome = outcome.merge("capture" => capture_summary) if capture_summary

      change_artifact = persist_change_artifact(outcome, work_item_id: work_item_id, run_id: run.fetch("id"), runner: runner)
      if change_artifact
        store.save("artifacts", change_artifact)
        outcome = outcome.merge("change_artifact" => outcome.fetch("change_artifact").except("source_path").merge("artifact_id" => change_artifact.fetch("id"), "path" => change_artifact.fetch("path")))
        Outcome.validate!(outcome)
      end
      verdict_artifact = persist_review_verdict(outcome, work_item_id: work_item_id, run_id: run.fetch("id"), runner: runner)
      store.save("artifacts", verdict_artifact) if verdict_artifact

      artifact = store.save("artifacts", @artifact_store.write(
        work_item_id: work_item_id,
        run_id: run.fetch("id"),
        name: "outcome.json",
        content: outcome,
        kind: "structured_outcome",
        provenance: { "adapter" => adapter_identifier(runner), "captured_at" => Records.timestamp }
      ))

      job, run, outcome = finish_execution(job, run, attempt, outcome, runner: runner)

      {
        "job" => job,
        "run" => run,
        "outcome" => outcome,
        "capture" => run["capture"],
        "artifact" => artifact,
        "change_artifact" => change_artifact,
        "review_verdict_artifact" => verdict_artifact
      }.compact
    rescue StandardError => error
      raise unless claimed

      # A run that failed on the way to its outcome still says what was captured before it did.
      # Recovery and the operator need the difference between "nothing was produced" and "output
      # was produced and we cannot account for all of it".
      run = record_capture(run, capture, final: true) if capture
      failure = Outcome.failure(summary: "#{error.class}: #{error.message}",
                                capture: capture && capture_block(capture))
      job, run, failure = finish_execution(job, run, attempt, failure, runner: runner)
      { "job" => job, "run" => run, "outcome" => failure, "capture" => run["capture"] }.compact
    end

    def active_execution?(work_item_id)
      store.list("runs").any? { |run| run["work_item_id"] == work_item_id && %w[queued running].include?(run["status"]) }
    end

    # The moment cancellation was asked for is a fact, so a repeated request keeps the first one and
    # writes nothing. That is also what makes the event idempotent: an acceptance the dispatcher
    # re-requests every pass appends history once.
    def request_cancel(run_id)
      loop do
        current = store.fetch!("runs", run_id)
        return current if current["cancellation_requested_at"]

        now = Records.timestamp
        updated = current.merge("cancellation_requested_at" => now, "updated_at" => now)
        requested = recorder.event(
          type: "execution.cancellation_requested",
          event_id: ActivityRecorder.event_id("execution.cancellation_requested", run_id),
          occurred_at: now,
          work_item_id: current["work_item_id"],
          job_id: current["job_id"],
          run_id: run_id,
          summary: "cancellation requested for run #{run_id} (#{current["status"]})",
          data: { "run_status" => current["status"], "phase" => current["phase"] }
        )
        begin
          return store.commit([["runs", updated]], expect: [
            { collection: "runs", id: run_id, fields: current.merge("cancellation_requested_at" => nil) }
          ], activity: [requested]).first
        rescue ActivityConflictError
          # Re-reading and retrying would only mint the same clashing id again. See Ports::Store.
          raise
        rescue ConflictError
          next
        end
      end
    end

    private

    def recorder
      @recorder ||= ActivityRecorder.new(store: store, adapter: "backstage.application.engine")
    end

    # Claiming a dispatch is where an execution starts, so the event rides the claim's own guarded
    # commit: a caller that loses the race for the job records neither the claim nor its history.
    # The transition that dispatched this job is the cause, and its event id is derivable in
    # process from the transition identity the job already carries.
    def started_event(work, job, run, attempt, runner)
      recorder.event(
        type: "execution.started",
        event_id: ActivityRecorder.event_id("execution.started", run.fetch("id")),
        occurred_at: run["created_at"],
        work_item_id: work.fetch("id"),
        target_id: work["target"],
        job_id: job.fetch("id"),
        run_id: run.fetch("id"),
        attempt_id: attempt.fetch("id"),
        transition_id: job["transition_id"],
        causation_event_id: job["transition_id"] && ActivityRecorder.event_id("work.transition_applied", job.fetch("transition_id")),
        summary: "run #{run.fetch("id")} started #{run.fetch("phase")} for #{work.fetch("id")} at #{work.fetch("state")}",
        data: {
          "phase" => run.fetch("phase"),
          "adapter" => adapter_identifier(runner),
          "work_state" => work.fetch("state"),
          "work_revision" => job["work_revision"],
          "attempt" => attempt.fetch("number"),
          "runtime_identity_before_launch" => run["runtime_identity_before_launch"] == true,
          "context_grants" => Array(run["context_grants"]).length
        }
      )
    end

    # Two facts, one commit, deliberately not the same event: what the runtime said about its own
    # work, and what the core recorded after adjudicating it. They differ whenever cancellation was
    # requested — a worker reporting success while it was being stopped is not a success — and a
    # reader must be able to tell the claim from the verdict. Neither carries reported text: the
    # outcome itself is already persisted on the run and in its artifact.
    def completion_events(run, attempt, reported, verified, runner, now)
      run_id = run.fetch("id")
      reported_status = reported.fetch("status")
      claim = recorder.event(
        type: "runtime.reported_completion",
        event_id: ActivityRecorder.event_id("runtime.reported_completion", run_id),
        provenance: "runtime_reported",
        adapter: runner ? adapter_identifier(runner) : nil,
        occurred_at: now,
        work_item_id: run["work_item_id"],
        job_id: run["job_id"],
        run_id: run_id,
        attempt_id: attempt.fetch("id"),
        causation_event_id: ActivityRecorder.event_id("execution.started", run_id),
        summary: "runtime reported #{reported_status} for run #{run_id}",
        data: { "reported_status" => reported_status, "phase" => run["phase"],
                "interrupted" => reported["interrupted"] == true }
      )
      [
        claim,
        recorder.event(
          type: "execution.verified_completion",
          event_id: ActivityRecorder.event_id("execution.verified_completion", run_id),
          occurred_at: now,
          work_item_id: run["work_item_id"],
          job_id: run["job_id"],
          run_id: run_id,
          attempt_id: attempt.fetch("id"),
          causation_event_id: claim.fetch("event_id"),
          summary: "run #{run_id} recorded as #{verified.fetch("status")}",
          data: {
            "status" => verified.fetch("status"),
            "reported_status" => reported_status,
            "phase" => run["phase"],
            "cancellation_requested" => !run["cancellation_requested_at"].nil?,
            "overrode_report" => verified.fetch("status") != reported_status
          }
        )
      ]
    end

    # One service per engine: it caches compiled workflows, and building a fresh one per call threw
    # that cache away on every dispatch check.
    def workflow_service
      @workflow_service ||= WorkflowService.new(store: store)
    end

    # Merge against current records so a cancellation arriving during outcome persistence or
    # runtime observation is retained. The batch also makes recovery see one complete finish.
    def finish_execution(job, run, attempt, outcome, runner: nil)
      loop do
        current_job = store.fetch!("jobs", job.fetch("id"))
        current_run = store.fetch!("runs", run.fetch("id"))
        current_attempt = store.fetch!("attempts", attempt.fetch("id"))
        if current_run["outcome"] && !%w[queued running].include?(current_run["status"])
          return [current_job, current_run, current_run.fetch("outcome")]
        end
        final_outcome = outcome
        if current_run["cancellation_requested_at"]
          # Detailed reported output is already captured in the artifact. Cancellation cannot
          # authorize a success transition, even if the worker reported success before stopping.
          final_outcome = outcome.merge("status" => "cancelled", "summary" => "execution cancelled; reported outcome retained")
        end
        now = Records.timestamp
        status = final_outcome.fetch("status")
        finished_run = current_run.merge("status" => status, "outcome" => final_outcome,
          "materialized_context_grants" => final_outcome["materialized_context_grants"], "finished_at" => now, "updated_at" => now).compact
        finished_job = current_job.merge("status" => status, "updated_at" => now)
        finished_attempt = current_attempt.merge("status" => %w[succeeded cancelled].include?(status) ? status : "failed", "outcome" => final_outcome, "finished_at" => now, "updated_at" => now)
        begin
          store.commit([["jobs", finished_job], ["runs", finished_run], ["attempts", finished_attempt]], expect: [
            { collection: "jobs", id: current_job.fetch("id"), fields: current_job },
            { collection: "runs", id: current_run.fetch("id"), fields: current_run.merge("cancellation_requested_at" => current_run["cancellation_requested_at"]) }
          ], activity: completion_events(current_run, finished_attempt, outcome, final_outcome, runner, now))
          return [finished_job, finished_run, final_outcome]
        rescue ActivityConflictError
          # A completion recorded twice under one identity with different content is a defect in
          # how the id was derived, not a lost race; retrying would loop on it forever.
          raise
        rescue ConflictError
          next
        end
      end
    end

    def update_run(run_id)
      loop do
        current = store.fetch!("runs", run_id)
        updated = yield(current).merge("updated_at" => Records.timestamp)
        begin
          return store.commit([["runs", updated]], expect: [{ collection: "runs", id: run_id,
            fields: current.merge("cancellation_requested_at" => current["cancellation_requested_at"]) }]).first
        rescue ActivityConflictError
          raise
        rescue ConflictError
          next
        end
      end
    end

    # The run's own answer to "how much of this run's output is accounted for". It rides the same
    # guarded update as the runtime identity, so a cancellation arriving mid-run is not lost to it.
    def record_capture(run, capture, final: false)
      block = capture_block(capture)
      return run unless block
      # Mid-run the run record is rewritten only when the *answer* changes — a stream appearing, or
      # its coverage moving — not when a byte count moves. Every chunk already committed its own
      # event; restating the totals on the run per chunk would be a second, noisier log of them.
      # The write after the runner returns is the one that carries the final counts.
      return run if run["capture"].is_a?(Hash) && unchanged?(run["capture"], block, final: final)

      update_run(run.fetch("id")) { |current| current.merge("capture" => block) }
    rescue Backstage::ActivityConflictError
      raise
    rescue Backstage::Error
      # Coverage is a summary of events that are already durable. Failing to restate it must not
      # turn a completed run into a failed one.
      run
    end

    # Nil rather than an empty "complete", because a run whose runner opened no stream has not
    # captured a complete anything — it simply has nothing to say.
    def capture_block(capture, existing = nil)
      summaries = capture.summaries
      return existing if summaries.empty?

      block = Outcome.capture_summary(summaries, limit_bytes: capture_limit_bytes,
                                      updated_at: Records.timestamp)
      return block unless existing.is_a?(Hash)

      # Only `error` is carried up from a runtime's single-stream block. `stream_id` and
      # `last_offset` are one stream's position, and a run has as many streams as its runner opened
      # — putting the last one's on the run-level block named a whole run by whichever stream
      # happened to close last. They are per-stream and they stay in `streams`.
      block.merge(existing.slice("error").compact)
    end

    def unchanged?(stored, block, final:)
      return shape(stored) == shape(block) unless final

      stored.reject { |key, _| key == "updated_at" } == block.reject { |key, _| key == "updated_at" }
    end

    # The part of a coverage block that answers "is any of this run's output unaccounted for".
    def shape(block)
      [block["status"], Array(block["streams"]).map { |stream| [stream["stream_id"], stream["coverage"]] }]
    end

    def capture_limit_bytes
      @capture_options.fetch(:max_run_bytes, Backstage::Application::RuntimeCapture::MAX_RUN_BYTES)
    end

    def symbolize(options)
      (options || {}).each_with_object({}) { |(key, value), row| row[key.to_sym] = value }
    end

    def observe_event(run, event)
      return run unless event.is_a?(Hash) && event["type"] == "runtime_started"

      update_run(run.fetch("id")) { |current| current.merge("runtime" => event.reject { |key, _| key == "type" }) }
    end

    def cancellation_requested?(run_id)
      run = store.fetch!("runs", run_id)
      return true if run["cancellation_requested_at"] || run["status"] != "running"

      job = store.fetch!("jobs", run.fetch("job_id"))
      !!workflow_service.superseded_dispatch?(run.fetch("work_item_id"), job)
    end

    def persist_change_artifact(outcome, work_item_id:, run_id:, runner:)
      metadata = outcome["change_artifact"]
      return unless metadata && metadata["source_path"]

      source_path = metadata.fetch("source_path")
      raise ContractError, "materialized patch artifact is missing" unless File.file?(source_path)

      content = File.binread(source_path)
      raise ContractError, "materialized patch artifact digest mismatch" unless Digest::SHA256.hexdigest(content) == metadata.fetch("patch_sha256")

      @artifact_store.write(
        work_item_id: work_item_id,
        run_id: run_id,
        name: "change.patch",
        content: content,
        kind: "binary_patch",
        provenance: { "adapter" => adapter_identifier(runner), "base_revision" => metadata.fetch("base_revision"), "branch" => metadata.fetch("branch"), "captured_at" => Records.timestamp }
      )
    end

    # A reviewer's verdict becomes its own artifact, bound to the candidate digest it reviewed, so a
    # later approval cannot be replayed against different content.
    def persist_review_verdict(outcome, work_item_id:, run_id:, runner:)
      review = outcome["review"]
      return unless review

      candidate = store.fetch!("runs", run_id).dig("reviewed_candidate", "sha256")
      @artifact_store.write(
        work_item_id: work_item_id,
        run_id: run_id,
        name: "review-verdict.json",
        content: review,
        kind: "review_verdict",
        provenance: {
          "adapter" => adapter_identifier(runner),
          "verdict" => review["verdict"],
          "independent" => review["independent"] == true,
          "reviewer_session_id" => review["reviewer_session_id"],
          "candidate_sha256" => candidate,
          "captured_at" => Records.timestamp
        }
      )
    end

    def adapter_identifier(runner)
      runner.respond_to?(:adapter_identifier) ? runner.adapter_identifier : runner.class.name
    end
  end
end

Backstage::Engine = Backstage::Application::Engine unless defined?(Backstage::Engine)
