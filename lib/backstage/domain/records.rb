# frozen_string_literal: true

require "securerandom"
require "socket"
require "time"

module Backstage::Domain
  module Records
    module_function

    def timestamp
      Time.now.utc.iso8601(6)
    end

    def host_name
      @host_name ||= Socket.gethostname
    rescue StandardError
      @host_name = "unknown"
    end

    def id(prefix)
      "#{prefix}-#{SecureRandom.hex(6)}"
    end

    # A work item owns its position in a configured workflow: `state`, an integer `revision`
    # bumped by every transition, and the immutable `workflow` binding it was admitted with.
    # Jobs, runs, and attempts carry `status` and describe execution only.
    def work_item(idempotency_key:, title:, description:, source:, source_ref:, workflow:, target: nil, source_instance: nil, source_identity: nil)
      now = timestamp
      {
        "schema_version" => 1,
        "id" => id("work"),
        "idempotency_key" => idempotency_key,
        "title" => title,
        "description" => description,
        "source" => source,
        "source_ref" => source_ref,
        "workflow" => workflow.binding,
        "state" => workflow.initial_state,
        "revision" => 0,
        "revisions_used" => 0,
        "created_at" => now,
        "updated_at" => now,
        "target" => target,
        "source_instance" => source_instance,
        "source_identity" => source_identity
      }.compact
    end

    def work_transition(work_item_id:, request_id:, transition:, from:, to:, workflow:, actor:, revision:, reason: nil, evidence: [], decision_id: nil, job_id: nil, run_id: nil, details: {})
      {
        "schema_version" => 1,
        "id" => id("transition"),
        "work_item_id" => work_item_id,
        "request_id" => request_id,
        "transition" => transition,
        "from" => from,
        "to" => to,
        "workflow_name" => workflow.fetch("name"),
        "workflow_version" => workflow.fetch("version"),
        "workflow_digest" => workflow.fetch("digest"),
        "actor" => actor,
        "revision" => revision,
        "reason" => reason,
        "evidence" => evidence,
        "decision_id" => decision_id,
        "job_id" => job_id,
        "run_id" => run_id,
        "recorded_at" => timestamp
      }.merge(details).compact
    end

    def decision(work_item_id:, work_revision:, transition:, question:, choices:, state:, candidate_sha256: nil, context: nil)
      now = timestamp
      {
        "schema_version" => 1,
        "id" => id("decision"),
        "work_item_id" => work_item_id,
        "work_revision" => work_revision,
        "raised_by_transition" => transition,
        "state" => state,
        "question" => question,
        "choices" => choices,
        "candidate_sha256" => candidate_sha256,
        "context" => context,
        "status" => "open",
        "created_at" => now,
        "updated_at" => now
      }.compact
    end

    def agent_request(work_item_id:, run_id:, actor:, transition:, reason: nil, evidence: [], request_id: nil, received_at: nil)
      {
        "schema_version" => 1,
        "id" => id("agentreq"),
        "work_item_id" => work_item_id,
        "run_id" => run_id,
        "actor" => actor,
        "transition" => transition,
        "reason" => reason,
        "evidence" => evidence,
        "request_id" => request_id,
        "status" => "received",
        "received_at" => received_at || timestamp,
        "updated_at" => timestamp
      }.compact
    end

    def job(work_item_id:, policy_name:, bundle: nil, phase: "implementation", transition_id: nil, request_id: nil)
      now = timestamp
      {
        "schema_version" => 1,
        "id" => id("job"),
        "work_item_id" => work_item_id,
        "policy_name" => policy_name,
        "bundle" => bundle,
        "phase" => phase,
        "transition_id" => transition_id,
        "request_id" => request_id,
        "status" => "queued",
        "created_at" => now,
        "updated_at" => now
      }.compact
    end

    def run(job_id:, phase: "implementation", work_item_id: nil, work_revision: nil)
      now = timestamp
      {
        "schema_version" => 1,
        "id" => id("run"),
        "job_id" => job_id,
        "work_item_id" => work_item_id,
        "work_revision" => work_revision,
        "phase" => phase,
        "status" => "queued",
        "owner_pid" => Process.pid,
        "owner_host" => host_name,
        "created_at" => now,
        "updated_at" => now
      }.compact
    end

    def attempt(run_id:, number:)
      now = timestamp
      {
        "schema_version" => 1,
        "id" => id("attempt"),
        "run_id" => run_id,
        "number" => number,
        "status" => "running",
        "started_at" => now,
        "updated_at" => now
      }
    end

    def artifact(work_item_id:, run_id:, kind:, path:, sha256:, provenance:)
      {
        "schema_version" => 1,
        "id" => id("artifact"),
        "work_item_id" => work_item_id,
        "run_id" => run_id,
        "kind" => kind,
        "path" => path,
        "sha256" => sha256,
        "provenance" => provenance,
        "created_at" => timestamp
      }
    end

    # Explicitly accepted work. The dispatcher only ever touches work that has one of these, so
    # nothing is silently adopted. `revision` guards concurrent writers the same way a work item's
    # does; `generation` fences the runs an earlier acceptance owned.
    def execution_intent(id:, work_item_id:, request_id:, request_fingerprint:, mode:, generation:, retry_policy:, due_at:, start_transition: nil, accepted_by: nil)
      now = timestamp
      {
        "schema_version" => 1,
        "id" => id,
        "work_item_id" => work_item_id,
        "request_id" => request_id,
        "request_fingerprint" => request_fingerprint,
        "generation" => generation,
        "mode" => mode,
        "start_transition" => start_transition,
        "retry_policy" => retry_policy,
        "status" => "queued",
        "revision" => 0,
        "attempts_used" => 0,
        "retries_used" => 0,
        "due_at" => due_at,
        "accepted_by" => accepted_by,
        "accepted_at" => now,
        "created_at" => now,
        "updated_at" => now
      }.compact
    end

    def external_action(work_item_id:, kind:, idempotency_key:, status:, reference: nil, response: nil)
      {
        "schema_version" => 1,
        "id" => id("action"),
        "work_item_id" => work_item_id,
        "kind" => kind,
        "idempotency_key" => idempotency_key,
        "status" => status,
        "reference" => reference,
        "response" => response,
        "created_at" => timestamp,
        "updated_at" => timestamp
      }.compact
    end
  end
end

Backstage::Records = Backstage::Domain::Records unless defined?(Backstage::Records)
