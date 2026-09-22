# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Domain
  # The immutable activity envelope. An activity event is an historical fact: once a store has
  # accepted one it is never rewritten, and its `sequence` is the only reading order that matters.
  # Operational records (`work_items`, `runs`, ...) stay authoritative for current state.
  #
  # The application chooses an event's meaning and its identities; the store assigns `sequence`
  # and `recorded_at` and enforces immutability. A builder here never talks to a store, so the
  # same envelope can be produced by a recorder, a test, or a future adapter.
  module Activity
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records

    SCHEMA_VERSION = 1
    SCHEMA = "activity-event-v1.json"

    # Events never live in a record collection; the name is reserved so `save` can never reach one.
    COLLECTION = "activity"

    # A summary is a bounded, redacted, human-readable line. Detail belongs in `data` or an
    # artifact reference, not here. The builder truncates; the contract refuses anything longer.
    SUMMARY_LIMIT = 500

    # How large one event's serialized `data` may be. History is append-only and fsynced inline
    # with the state change it explains, so an unbounded payload is a permanent cost paid by every
    # later read of the log — and a place for an outcome, a transcript, or a diff to hide. 16 KiB
    # is far more than the ids, states, counts and revisions `data` is for, and far less than
    # anything worth an artifact reference instead.
    DATA_BYTE_LIMIT = 16 * 1024

    # The largest page `read_activity` will serve. A page is materialized in memory by every store
    # and crosses every surface, so the bound belongs to the contract rather than to one adapter's
    # judgment. A consumer that wants more history reads more pages; that is what the cursor is for.
    MAX_READ_LIMIT = 1000

    # Free text a producer did not author — an operator's `--reason`, a source's failure line — is
    # bounded before it becomes history. Shorter than a summary because it is one clause inside a
    # payload, not the event's headline.
    REASON_LIMIT = 200

    # The one authoritative vocabulary. `schemas/activity-event-v1.json` carries the same list as
    # its `type` enum and a test asserts they stay identical, so code and contract cannot drift.
    # Note that a runtime's self-reported completion and the core's verified completion are
    # deliberately distinct types: an agent saying it finished is not evidence that it did.
    TYPES = %w[
      work.admitted
      work.transition_applied
      decision.raised
      decision.answered
      decision.cancelled
      execution.accepted
      execution.started
      execution.finished
      execution.retry_scheduled
      execution.cancellation_requested
      execution.verified_completion
      runtime.observed
      runtime.reported_completion
      agent.message_observed
      agent.tool_observed
      artifact.available
      effect.proposed
      effect.approved
      effect.attempted
      effect.verified
      effect.failed
      effect.uncertain
      delivery.queued
      delivery.sent
      delivery.failed
      delivery.uncertain
      reconciliation.finding
      source.checked
      dispatcher.health_observed
      activity.stream_started
    ].freeze

    # The marker a store appends the first time a deployment records anything, so a reader can
    # tell "nothing happened before this" from "history was lost or never imported".
    STREAM_STARTED = "activity.stream_started"

    # How much authority the producer of an event had. This is the difference between what
    # Backstage verified, what a runtime claimed, and what a model said about itself.
    PROVENANCE = %w[
      core
      store
      runtime_reported
      agent_reported
      operator
      external_observation
    ].freeze

    # Durable relationship ids an event can be found by. `related_id` searches all of them plus
    # the event's own id, its typed links, and its artifact references.
    RELATIONSHIP_FIELDS = %w[
      work_item_id
      target_id
      job_id
      run_id
      attempt_id
      request_id
      transition_id
      decision_id
      action_id
      delivery_id
      causation_event_id
      correlation_id
      provider_session_id
      provider_event_id
    ].freeze

    # Store-assigned fields are excluded from the fingerprint: an exact retry of the same logical
    # event is the same fact even though it would land at a different position and wall time.
    FINGERPRINT_EXCLUDED = %w[sequence recorded_at].freeze

    FILTER_KEYS = %w[work_item_id run_id target_id type related_id].freeze

    module_function

    # Builds one event. `deployment_id` comes from the store (`store.deployment_id`), never from a
    # hostname or path. `event_id` is minted unless the caller supplies a deterministic id, which
    # is how a producer makes a retry reconcile instead of duplicating history. `sequence` and
    # `recorded_at` are absent by construction — a store refuses an event that carries either.
    def event(type:, deployment_id:, source:, occurred_at: nil, event_id: nil,
              work_item_id: nil, target_id: nil, job_id: nil, run_id: nil, attempt_id: nil,
              request_id: nil, transition_id: nil, decision_id: nil, action_id: nil, delivery_id: nil,
              causation_event_id: nil, correlation_id: nil, links: nil,
              provider_session_id: nil, provider_event_id: nil,
              summary: nil, data: nil, artifact_refs: nil)
      raise ContractError, "unknown activity type #{type.inspect}" unless TYPES.include?(type.to_s)
      raise ContractError, "activity deployment_id is required" if deployment_id.to_s.empty?

      {
        "schema_version" => SCHEMA_VERSION,
        "event_id" => event_id || Records.id("event"),
        "type" => type.to_s,
        "deployment_id" => deployment_id,
        "occurred_at" => occurred_at || Records.timestamp,
        "source" => normalize_source(source),
        "work_item_id" => work_item_id,
        "target_id" => target_id,
        "job_id" => job_id,
        "run_id" => run_id,
        "attempt_id" => attempt_id,
        "request_id" => request_id,
        "transition_id" => transition_id,
        "decision_id" => decision_id,
        "action_id" => action_id,
        "delivery_id" => delivery_id,
        "causation_event_id" => causation_event_id,
        "correlation_id" => correlation_id,
        "links" => links && normalize_links(links),
        "provider_session_id" => provider_session_id,
        "provider_event_id" => provider_event_id,
        "summary" => summary && bounded_summary(summary),
        "data" => data,
        "artifact_refs" => artifact_refs && Array(artifact_refs).map(&:to_s)
      }.compact
    end

    # Trusted producer identity. `adapter` names the code that produced the event, `instance` the
    # process or container it ran in, `offset` the producer's own event id or durable stream
    # offset (so replaying the same source chunk deduplicates), `provenance` the authority class.
    def source(adapter:, provenance:, instance: nil, offset: nil)
      raise ContractError, "unknown activity provenance #{provenance.inspect}" unless PROVENANCE.include?(provenance.to_s)
      raise ContractError, "activity source adapter is required" if adapter.to_s.empty?

      {
        "adapter" => adapter.to_s,
        "provenance" => provenance.to_s,
        "instance" => instance&.to_s,
        "offset" => offset&.to_s
      }.compact
    end

    # The identity of an event's content. Two events with the same id and the same fingerprint are
    # the same fact and reconcile; the same id with a different fingerprint is a conflict a store
    # refuses rather than a history it rewrites.
    def canonical_fingerprint(event)
      payload = stringify(event).reject { |key, _| FINGERPRINT_EXCLUDED.include?(key) }
      Digest::SHA256.hexdigest(canonical_json(payload))
    end

    # JSON with object keys in sorted order at every depth, so a fingerprint does not depend on
    # the order a producer happened to build its hashes in. Array order is content and is kept.
    def canonical_json(value)
      case value
      when Hash then "{#{value.keys.map(&:to_s).sort.map { |key| "#{JSON.generate(key)}:#{canonical_json(value[key])}" }.join(",")}}"
      when Array then "[#{value.map { |nested| canonical_json(nested) }.join(",")}]"
      else JSON.generate(value)
      end
    end

    # The schema cannot express "this object serializes to at most N bytes", so the size bound is
    # checked here — beside the schema check, in the one place every store already calls before an
    # event becomes durable — rather than left to each adapter to remember.
    def validate!(event, validator: Backstage::Contracts::Validator.new)
      validator.validate!(SCHEMA, event)
      check_data_size!(event)
      event
    end

    def check_data_size!(event)
      data = event["data"] || event[:data]
      return if data.nil?

      size = JSON.generate(data).bytesize
      return if size <= DATA_BYTE_LIMIT

      raise ContractError,
            "activity data is #{size} bytes, over the #{DATA_BYTE_LIMIT}-byte limit; " \
            "reference an artifact instead of inlining the payload"
    end

    # The page bound, refused rather than silently clamped: a caller that asked for 5,000 events
    # and quietly got 1,000 would read the short page as "that is all there is".
    def check_read_limit!(limit)
      limit = Integer(limit)
      raise ContractError, "activity limit must be positive" unless limit.positive?
      raise ContractError, "activity limit #{limit} is over the maximum of #{MAX_READ_LIMIT}" if limit > MAX_READ_LIMIT

      limit
    end

    # Every id this event can be found by. Correlation is a lookup aid, never an authority claim.
    def related_ids(event)
      ids = [event["event_id"]]
      RELATIONSHIP_FIELDS.each { |field| ids << event[field] }
      Array(event["links"]).each { |link| ids << link["id"] if link.is_a?(Hash) }
      Array(event["artifact_refs"]).each { |reference| ids << reference }
      ids.compact.map(&:to_s).uniq
    end

    # Filter semantics belong to the contract, not to one adapter, so every store answers the same
    # question the same way. Unknown filter keys and unknown types are refused rather than ignored:
    # silently dropping a filter would hand a caller more history than it asked for.
    def normalize_filters(filters)
      (filters || {}).each_with_object({}) do |(key, value), normalized|
        name = key.to_s
        raise ContractError, "unknown activity filter #{name}" unless FILTER_KEYS.include?(name)
        next if value.nil? || (value.is_a?(Array) && value.empty?)

        if name == "type"
          types = Array(value).map(&:to_s).uniq.sort
          types.each { |type| raise ContractError, "unknown activity type #{type.inspect}" unless TYPES.include?(type) }
          normalized[name] = types
        else
          normalized[name] = value.to_s
        end
      end
    end

    # Identifies the normalized filter set a cursor was issued for. A store binds cursors to this
    # so resuming with different filters is refused instead of quietly skipping history.
    def filter_fingerprint(normalized_filters)
      Digest::SHA256.hexdigest(canonical_json(normalized_filters))[0, 16]
    end

    def matches?(event, normalized_filters)
      normalized_filters.all? do |key, value|
        case key
        when "type" then value.include?(event["type"])
        when "related_id" then related_ids(event).include?(value)
        else !event[key].nil? && event[key].to_s == value
        end
      end
    end

    def bounded_summary(summary)
      bounded_text(summary, limit: SUMMARY_LIMIT)
    end

    def bounded_text(text, limit: REASON_LIMIT)
      text = text.to_s
      return text if text.length <= limit

      "#{text[0, limit - 3]}..."
    end

    def normalize_source(source)
      raise ContractError, "activity source must be an object" unless source.is_a?(Hash)

      stringify(source)
    end

    def normalize_links(links)
      raise ContractError, "activity links must be an array" unless links.is_a?(Array)

      links.map do |link|
        raise ContractError, "activity link must be an object with type and id" unless link.is_a?(Hash)

        stringify(link)
      end
    end

    def stringify(value)
      JSON.parse(JSON.generate(value))
    end
  end
end
