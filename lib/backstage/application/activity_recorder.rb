# frozen_string_literal: true

require "digest"

module Backstage::Application
  # How an application service puts an event in the same commit as the state change it explains.
  #
  # The recorder owns the envelope defaults no emitter should have to repeat: deployment identity
  # comes from the store, producer identity from the service that composed the recorder, and
  # correlation defaults to the work item — or, for a system event with no work, to the durable
  # request identity. It builds envelopes and nothing else. It never writes, keeps nothing between
  # calls and starts no threads, so a caller hands what it built straight to
  # `store.commit(writes, expect:, activity:)` and lets one guard decide whether both land.
  class ActivityRecorder
    Activity = Backstage::Domain::Activity
    Records = Backstage::Domain::Records

    # A deterministic event id for a fact a retry can tell twice. Derive it from the durable
    # identity the retry shares — a transition id, a run id, an intent id and attempt number — so
    # the store reconciles the second telling instead of growing history. A fact that can only
    # happen once, because a fresh record was minted for it, may take a random id instead.
    def self.event_id(*parts)
      "event-#{Digest::SHA256.hexdigest(parts.map(&:to_s).join(":"))}"
    end

    # The ownership fields of an actor, read from the entry context the transition operation
    # normalized. Never from request text, and never from anything a model wrote.
    def self.actor_data(actor)
      { "role" => actor["role"], "entry" => actor["entry"], "id" => actor["id"] }.compact
    end

    def initialize(store:, adapter:, instance: nil)
      @store = store
      @adapter = adapter.to_s
      @instance = (instance || "#{Records.host_name}:#{Process.pid}").to_s
    end

    # `provenance` is the authority class of what the event *says*: `core` for something Backstage
    # checked and applied itself, `runtime_reported` for what a runtime claimed about its own work,
    # `operator` for what a person asked for through a trusted entry context. `adapter` names the
    # code that produced the event, which is a different question and defaults to this recorder's.
    #
    # Keep `summary` built from ids and states and `data` down to ids, states, counts and
    # revisions: a summary is not the place for model text, and a payload is not the place for an
    # outcome or a log.
    def event(type:, event_id: nil, provenance: "core", adapter: nil, offset: nil,
              work_item_id: nil, request_id: nil, correlation_id: nil, occurred_at: nil,
              summary: nil, data: nil, **relations)
      Activity.event(
        type: type,
        deployment_id: @store.deployment_id,
        event_id: event_id,
        occurred_at: occurred_at,
        source: Activity.source(adapter: adapter || @adapter, provenance: provenance,
                                instance: @instance, offset: offset),
        work_item_id: work_item_id,
        request_id: request_id,
        correlation_id: correlation_id || work_item_id || request_id,
        summary: summary,
        data: data && payload(data),
        **relations
      )
    end

    private

    def payload(data)
      raise Backstage::ContractError, "activity data must be an object" unless data.is_a?(Hash)

      data.each_with_object({}) do |(key, value), row|
        next if value.nil?

        row[key.to_s] = value
      end
    end
  end
end
