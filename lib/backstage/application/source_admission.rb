# frozen_string_literal: true

require "digest"
require "json"

module Backstage::Application
  # Sources expose native documents. Admission adds only execution routing and provenance;
  # discovery never consumes an item before the engine has committed its assignment.
  class SourceAdmission
    ContractError = Backstage::ContractError
    CHECKS = "source_checks"

    def initialize(engine:, configuration:, adapter_factory:)
      @engine = engine
      @configuration = configuration
      @adapter_factory = adapter_factory
    end

    def submit(connection:, ref:, target: nil)
      binding = @configuration.source_binding(connection, target: target)
      snapshot = snapshot_for(connection, ref)
      admit(binding, snapshot)
    end

    def poll(connection:, target: nil)
      binding = @configuration.source_binding(connection, target: target)
      refs = @adapter_factory.call(connection).discover
      raise ContractError, "source discovery must return an array of native references" unless refs.is_a?(Array) && refs.all? { |ref| ref.is_a?(String) && !ref.empty? }

      work = refs.map { |ref| admit(binding, snapshot_for(connection, ref)) }
      record_check(connection: connection, status: "ok", refs: refs, target: binding.fetch(:target))
      work
    rescue Backstage::ActivityConflictError
      raise
    rescue StandardError => error
      record_check(connection: connection, status: "failed", refs: [], error: error, target: binding && binding[:target]) if binding
      raise
    end

    # Refresh is explicit replacement, never a mutation of accepted input. New content remains
    # unaccepted until an operator accepts it; the previous assignment cannot be executed again.
    def refresh(work_id)
      previous = @engine.store.fetch!("work_items", work_id)
      source = previous["source"] || raise(ContractError, "manual work has no source to refresh; submit a new document")
      @configuration.validate_source_binding(source, target: previous.fetch("target"))
      binding = @configuration.source_binding(source.fetch("connection"), target: previous.fetch("target"))
      snapshot = snapshot_for(source.fetch("connection"), source.fetch("ref"))
      unless snapshot.fetch("ref") == source.fetch("ref")
        raise ContractError, "refresh cannot change a native source reference"
      end
      digest = Digest::SHA256.hexdigest(snapshot.fetch("content"))
      if digest == previous.dig("input", "sha256") && snapshot["version"] == source["version"] &&
         snapshot.fetch("media_type") == previous.dig("input", "media_type") && snapshot.fetch("title") == previous.fetch("title")
        ensure_inactive!(previous.fetch("id"))
        return previous
      end
      key = "source-refresh:v2:#{Digest::SHA256.hexdigest(JSON.generate([work_id, digest, snapshot["version"], snapshot.fetch("media_type"), snapshot.fetch("title")]))}"
      admit(binding, snapshot, key: key, predecessor: work_id)
    end

    private

    def snapshot_for(connection, ref)
      snapshot = @adapter_factory.call(connection).snapshot(ref)
      raise ContractError, "source snapshot must be an object" unless snapshot.is_a?(Hash)

      snapshot = snapshot.transform_keys(&:to_s)
      %w[ref title content media_type].each do |key|
        raise ContractError, "source snapshot #{key} must be a string" unless snapshot[key].is_a?(String)
      end
      raise ContractError, "source snapshot ref must not be empty" if snapshot.fetch("ref").empty?
      if snapshot.key?("version") && !snapshot["version"].is_a?(String)
        raise ContractError, "source snapshot version must be an opaque string"
      end
      snapshot
    end

    def admit(binding, snapshot, key: nil, predecessor: nil)
      source = binding.fetch(:source).merge("ref" => snapshot.fetch("ref"))
      source["version"] = snapshot["version"] if snapshot.key?("version")
      key ||= "source:v2:#{Digest::SHA256.hexdigest(JSON.generate([source.values_at("connection", "kind", "identity", "ref"), binding.fetch(:target)]))}"
      @engine.submit(
        idempotency_key: key, title: snapshot.fetch("title"),
        input: snapshot.slice("content", "media_type"),
        source: source, target: binding.fetch(:target),
        workflow: @configuration.workflow_for_target(binding.fetch(:target)), predecessor: predecessor
      )
    end

    def ensure_inactive!(work_id)
      active = @engine.store.fetch!("work_items", work_id)["source_delivery_owner"] || @engine.active_execution?(work_id) || @engine.store.list("execution_intents").any? do |row|
        row["work_item_id"] == work_id && !%w[completed cancelled exhausted].include?(row["status"])
      end
      raise Backstage::ConflictError, "cannot refresh work with active execution or intent" if active
    end

    # Change-only source health receipts retain failure/recovery history without one event per
    # polling tick. Ref fingerprints remain opaque; source text stays in the work document.
    def record_check(connection:, status:, refs:, error: nil, target: nil)
      source = @configuration.sources.fetch(connection.to_s)
      id = "source:#{Digest::SHA256.hexdigest(JSON.generate([connection.to_s, source.fetch("kind"), source.fetch("identity"), target]))}"
      failure = error && error.class.name
      fingerprint = Digest::SHA256.hexdigest(JSON.generate([status, refs.sort, failure]))
      existing = @engine.store.fetch(CHECKS, id)
      return nil if existing && existing["fingerprint"] == fingerprint

      now = Backstage::Domain::Records.timestamp
      sequence = existing ? existing.fetch("check_sequence") + 1 : 1
      receipt = {
        "schema_version" => 2, "id" => id, "connection" => connection.to_s,
        "kind" => source.fetch("kind"), "identity" => source.fetch("identity"), "target" => target,
        "status" => status, "observed_at" => now, "check_sequence" => sequence,
        "discovered" => refs.length, "fingerprint" => fingerprint, "error" => failure
      }.compact
      expect = existing ? [{ collection: CHECKS, id: id, fields: existing.slice("fingerprint", "check_sequence") }] :
                          [{ collection: CHECKS, id: id, revision: nil }]
      recorder = ActivityRecorder.new(store: @engine.store, adapter: "backstage.application.source_admission")
      event = recorder.event(
        type: "source.checked",
        event_id: ActivityRecorder.event_id("source.checked", id, sequence, fingerprint),
        provenance: "external_observation", offset: "#{sequence}:#{fingerprint}", occurred_at: now,
        correlation_id: id, target_id: target,
        summary: "source #{connection} checked: #{status}, #{refs.length} discovered item(s)",
        data: receipt.slice("connection", "kind", "status", "check_sequence", "discovered", "error")
      )
      @engine.store.commit([[CHECKS, receipt]], expect: expect, activity: [event])
      receipt
    rescue Backstage::ActivityConflictError
      raise
    rescue Backstage::ConflictError
      nil
    end
  end
end
