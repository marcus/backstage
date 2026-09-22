# frozen_string_literal: true

require_relative "test_helper"

class ActivityDomainTest < Minitest::Test
  Activity = Backstage::Domain::Activity

  def source(**overrides)
    Activity.source(**{ adapter: "backstage.core", provenance: "core" }.merge(overrides))
  end

  def build(**overrides)
    Activity.event(**{ type: "work.admitted", deployment_id: "deployment-1", source: source }.merge(overrides))
  end

  def test_schema_enum_and_the_code_vocabulary_are_the_same_list
    schema = JSON.parse(File.read(File.expand_path("../schemas/activity-event-v1.json", __dir__)))
    assert_equal Activity::TYPES, schema.dig("properties", "type", "enum")
    assert_equal Activity::PROVENANCE, schema.dig("properties", "source", "properties", "provenance", "enum")
    assert_includes Activity::TYPES, Activity::STREAM_STARTED
    # A runtime saying it finished is not evidence that it did.
    assert_includes Activity::TYPES, "runtime.reported_completion"
    assert_includes Activity::TYPES, "execution.verified_completion"
  end

  def test_envelope_mints_an_id_omits_absent_optionals_and_never_carries_store_fields
    event = build(work_item_id: "work-1", summary: "admitted")

    assert_match(/\Aevent-[0-9a-f]{12}\z/, event.fetch("event_id"))
    assert_equal 1, event.fetch("schema_version")
    assert_equal "deployment-1", event.fetch("deployment_id")
    refute event.key?("sequence"), "sequence is the store's to assign"
    refute event.key?("recorded_at"), "recorded_at is the store's to assign"
    refute event.key?("run_id")
    refute event.key?("links")
    Activity.validate!(event)
  end

  def test_a_deterministic_id_is_what_makes_a_retry_reconcilable
    first = build(event_id: "event-fixed", occurred_at: "2026-01-01T00:00:00.000000Z")
    second = build(event_id: "event-fixed", occurred_at: "2026-01-01T00:00:00.000000Z")

    assert_equal first.fetch("event_id"), second.fetch("event_id")
    assert_equal Activity.canonical_fingerprint(first), Activity.canonical_fingerprint(second)
  end

  def test_the_fingerprint_ignores_position_and_ingestion_time_but_nothing_else
    event = build(event_id: "event-fixed", occurred_at: "2026-01-01T00:00:00.000000Z", data: { "b" => 1, "a" => 2 })
    baseline = Activity.canonical_fingerprint(event)

    assert_equal baseline, Activity.canonical_fingerprint(event.merge("sequence" => 7, "recorded_at" => "later"))
    # Key insertion order is not content; a producer building its hash differently is the same fact.
    assert_equal baseline, Activity.canonical_fingerprint(event.merge("data" => { "a" => 2, "b" => 1 }))
    refute_equal baseline, Activity.canonical_fingerprint(event.merge("summary" => "changed"))
    refute_equal baseline, Activity.canonical_fingerprint(event.merge("occurred_at" => "2026-01-02T00:00:00.000000Z"))
  end

  def test_unknown_types_provenance_and_missing_identity_are_refused_at_build_time
    assert_raises(Backstage::ContractError) { build(type: "work.invented") }
    assert_raises(Backstage::ContractError) { build(deployment_id: "") }
    assert_raises(Backstage::ContractError) { Activity.source(adapter: "x", provenance: "trust_me") }
    assert_raises(Backstage::ContractError) { Activity.source(adapter: "", provenance: "core") }
    assert_raises(Backstage::ContractError) { build(links: [{ "id" => "x" }, "not-a-link"]) }
  end

  def test_contract_rejects_a_malformed_event
    validator = Backstage::ContractValidator.new
    valid = build(summary: "fine")

    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.reject { |key, _| key == "source" }) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("type" => "work.invented")) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("schema_version" => 2)) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("invented_field" => "x")) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("source" => { "adapter" => "x" })) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("links" => [{ "type" => "run" }])) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("summary" => "x" * (Activity::SUMMARY_LIMIT + 1))) }
    assert_raises(Backstage::ContractError) { validator.validate!(Activity::SCHEMA, valid.merge("work_item_id" => 7)) }
  end

  def test_a_builder_summary_is_bounded_rather_than_committing_an_oversized_line
    event = build(summary: "x" * 5_000)

    assert_equal Activity::SUMMARY_LIMIT, event.fetch("summary").length
    assert event.fetch("summary").end_with?("...")
    Activity.validate!(event)
  end

  def test_related_ids_cover_relationships_links_and_artifacts
    event = build(event_id: "event-1", work_item_id: "work-1", run_id: "run-1", decision_id: "decision-1",
                  links: [{ "type" => "work_item", "id" => "work-2", "relation" => "depends_on" }],
                  artifact_refs: ["artifact-1"])

    %w[event-1 work-1 run-1 decision-1 work-2 artifact-1].each do |id|
      assert_includes Activity.related_ids(event), id
    end
    assert_equal Activity.related_ids(event).uniq, Activity.related_ids(event)
  end

  def test_filters_are_normalized_and_unknown_ones_are_refused_not_ignored
    normalized = Activity.normalize_filters(type: "work.admitted", work_item_id: "work-1", run_id: nil)

    assert_equal({ "type" => ["work.admitted"], "work_item_id" => "work-1" }, normalized)
    # Order and duplication in a caller's type list must not change which cursor is valid.
    assert_equal Activity.filter_fingerprint(Activity.normalize_filters(type: %w[decision.raised work.admitted])),
                 Activity.filter_fingerprint(Activity.normalize_filters(type: %w[work.admitted decision.raised work.admitted]))
    refute_equal Activity.filter_fingerprint(normalized), Activity.filter_fingerprint({})
    assert_raises(Backstage::ContractError) { Activity.normalize_filters(work_item: "work-1") }
    assert_raises(Backstage::ContractError) { Activity.normalize_filters(type: "work.invented") }
  end

  def test_matching_uses_the_same_rules_every_store_must_answer_with
    event = build(work_item_id: "work-1", run_id: "run-1", links: [{ "type" => "target", "id" => "target-9" }])

    assert Activity.matches?(event, Activity.normalize_filters(work_item_id: "work-1"))
    assert Activity.matches?(event, Activity.normalize_filters(type: %w[work.admitted decision.raised]))
    assert Activity.matches?(event, Activity.normalize_filters(related_id: "target-9"))
    refute Activity.matches?(event, Activity.normalize_filters(work_item_id: "work-2"))
    refute Activity.matches?(event, Activity.normalize_filters(target_id: "target-9")), "a link is not the target field"
    refute Activity.matches?(event, Activity.normalize_filters(work_item_id: "work-1", type: "decision.raised"))
  end
end
