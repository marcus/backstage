# frozen_string_literal: true

require_relative "test_helper"

# The compatibility boundary between outcome v1 and v2.
#
# The reason v2 exists is that `raw.events` had to stop being an inlined transcript, and a document
# still stamped `schema_version: 1` may not quietly change what it means. So these assert both
# directions: an old document still reads, and a new one can still be handed to a v1 reader.
class OutcomeProjectionTest < Minitest::Test
  Outcome = Backstage::Domain::Outcome

  def v1
    {
      "schema_version" => 1,
      "status" => "succeeded",
      "summary" => "pi run succeeded",
      "process" => { "exit_code" => 0, "signal" => nil },
      "usage" => { "input_tokens" => 10, "output_tokens" => 2, "cost" => nil, "cost_trusted" => false },
      "raw" => { "vendor" => "pi", "model" => "m", "authoritative_message" => { "role" => "assistant" },
                 "events" => [{ "type" => "session" }, { "type" => "agent_settled" }] }
    }
  end

  def v2
    {
      "schema_version" => 2,
      "status" => "succeeded",
      "summary" => "pi run succeeded",
      "process" => { "exit_code" => 0, "signal" => nil },
      "logs" => "tail\n",
      "logs_truncated" => true,
      "log_tail_bytes" => 65_536,
      "capture" => {
        "status" => "truncated", "bytes" => 128, "limit_bytes" => 1024,
        "streams" => [{ "stream_id" => "run-1:1:0:harness", "step" => "harness", "coverage" => "truncated",
                        "bytes" => 128, "records" => 4, "chunks" => 1, "malformed" => 0, "last_offset" => 128 }]
      },
      "raw" => { "vendor" => "pi", "model" => "m", "authoritative_message" => nil,
                 "stream_refs" => [{ "stream_id" => "run-1:1:0:harness", "artifact_id" => "artifact-abc",
                                     "records" => 4, "bytes" => 128, "coverage" => "truncated" }] }
    }
  end

  def test_a_v1_outcome_upgrades_without_losing_what_it_carried
    upgraded = Outcome.upgrade(v1)

    assert_equal 2, upgraded.fetch("schema_version")
    assert_equal [], upgraded.dig("raw", "stream_refs")
    assert_equal false, upgraded.fetch("logs_truncated")
    # The transcript an old document already holds is part of what that document means. Dropping it
    # on the way in would be the same silent change v2 exists to avoid, in the other direction.
    assert_equal 2, upgraded.dig("raw", "events").length
    Outcome.validate!(upgraded)
  end

  def test_both_versions_validate_against_their_own_schema
    assert_equal v1, Outcome.validate!(v1)
    assert_equal v2, Outcome.validate!(v2)
    error = assert_raises(Backstage::ContractError) { Outcome.validate!(v2.merge("schema_version" => 3)) }
    assert_includes error.message, "unsupported outcome schema_version"
  end

  def test_a_v2_outcome_projects_to_a_document_a_v1_reader_can_read
    projected = Outcome.project_v1(v2)

    # The assertion that matters: it validates against the untouched v1 contract file.
    Backstage::Contracts::Validator.new.validate!("outcome-v1.json", projected)
    assert_equal 1, projected.fetch("schema_version")
    assert_equal 2, projected.dig("raw", "projected_from_schema_version")
    refute projected.dig("raw").key?("events"),
           "a projection must not invent an events array that claims the transcript was empty"
    assert_equal "run-1:1:0:harness", projected.dig("raw", "stream_refs", 0, "stream_id")
    assert_equal "truncated", projected.dig("capture", "status")
  end

  def test_projecting_a_v1_outcome_is_the_identity
    assert_equal v1, Outcome.project_v1(v1)
  end

  def test_normalize_accepts_either_version_and_always_returns_a_valid_v2
    assert_equal 2, Outcome.normalize(v1).fetch("schema_version")
    assert_equal 2, Outcome.normalize(v2).fetch("schema_version")
  end

  def test_a_synthetic_failure_names_its_capture_instead_of_being_a_bare_hash
    failure = Outcome.failure(summary: "execution ended without a recorded outcome", interrupted: true,
                              capture: { "status" => "gap", "streams" => [] })

    assert_equal 2, failure.fetch("schema_version")
    assert_equal "failed", failure.fetch("status")
    assert_equal true, failure.fetch("interrupted")
    assert_equal "gap", failure.dig("capture", "status")
    assert_nil failure.dig("process", "exit_code")
  end

  def test_run_coverage_is_the_weakest_any_stream_reached
    summaries = [
      { "stream_id" => "s1", "coverage" => "complete", "bytes" => 10, "records" => 1, "chunks" => 1, "malformed" => 0, "last_offset" => 10 },
      { "stream_id" => "s2", "coverage" => "gap", "bytes" => 5, "records" => 1, "chunks" => 1, "malformed" => 0, "last_offset" => 5 }
    ]

    block = Outcome.capture_summary(summaries, limit_bytes: 100, updated_at: "now")

    assert_equal "gap", block.fetch("status"), "one unaccounted stream makes the run unaccounted for"
    assert_equal 15, block.fetch("bytes")
    assert_equal 100, block.fetch("limit_bytes")
    assert_equal %w[failed gap open truncated complete complete],
                 %w[failed gap open truncated complete none].map { |coverage| Outcome.weakest_coverage([coverage, "complete"]) }
    assert_equal "open", Outcome.weakest_coverage(%w[open truncated]),
                 "a stream still being written is less settled than one a limit stopped"
    assert_equal "gap", Outcome.weakest_coverage(%w[open gap]),
                 "but a concluded gap is weaker than a stream that is merely still running"
  end
end
