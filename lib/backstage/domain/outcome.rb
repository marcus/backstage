# frozen_string_literal: true

module Backstage::Domain
  # The structured outcome of one execution, and the compatibility boundary between its two
  # versions.
  #
  # v1 inlined the vendor transcript at `raw.events`. That is the accumulator this slice removes,
  # and removing it while still stamping `schema_version: 1` would quietly change what an
  # already-produced document means to anyone reading one. So the new shape is v2 — the transcript
  # lives in captured streams that `raw.stream_refs` names, `logs` is an explicitly bounded tail,
  # and `capture` says what is actually known about the run's output.
  #
  # Three directions, all explicit:
  #
  # - `validate!` accepts either version and checks it against that version's schema.
  # - `upgrade` reads a v1 document as v2. It never discards what a v1 document carried: an old
  #   outcome that has `raw.events` keeps them, because a read-time projection that dropped them
  #   would be the same silent meaning change in the other direction.
  # - `project_v1` renders a v2 document for a reader still holding the v1 contract, and says so
  #   in the document (`raw.projected_from_schema_version`) rather than pretending to be original.
  module Outcome
    ContractError = Backstage::ContractError
    V1 = "outcome-v1.json"
    V2 = "outcome-v2.json"
    VERSIONS = [1, 2].freeze

    module_function

    def validator
      @validator ||= Backstage::Contracts::Validator.new
    end

    def schema_version(outcome)
      raise ContractError, "outcome must be an object" unless outcome.is_a?(Hash)

      version = outcome.fetch("schema_version", 1)
      raise ContractError, "unsupported outcome schema_version #{version.inspect}" unless VERSIONS.include?(version)

      version
    end

    # Validates against whichever version the document claims. Returns it unchanged.
    def validate!(outcome)
      validator.validate!(schema_version(outcome) == 1 ? V1 : V2, outcome)
    end

    # Reads any accepted outcome as a validated v2 document. A v2 document is validated and
    # returned; a v1 one is upgraded first. This is the one entry point a producer's caller needs.
    def normalize(outcome)
      validate!(upgrade(outcome))
    end

    # v1 -> v2. Additive: the two fields v2 introduces get their empty forms, and everything the
    # v1 document carried — `raw.events` included — comes through untouched.
    def upgrade(outcome)
      return outcome if schema_version(outcome) == 2

      raw = outcome["raw"]
      upgraded = outcome.merge("schema_version" => 2)
      upgraded["logs_truncated"] = false unless upgraded.key?("logs_truncated")
      upgraded["raw"] = raw.merge("stream_refs" => Array(raw["stream_refs"])) if raw.is_a?(Hash)
      upgraded
    end

    # v2 -> v1, for a reader still on the v1 contract. The transcript cannot be reconstituted
    # here — it is in the stream artifacts — so the projection carries `raw.stream_refs` and marks
    # itself projected instead of inventing an `events` array that would claim there were none.
    def project_v1(outcome)
      return validator.validate!(V1, outcome) if schema_version(outcome) == 1

      projected = outcome.merge("schema_version" => 1)
      raw = outcome["raw"]
      if raw.is_a?(Hash)
        projected["raw"] = raw.merge("projected_from_schema_version" => 2)
      elsif outcome.key?("raw")
        projected["raw"] = raw
      end
      validator.validate!(V1, projected)
    end

    # The failure outcome Backstage writes when it, rather than a runtime, is the one reporting.
    # It was three literal hashes in Engine, Recovery and the fake runner, which is how they drifted
    # apart; a synthetic failure that has to name its capture coverage is exactly the kind of thing
    # that must not be retyped.
    def failure(summary:, status: "failed", interrupted: false, capture: nil, exit_code: nil,
                signal: nil, **extra)
      outcome = {
        "schema_version" => 2,
        "status" => status,
        "summary" => summary,
        "process" => { "exit_code" => exit_code, "signal" => signal }
      }
      outcome["interrupted"] = true if interrupted
      outcome["capture"] = capture if capture
      validate!(outcome.merge(stringify(extra)))
    end

    # The run-level `capture` block: one honest answer for the whole run, plus the per-stream
    # detail behind it. `status` is the weakest coverage any stream reached — a run with one failed
    # stream is not a complete capture, whatever the others managed.
    def capture_summary(summaries, limit_bytes: nil, updated_at: nil, status: nil, error: nil)
      streams = Array(summaries).map { |summary| stream_ref(summary) }
      block = {
        "status" => status || weakest_coverage(streams.map { |stream| stream["coverage"] }),
        "bytes" => streams.sum { |stream| stream["bytes"].to_i },
        "streams" => streams
      }
      block["limit_bytes"] = limit_bytes if limit_bytes
      block["updated_at"] = updated_at if updated_at
      block["error"] = error if error
      block
    end

    # The bounded per-stream row that rides on both the run record and the outcome. Everything here
    # is a count, an id or a digest-sized string; no captured text reaches it.
    def stream_ref(summary)
      {
        "stream_id" => summary.fetch("stream_id"),
        "step" => summary["step"],
        "phase" => summary["phase"],
        "bytes" => summary["bytes"].to_i,
        "records" => summary["records"].to_i,
        "chunks" => summary["chunks"].to_i,
        "malformed" => summary["malformed"].to_i,
        "coverage" => summary.fetch("coverage"),
        "last_offset" => summary["last_offset"].to_i,
        "artifact_ids" => Array(summary["artifact_ids"]),
        "manifest_artifact_id" => summary["manifest_artifact_id"]
      }.compact
    end

    # Ordered weakest-first, so the run reports the least it can honestly claim.
    #
    # `open` sits between `gap` and `truncated`: a stream still being written is not accounted for
    # yet, but nobody has concluded anything is missing either. Only a reader who knows the run is
    # over — recovery — may turn an `open` stream into a `gap`.
    COVERAGE_ORDER = %w[failed gap open truncated complete].freeze

    def weakest_coverage(coverages)
      present = Array(coverages).compact
      return "complete" if present.empty?

      COVERAGE_ORDER.find { |coverage| present.include?(coverage) } || "complete"
    end

    def stringify(hash)
      hash.each_with_object({}) { |(key, value), result| result[key.to_s] = value }
    end
  end
end
