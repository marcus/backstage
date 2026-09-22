# frozen_string_literal: true

require_relative "test_helper"

# What the rest of the system learns from capture: the run record's coverage, the outcome's
# `capture` block, what recovery can say about a run that died mid-stream, and what the CLI shows.
class CaptureWiringTest < Minitest::Test
  class StubPresence < Backstage::Ports::RuntimePresence
    def initialize(answer) = @answer = answer
    def status(_runtime) = @answer
  end

  # Two records a quarter of a second apart, against a clock that only moves when the script says
  # so, so the time-based flush boundary falls exactly where the test says it does.
  SCRIPT = [[0.0, "worker starting\n"], [0.3, "worker finished\n"]].freeze

  def build(directory, script: SCRIPT, clock: nil)
    clock ||= Backstage::Adapters::Fake::Clock.new
    engine = build_engine(directory, clock: clock)
    work = submit_work(engine)
    runner = Backstage::Adapters::Fake::Runner.new(script: script, clock: clock)
    [engine, work, runner]
  end

  def test_a_run_record_carries_the_coverage_of_what_its_worker_printed
    in_tmpdir do |directory|
      engine, work, runner = build(directory)

      result = engine.execute(dispatch(engine, work), runner: runner)

      run = engine.store.fetch!("runs", result.fetch("run").fetch("id"))
      assert_equal "complete", run.dig("capture", "status")
      assert_equal 32, run.dig("capture", "bytes")
      assert_equal 1, run.dig("capture", "streams").length
      assert_equal "fake", run.dig("capture", "streams", 0, "step")
      assert_equal 2, run.dig("capture", "streams", 0, "records")
      # 64 KiB never filled, but 250 ms elapsed between the records, so the first flushed on time.
      assert_equal 2, run.dig("capture", "streams", 0, "chunks")
      assert_equal run["capture"], result.fetch("capture")
      assert_equal "complete", result.dig("outcome", "capture", "status")
      assert_equal 2, result.dig("outcome", "schema_version")
    end
  end

  def test_the_chunks_a_run_acknowledged_are_on_disk_and_named_by_its_events
    in_tmpdir do |directory|
      engine, work, runner = build(directory)

      result = engine.execute(dispatch(engine, work), runner: runner)
      run_id = result.fetch("run").fetch("id")

      observed = engine.store.read_activity(limit: 200).fetch("events")
                       .select { |event| event.fetch("type") == "runtime.observed" && event["run_id"] == run_id }
      refute_empty observed
      observed.each do |event|
        artifact_id = event.fetch("artifact_refs").first
        artifact = engine.store.fetch!("artifacts", artifact_id)
        assert_equal Digest::SHA256.hexdigest(File.binread(artifact.fetch("path"))), artifact.fetch("sha256"),
                     "an acknowledged event must name bytes that are actually on the disk"
        assert_equal event.dig("data", "sha256"), artifact.fetch("sha256")
      end
      manifest = engine.store.list("artifacts").find { |artifact| artifact["kind"].to_s.end_with?("_manifest") }
      refute_nil manifest, "a closed stream leaves an index that history refers to"
    end
  end

  def test_a_stream_that_was_never_closed_is_reported_as_a_gap_with_its_artifacts
    in_tmpdir do |directory|
      engine = build_engine(directory)
      workflows = build_workflows(engine)
      work = submit_work(engine)
      recovery = Backstage::Application::Recovery.new(engine: engine, workflows: workflows,
                                                      presence: StubPresence.new("gone"))
      job = workflows.request_transition(work_item_id: work.fetch("id"), transition: "start",
                                         actor: operator("system"), request_id: "start-1").fetch("job")
      job = engine.store.save("jobs", job.merge("status" => "running"))
      run = engine.store.save("runs", Backstage::Domain::Records.run(
        job_id: job.fetch("id"), phase: "implementation", work_item_id: work.fetch("id"),
        work_revision: job.fetch("work_revision")
      ).merge("status" => "running", "owner_pid" => 99_999_999, "runtime" => { "container_name" => "gone" }))

      # A worker that produced acknowledged output and then died: chunks committed, stream never
      # closed. This is the shape a kill mid-stream leaves behind.
      capture = Backstage::Application::RuntimeCapture.new(
        sink: Backstage::Application::CaptureSink::Durable.new(
          store: engine.store, artifact_store: engine.artifact_store,
          work_item_id: work.fetch("id"), run: run, attempt: 1
        ),
        clock: Backstage::Adapters::Fake::Clock.new, run: run, attempt: 1, flush_bytes: 8
      )
      writer = capture.open(step: "harness")
      writer.write("acknowledged output\n")

      # While the run is live the writer itself will only say `open`: what is durable is known,
      # what may still come is not. Turning that into a `gap` is recovery's call, not the writer's.
      assert_equal "open", capture.summaries.fetch(0).fetch("coverage")
      assert_equal "open", Backstage::Domain::Outcome.capture_summary(capture.summaries).fetch("status")

      report = recovery.reconcile.fetch("work_items").flat_map { |row| row.fetch("findings") }
      finding = report.find { |row| row.fetch("kind") == "worker_interrupted" }

      refute_nil finding
      assert_equal "gap", finding.dig("capture", "status")
      unobserved = finding.dig("capture", "unobserved")
      assert_equal 1, unobserved.length
      assert_equal writer.id, unobserved.fetch(0).fetch("stream_id")
      assert_operator unobserved.fetch(0).fetch("observed_through_offset"), :>, 0

      outcome = engine.store.fetch!("runs", run.fetch("id")).fetch("outcome")
      assert_equal "failed", outcome.fetch("status")
      assert_equal true, outcome.fetch("interrupted")
      assert_equal "gap", outcome.dig("capture", "status")
      assert_equal "gap", outcome.dig("capture", "streams", 0, "coverage")
      refute_empty outcome.dig("capture", "streams", 0, "artifact_ids"),
                   "a synthetic outcome must point at the bytes that were acknowledged"
      assert_equal writer.id, outcome.dig("raw", "stream_refs", 0, "stream_id")
      Backstage::Domain::Outcome.validate!(outcome)
    end
  end

  def test_a_closed_stream_leaves_recovery_nothing_to_call_a_gap
    in_tmpdir do |directory|
      engine, work, runner = build(directory)
      workflows = build_workflows(engine)
      engine.execute(dispatch(engine, work), runner: runner)
      rows = engine.store.list(Backstage::Ports::RuntimeCapture::STREAM_COLLECTION)

      refute_empty rows
      assert rows.all? { |row| row["closed_at"] }, "a run that finished closes every stream it opened"
      recovery = Backstage::Application::Recovery.new(engine: engine, workflows: workflows,
                                                      presence: StubPresence.new("gone"))
      findings = recovery.reconcile.fetch("work_items").flat_map { |row| row.fetch("findings") }
      assert_empty findings.select { |row| row.fetch("kind") == "worker_interrupted" }
    end
  end

  # A runtime that prints a fixture into its capture stream and nothing else.
  class ScriptedRuntime
    def initialize(output) = @output = output
    def runtime_identity_before_launch? = true

    def run(bundle:, secrets: {}, cancellation: nil, capture: nil)
      capture&.write(@output)
      capture&.close(reason: "close")
      { "schema_version" => 2, "status" => "succeeded", "summary" => "ok", "logs" => "",
        "process" => { "exit_code" => 0, "signal" => nil } }
    end
  end

  def test_what_the_agent_said_and_did_becomes_committed_activity
    in_tmpdir do |directory|
      engine = build_engine(directory)
      work = submit_work(engine)
      fixture = File.binread(File.expand_path("fixtures/pi_success.jsonl", __dir__))
      broker = example_broker( { "OPENROUTER_API_KEY" => "model-secret" })
      harness = Backstage::Adapters::Pi::Harness.new(runtime: ScriptedRuntime.new(fixture), credential_broker: broker)
      bundle = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
      bundle["harness"] = { "adapter" => "pi", "provider" => "openrouter", "model" => "m",
                            "prompt" => "sealed", "credential_ref" => "openrouter", "options" => {} }
      bundle["execution"]["credential_refs"] = ["openrouter"]

      result = engine.execute(dispatch(engine, work), runner: harness, bundle: bundle)

      events = engine.store.read_activity(limit: 200).fetch("events")
      tools = events.select { |event| event.fetch("type") == "agent.tool_observed" }
      messages = events.select { |event| event.fetch("type") == "agent.message_observed" }
      assert_equal 2, tools.length
      assert_equal 1, messages.length
      assert_equal %w[start end], tools.map { |event| event.dig("data", "phase") }
      assert_equal "bash", tools.first.dig("data", "tool_name")
      assert_equal "agent_reported", tools.first.dig("source", "provenance")
      assert_equal "pi-session-1", messages.first["provider_session_id"],
                   "the session line names the vendor session on every event that follows it"
      assert_equal "Implemented and tested.", messages.first.fetch("summary")
      assert_equal "Implemented and tested.", result.dig("outcome", "assistant_text")
      # Every semantic event points at the chunk whose bytes it was read from.
      assert (tools + messages).all? { |event| Array(event["artifact_refs"]).length == 1 }
    end
  end

  def test_runtime_streams_is_a_collection_this_deployment_knows_it_holds
    assert_includes Backstage::Application::Engine::COLLECTIONS, "runtime_streams"
  end

  def test_show_and_process_surface_capture_without_anyone_opening_a_run
    in_tmpdir do |directory|
      engine, work, runner = build(directory)
      engine.execute(dispatch(engine, work), runner: runner)

      shown = engine.show_work(work.fetch("id"))

      assert_equal 1, shown.fetch("capture").length
      assert_equal "complete", shown.dig("capture", 0, "status")
      assert_equal "implementation", shown.dig("capture", 0, "phase")
      assert_equal shown.dig("runs", 0, "id"), shown.dig("capture", 0, "run_id")
    end
  end

  def test_the_pack_can_tighten_the_bounds_capture_runs_under
    in_tmpdir do |directory|
      clock = Backstage::Adapters::Fake::Clock.new
      engine = build_engine(directory, clock: clock, capture_options: { "flush_bytes" => 8 })
      work = submit_work(engine)
      runner = Backstage::Adapters::Fake::Runner.new(script: [[0.0, "abcdefghijklmnop\n"]], clock: clock)

      result = engine.execute(dispatch(engine, work), runner: runner)

      assert_operator result.dig("capture", "streams", 0, "chunks"), :>=, 2,
                      "a smaller flush size means more, smaller chunks"
    end
  end

  def test_a_pack_capture_block_is_validated_like_every_other_policy
    in_tmpdir do |directory|
      write_minimal_workflow(directory)
      FileUtils.cp_r(File.join(BackstageTestHelpers::PACK, "targets"), directory)
      FileUtils.cp_r(File.join(BackstageTestHelpers::PACK, "instructions"), directory)
      base = YAML.safe_load_file(File.join(BackstageTestHelpers::PACK, "backstage.yml"))
      base["workflows"] = { "default" => "minimal" }

      File.write(File.join(directory, "backstage.yml"), YAML.dump(base.merge("capture" => { "flush_bytes" => 4096 })))
      assert_equal({ "flush_bytes" => 4096 }, Backstage::Configuration::DeploymentPack.new(directory).capture_policy)

      File.write(File.join(directory, "backstage.yml"), YAML.dump(base.merge("capture" => { "flush_bytes" => 0 })))
      error = assert_raises(Backstage::ContractError) { Backstage::Configuration::DeploymentPack.new(directory).check }
      assert_includes error.message, "capture flush_bytes must be a positive integer"

      File.write(File.join(directory, "backstage.yml"), YAML.dump(base.merge("capture" => { "flush_kb" => 64 })))
      error = assert_raises(Backstage::ContractError) { Backstage::Configuration::DeploymentPack.new(directory).check }
      assert_includes error.message, "unknown capture keys: flush_kb"
    end
  end

  # The bounds were validated and then reported nowhere, so no surface could tell an operator — or
  # an agent — how large a record or a run's output may get here before something is cut. `check`
  # reports the effective policy: every bound with the value a run actually gets, and which of them
  # the pack set, because an unset bound and an unbounded one are not the same answer.
  def test_config_check_reports_the_bounds_capture_runs_under
    in_tmpdir do |directory|
      write_minimal_workflow(directory)
      FileUtils.cp_r(File.join(BackstageTestHelpers::PACK, "targets"), directory)
      FileUtils.cp_r(File.join(BackstageTestHelpers::PACK, "instructions"), directory)
      base = YAML.safe_load_file(File.join(BackstageTestHelpers::PACK, "backstage.yml"))
      base["workflows"] = { "default" => "minimal" }
      File.write(File.join(directory, "backstage.yml"), YAML.dump(base.merge("capture" => { "max_record_bytes" => 4096 })))

      capture = Backstage::Configuration::DeploymentPack.new(directory).check.fetch("capture")

      assert_equal ["max_record_bytes"], capture.fetch("configured")
      assert_equal 4096, capture.fetch("max_record_bytes")
      assert_equal 8 * 1024 * 1024, capture.fetch("max_protocol_record_bytes"), "the default is reported too"
      assert_equal 65_536, capture.fetch("flush_bytes")
      assert_equal 64 * 1024 * 1024, capture.fetch("max_run_bytes")
    end
  end

  # A run has as many streams as its runner opened, so a run-level `stream_id` names it by whichever
  # stream happened to close last. Those two fields are per-stream and stay in `streams`.
  def test_the_run_level_capture_block_does_not_claim_one_streams_position
    in_tmpdir do |directory|
      engine, work, runner = build(directory)

      result = engine.execute(dispatch(engine, work), runner: runner)
      block = result.fetch("capture")

      refute block.key?("stream_id"), "a run is not named by one of its streams"
      refute block.key?("last_offset")
      assert_equal 1, block.fetch("streams").length
      refute_nil block.dig("streams", 0, "stream_id")
      refute_nil block.dig("streams", 0, "last_offset")
      assert_equal block.fetch("streams").sum { |stream| stream.fetch("bytes") }, block.fetch("bytes")
    end
  end
end
