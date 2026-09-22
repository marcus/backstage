# frozen_string_literal: true

require "delegate"
require_relative "test_helper"

# What happens when the durable half of capture stops working mid-run.
#
# The claim is that this is loud everywhere it matters: the command surface says the capture
# failed, the child process is stopped rather than left working unobserved, nothing further is
# authorized on the run, the state log never names bytes that are not fully on the disk, and a
# later reconciliation still finds the failure written down.
#
# The finalize/publish half of that claim already has a test —
# `container_phase_runner_test.rb#test_publication_is_refused_when_a_stream_could_not_be_audited`
# proves a phase runner returns a failed outcome naming the coverage instead of publishing a draft.
# What is added here is the *persistence* failure that produces that coverage in the first place:
# a real `LocalFiles::ArtifactStore` whose chunk append starts raising partway through a run.
class CapturePersistenceFailureTest < Minitest::Test
  class StubPresence < Backstage::Ports::RuntimePresence
    def initialize(answer) = @answer = answer
    def status(_runtime) = @answer
  end

  # The artifact volume disappears after `after` chunk appends. Everything else about the store
  # keeps working, which is the realistic shape: the outcome and bundle artifacts still land, so
  # the failure has somewhere to be recorded.
  class FailingChunkStore < SimpleDelegator
    def initialize(store, after: 0)
      super(store)
      @after = after
      @appends = 0
    end

    def open_stream(**options)
      Handle.new(__getobj__.open_stream(**options), self)
    end

    def failing?
      @appends += 1
      @appends > @after
    end

    class Handle < SimpleDelegator
      def initialize(handle, owner)
        super(handle)
        @owner = owner
      end

      def append(bytes, index:)
        raise Errno::EIO, "artifact volume is gone" if @owner.failing?

        super
      end
    end
  end

  def stub_docker(directory, body)
    path = File.join(directory, "docker-stub")
    File.write(path, <<~SCRIPT)
      #!#{RbConfig.ruby}
      exit 0 unless ARGV.first == "run"
      STDIN.read
      STDOUT.binmode
      STDOUT.sync = true
      #{body}
    SCRIPT
    FileUtils.chmod(0o755, path)
    path
  end

  def job_bundle
    JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__))).tap do |value|
      value["execution"]["command"] = %w[noop]
      value["execution"]["credential_refs"] = []
    end
  end

  def engine_with_failing_chunks(directory, after: 0, **options)
    guard = Backstage::SecretGuard.new(secret_values: [])
    Backstage::Application::Engine.new(
      store: Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard),
      artifact_store: FailingChunkStore.new(
        Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard), after: after
      ),
      secret_guard: guard, **options
    )
  end

  # Every artifact any event names must be complete on the disk. This is the invariant that makes
  # a crash between "bytes written" and "event committed" safe: an unreferenced chunk is garbage,
  # a referenced incomplete chunk would be a lie.
  def assert_no_event_names_an_incomplete_chunk(store, directory)
    store.read_activity(limit: 1000).fetch("events").each do |event|
      Array(event["artifact_refs"]).each do |artifact_id|
        artifact = store.fetch("artifacts", artifact_id)
        refute_nil artifact, "#{event.fetch("event_id")} names #{artifact_id}, which is not a record"
        assert File.file?(artifact.fetch("path")), "#{artifact_id} names a file that is not there"
        assert_equal artifact.fetch("sha256"), Digest::SHA256.hexdigest(File.binread(artifact.fetch("path")))
      end
    end
    Dir.glob(File.join(directory, "artifacts", "**", "*.part")).each do |path|
      refute_includes File.read(File.join(directory, "state.jsonl")), File.basename(path),
                      "a torn part file is named by nothing"
    end
  end

  def test_a_failing_chunk_append_stops_the_child_and_fails_the_capture_not_the_audit
    in_tmpdir do |directory|
      # The child keeps printing and then sleeps: if the runtime did not stop it on the capture
      # failure, this test would take 30 seconds and the container would still be working with
      # nobody recording what it did.
      docker = stub_docker(directory, <<~BODY)
        400.times { |index| STDOUT.print("line \#{index} " + ("v" * 200) + "\\n") }
        sleep 30
      BODY
      engine = engine_with_failing_chunks(directory, after: 1, capture_options: { "flush_bytes" => 4096 })
      work = submit_work(engine)
      job = dispatch(engine, work)
      broker = example_broker( {})
      runtime = Backstage::DockerRuntime.new(docker: docker, read_bytes: 4096, queue_limit: 4)
      runner = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
      bundle = job_bundle.merge("harness" => { "adapter" => "pi", "provider" => "openrouter",
                                               "model" => "m", "prompt" => "sealed",
                                               "credential_ref" => "openrouter", "options" => {} })
      bundle["execution"]["credential_refs"] = ["openrouter"]

      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      result = engine.execute(job, runner: runner, bundle: bundle,
                              secrets: { "openrouter" => "model-secret" })
      took = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

      assert_operator took, :<, 25, "a failed append stops the child instead of waiting it out"
      assert_equal "failed", result.dig("capture", "status")
      assert_equal "failed", result.dig("outcome", "capture", "status")
      refute_equal "succeeded", result.dig("outcome", "status")
      assert_equal "failed", engine.store.fetch!("runs", result.dig("run", "id")).dig("capture", "status")

      # The failure is recorded in storage that still works — never appended into the storage that
      # failed — and nothing acknowledged points at bytes that are not there.
      assert_no_event_names_an_incomplete_chunk(engine.store, directory)
      # The checkpoint row is the last chunk that actually landed, and it was never closed: a
      # failed commit writes no row, which is exactly why the run record — not the row — is what
      # says the capture failed.
      row = engine.store.list(Backstage::Ports::RuntimeCapture::STREAM_COLLECTION).last
      assert_nil row["closed_at"]
      assert_operator row.fetch("last_offset"), :>, 0
    end
  end

  def test_a_reconciliation_after_the_restart_still_finds_the_failure
    in_tmpdir do |directory|
      docker = stub_docker(directory, %(200.times { |index| STDOUT.print("line \#{index} " + ("v" * 200) + "\\n") }))
      engine = engine_with_failing_chunks(directory, after: 1, capture_options: { "flush_bytes" => 4096 })
      work = submit_work(engine)
      job = dispatch(engine, work)
      broker = example_broker( {})
      runtime = Backstage::DockerRuntime.new(docker: docker, read_bytes: 4096, queue_limit: 4)
      runner = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
      bundle = job_bundle.merge("harness" => { "adapter" => "pi", "provider" => "openrouter",
                                               "model" => "m", "prompt" => "sealed",
                                               "credential_ref" => "openrouter", "options" => {} })
      bundle["execution"]["credential_refs"] = ["openrouter"]
      result = engine.execute(job, runner: runner, bundle: bundle,
                              secrets: { "openrouter" => "model-secret" })
      run_id = result.dig("run", "id")

      # A restart: a new process, a working artifact store, and only the state log to go on.
      restarted = build_engine(directory)
      workflows = build_workflows(restarted)
      recovery = Backstage::Application::Recovery.new(engine: restarted, workflows: workflows,
                                                      presence: StubPresence.new("gone"))
      findings = recovery.reconcile.fetch("work_items").flat_map { |row| row.fetch("findings") }

      run = restarted.store.fetch!("runs", run_id)
      assert_equal "failed", run.dig("capture", "status")
      assert_equal "failed", run.dig("outcome", "capture", "status")
      refute_equal "succeeded", run.fetch("status")
      finding = findings.find { |row| row.fetch("kind") == "outcome_recorded" }
      refute_nil finding, "the failure was recorded, so recovery reconciles it rather than inventing one"
      refute_equal "succeeded", finding.fetch("status")
      assert_equal "execution_failed", finding["transition"]
      assert_no_event_names_an_incomplete_chunk(restarted.store, directory)
    end
  end

  def test_the_command_surface_reports_the_failed_capture_without_anyone_opening_a_run
    in_tmpdir do |directory|
      # The composed system the CLI's `process` command builds, with one thing replaced: the
      # artifact store's chunk appends fail. Everything else — the scripted fake runner, the
      # controller, the workflow service — is exactly what `backstage process WORK_ID` runs.
      engine = engine_with_failing_chunks(directory, after: 0)
      system = Backstage::Bootstrap::System.new(
        engine: engine,
        credential_broker: example_broker( {}),
        secret_guard: Backstage::SecretGuard.new(secret_values: []),
        runtime_factory: ->(secret_guard:) { raise "no container in this test" },
        state_path: File.join(directory, "state.jsonl")
      )
      configuration = Backstage::Configuration.new(PACK)
      work = submit_work(engine, **configuration.binding_for("widgets"))

      result = system.controller(configuration: configuration,
                                 workspace_root: File.join(directory, "workspaces"))
                     .process(work_item_id: work.fetch("id"))

      capture = Array(result["runs"]).filter_map { |run| run["capture"] }.first ||
                engine.show_work(work.fetch("id")).fetch("capture").first
      assert_equal "failed", capture.fetch("status"),
                   "a capture that could not be persisted is failed on the surface an operator reads"

      shown = engine.show_work(work.fetch("id"))
      assert(shown.fetch("capture").any? { |block| block.fetch("status") == "failed" })
      assert(shown.fetch("runs").any? { |run| run.dig("outcome", "capture", "status") == "failed" })
      # Nothing was authorized on the unaudited run: no external action, no published change.
      assert_empty shown.fetch("external_actions")
      assert(shown.fetch("runs").none? { |run| run.dig("outcome", "review_change") })
      assert_no_event_names_an_incomplete_chunk(engine.store, directory)
    end
  end
end
