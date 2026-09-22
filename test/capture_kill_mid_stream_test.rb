# frozen_string_literal: true

require_relative "test_helper"

# What survives when the host dies mid-stream.
#
# The run happens in a real forked process against a real child process (a Ruby stub standing in
# for `docker`, streaming pi's JSON-lines protocol forever). The parent watches the durable
# activity log until the worker has acknowledged output, then SIGKILLs it — no ensure block, no
# flush, no chance to close anything. Everything asserted afterwards is read back from a fresh
# store instance in a process that never saw the writer.
#
# The claim under test is the one the plan makes: acknowledged activity survives, the chunks it
# names are on disk and hash-match, the unclosed stream is visible as an unclosed stream, and
# recovery turns that into an explicit `gap` rather than inventing an outcome.
class CaptureKillMidStreamTest < Minitest::Test
  class StubPresence < Backstage::Ports::RuntimePresence
    def initialize(answer) = @answer = answer
    def status(_runtime) = @answer
  end

  # A stub `docker` that streams pi protocol lines forever, so the capture path frames records,
  # the interpreter produces semantic observations, and chunks keep committing until it is killed.
  #
  # Two guards keep it from outliving the test: it stops once its parent is gone (SIGKILL to the
  # forked worker leaves this process reparented to init) and it stops on its own after 60 s.
  def stub_docker(directory)
    path = File.join(directory, "docker-stub")
    File.write(path, <<~SCRIPT)
      #!#{RbConfig.ruby}
      require "json"
      exit 0 unless ARGV.first == "run"
      STDIN.read
      STDOUT.binmode
      STDOUT.sync = true
      deadline = Time.now + 60
      index = 0
      loop do
        exit 0 if Process.ppid == 1 || Time.now > deadline
        STDOUT.print(JSON.generate({ "type" => "tool_execution_start", "toolCallId" => "tool-\#{index}",
                                     "toolName" => "bash", "args" => { "command" => "echo \#{index}" },
                                     "timestamp" => "2026-01-01T00:00:00Z" }) + "\\n")
        STDOUT.print(JSON.generate({ "type" => "tool_execution_end", "toolCallId" => "tool-\#{index}",
                                     "toolName" => "bash", "result" => { "content" => "ok " + ("z" * 512) },
                                     "isError" => false, "timestamp" => "2026-01-01T00:00:00Z" }) + "\\n")
        index += 1
        sleep 0.01
      end
    SCRIPT
    FileUtils.chmod(0o755, path)
    path
  end

  def harness_bundle
    bundle = JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
    bundle["harness"] = { "adapter" => "pi", "provider" => "openrouter", "model" => "m",
                          "prompt" => "sealed", "credential_ref" => "openrouter", "options" => {} }
    bundle["execution"]["credential_refs"] = ["openrouter"]
    bundle
  end

  def store_for(directory)
    Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
  end

  # Reads the durable log the way any other process would, and answers "has the worker
  # acknowledged output yet?" without touching anything the worker owns.
  def acknowledged(directory, type: nil)
    store_for(directory).read_activity(limit: 500).fetch("events").select do |event|
      type.nil? || event.fetch("type") == type
    end
  end

  def wait_until(seconds: 20)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + seconds
    loop do
      value = yield
      return value if value
      return nil if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

      sleep 0.05
    end
  end

  def test_a_worker_killed_mid_stream_leaves_every_acknowledged_event_backed_by_real_bytes
    in_tmpdir do |directory|
      # Set up in the parent so the child only has to execute a job that already exists.
      engine = build_engine(directory, capture_options: { "flush_bytes" => 4096 })
      work = submit_work(engine)
      job = dispatch(engine, work)
      docker = stub_docker(directory)

      pid = fork do
        # A fresh composition in the child: its own store handle, its own artifact store, its own
        # engine. Nothing is shared but the files.
        child = build_engine(directory, capture_options: { "flush_bytes" => 4096 })
        broker = example_broker( { "OPENROUTER_API_KEY" => "model-secret" })
        runtime = Backstage::DockerRuntime.new(docker: docker, read_bytes: 4096, queue_limit: 8)
        harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
        begin
          child.execute(job, runner: harness, bundle: harness_bundle)
        rescue StandardError
          nil
        end
        exit!(0)
      end

      begin
        # Wait for real durable evidence: several coalesced chunk events and at least one semantic
        # event, so the kill lands after acknowledgement and before any close.
        observed = wait_until { acknowledged(directory, type: "runtime.observed").length >= 3 && acknowledged(directory, type: "runtime.observed") }
        tools = wait_until { acknowledged(directory, type: "agent.tool_observed").length >= 2 && acknowledged(directory, type: "agent.tool_observed") }
        refute_nil observed, "the worker never acknowledged any output"
        refute_nil tools, "the worker never acknowledged a semantic observation"

        Process.kill("KILL", pid)
      ensure
        Process.wait(pid)
      end

      assert_equal "KILL", Signal.signame($?.termsig.to_i), "the worker was killed, not asked to stop"

      # ---- Everything below reads from a fresh store in the surviving process. ----
      after = store_for(directory)
      events = after.read_activity(limit: 1000).fetch("events")
      survived = events.select { |event| %w[runtime.observed agent.message_observed agent.tool_observed].include?(event.fetch("type")) }
      assert_operator survived.length, :>=, 5, "acknowledged activity survives the kill"

      survived.each do |event|
        refs = Array(event["artifact_refs"])
        refute_empty refs, "#{event.fetch("type")} #{event.fetch("event_id")} names no chunk"
        refs.each do |artifact_id|
          artifact = after.fetch!("artifacts", artifact_id)
          path = artifact.fetch("path")
          assert File.file?(path), "#{artifact_id} names #{path}, which is not on the disk"
          assert_equal artifact.fetch("sha256"), Digest::SHA256.hexdigest(File.binread(path)),
                       "#{artifact_id} names bytes whose digest does not match what was acknowledged"
          assert_equal artifact.fetch("bytes"), File.size(path)
        end
      end

      # No `.part` file is ever referenced: a torn append is invisible to history by construction.
      assert_empty Dir.glob(File.join(directory, "artifacts", "**", "*.part")).select { |path| File.size(path).positive? && !File.exist?(path.sub(/\.part\z/, "")) },
                   "a torn part file must be referenced by nothing"

      rows = after.list(Backstage::Ports::RuntimeCapture::STREAM_COLLECTION)
      assert_equal 1, rows.length
      row = rows.fetch(0)
      assert_nil row["closed_at"], "a killed worker cannot close its stream"
      assert_operator row.fetch("last_offset"), :>, 0
      assert_operator row.fetch("chunk_index"), :>, 0
    end
  end

  def test_recovery_calls_the_unobserved_interval_a_gap_and_fabricates_no_success
    in_tmpdir do |directory|
      run_id, work_id = kill_a_worker(directory)

      # A new process, a new store, a new engine: recovery sees only what was written down.
      engine = build_engine(directory)
      workflows = build_workflows(engine)
      recovery = Backstage::Application::Recovery.new(engine: engine, workflows: workflows,
                                                      presence: StubPresence.new("gone"))

      findings = recovery.reconcile.fetch("work_items").flat_map { |row| row.fetch("findings") }
      finding = findings.find { |row| row.fetch("kind") == "worker_interrupted" }

      refute_nil finding, "a dead worker with no outcome is an interruption, not a silence"
      assert_equal "gap", finding.dig("capture", "status")
      unobserved = finding.dig("capture", "unobserved")
      assert_equal 1, unobserved.length
      assert_operator unobserved.fetch(0).fetch("observed_through_offset"), :>, 0
      assert_match(/never acknowledged/, unobserved.fetch(0).fetch("detail"))

      run = engine.store.fetch!("runs", run_id)
      outcome = run.fetch("outcome")
      refute_equal "succeeded", outcome.fetch("status")
      assert_equal "failed", outcome.fetch("status")
      assert_equal true, outcome.fetch("interrupted")
      assert_equal "gap", outcome.dig("capture", "status")
      assert_equal "gap", outcome.dig("capture", "streams", 0, "coverage")
      refute_empty outcome.dig("capture", "streams", 0, "artifact_ids")
      refute_empty outcome.dig("raw", "stream_refs")
      assert_equal unobserved.fetch(0).fetch("stream_id"), outcome.dig("raw", "stream_refs", 0, "stream_id")
      Backstage::Domain::Outcome.validate!(outcome)

      # And nothing anywhere in the record claims the run worked.
      assert_empty engine.store.list("runs").select { |row| row["status"] == "succeeded" }
      assert_empty engine.store.list("attempts").select { |row| row.dig("outcome", "status") == "succeeded" }
      assert_equal work_id, run.fetch("work_item_id")
    end
  end

  def test_the_cli_shows_what_survived_and_the_capture_it_could_not_finish
    in_tmpdir do |directory|
      run_id, work_id = kill_a_worker(directory)
      base = ["--state", File.join(directory, "state.jsonl"), "--artifacts", File.join(directory, "artifacts"), "--pack", PACK]

      page = cli_json(base + ["--json", "activity", "list", "--work", work_id, "--limit", "200"])
      types = page.fetch("events").map { |event| event.fetch("type") }
      assert_includes types, "runtime.observed"
      assert_includes types, "agent.tool_observed"
      assert page.fetch("events").all? { |event| event["work_item_id"] == work_id }

      observed = page.fetch("events").find { |event| event.fetch("type") == "runtime.observed" }
      shown = cli_json(base + ["--json", "activity", "show", observed.fetch("event_id")])
      assert_equal observed.fetch("event_id"), shown.fetch("event_id")
      assert_equal observed.fetch("artifact_refs"), shown.fetch("artifact_refs")
      assert_equal run_id, shown.fetch("run_id")

      # Recovery has not run yet, so `show` reports the coverage the live run last managed to
      # write: `open`, the honest answer for a stream nobody closed. The byte counts on that block
      # are deliberately not restated per chunk (Engine#record_capture rewrites the run only when
      # the *answer* changes), so the durable numbers live on the checkpoint row until a reader
      # who knows the run is over settles them.
      work = cli_json(base + ["--json", "show", work_id])
      assert_equal 1, work.fetch("capture").length
      assert_equal run_id, work.dig("capture", 0, "run_id")
      assert_equal "open", work.dig("capture", 0, "status")
      assert_equal "implementation", work.dig("capture", 0, "phase")
      row = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
                                 .list(Backstage::Ports::RuntimeCapture::STREAM_COLLECTION).fetch(0)
      assert_operator row.fetch("bytes"), :>, 0, "the checkpoint holds what was actually acknowledged"

      # After recovery the same surface says `gap`, with the acknowledged bytes settled onto it.
      engine = build_engine(directory)
      Backstage::Application::Recovery.new(engine: engine, workflows: build_workflows(engine),
                                           presence: StubPresence.new("gone")).reconcile
      recovered = cli_json(base + ["--json", "show", work_id])
      assert_equal "gap", recovered.dig("runs", 0, "outcome", "capture", "status")
      assert_equal row.fetch("bytes"), recovered.dig("runs", 0, "outcome", "capture", "bytes")
      refute_empty recovered.dig("runs", 0, "outcome", "capture", "streams", 0, "artifact_ids")
    end
  end

  private

  def cli_json(argv)
    out = StringIO.new
    err = StringIO.new
    code = Backstage::CLI.new(argv, out: out, err: err, env: {}).call
    assert_equal 0, code, err.string
    JSON.parse(out.string)
  end

  # Runs a worker to the point where it has acknowledged output, then kills it. Returns the run id
  # and work id the surviving process has to reason about.
  def kill_a_worker(directory)
    engine = build_engine(directory, capture_options: { "flush_bytes" => 4096 })
    work = submit_work(engine)
    job = dispatch(engine, work)
    docker = stub_docker(directory)

    pid = fork do
      child = build_engine(directory, capture_options: { "flush_bytes" => 4096 })
      broker = example_broker( { "OPENROUTER_API_KEY" => "model-secret" })
      runtime = Backstage::DockerRuntime.new(docker: docker, read_bytes: 4096, queue_limit: 8)
      harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: broker)
      begin
        child.execute(job, runner: harness, bundle: harness_bundle)
      rescue StandardError
        nil
      end
      exit!(0)
    end

    begin
      ready = wait_until do
        acknowledged(directory, type: "runtime.observed").length >= 3 &&
          acknowledged(directory, type: "agent.tool_observed").length >= 2
      end
      refute_nil ready, "the worker never acknowledged output"
      Process.kill("KILL", pid)
    ensure
      Process.wait(pid)
    end

    run = store_for(directory).list("runs").last
    [run.fetch("id"), work.fetch("id")]
  end
end
