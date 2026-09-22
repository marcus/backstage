# frozen_string_literal: true

require_relative "test_helper"

class RuntimeProgressTest < Minitest::Test
  def bundle
    JSON.parse(File.read(File.expand_path("fixtures/job_bundle.json", __dir__)))
  end

  def test_quiet_runtime_drains_channel_and_streams_pi_before_exit_on_controller_thread
    in_tmpdir do |directory|
      executable = File.join(directory, "docker-stub")
      heartbeat_seen = File.join(directory, "heartbeat-seen")
      message_seen = File.join(directory, "message-seen")
      File.write(executable, <<~SCRIPT)
        #!#{RbConfig.ruby}
        require "json"
        STDIN.read
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 5
        until File.exist?(#{heartbeat_seen.inspect})
          exit 2 if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          sleep 0.01
        end
        STDOUT.sync = true
        puts JSON.generate(type: "message_end", message: {role: "assistant", content: [{type: "text", text: "done"}], stopReason: "stop"})
        until File.exist?(#{message_seen.inspect})
          exit 3 if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
          sleep 0.01
        end
        puts JSON.generate(type: "agent_settled")
      SCRIPT
      FileUtils.chmod(0o755, executable)
      value = bundle
      value["harness"] = {"adapter" => "pi", "provider" => "openrouter", "model" => "test", "prompt" => "test", "credential_ref" => "openrouter", "options" => {}}
      harness = Backstage::PiHarness.new(runtime: Backstage::DockerRuntime.new(docker: executable), credential_broker: nil)
      controller = Thread.current
      events = []
      outcome = harness.run(bundle: value, secrets: {"OPENROUTER_API_KEY" => "test-secret"}) do |event|
        assert_same controller, Thread.current
        events << event
        File.write(heartbeat_seen, "seen") if event["type"] == "runtime_heartbeat"
        File.write(message_seen, "seen") if event["type"] == "message"
      end
      assert_equal "succeeded", outcome["status"]
      assert_equal "runtime_started", events.first["type"]
      assert_equal 1, events.count { |event| event["type"] == "message" }
      # The transcript is in the stream, not in the outcome; what the outcome keeps is where to
      # find it and how much of it was captured.
      assert_equal 2, outcome.dig("raw", "stream_refs", 0, "records")
      assert_equal "complete", outcome.dig("capture", "status")
    end
  end

  def test_container_runner_forwards_live_identity_from_every_phase_and_retains_secret_boundaries
    in_tmpdir do |directory|
      calls = []
      events = []
      runtime = Object.new
      runtime.define_singleton_method(:run) do |bundle:, secrets:, cancellation:, capture: nil, &emit|
        step = bundle.dig("execution", "command", 1)
        calls << [step, secrets.keys]
        emit.call("type" => "runtime_started", "container_name" => "container-#{step}")
        raise "event was delayed" unless events.last["step"] == step
        artifact = case step
                   when "context"
                     {"type" => "context_materialized", "name" => "docs", "mount" => "docs"}
                   when "finalize"
                     workspace = bundle.dig("execution", "mounts", 0, "source")
                     File.write(File.join(workspace, "change.patch"), "patch")
                     {"type" => "repository_materialized", "branch" => bundle.dig("repository", "branch"), "base_revision" => "abc", "patch_size" => 5, "patch_sha256" => Digest::SHA256.hexdigest("patch")}
                   when "publish"
                     {"type" => "repository_published", "number" => 1}
                   else
                     {}
                   end
        capture&.write(JSON.generate(artifact) + "\n")
        capture&.close(reason: "close")
        {"status" => "succeeded", "logs" => JSON.generate(artifact), "process" => {"exit_code" => 0}}
      end
      harness = Object.new
      harness.define_singleton_method(:run) do |bundle:, secrets:, cancellation:, capture: nil, &emit|
        calls << ["harness", secrets.keys]
        emit.call("type" => "runtime_started", "container_name" => "container-harness")
        raise "harness event delayed" unless events.last["step"] == "harness"
        {"status" => "succeeded"}
      end
      publisher = Object.new
      publisher.define_singleton_method(:begin_publication) { |**| nil }
      publisher.define_singleton_method(:complete_publication) { |**| {} }
      value = bundle
      value["context_grants"] = [{}]
      runner = Backstage::ContainerPhaseRunner.new(phase: "implementation", runtime: runtime, harness: harness, credential_broker: nil, workspace_root: directory, review_change: publisher, work_item_id: "work")
      result = runner.run(bundle: value, secrets: {"GH_TOKEN" => "github", "OPENROUTER_API_KEY" => "model"}) { |event| events << event }
      assert_equal "succeeded", result["status"]
      assert_equal %w[prepare context harness finalize publish], events.map { |event| event["step"] }
      assert events.all? { |event| event["phase"] == "implementation" }
      assert_equal [["prepare", ["GH_TOKEN"]], ["context", ["GH_TOKEN"]], ["harness", ["OPENROUTER_API_KEY"]], ["finalize", []], ["publish", ["GH_TOKEN"]]], calls
    end
  end
end
