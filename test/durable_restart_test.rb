# frozen_string_literal: true

require_relative "test_helper"
require "open3"

# Crash behavior proven with real processes: a killed dispatcher, a state log with a torn tail, and
# the shipped CLI rather than an in-process stand-in. Nothing here calls a model or a container.
class DurableRestartTest < Minitest::Test
  BIN = File.expand_path("../bin/backstage", __dir__)

  def backstage(*argv, directory:)
    stdout, stderr, status = Open3.capture3(
      RbConfig.ruby, BIN, "--state", state_path(directory), "--artifacts", File.join(directory, "artifacts"),
      "--pack", PACK, "--json", *argv
    )
    [status.exitstatus, stdout, stderr]
  end

  def json(*argv, directory:)
    code, out, err = backstage(*argv, directory: directory)
    assert_equal 0, code, "#{argv.join(" ")} failed: #{err}"
    JSON.parse(out)
  end

  def state_path(directory) = File.join(directory, "state.jsonl")

  def submit(directory)
    json("submit", "--target", "widgets", "--title", "Durable restart", "--idempotency-key", "restart-1",
      "--workflow", "minimal", directory: directory).fetch("id")
  end

  def store(directory)
    Backstage::JsonlStore.new(state_path(directory))
  end

  def test_accepted_work_completes_once_when_the_accepting_process_exits_before_dispatch
    in_tmpdir do |directory|
      work_id = submit(directory)
      json("dispatch", "accept", work_id, directory: directory)

      # A separate process does the dispatching, exactly as a supervisor would.
      first = json("dispatch", "pass", directory: directory)
      second = json("dispatch", "pass", directory: directory)

      assert_equal 1, first.fetch("dispatched")
      assert_equal 0, second.fetch("dispatched")
      assert_equal "done", json("show", work_id, directory: directory).fetch("state")
      assert_equal 1, store(directory).list("runs").length, "restarting never runs the work twice"
    end
  end

  def test_a_killed_dispatcher_releases_the_store_and_its_run_is_reconciled_then_retried
    in_tmpdir do |directory|
      work_id = submit(directory)
      json("dispatch", "accept", work_id, "--max-retries", "1", "--retry-delay", "1", directory: directory)
      marker = File.join(directory, "held")
      child = spawn(RbConfig.ruby, "-e", holder_script(directory, work_id, marker))
      wait_for { File.exist?(marker) }

      # While it is alive: a second dispatcher refuses, and nothing relaunches the work.
      code, _out, err = backstage("dispatch", "pass", directory: directory)
      assert_equal 1, code
      assert_match(/another dispatcher owns this store/, JSON.parse(err).fetch("error").fetch("message"))
      findings = json("recover", work_id, directory: directory).fetch("work_items").flat_map { |row| row.fetch("findings") }
      assert_includes findings.map { |row| row.fetch("kind") }, "runtime_unknown"
      direct_code, _out, direct_err = backstage("process", work_id, directory: directory)
      assert_equal 1, direct_code, "a direct caller may not step past a live dispatcher"
      assert_match(/is accepted as intent-/, JSON.parse(direct_err).fetch("error").fetch("message"))
      assert_equal 1, store(directory).list("runs").length, "a live owner's run is never duplicated"

      Process.kill("KILL", child)
      Process.wait(child)

      reconciled = json("dispatch", "pass", directory: directory)
      run = store(directory).list("runs").first
      assert_equal "interrupted", run.fetch("status"), "a provably dead runtime is reconciled, not retried in place"
      assert_equal "delayed", reconciled.fetch("intents").first.fetch("status")
      assert_equal 1, store(directory).list("runs").length

      sleep(1.2)
      completed = json("dispatch", "pass", directory: directory)
      assert_equal 1, completed.fetch("dispatched")
      assert_equal "done", json("show", work_id, directory: directory).fetch("state")
      assert_equal 2, store(directory).list("runs").length, "one interrupted run and one bounded retry"
      assert_equal 1, json("dispatch", "show", work_id, directory: directory).fetch("retries_used")
    end
  end

  def test_two_real_dispatcher_processes_cannot_run_the_same_accepted_work_twice
    in_tmpdir do |directory|
      work_id = submit(directory)
      json("dispatch", "accept", work_id, directory: directory)
      gate = File.join(directory, "go")

      # Four separate processes claim and run the same acceptance at once, with no ownership lock
      # between them, so only the store guards decide.
      children = Array.new(4) { spawn(RbConfig.ruby, "-e", racer_script(directory, gate)) }
      wait_for { Dir[File.join(directory, "ready-*")].length == 4 }
      File.write(gate, "go")
      children.each { |child| Process.wait(child) }

      runs = store(directory).list("runs")
      assert_equal "done", json("show", work_id, directory: directory).fetch("state")
      assert_equal 1, runs.length, "exactly one process launched the work"
      assert_equal 1, runs.count { |run| run["status"] == "succeeded" }
      assert_equal 1, json("dispatch", "show", work_id, directory: directory).fetch("attempts_used")
    end
  end

  # A blocked acceptance is the one state a single test process almost never classifies first, so it
  # gets its own restart: a fresh CLI invocation whose first classified intent is already blocked.
  def test_a_blocked_acceptance_survives_a_process_restart
    in_tmpdir do |directory|
      work_id = submit(directory)
      json("dispatch", "accept", work_id, directory: directory)
      job = Backstage::Application::WorkflowService.new(store: store(directory)).request_transition(
        work_item_id: work_id, transition: "start",
        actor: { "role" => "system", "id" => "test", "entry" => "controller" }, request_id: "start-1"
      ).fetch("job")
      # A job nothing will run, from a state the workflow offers no automatic dispatch out of.
      store(directory).save("jobs", job.merge("status" => "cancelled"))

      blocked = json("dispatch", "pass", directory: directory).fetch("intents").first
      assert_equal "blocked", blocked.fetch("status")

      # A brand new process whose very first classification is that blocked acceptance.
      code, out, err = backstage("dispatch", "pass", directory: directory)

      assert_equal 0, code, "a restart with a blocked acceptance must not crash the pass: #{err}"
      report = JSON.parse(out)
      assert_equal "blocked", report.fetch("intents").first.fetch("status")
      assert_equal "skipped", report.fetch("intents").first.fetch("action")
      assert_equal 0, report.fetch("dispatched")
      assert_empty err
      assert_equal "in_progress", json("show", work_id, directory: directory).fetch("state")
    end
  end

  def test_persisted_success_survives_a_torn_state_log_tail
    in_tmpdir do |directory|
      work_id = submit(directory)
      json("dispatch", "accept", work_id, directory: directory)
      json("dispatch", "pass", directory: directory)
      complete = File.read(state_path(directory))

      # A process killed mid-append leaves an unterminated line behind.
      File.open(state_path(directory), "a") { |file| file.write('{"transaction_version":1,"events":[{"collection":"work_items"') }

      described = json("dispatch", "show", work_id, directory: directory)
      assert_equal "completed", described.fetch("status")
      assert_equal "done", json("show", work_id, directory: directory).fetch("state")

      json("submit", "--target", "widgets", "--title", "After the tear", "--idempotency-key", "restart-2", "--workflow", "minimal", directory: directory)

      after = File.read(state_path(directory))
      assert after.start_with?(complete), "complete transactions were preserved"
      assert(after.lines.all? { |line| line.end_with?("\n") && JSON.parse(line) }, "every line is a complete transaction; a kept tail would leave an unparsable one")
      assert_operator after.lines.length, :>, complete.lines.length, "later transactions were appended after the tear"
      assert_equal "completed", json("dispatch", "show", work_id, directory: directory).fetch("status")
      assert_equal 2, json("list", directory: directory).length
    end
  end

  private

  # A real dispatcher process: it takes ownership, claims the accepted attempt, and hangs inside its
  # agent run until the test kills it.
  def holder_script(directory, _work_id, marker)
    <<~RUBY
      $LOAD_PATH.unshift(#{File.expand_path("../lib", __dir__).inspect})
      require "backstage"
      system = Backstage::Bootstrap::System.build(state: #{state_path(directory).inspect}, artifacts: #{File.join(directory, "artifacts").inspect})
      abort("ownership refused") unless system.dispatch_ownership.acquire("role" => "dispatcher")
      runner = Object.new
      runner.define_singleton_method(:runtime_identity_before_launch?) { true }
      runner.define_singleton_method(:adapter_identifier) { "Test::HangingRunner" }
      runner.define_singleton_method(:run) do |bundle:, secrets: {}, cancellation: nil, capture: nil, &emit|
        File.write(#{marker.inspect}, Process.pid.to_s)
        sleep(120)
      end
      dispatcher = Backstage::Application::Dispatcher.new(
        engine: system.engine, workflows: system.workflows, recovery: system.recovery,
        clock: system.clock, authorized_mode: "fake",
        controller_factory: lambda do |_mode|
          Backstage::Application::Controller.new(
            engine: system.engine, workflows: system.workflows,
            configuration: system.configuration(#{PACK.inspect}),
            runner_factory: ->(_phase, _bundle, _work) { runner }, mode: "fake"
          )
        end
      )
      dispatcher.pass
    RUBY
  end

  # A dispatcher that waits on a gate file so several of them start their pass at the same instant.
  def racer_script(directory, gate)
    <<~RUBY
      $LOAD_PATH.unshift(#{File.expand_path("../lib", __dir__).inspect})
      require "backstage"
      system = Backstage::Bootstrap::System.build(state: #{state_path(directory).inspect}, artifacts: #{File.join(directory, "artifacts").inspect})
      dispatcher = system.dispatcher(configuration: system.configuration(#{PACK.inspect}))
      File.write(File.join(#{directory.inspect}, "ready-\#{Process.pid}"), "1")
      sleep(0.01) until File.exist?(#{gate.inspect})
      begin
        dispatcher.pass
      rescue Backstage::Error
        # Losing the race is a legitimate outcome for every process but one.
      end
    RUBY
  end

  def wait_for(timeout: 15)
    deadline = Time.now + timeout
    sleep(0.02) until yield || Time.now > deadline
    raise "condition was not met within #{timeout}s" unless yield
  end
end
