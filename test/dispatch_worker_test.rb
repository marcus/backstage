# frozen_string_literal: true

require_relative "test_helper"

# The foreground worker and the local ownership it takes. Ownership must survive a crash without
# leaving the queue owned, so one case really does kill a child process.
class DispatchWorkerTest < Minitest::Test
  Worker = Backstage::Application::Worker
  Ownership = Backstage::Adapters::LocalFiles::DispatchOwnership

  class RecordingDispatcher
    attr_reader :passes

    def initialize(reports = [])
      @reports = reports
      @passes = 0
    end

    attr_reader :stops

    def pass(limit: nil, work_item_id: nil, stop: nil)
      @passes += 1
      (@stops ||= []) << stop
      @reports.shift || { "dispatched" => 0, "next_wake_up" => nil, "intents" => [] }
    end

    def next_wake_up = nil
  end

  class TestClock < Backstage::Ports::Clock
    attr_reader :waits

    def initialize
      @now = Time.utc(2026, 1, 1)
      @waits = []
      @on_wait = nil
    end

    def now = @now
    def on_wait(&block) = @on_wait = block

    def wait(seconds, interrupt: nil)
      @waits << seconds
      @now += seconds
      @on_wait&.call
      true
    end
  end

  def test_a_second_owner_exits_clearly_instead_of_running_beside_the_first
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl.dispatcher.lock")
      first = Ownership.new(path)
      lease = first.acquire("role" => "dispatcher")
      refute_nil lease

      worker = Worker.new(dispatcher: RecordingDispatcher.new, ownership: Ownership.new(path), clock: TestClock.new)
      error = assert_raises(Backstage::ConflictError) { worker.once }

      assert_match(/another dispatcher owns this store/, error.message)
      assert_match(/pid #{Process.pid}/, error.message)
      first.release
    end
  end

  def test_ownership_is_released_by_a_crash_not_only_by_an_orderly_exit
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl.dispatcher.lock")
      marker = File.join(directory, "held")
      script = <<~RUBY
        $LOAD_PATH.unshift(#{File.expand_path("../lib", __dir__).inspect})
        require "backstage"
        ownership = Backstage::Adapters::LocalFiles::DispatchOwnership.new(#{path.inspect})
        abort("could not acquire") unless ownership.acquire("role" => "dispatcher")
        File.write(#{marker.inspect}, Process.pid.to_s)
        sleep(60)
      RUBY
      child = spawn(RbConfig.ruby, "-e", script)
      wait_for { File.exist?(marker) }
      assert_nil Ownership.new(path).acquire, "the live child still owns the store"

      Process.kill("KILL", child)
      Process.wait(child)

      taken = Ownership.new(path)
      refute_nil taken.acquire("role" => "dispatcher"), "a killed owner releases the store"
      assert_equal Process.pid, taken.current.fetch("owner_pid")
      taken.release
      assert_equal Process.pid, taken.current.fetch("owner_pid"), "the last holder stays visible for inspection"
      refute_nil taken.current.fetch("released_at")
    end
  end

  def test_the_loop_runs_bounded_passes_and_waits_between_them
    in_tmpdir do |directory|
      clock = TestClock.new
      dispatcher = RecordingDispatcher.new
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(File.join(directory, "lock")), clock: clock, interval: 7)

      result = worker.run(max_passes: 3)

      assert_equal 3, dispatcher.passes
      assert_equal "max_passes", result.fetch("stop_reason")
      assert_equal [7, 7], clock.waits, "it waits between passes, never after the last one"
    end
  end

  def test_the_loop_never_sleeps_past_a_deadline_it_already_knows_about
    in_tmpdir do |directory|
      clock = TestClock.new
      due = (clock.now + 2).utc.iso8601(6)
      dispatcher = RecordingDispatcher.new([{ "dispatched" => 0, "next_wake_up" => due, "intents" => [] }])
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(File.join(directory, "lock")), clock: clock, interval: 30)

      worker.run(max_passes: 2)

      assert_equal [2.0], clock.waits
    end
  end

  def test_work_that_is_due_but_declined_does_not_spin_the_loop
    in_tmpdir do |directory|
      clock = TestClock.new
      # A deadline already in the past: the pass declined this work (unauthorized mode, or a durable
      # block), so waking immediately would only burn the CPU.
      overdue = (clock.now - 90).utc.iso8601(6)
      dispatcher = RecordingDispatcher.new(Array.new(3) { { "dispatched" => 0, "next_wake_up" => overdue, "intents" => [] } })
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(File.join(directory, "lock")), clock: clock, interval: 5)

      worker.run(max_passes: 3)

      assert_equal [5, 5], clock.waits
    end
  end

  def test_an_unattended_loop_keeps_only_counters_and_the_last_pass
    in_tmpdir do |directory|
      clock = TestClock.new
      reports = Array.new(50) { { "dispatched" => 1, "next_wake_up" => nil, "intents" => [{ "intent_id" => "intent-x" }] } }
      dispatcher = RecordingDispatcher.new(reports)
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(File.join(directory, "lock")), clock: clock, interval: 1)

      result = worker.run(max_passes: 50)

      assert_equal 50, result.fetch("passes")
      assert_equal 50, result.fetch("dispatched")
      assert_equal 1, result.fetch("last_pass").fetch("intents").length
      assert_equal %w[schema_version owner passes stop_reason dispatched last_pass].sort, result.keys.sort,
        "the result carries no per-pass history that would grow without bound"
    end
  end

  def test_work_left_only_by_the_pass_limit_is_picked_up_immediately
    in_tmpdir do |directory|
      clock = TestClock.new
      reports = Array.new(2) { { "dispatched" => 1, "deferred" => 2, "deferred_by_limit" => 2, "next_wake_up" => nil, "intents" => [] } }
      dispatcher = RecordingDispatcher.new(reports)
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(File.join(directory, "lock")), clock: clock, interval: 30)

      worker.run(max_passes: 2, limit: 1)

      assert_equal [0], clock.waits, "--limit bounds how much starts at once, not throughput"
    end
  end

  def test_a_pass_that_started_nothing_never_wakes_instantly
    in_tmpdir do |directory|
      clock = TestClock.new
      # --limit 0 defers everything and starts nothing; waking immediately would be a hot loop.
      reports = Array.new(3) { { "dispatched" => 0, "deferred" => 3, "deferred_by_limit" => 3, "next_wake_up" => nil, "intents" => [] } }
      worker = Worker.new(dispatcher: RecordingDispatcher.new(reports), ownership: Ownership.new(File.join(directory, "lock")),
        clock: clock, interval: 5)

      worker.run(max_passes: 3, limit: 0)

      assert_equal [5, 5], clock.waits
    end
  end

  def test_taking_ownership_survives_a_concurrent_probe
    in_tmpdir do |directory|
      path = File.join(directory, "lock")
      File.write(path, "{}")
      probe = File.open(path, File::RDONLY)
      probe.flock(File::LOCK_SH)
      releasing = Thread.new do
        sleep(0.015)
        probe.flock(File::LOCK_UN)
        probe.close
      end

      lease = Ownership.new(path).acquire("role" => "dispatcher")

      assert lease, "a momentary reader must not make a starting dispatcher believe the store is taken"
      releasing.join
    end
  end

  def test_a_signal_stops_new_dispatches_and_reports_why
    in_tmpdir do |directory|
      clock = TestClock.new
      interrupts = Backstage::Support::Interrupts.new
      dispatcher = RecordingDispatcher.new
      clock.on_wait { interrupts.stop("TERM") }
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(File.join(directory, "lock")),
        clock: clock, interval: 1, interrupts: interrupts)

      result = worker.run

      assert_equal 1, dispatcher.passes, "the signal stopped new dispatches after the current pass"
      assert_equal "signal:TERM", result.fetch("stop_reason")
      refute_nil dispatcher.stops.first, "the pass itself can see that the worker is stopping"
      interrupts.close
    end
  end

  def test_ownership_is_released_after_a_failing_pass
    in_tmpdir do |directory|
      path = File.join(directory, "lock")
      dispatcher = Object.new
      dispatcher.define_singleton_method(:pass) { |**| raise Backstage::ContractError, "pack is broken" }
      worker = Worker.new(dispatcher: dispatcher, ownership: Ownership.new(path), clock: TestClock.new)

      assert_raises(Backstage::ContractError) { worker.once }

      taken = Ownership.new(path)
      refute_nil taken.acquire, "a failed pass must not leave the store owned"
      taken.release
    end
  end

  def test_an_interruptible_wait_ends_as_soon_as_the_signal_arrives
    clock = Backstage::Adapters::Environment::SystemClock.new
    interrupts = Backstage::Support::Interrupts.new
    interrupts.stop("INT")

    started = Time.now
    completed = clock.wait(30, interrupt: interrupts.reader)

    assert_equal false, completed, "the wait was cut short"
    assert_operator Time.now - started, :<, 5
    interrupts.close
  end

  private

  def wait_for(timeout: 10)
    deadline = Time.now + timeout
    sleep(0.02) until yield || Time.now > deadline
    raise "condition was not met within #{timeout}s" unless yield
  end
end
