# frozen_string_literal: true

require "time"

module Backstage::Application
  # The foreground dispatcher process.
  #
  # It owns nothing of its own: it takes local ownership, repeats the dispatcher's bounded pass, and
  # waits between passes on an injected clock. There is deliberately no daemonization, no PID file
  # and no service definition here — an external supervisor runs this in the foreground and restarts
  # it, and a restart is exactly the recovery path every pass already takes.
  class Worker
    ConflictError = Backstage::ConflictError
    Interrupts = Backstage::Support::Interrupts
    DEFAULT_INTERVAL = 5

    attr_reader :ownership

    def initialize(dispatcher:, ownership:, clock:, interval: DEFAULT_INTERVAL, interrupts: nil)
      @dispatcher = dispatcher
      @ownership = ownership
      @clock = clock
      @interval = interval.to_f
      @interrupts = interrupts
    end

    # One bounded pass under ownership. This is the same operation the loop repeats, so an operator
    # or a cron-style supervisor can make progress without holding a process open.
    def once(limit: nil, work_item_id: nil)
      hold do |lease|
        report = @dispatcher.pass(limit: limit, work_item_id: work_item_id, stop: @interrupts && -> { @interrupts.stopped? })
        report.merge("owner" => lease, "passes" => 1)
      end
    end

    def run(max_passes: nil, limit: nil)
      owned = @interrupts.nil?
      interrupts = @interrupts || Interrupts.new.install
      hold do |lease|
        # Counters and the last pass only: an unattended worker runs for weeks, so nothing here may
        # grow with the number of passes.
        passes = 0
        dispatched = 0
        last_pass = nil
        stop_reason = "max_passes"
        loop do
          if interrupts.stopped?
            stop_reason = "signal:#{interrupts.reason}"
            break
          end
          # The signal is checked before every dispatch inside the pass too, so stopping means
          # stopping: the launch in flight drains, and no further work is started.
          last_pass = @dispatcher.pass(limit: limit, stop: -> { interrupts.stopped? })
          passes += 1
          dispatched += last_pass.fetch("dispatched")
          if max_passes && passes >= max_passes
            stop_reason = "max_passes"
            break
          end
          if interrupts.stopped?
            stop_reason = "signal:#{interrupts.reason}"
            break
          end
          @clock.wait(delay_for(last_pass), interrupt: interrupts.reader)
        end
        {
          "schema_version" => 1,
          "owner" => lease,
          "passes" => passes,
          "stop_reason" => stop_reason,
          "dispatched" => dispatched,
          "next_wake_up" => @dispatcher.next_wake_up,
          "last_pass" => last_pass
        }.compact
      end
    ensure
      interrupts.close if owned && interrupts
    end

    private

    # Polling only discovers due work, so the loop never sleeps past a deadline it already knows
    # about. A deadline already in the past means the pass just declined that work — an unauthorized
    # mode, a durable block — and waking immediately would only spin, so it waits the normal interval.
    def delay_for(pass)
      # Work this pass left only because of its own limit is actionable right now; making the
      # operator wait a poll interval for it would turn --limit into a throughput cap. A pass that
      # started nothing is a different matter — waking instantly there is a hot loop, not progress.
      return 0 if pass["deferred_by_limit"].to_i.positive? && pass["dispatched"].to_i.positive?

      due = pass["next_wake_up"]
      return @interval unless due

      remaining = (Time.parse(due) - @clock.now).to_f
      return @interval if remaining <= 0

      [remaining, @interval].min
    rescue ArgumentError, TypeError
      @interval
    end

    def hold
      lease = @ownership.acquire("role" => "dispatcher")
      unless lease
        held = @ownership.current || {}
        raise ConflictError,
              "another dispatcher owns this store (pid #{held["owner_pid"] || "unknown"} on #{held["owner_host"] || "unknown"}); " \
              "stop it before starting another"
      end

      yield(lease)
    ensure
      @ownership.release
    end
  end
end
