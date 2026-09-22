# frozen_string_literal: true

require "time"

module Backstage::Adapters::Environment
  # The real clock. Waiting is interruptible: when the worker hands it the read end of its signal
  # pipe, a SIGINT or SIGTERM ends the delay immediately instead of after the whole poll interval.
  class SystemClock < Backstage::Ports::Clock
    def now
      Time.now.utc
    end

    def wait(seconds, interrupt: nil)
      seconds = seconds.to_f
      return true if seconds <= 0

      return interrupt.wait_readable(seconds).nil? if interrupt

      sleep(seconds)
      true
    end
  end
end
