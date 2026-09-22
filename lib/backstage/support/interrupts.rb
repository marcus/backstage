# frozen_string_literal: true

module Backstage::Support
  # Turns SIGINT/SIGTERM into something a loop can poll and a wait can watch.
  #
  # A trap handler may do very little safely, so it writes one byte to a pipe. The worker checks
  # `stopped?` between passes and hands `reader` to the clock so a delay ends as soon as the signal
  # arrives instead of running out the poll interval.
  class Interrupts
    SIGNALS = %w[INT TERM].freeze

    attr_reader :reader

    def initialize(signals: SIGNALS)
      @signals = signals
      @reader, @writer = IO.pipe
      @stopped = false
      @previous = {}
      @reason = nil
    end

    def install
      @signals.each do |name|
        @previous[name] = Signal.trap(name) { note(name) }
      rescue ArgumentError
        # A platform without this signal simply cannot deliver it.
        next
      end
      self
    end

    def restore
      @previous.each { |name, handler| Signal.trap(name, handler || "DEFAULT") }
      @previous.clear
      self
    end

    def stop(reason = "requested")
      note(reason)
    end

    def stopped?
      @stopped
    end

    def reason
      @reason
    end

    def close
      restore
      [@reader, @writer].each { |io| io.close unless io.closed? }
      nil
    end

    private

    def note(reason)
      @stopped = true
      @reason ||= reason
      @writer.write_nonblock(".")
    rescue IOError, Errno::EAGAIN, Errno::EPIPE
      nil
    end
  end
end
