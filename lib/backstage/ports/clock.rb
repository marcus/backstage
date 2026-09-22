# frozen_string_literal: true

module Backstage::Ports
  # Time and waiting, injected so durable scheduling can be proven without real sleeping.
  #
  # `now` is the wall clock that due times are recorded against; `wait` is the only place the
  # dispatcher is allowed to pause, and it may return early when something asks it to stop.
  class Clock
    def now
      raise NotImplementedError
    end

    def timestamp
      now.utc.iso8601(6)
    end

    # Waits up to `seconds`. Returns true when the full delay elapsed, false when it was cut short.
    def wait(_seconds, interrupt: nil)
      raise NotImplementedError
    end
  end
end
