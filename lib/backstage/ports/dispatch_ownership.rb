# frozen_string_literal: true

module Backstage::Ports
  # One local dispatcher owner per store.
  #
  # Ownership must be released by a crash, not only by an orderly exit, so an implementation holds
  # something the operating system reclaims when the process dies rather than a record it has to
  # remember to clean up. `acquire` returns a lease describing the holder, or nil when someone else
  # holds it; `current` reports the recorded holder for operator inspection.
  class DispatchOwnership
    def acquire(_identity = {})
      raise NotImplementedError
    end

    def release
      raise NotImplementedError
    end

    def current
      raise NotImplementedError
    end

    def held?
      raise NotImplementedError
    end

    # True when nobody holds ownership at this instant. Reporting a recorded holder as the current
    # one is how an operator ends up believing a dead process is still running.
    def free?
      raise NotImplementedError
    end
  end
end
