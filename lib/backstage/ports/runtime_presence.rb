# frozen_string_literal: true

module Backstage::Ports
  # Answers whether a recorded runtime identity is still alive.
  #
  # The only three honest answers are "alive", "gone", and "unknown". Recovery acts on the first
  # two and leaves the third visible and actionable rather than guessing death or success.
  class RuntimePresence
    ALIVE = "alive"
    GONE = "gone"
    UNKNOWN = "unknown"

    def status(_runtime)
      UNKNOWN
    end
  end
end
