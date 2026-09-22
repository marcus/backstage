# frozen_string_literal: true

module Backstage::Ports
  # The bounded seam a running worker uses to ask for a workflow transition.
  #
  # Workers never hold the authoritative store, host credentials, or operator CLI authority. They
  # append requests here; the controller drains them, derives the actor role from the run that owns
  # the channel, and puts each request through the same transition operation as everything else.
  class AgentRequestChannel
    # Absolute path a worker appends newline-delimited JSON requests to.
    def path
      raise NotImplementedError
    end

    # Reads whatever the worker has written so far and applies it. Returns the recorded requests.
    # Draining never raises because of worker input; a bad request is recorded as rejected.
    def drain(run:)
      raise NotImplementedError
    end
  end
end
