# frozen_string_literal: true

module Backstage::Application
  # The capture an opener uses when nobody composed one for it.
  #
  # Framing and interpretation are not optional — a harness that cannot read its own output is
  # broken whether or not a store is present — so "no capture" means the null *sink*, not a missing
  # component. A runner invoked directly, without an Engine and without a store, still frames
  # records, still runs its interpreter, still reports sentinels and still obeys its bounds; it
  # simply writes nothing down. That is what keeps the composed path and the bare path the same
  # code rather than two behaviors that drift.
  module CaptureDefaults
    # The clock the null path runs on when the caller has none.
    #
    # It is not a second implementation of the system clock and must not become one: nothing is
    # persisted through a null sink, so the only thing time decides here is where a flush boundary
    # falls, and no one observes those flushes. Waiting is not part of it at all — a capture never
    # waits. The real clock is an adapter, injected wherever a composed system builds a capture; the
    # application layer may not name one, and does not need to for this.
    class WallClock < Backstage::Ports::Clock
      def now
        Time.now.utc
      end

      def wait(_seconds, interrupt: nil)
        raise Backstage::ContractError, "capture never waits"
      end
    end

    module_function

    def null(run: "local", attempt: nil, phase: nil, clock: nil, secret_guard: nil, **options)
      RuntimeCapture.new(
        sink: CaptureSink::Null.new,
        clock: clock || WallClock.new,
        run: run, attempt: attempt, phase: phase,
        **(secret_guard ? { secret_guard: secret_guard } : {}),
        **options
      )
    end
  end
end
