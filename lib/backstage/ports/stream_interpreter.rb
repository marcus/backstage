# frozen_string_literal: true

module Backstage::Ports
  # Turns framed records into meaning. One provider's stream format lives behind one interpreter,
  # so the capture path never learns what a harness event looks like and there is exactly one
  # place that parses a given format.
  #
  # The base class is the null interpreter: it observes nothing, which is the correct behavior for
  # a stream whose bytes matter but whose contents Backstage makes no claims about.
  #
  # An interpreter is pure with respect to the world: no IO, no store, no clock beyond what a
  # record carries. Its `state` is checkpointed alongside the framer's, so it must be bounded and
  # JSON-safe — an interpreter that accumulates every record it has seen is a memory leak with a
  # checkpoint attached.
  class StreamInterpreter
    # Observes one frame (see Support::RecordFramer). Returns observations, usually none.
    #
    # An observation is a Hash:
    #
    #   { "type" => "agent.tool_observed",   # an Activity type; the sink emits it as an event
    #     "summary" => "...",                # bounded, redacted, human-readable
    #     "data" => { ... },                 # bounded ids, counts, digests, capped previews
    #     "provenance" => "agent_reported",  # authority class of what the observation *says*
    #     "occurred_at" => "..." or nil,     # provider time; nil means the capture's own
    #     "provider_event_id" => ... or nil,
    #     "provider_session_id" => ... or nil,
    #     "sentinel" => { "name" => "repository_prepared", "payload" => { ... } } or nil }
    #
    # Three special shapes:
    #
    # - `{"malformed" => true}` (no `type`) says the frame could not be parsed. It is counted and
    #   reported in a bounded way, never dropped: a line a parser cannot read is evidence about the
    #   runtime, not noise.
    # - An observation with **no `type`** is metadata, not history. Its `provider_session_id` and
    #   its `sentinel` are applied to the stream — the checkpoint row, the close summary — and no
    #   event is built from it. A provider's session line is the standing example: it names the
    #   session every later event belongs to and is not itself a fact worth a place in history.
    # - `sentinel` payloads are collected into the close summary's `sentinels`, so a runner reads a
    #   parsed value instead of re-scanning the log a second time. A sentinel may ride on a typed
    #   observation or stand alone as metadata.
    #
    # The capture stamps `record_index` and the stream id onto each observation before the sink
    # sees it, so an event id can be derived from a durable position rather than a counter that
    # restarts.
    def observe(_frame)
      []
    end

    # Whether this stream's records are a machine protocol rather than human-readable output.
    #
    # It decides one thing: how large a single record may be before the framer caps it. That cap is
    # a memory bound, not a semantic limit, and the two kinds of stream want very different numbers
    # from it. A shell step printing a megabyte with no newline is a runaway process and 64 KiB is
    # the right ceiling. A JSON-lines harness protocol is different in kind: one record is one
    # complete statement, a tool result over 64 KiB is ordinary, and cutting it produces not a
    # shortened line but an unreadable one — which is how a successful run came to be reported as a
    # failure with empty fields. A protocol stream therefore gets the capture's generous bound.
    #
    # It is a property of the format, so the interpreter that owns the format answers it.
    def protocol?
      false
    end

    # Bounded, JSON-safe resumption state.
    def state
      {}
    end

    # Rebuilds an interpreter from checkpointed state.
    def self.restore(_state)
      new
    end
  end
end
