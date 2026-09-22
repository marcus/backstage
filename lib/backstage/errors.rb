# frozen_string_literal: true

module Backstage
  class Error < StandardError; end
  class NotFound < Error; end
  class InvalidTransition < Error; end
  class ContractError < Error; end
  class AuthorityError < Error; end
  class ConflictError < Error; end

  # A cursor a store will not read from. An activity cursor is opaque and bound to the deployment
  # and the exact filter set it was issued for, so a consumer that changes either — or restores a
  # store from elsewhere — is told rather than silently handed unrelated history.
  # `cursor_expired` is reserved for retention: nothing prunes activity yet, and when something
  # does it reports the oldest position still available instead of quietly skipping a gap.
  # `cursor_ahead_of_stream` is a cursor that addresses a position this deployment has never
  # committed — a restored-from-backup log, a hand-edited checkpoint, a consumer that kept a cursor
  # across a rebuild. Accepting it would leave a follower permanently "behind" a watermark it can
  # never reach, so the store refuses and reports the position the stream actually reaches.
  class ActivityCursorError < ContractError
    CODES = %w[
      cursor_malformed
      cursor_deployment_mismatch
      cursor_filter_mismatch
      cursor_expired
      cursor_ahead_of_stream
    ].freeze

    attr_reader :code, :deployment_id, :oldest_position, :high_water

    def initialize(message, code:, deployment_id: nil, oldest_position: nil, high_water: nil)
      raise ArgumentError, "unknown activity cursor code #{code.inspect}" unless CODES.include?(code.to_s)

      super(message)
      @code = code.to_s
      @deployment_id = deployment_id
      @oldest_position = oldest_position
      @high_water = high_water
    end
  end
  # An activity event id was reused for a different fact. Unlike an ordinary ConflictError — a lost
  # optimistic race, which the next pass re-reads and retries — this one says a producer minted two
  # different histories under one identity. Retrying cannot fix it and swallowing it loses the
  # event, so every retry and swallow site re-raises this while still absorbing a stale guard.
  class ActivityConflictError < ConflictError
    attr_reader :event_id

    def initialize(message, event_id: nil)
      super(message)
      @event_id = event_id
    end
  end

  # Durable capture of a runtime's output failed: a chunk could not be made durable, or the commit
  # that would have acknowledged it was refused. It is deliberately loud. The alternative — a
  # silent in-memory success trail — would let an authorized action run against a stream nobody
  # can prove was audited, which is exactly what the activity plan forbids.
  #
  # `stream_id` names the stream, `offset` the last byte position that *is* durable (so a caller
  # can report the unobserved interval), and `cause_class` the underlying failure's class name.
  # The original exception stays reachable through Ruby's own `Exception#cause` when this was
  # raised from inside a rescue; `cause_class` is the part that survives into a stored record.
  class CaptureError < Error
    attr_reader :stream_id, :offset, :cause_class

    def initialize(message, stream_id: nil, offset: nil, cause: nil)
      super(message)
      @stream_id = stream_id
      @offset = offset
      @cause_class = case cause
                     when nil then nil
                     when Exception then cause.class.name
                     when Class then cause.name
                     else cause.to_s
                     end
    end

    # The bounded, secret-free shape a runner puts on a run record or a command result.
    def to_h
      { "status" => "failed", "stream_id" => @stream_id, "last_offset" => @offset,
        "error" => message, "cause" => @cause_class }.compact
    end
  end

  class ExternalCommandError < Error
    attr_reader :argv, :status, :stdout, :stderr

    def initialize(message, argv:, status:, stdout:, stderr:)
      super(message)
      @argv = argv
      @status = status
      @stdout = stdout
      @stderr = stderr
    end
  end
end
