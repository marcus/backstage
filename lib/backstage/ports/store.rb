# frozen_string_literal: true

module Backstage::Ports
  # The owned persistence seam: an ordered record store with guarded atomic batches and an
  # append-only activity stream. It is deliberately a generic contract — no SQL, no file offsets,
  # no byte positions cross it — so JSONL today and something else later are the same shape.
  class Store
    # What `commit` returns. It is an Array of the written records, so every existing caller that
    # does `.first` or `.map` on a commit keeps working unchanged, plus `activity`: the committed
    # events in the order the caller passed them, each carrying its store-assigned `sequence` and
    # `recorded_at`. A reconciled duplicate yields the *original* event, not a new one.
    #
    # `activity` is empty for a commit that passed no events. A store may append its own
    # `activity.stream_started` marker ahead of a deployment's first events; that marker is part of
    # the stream and readable through `read_activity`, but it is not the caller's event and does
    # not appear here.
    class CommitResult < Array
      attr_reader :activity

      def initialize(records, activity: [])
        super()
        replace(records)
        @activity = activity.freeze
      end
    end

    def save(_collection, _record)
      raise NotImplementedError
    end

    def fetch(_collection, _id)
      raise NotImplementedError
    end

    def list(_collection)
      raise NotImplementedError
    end

    def find(_collection, **_fields)
      raise NotImplementedError
    end

    # Applies several records as one unit after checking that each expectation still holds.
    #
    # `writes` is a list of `[collection, record]` pairs. `expect` is a list of
    # `{ collection:, id:, revision: }` guards; a nil revision means the record must not exist yet.
    # A guard may instead (or additionally) use `fields: { state: "queued", run_id: nil }`
    # to require an existing record whose named fields match exactly. Nil matches an absent or
    # nil field; absence guards cannot include fields. All guards use the same current snapshot.
    # Readers must never observe a partial batch, including after an interrupted write.
    # Implementations raise Backstage::ConflictError without writing anything when a guard fails,
    # which is how the application fences stale callers and late results.
    #
    # `activity` is a list of activity event envelopes (see Backstage::Domain::Activity) appended
    # atomically with the writes. The business change and its history land together or not at all:
    # a guard failure commits neither, and a reader never sees an event whose state change is
    # missing. There is no second audit append after a successful state commit.
    #
    # The store owns ordering and immutability; the application owns meaning and identity:
    #
    # - `sequence` is assigned by the store, contiguous within one commit and strictly increasing
    #   per deployment. A caller that supplies `sequence` or `recorded_at` is refused
    #   (Backstage::ContractError) — those are the store's to set, and accepting them would let a
    #   producer forge reading order.
    # - Every event is validated against `activity-event-v1` before it becomes durable, including
    #   the bound the schema cannot express: serialized `data` may be at most
    #   Backstage::Domain::Activity::DATA_BYTE_LIMIT (16 KiB). `data` carries ids, states, counts
    #   and revisions; an outcome, a transcript or a diff belongs behind an artifact reference.
    #   Over the bound is a Backstage::ContractError, refused rather than truncated — a silently
    #   shortened payload would be a fact that reads as complete and is not.
    # - An event id already in the stream whose canonical fingerprint matches is *reconciled*: the
    #   original event is returned, nothing is appended, and history does not grow. This is what
    #   makes an exact retry of a command safe.
    # - The same event id with a different fingerprint is a Backstage::ActivityConflictError (a
    #   Backstage::ConflictError, so existing rescues still see it) and nothing at all is written —
    #   neither the events nor the accompanying records. It is a distinct type because it is not a
    #   lost optimistic race: a producer minted two different facts under one identity, and no
    #   retry can resolve that. A caller that retries or swallows ConflictError must re-raise this.
    # - If every record write and every event in a commit is an exact duplicate of what is already
    #   stored, nothing is appended at all.
    # - `save` and `commit` write records; neither can ever overwrite an event. The collection name
    #   Backstage::Domain::Activity::COLLECTION is reserved so no record write can reach one.
    #
    # Returns a CommitResult.
    def commit(_writes, expect: [], activity: [])
      raise NotImplementedError
    end

    # This deployment's stable identity: the thing a sequence is monotonic within and a cursor is
    # bound to. It is minted once and persisted, never derived from a hostname or a filesystem
    # path, so a moved or renamed store keeps its stream and a restored copy does not silently
    # inherit another deployment's cursors. Calling this may mint and persist the identity; it is
    # the only method here that can write without being asked to commit something.
    def deployment_id
      raise NotImplementedError
    end

    # One bounded page of the activity stream, oldest first.
    #
    #   { "events" => [...],              # matching events, ascending by sequence
    #     "cursor" => <opaque or nil>,    # the `after` this page was read from
    #     "next_cursor" => <opaque>,      # pass as `after` to continue
    #     "high_water_mark" => <opaque>,  # latest committed position at read time
    #     "deployment_id" => <string or nil> }
    #
    # Semantics a consumer can rely on:
    #
    # - Cursors are opaque strings. Do not parse, compare, or order them; the only valid uses are
    #   passing one back as `after` and storing it. Comparing `next_cursor` to `high_water_mark`
    #   for equality is the supported way to ask "am I caught up?".
    # - A cursor is bound to the deployment *and* to the exact normalized filter set it was issued
    #   for. Resuming with different filters, or against a different deployment, raises
    #   Backstage::ActivityCursorError rather than skipping or repeating history. (A cursor from an
    #   empty stream addresses no history and is accepted by any deployment.)
    # - A cursor addressing a position this deployment has never committed — a restored backup, a
    #   hand-edited checkpoint, a cursor kept across a rebuild — raises Backstage::ActivityCursorError
    #   with code `cursor_ahead_of_stream` and `high_water`, the newest position the stream reaches.
    #   It is refused rather than accepted because such a cursor can never catch up: every page it
    #   yields is empty and its `next_cursor` never reaches a watermark that is behind it, so a
    #   follower would poll forever believing it was behind.
    # - `limit` must be positive and at most Backstage::Domain::Activity::MAX_READ_LIMIT (1000).
    #   A larger request is a Backstage::ContractError, not a quiet clamp: a caller that asked for
    #   5,000 and silently got 1,000 would read the short page as "that is all there is".
    # - `high_water_mark` is a finite-page snapshot watermark: the latest committed sequence at the
    #   moment of this read. Events committed after it appear on later reads, after the last
    #   acknowledged cursor; a wall-clock change never reorders history. Follow is this same
    #   bounded read repeated — there is no subscription and no server.
    # - Filtered pages advance across scanned positions even when nothing matched, so `next_cursor`
    #   moves past scanned events and a sparse filter does not rescan them forever. A page may
    #   therefore be shorter than `limit`, or empty, while more history remains: keep reading until
    #   `next_cursor` reaches `high_water_mark`.
    # - `filters` accepts `work_item_id`, `run_id`, `target_id`, `type` (one type or many), and
    #   `related_id`, which matches any relationship id, typed link, artifact reference, or the
    #   event id itself. Unknown filter keys and unknown types are refused, never ignored.
    # - Reads never write. They do not mint a deployment identity, drain effects, replay
    #   transitions, or advance any workflow state; running one leaves the store's bytes untouched.
    # - Persist a cursor only after successfully processing the page it came with.
    # - Nothing prunes activity yet. When retention arrives, a cursor pointing before the oldest
    #   retained position raises Backstage::ActivityCursorError with code `cursor_expired` and the
    #   oldest position still available.
    def read_activity(after: nil, filters: {}, limit: 100)
      raise NotImplementedError
    end

    # One event by its id, or nil. Like `read_activity`, this never writes.
    def fetch_activity(_event_id)
      raise NotImplementedError
    end
  end
end

Backstage::Store = Backstage::Ports::Store unless defined?(Backstage::Store)
