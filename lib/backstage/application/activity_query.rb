# frozen_string_literal: true

module Backstage::Application
  # The read-only activity projection every surface shares: the CLI today, a UI/API tomorrow. It
  # adds no filtering or paging rules of its own — Ports::Store already owns cursor opacity, the
  # deployment/filter binding on a cursor, the finite-page watermark, and "keep reading until
  # next_cursor reaches high_water_mark"; this class only shapes a small convenience (`caught_up`)
  # and the `follow` polling loop on top of that contract. It performs no writes: every method
  # here, including `follow`, only ever calls `read_activity` / `fetch_activity`.
  class ActivityQuery
    NotFound = Backstage::NotFound

    def initialize(store:)
      @store = store
    end

    # One bounded page, plus whether this page has reached the high-water mark the store observed
    # at read time. `caught_up` is exactly `next_cursor == high_water_mark` — the store's own
    # documented way to ask "am I caught up?" — surfaced here so a caller does not re-derive it.
    # A caller still walks pages by cursor until it sees this rather than assuming one page, or a
    # short/empty one, is ever the whole story.
    def list(after: nil, filters: {}, limit: 100)
      page = @store.read_activity(after: after, filters: filters, limit: limit)
      page.merge("caught_up" => page.fetch("next_cursor") == page.fetch("high_water_mark"))
    end

    # One event by id. Unlike the store's `fetch_activity`, which answers nil for "not here" the
    # same way every other reader in this codebase does, a surface asking for one event by id
    # wants the ordinary not-found signal so the CLI's existing error path (NotFound -> exit 1)
    # covers it without a special case.
    def show(event_id)
      @store.fetch_activity(event_id) || raise(NotFound, "activity event #{event_id.inspect} was not found")
    end

    # Repeats the bounded read a consumer would otherwise hand-roll, oldest-first.
    #
    # Every pass yields its page, including an empty one: a sparse filter's cursor still advances
    # across scanned positions with nothing matching, and a caller watching for freshness (a UI
    # showing "caught up as of ...", a checkpoint it means to persist) needs to see that advance
    # rather than sit through silent passes it cannot distinguish from "not polling at all". A
    # pass waits `interval` on the clock only once it is caught up; while committed history remains
    # unread it keeps draining immediately, exactly as `read_activity` documents.
    #
    # A pass also waits when it read nothing *and* the cursor did not move, whether or not the
    # store called it caught up. That is a stream with nothing left to give this reader, and
    # spinning on it would burn a core against a log that is not changing.
    #
    # `interrupt`, when given, is anything answering `stopped?` and `reader`
    # (Backstage::Support::Interrupts is the concrete case) — checked before every read so a
    # signal lands before the next page starts, and handed to the clock so a wait ends immediately
    # rather than running out its interval. `max_passes` bounds the number of reads instead, which
    # is what makes this callable deterministically from a test without any real waiting or
    # signalling at all.
    #
    # Returns the last cursor observed, so a caller that stops here (interrupted, or at
    # `max_passes`) can resume a later `follow` or `list` exactly where this left off.
    def follow(after: nil, filters: {}, limit: 100, interval: 1, max_passes: nil, clock:, interrupt: nil)
      cursor = after
      passes = 0
      loop do
        break if interrupt&.stopped?

        page = list(after: cursor, filters: filters, limit: limit)
        passes += 1
        yield page if block_given?
        idle = page.fetch("events").empty? && page.fetch("next_cursor") == cursor
        cursor = page.fetch("next_cursor")
        break if max_passes && passes >= max_passes
        break if interrupt&.stopped?

        clock.wait(interval, interrupt: interrupt&.reader) if page.fetch("caught_up") || idle
      end
      cursor
    end
  end
end
