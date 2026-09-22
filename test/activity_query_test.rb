# frozen_string_literal: true

require_relative "test_helper"

# The application-layer projection every surface shares. It adds only `caught_up` and the
# `follow` polling loop on top of Ports::Store's own cursor/paging contract, so these tests lean
# on activity_store_test.rb's coverage of that contract and focus on what this class adds.
class ActivityQueryTest < Minitest::Test
  Activity = Backstage::Domain::Activity
  ActivityQuery = Backstage::Application::ActivityQuery

  class TestClock < Backstage::Ports::Clock
    attr_reader :waits

    def initialize
      super()
      @now = Time.utc(2026, 1, 1)
      @waits = []
      @on_wait = nil
    end

    def now = @now
    def on_wait(&block) = @on_wait = block

    def wait(seconds, interrupt: nil)
      @waits << seconds
      @on_wait&.call
      true
    end
  end

  class TestInterrupt
    def initialize
      @stopped = false
    end

    def stop
      @stopped = true
    end

    def stopped?
      @stopped
    end

    def reader
      nil
    end
  end

  # A store whose pages never advance and never call themselves caught up. Real stores reach this
  # shape whenever the watermark a page was measured against is not the one its cursor can reach —
  # a filtered read that scanned nothing new, an adapter whose watermark is taken elsewhere. What
  # matters here is that `follow` must not spin on it.
  class StallingStore
    attr_reader :reads

    def initialize
      @reads = 0
    end

    def read_activity(after: nil, filters: {}, limit: 100)
      @reads += 1
      { "events" => [], "cursor" => after, "next_cursor" => "cursor-stuck",
        "high_water_mark" => "cursor-elsewhere", "deployment_id" => "deployment-1" }
    end
  end

  def build_store(directory)
    Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
  end

  def event(store, type: "runtime.observed", **overrides)
    Activity.event(
      type: type,
      deployment_id: store.deployment_id,
      source: Activity.source(adapter: "test.producer", provenance: "core"),
      **overrides
    )
  end

  def test_list_reports_caught_up_by_comparing_next_cursor_to_the_high_water_mark
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)

      empty = query.list
      assert_equal true, empty.fetch("caught_up")

      store.commit([], activity: [event(store, event_id: "event-1", run_id: "run-1")])
      bounded = query.list(limit: 1)
      refute_equal true, bounded.fetch("caught_up"), "a short page below the watermark has not caught up"

      whole = query.list(limit: 100)
      assert_equal true, whole.fetch("caught_up")
    end
  end

  def test_show_returns_the_event_and_raises_not_found_for_an_absent_id
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      store.commit([], activity: [event(store, event_id: "event-1", run_id: "run-1", summary: "hello")])

      assert_equal "hello", query.show("event-1").fetch("summary")
      assert_raises(Backstage::NotFound) { query.show("event-absent") }
    end
  end

  def test_list_and_show_never_write
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      store.commit([], activity: [event(store, event_id: "event-1", run_id: "run-1")])
      bytes = File.binread(path)

      query.list
      query.list(filters: { run_id: "run-1" }, limit: 1)
      query.show("event-1")
      begin
        query.show("event-absent")
      rescue Backstage::NotFound
        nil
      end

      assert_equal bytes, File.binread(path)
    end
  end

  # A limit of 1 forces several passes to drain a sparse filter (mirroring
  # activity_store_test.rb's own filtered-pagination proof), and the clock is rigged to stop the
  # loop the moment it is ever asked to wait — so whatever pass first triggers a wait is
  # unambiguously the one where the page reported `caught_up`, regardless of exactly how many
  # scanned positions the store's own budget needed to get there.
  def test_follow_drains_every_scanned_position_before_it_ever_waits
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      matching = []
      6.times do |index|
        interesting = index.even?
        identity = "event-#{index}"
        matching << identity if interesting
        store.commit([], activity: [event(store, event_id: identity, run_id: "run-#{index}",
                                           type: interesting ? "decision.raised" : "runtime.observed")])
      end
      clock = TestClock.new
      interrupt = TestInterrupt.new
      clock.on_wait { interrupt.stop }

      pages = []
      query.follow(filters: { type: "decision.raised" }, limit: 1, interval: 5, clock: clock, interrupt: interrupt) { |page| pages << page }

      assert_equal matching, pages.flat_map { |page| page.fetch("events") }.map { |committed| committed.fetch("event_id") }
      assert_equal [5], clock.waits, "the loop waits exactly once, on the pass that first reports caught_up"
      assert pages[0..-2].all? { |page| !page.fetch("caught_up") }, "every page before the wait still had scanned history ahead of it"
      assert pages.last.fetch("caught_up")
    end
  end

  def test_follow_yields_empty_pages_so_a_caller_can_see_it_is_caught_up
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      clock = TestClock.new

      pages = []
      query.follow(max_passes: 3, interval: 1, clock: clock) { |page| pages << page }

      assert_equal 3, pages.length
      assert pages.all? { |page| page.fetch("events").empty? }
      assert pages.all? { |page| page.fetch("caught_up") }
      assert_equal [1, 1], clock.waits, "the third pass hits max_passes before it would wait a third time"
    end
  end

  def test_follow_sees_events_committed_between_passes_through_the_injected_clock
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      # A store appends its own stream-started marker ahead of a deployment's *first* activity
      # commit (see Ports::Store#commit). Seeding one up front, and starting `after` it, keeps that
      # one-time marker out of the way so the passes below only ever see the events this test adds.
      store.commit([], activity: [event(store, event_id: "event-seed", run_id: "run-seed")])
      started_at = query.list.fetch("next_cursor")
      clock = TestClock.new
      committed = 0
      clock.on_wait do
        store.commit([], activity: [event(store, event_id: "event-late-#{committed}", run_id: "run-late")])
        committed += 1
      end

      pages = []
      cursor = query.follow(after: started_at, max_passes: 3, interval: 1, clock: clock) { |page| pages << page }

      # Pass 1: nothing yet, caught up, waits (committing event-late-0). Pass 2: reads it, caught
      # up again, waits (committing event-late-1). Pass 3: reads that one and stops at max_passes
      # before it would wait a third time.
      assert_equal [[], ["event-late-0"], ["event-late-1"]], pages.map { |page| page.fetch("events").map { |committed_event| committed_event.fetch("event_id") } }
      assert_equal [1, 1], clock.waits
      assert_equal query.list.fetch("high_water_mark"), cursor
    end
  end

  def test_follow_stops_at_max_passes_even_when_never_caught_up
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      10.times { |index| store.commit([], activity: [event(store, event_id: "event-#{index}", run_id: "run-#{index}")]) }
      clock = TestClock.new

      passes = 0
      query.follow(limit: 1, max_passes: 3, interval: 1, clock: clock) { passes += 1 }

      assert_equal 3, passes
      assert_empty clock.waits, "history remained on every pass, so none of them waited"
    end
  end

  # The defect this reproduces: `follow` waited only on `caught_up`, so a page that returned no
  # events and did not move its cursor sent it straight back into another read. Four passes, zero
  # waits, one pegged core.
  def test_follow_waits_when_a_pass_reads_nothing_and_the_cursor_does_not_move
    store = StallingStore.new
    query = ActivityQuery.new(store: store)
    clock = TestClock.new

    query.follow(max_passes: 4, interval: 7, clock: clock)

    assert_equal 4, store.reads
    # Pass 1 moves the cursor off nil so it cannot yet tell; passes 2 and 3 see it stuck and wait;
    # pass 4 hits max_passes before it would wait again.
    assert_equal [7, 7], clock.waits, "a stalled stream must be polled on the clock, not spun on"
  end

  def test_follow_does_not_wait_while_a_page_is_still_advancing_the_cursor
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      # More history than three pages of scan budget can reach, so none of these passes is caught up.
      40.times { |index| store.commit([], activity: [event(store, event_id: "event-#{index}", run_id: "run-#{index}")]) }
      clock = TestClock.new

      # A sparse filter matches nothing, so every page is empty — but the cursor keeps moving
      # across the scanned positions, which is progress and must not be slowed to the interval.
      pages = []
      query.follow(filters: { "run_id" => "run-absent" }, limit: 1, max_passes: 3, interval: 1, clock: clock) { |page| pages << page }

      assert pages.all? { |page| page.fetch("events").empty? && !page.fetch("caught_up") }
      assert_empty clock.waits, "an empty page whose cursor advanced is progress, not a stall"
    end
  end

  def test_follow_stops_when_the_interrupt_fires_and_the_clock_wait_is_cut_short
    in_tmpdir do |directory|
      store = build_store(directory)
      query = ActivityQuery.new(store: store)
      interrupt = TestInterrupt.new
      clock = TestClock.new
      clock.on_wait { interrupt.stop }

      passes = 0
      query.follow(max_passes: 100, interval: 1, clock: clock, interrupt: interrupt) { passes += 1 }

      assert_equal 1, passes, "the interrupt fired during the first pass's wait, so a second pass never starts"
    end
  end
end
