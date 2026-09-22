# frozen_string_literal: true

require_relative "test_helper"

# The store half of durable activity: events land in the same fsynced transaction as the state
# change they explain, sequences are the store's to assign, and history is never rewritten.
class ActivityStoreTest < Minitest::Test
  Activity = Backstage::Domain::Activity

  def build_store(directory, name: "state.jsonl")
    Backstage::JsonlStore.new(File.join(directory, name))
  end

  def event(store, type: "work.admitted", **overrides)
    Activity.event(
      type: type,
      deployment_id: store.deployment_id,
      source: Activity.source(adapter: "test.producer", provenance: "core", instance: "test-1"),
      **overrides
    )
  end

  def drain(store, filters: {}, limit: 100)
    events = []
    cursor = nil
    pages = 0
    loop do
      page = store.read_activity(after: cursor, filters: filters, limit: limit)
      pages += 1
      events.concat(page.fetch("events"))
      cursor = page.fetch("next_cursor")
      break if cursor == page.fetch("high_water_mark")
      raise "runaway pagination" if pages > 200
    end
    [events, pages]
  end

  def test_state_and_activity_commit_together_and_the_stream_declares_its_own_start
    in_tmpdir do |directory|
      store = build_store(directory)
      admitted = event(store, work_item_id: "work-1", summary: "admitted")

      result = store.commit([["work_items", { "id" => "work-1", "state" => "new" }]], activity: [admitted])

      assert_equal ["work-1"], result.map { |record| record["id"] }, "existing callers still read written records"
      assert_equal [admitted.fetch("event_id")], result.activity.map { |committed| committed.fetch("event_id") }
      assert_equal 2, result.activity.first.fetch("sequence")
      refute_nil result.activity.first.fetch("recorded_at")

      events = store.read_activity.fetch("events")
      assert_equal [Activity::STREAM_STARTED, "work.admitted"], events.map { |committed| committed.fetch("type") }
      assert_equal [1, 2], events.map { |committed| committed.fetch("sequence") }
      assert_equal({ "prior_history" => "none", "imported" => false }, events.first.fetch("data"))
      assert_equal store.deployment_id, events.first.fetch("deployment_id")
      assert_equal "store", events.first.dig("source", "provenance")
    end
  end

  def test_a_commit_with_no_activity_returns_exactly_what_it_always_did
    in_tmpdir do |directory|
      store = build_store(directory)

      result = store.commit([["jobs", { "id" => "job-1" }], ["runs", { "id" => "run-1" }]])

      assert_equal [{ "id" => "job-1" }, { "id" => "run-1" }], result
      assert_equal [], result.activity
      assert_equal "job-1", store.save("jobs", { "id" => "job-1", "state" => "queued" }).fetch("id")
      assert_empty store.read_activity.fetch("events")
      assert_nil store.read_activity.fetch("deployment_id")
    end
  end

  def test_sequence_and_recorded_at_supplied_by_a_caller_are_refused
    in_tmpdir do |directory|
      store = build_store(directory)
      base = event(store)

      assert_raises(Backstage::ContractError) { store.commit([], activity: [base.merge("sequence" => 1)]) }
      assert_raises(Backstage::ContractError) { store.commit([], activity: [base.merge("recorded_at" => "2026-01-01T00:00:00Z")]) }
      assert_raises(Backstage::ContractError) { store.commit([], activity: [base, base]) }
      assert_raises(Backstage::ContractError) { store.commit([], activity: [base.merge("deployment_id" => "deployment-elsewhere")]) }
      assert_empty store.read_activity.fetch("events")
    end
  end

  def test_a_malformed_event_never_becomes_durable_history
    in_tmpdir do |directory|
      store = build_store(directory)
      valid = event(store, work_item_id: "work-1")

      assert_raises(Backstage::ContractError) { store.commit([], activity: [valid.merge("type" => "work.invented")]) }
      assert_raises(Backstage::ContractError) { store.commit([], activity: [valid.reject { |key, _| key == "occurred_at" }]) }
      assert_raises(Backstage::ContractError) { store.commit([], activity: [valid.merge("summary" => "x" * 5_000)]) }
      assert_raises(Backstage::ContractError) { store.commit([], activity: ["not an event"]) }
      assert_empty store.read_activity.fetch("events")
    end
  end

  def test_records_can_never_be_written_into_the_activity_collection
    in_tmpdir do |directory|
      store = build_store(directory)

      assert_raises(Backstage::ContractError) { store.save(Activity::COLLECTION, { "id" => "event-forged" }) }
      assert_raises(Backstage::ContractError) { store.commit([[Activity::COLLECTION, { "id" => "event-forged", "sequence" => 1 }]]) }
      assert_empty store.read_activity.fetch("events")
    end
  end

  def test_an_exact_retry_returns_the_original_event_and_appends_nothing
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      admitted = event(store, event_id: "event-deterministic", work_item_id: "work-1", summary: "admitted")
      writes = [["work_items", { "id" => "work-1", "state" => "new" }]]
      original = store.commit(writes, activity: [admitted]).activity.first
      bytes = File.binread(path)

      replay = store.commit(writes, activity: [admitted])

      assert_equal original, replay.activity.first, "a retry is the same fact, told twice"
      assert_equal 2, replay.activity.first.fetch("sequence")
      assert_equal bytes, File.binread(path), "an exact replay of a whole commit writes nothing at all"
      assert_equal 2, store.read_activity.fetch("events").length
    end
  end

  def test_a_partial_retry_reconciles_its_duplicate_and_appends_only_what_is_new
    in_tmpdir do |directory|
      store = build_store(directory)
      first = event(store, event_id: "event-one", work_item_id: "work-1")
      store.commit([["work_items", { "id" => "work-1", "state" => "new" }]], activity: [first])
      second = event(store, event_id: "event-two", work_item_id: "work-1", type: "work.transition_applied")

      result = store.commit([["work_items", { "id" => "work-1", "state" => "started" }]], activity: [first, second])

      assert_equal %w[event-one event-two], result.activity.map { |committed| committed.fetch("event_id") }
      assert_equal [2, 3], result.activity.map { |committed| committed.fetch("sequence") }
      assert_equal %w[event-one event-two], store.read_activity.fetch("events").drop(1).map { |committed| committed.fetch("event_id") }
    end
  end

  def test_reusing_an_event_id_for_different_content_commits_neither_records_nor_events
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      admitted = event(store, event_id: "event-deterministic", work_item_id: "work-1", summary: "admitted")
      store.commit([["work_items", { "id" => "work-1", "state" => "new" }]], activity: [admitted])
      bytes = File.binread(path)

      error = assert_raises(Backstage::ActivityConflictError) do
        store.commit([["work_items", { "id" => "work-1", "state" => "started" }], ["runs", { "id" => "run-1" }]],
                     activity: [admitted.merge("summary" => "a different story")])
      end

      # Its own type, so a retry loop can absorb a lost guard without also absorbing this — but
      # still a ConflictError, so no existing caller stops seeing it. See Ports::Store#commit.
      assert_kind_of Backstage::ConflictError, error
      assert_equal "event-deterministic", error.event_id
      assert_equal bytes, File.binread(path)
      assert_equal "new", store.fetch!("work_items", "work-1").fetch("state")
      assert_empty store.list("runs")
      assert_equal 2, store.read_activity.fetch("events").length
    end
  end

  # A guard conflict is a lost race a retry fixes. An event-id conflict is not, and the only way a
  # caller can tell them apart is the type — so the guard path must stay the plain ConflictError.
  def test_a_lost_guard_is_not_reported_as_an_activity_conflict
    in_tmpdir do |directory|
      store = build_store(directory)
      store.save("work_items", { "id" => "work-1", "state" => "new", "revision" => 1 })

      error = assert_raises(Backstage::ConflictError) do
        store.commit([["work_items", { "id" => "work-1", "state" => "started", "revision" => 2 }]],
                     expect: [{ collection: "work_items", id: "work-1", revision: 5 }],
                     activity: [event(store, work_item_id: "work-1", type: "work.transition_applied")])
      end

      refute_kind_of Backstage::ActivityConflictError, error
    end
  end

  # --- bounds ---------------------------------------------------------------------------------

  def test_an_oversized_data_payload_is_refused_and_nothing_is_written
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      store.commit([], activity: [event(store, event_id: "event-small", data: { "state" => "new" })])
      bytes = File.binread(path)
      oversized = event(store, event_id: "event-big", data: { "blob" => "x" * (Activity::DATA_BYTE_LIMIT + 1) })

      error = assert_raises(Backstage::ContractError) { store.commit([["runs", { "id" => "run-1" }]], activity: [oversized]) }

      assert_match(/over the #{Activity::DATA_BYTE_LIMIT}-byte limit/, error.message)
      assert_equal bytes, File.binread(path), "an over-limit event takes its accompanying records down with it"
      assert_empty store.list("runs")
    end
  end

  def test_a_payload_just_under_the_cap_still_commits
    in_tmpdir do |directory|
      store = build_store(directory)
      # JSON adds `{"blob":""}` = 11 bytes around the string, so this is the largest string that fits.
      fits = event(store, event_id: "event-edge", data: { "blob" => "x" * (Activity::DATA_BYTE_LIMIT - 11) })

      store.commit([], activity: [fits])

      assert_equal Activity::DATA_BYTE_LIMIT, JSON.generate(store.fetch_activity("event-edge").fetch("data")).bytesize
    end
  end

  def test_a_page_larger_than_the_contract_maximum_is_refused_rather_than_quietly_clamped
    in_tmpdir do |directory|
      store = build_store(directory)
      store.commit([], activity: [event(store, event_id: "event-1")])

      error = assert_raises(Backstage::ContractError) { store.read_activity(limit: Activity::MAX_READ_LIMIT + 1) }

      assert_match(/maximum of #{Activity::MAX_READ_LIMIT}/, error.message)
      assert_raises(Backstage::ContractError) { store.read_activity(limit: 0) }
      assert_equal 2, store.read_activity(limit: Activity::MAX_READ_LIMIT).fetch("events").length
    end
  end

  # --- cursors that cannot catch up ---------------------------------------------------------------

  def test_a_cursor_past_the_newest_event_is_refused_with_the_position_the_stream_reaches
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      store.commit([], activity: [event(store, event_id: "event-1")])
      store.commit([], activity: [event(store, event_id: "event-2")])
      ahead = store.read_activity.fetch("high_water_mark")
      # A restored backup: the same deployment, a log that no longer reaches the position a
      # consumer checkpointed. Without this refusal every later page is empty, `next_cursor` never
      # reaches a watermark that is behind it, and a follower polls forever believing it is behind.
      File.write(path, File.readlines(path)[0..-2].join)
      reopened = build_store(directory)

      assert_equal 2, reopened.read_activity.fetch("events").length
      error = assert_raises(Backstage::ActivityCursorError) { reopened.read_activity(after: ahead) }

      assert_equal "cursor_ahead_of_stream", error.code
      assert_equal 2, error.high_water
      assert_equal reopened.deployment_id, error.deployment_id
    end
  end

  def test_a_cursor_exactly_at_the_high_water_mark_is_caught_up_not_ahead
    in_tmpdir do |directory|
      store = build_store(directory)
      store.commit([], activity: [event(store, event_id: "event-1")])
      mark = store.read_activity.fetch("high_water_mark")

      page = store.read_activity(after: mark)

      assert_empty page.fetch("events")
      assert_equal page.fetch("high_water_mark"), page.fetch("next_cursor")
    end
  end

  def test_a_guard_conflict_commits_neither_state_nor_activity
    in_tmpdir do |directory|
      store = build_store(directory)
      store.save("work_items", { "id" => "work-1", "state" => "new", "revision" => 1 })

      assert_raises(Backstage::ConflictError) do
        store.commit([["work_items", { "id" => "work-1", "state" => "started", "revision" => 2 }]],
                     expect: [{ collection: "work_items", id: "work-1", revision: 5 }],
                     activity: [event(store, work_item_id: "work-1", type: "work.transition_applied")])
      end

      assert_equal "new", store.fetch!("work_items", "work-1").fetch("state")
      assert_empty store.read_activity.fetch("events"), "history must not outlive the change it explains"
    end
  end

  def test_every_truncated_activity_transaction_is_invisible_and_repaired_before_the_next_append
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      store.commit([["work_items", { "id" => "work-1", "state" => "new" }]],
                   activity: [event(store, event_id: "event-first", work_item_id: "work-1")])
      baseline = File.binread(path)
      baseline_lines = File.readlines(path).length
      store.commit([["work_items", { "id" => "work-1", "state" => "started" }], ["runs", { "id" => "run-1" }]],
                   activity: [event(store, event_id: "event-second", work_item_id: "work-1", type: "work.transition_applied"),
                              event(store, event_id: "event-third", run_id: "run-1", type: "execution.started")])
      batch = File.binread(path).byteslice(baseline.bytesize..)

      (1...batch.bytesize).each do |length|
        File.binwrite(path, baseline + batch.byteslice(0, length))
        reopened = build_store(directory)

        assert_equal "new", reopened.fetch!("work_items", "work-1").fetch("state"), "partial batch at byte #{length}"
        assert_empty reopened.list("runs")
        events = reopened.read_activity.fetch("events")
        assert_equal %w[activity.stream_started work.admitted], events.map { |committed| committed.fetch("type") }
        assert_nil reopened.fetch_activity("event-second")

        reopened.commit([["runs", { "id" => "run-repaired" }]],
                        activity: [event(reopened, event_id: "event-repaired", run_id: "run-repaired", type: "execution.started")])
        assert_equal baseline_lines + 1, File.readlines(path).length
        assert_equal [1, 2, 3], reopened.read_activity.fetch("events").map { |committed| committed.fetch("sequence") },
                     "a repaired append reuses the position the lost transaction never claimed"
      end
    end
  end

  def test_concurrent_writers_produce_one_gap_free_ordering_and_one_deployment
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      commits = 6
      start_read, start_write = IO.pipe
      children = 2.times.map do |number|
        fork do
          start_write.close
          start_read.read(1)
          local = build_store(directory)
          commits.times do |index|
            local.commit([["runs", { "id" => "run-#{number}-#{index}" }]],
                         activity: [event(local, event_id: "event-#{number}-#{index}", run_id: "run-#{number}-#{index}",
                                          type: "execution.started", data: { "writer" => number, "index" => index })])
          end
          exit! 0
        end
      end
      start_read.close
      start_write.write("xx")
      start_write.close
      assert_equal [0, 0], children.map { |pid| Process.wait2(pid).last.exitstatus }

      store = build_store(directory)
      events, = drain(store)
      sequences = events.map { |committed| committed.fetch("sequence") }

      assert_equal (1..(2 * commits) + 1).to_a, sequences, "sequences are contiguous and strictly increasing"
      assert_equal events.length, events.map { |committed| committed.fetch("event_id") }.uniq.length
      assert_equal 1, events.count { |committed| committed.fetch("type") == Activity::STREAM_STARTED },
                   "a racing mint produces one stream, not two"
      assert_equal 1, store.list("deployments").length
      assert_equal [store.deployment_id], events.map { |committed| committed.fetch("deployment_id") }.uniq
      2.times do |number|
        indexes = events.select { |committed| committed.dig("data", "writer") == number }.map { |committed| committed.dig("data", "index") }
        assert_equal (0...commits).to_a, indexes, "one writer's events keep the order it committed them in"
      end
      assert_equal (2 * commits) + 1, File.readlines(path).length, "one line per commit, plus the guarded deployment mint"
    end
  end

  def test_filtered_pagination_resumes_without_misses_repeats_or_stalling_on_unmatched_pages
    in_tmpdir do |directory|
      store = build_store(directory)
      matching = []
      25.times do |index|
        interesting = index.zero? || index == 24
        identity = "event-#{format("%02d", index)}"
        matching << identity if interesting
        store.commit([["runs", { "id" => "run-#{index}" }]],
                     activity: [event(store, event_id: identity, run_id: "run-#{index}",
                                      type: interesting ? "decision.raised" : "runtime.observed")])
      end

      filters = { type: "decision.raised" }
      events, pages = drain(store, filters: filters, limit: 1)

      assert_equal matching, events.map { |committed| committed.fetch("event_id") }
      assert events.map { |committed| committed.fetch("sequence") }.each_cons(2).all? { |a, b| a < b }
      assert pages > matching.length, "a sparse filter advances across scanned positions instead of rescanning them"

      # The same history, read whole, contains exactly the same matches and nothing extra.
      everything, = drain(store, limit: 4)
      assert_equal 26, everything.length
      assert_equal matching, everything.select { |committed| committed.fetch("type") == "decision.raised" }.map { |committed| committed.fetch("event_id") }
      assert_equal everything.map { |committed| committed.fetch("event_id") }.uniq, everything.map { |committed| committed.fetch("event_id") }
    end
  end

  def test_a_page_is_bounded_and_the_watermark_is_the_position_at_read_time
    in_tmpdir do |directory|
      store = build_store(directory)
      5.times { |index| store.commit([], activity: [event(store, event_id: "event-#{index}", run_id: "run-#{index}")]) }

      page = store.read_activity(limit: 2)

      assert_equal 2, page.fetch("events").length
      assert_equal [1, 2], page.fetch("events").map { |committed| committed.fetch("sequence") }
      refute_equal page.fetch("next_cursor"), page.fetch("high_water_mark")
      assert_nil page.fetch("cursor")
      assert_equal page.fetch("next_cursor"), store.read_activity(after: page.fetch("next_cursor")).fetch("cursor")

      watermark = store.read_activity(limit: 100).fetch("high_water_mark")
      store.commit([], activity: [event(store, event_id: "event-late", run_id: "run-late")])
      later = store.read_activity(after: watermark)

      assert_equal ["event-late"], later.fetch("events").map { |committed| committed.fetch("event_id") },
                   "a later commit appears after the last acknowledged cursor, not inside it"
      assert_equal later.fetch("next_cursor"), later.fetch("high_water_mark")
    end
  end

  def test_a_cursor_from_another_filter_set_or_another_deployment_is_refused
    in_tmpdir do |directory|
      store = build_store(directory)
      store.commit([], activity: [event(store, run_id: "run-1")])
      cursor = store.read_activity(filters: { run_id: "run-1" }).fetch("next_cursor")

      error = assert_raises(Backstage::ActivityCursorError) { store.read_activity(after: cursor) }
      assert_equal "cursor_filter_mismatch", error.code
      assert_raises(Backstage::ActivityCursorError) { store.read_activity(after: cursor, filters: { run_id: "run-2" }) }
      assert_kind_of Backstage::ContractError, error

      other = build_store(directory, name: "other.jsonl")
      other.commit([], activity: [event(other, run_id: "run-1")])
      refute_equal store.deployment_id, other.deployment_id
      mismatch = assert_raises(Backstage::ActivityCursorError) { other.read_activity(after: cursor, filters: { run_id: "run-1" }) }
      assert_equal "cursor_deployment_mismatch", mismatch.code
      assert_equal other.deployment_id, mismatch.deployment_id

      malformed = assert_raises(Backstage::ActivityCursorError) { store.read_activity(after: "not-a-cursor") }
      assert_equal "cursor_malformed", malformed.code
      assert_raises(Backstage::ContractError) { store.read_activity(filters: { invented: "x" }) }
      assert_raises(Backstage::ContractError) { store.read_activity(limit: 0) }
    end
  end

  def test_a_cursor_taken_before_any_history_still_resumes_that_stream
    in_tmpdir do |directory|
      store = build_store(directory)
      empty = store.read_activity

      assert_nil empty.fetch("deployment_id")
      assert_equal empty.fetch("next_cursor"), empty.fetch("high_water_mark")
      store.commit([], activity: [event(store, event_id: "event-first", run_id: "run-1")])

      resumed = store.read_activity(after: empty.fetch("next_cursor"))
      assert_equal [Activity::STREAM_STARTED, "work.admitted"], resumed.fetch("events").map { |committed| committed.fetch("type") }
    end
  end

  def test_reading_activity_never_writes
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      store.commit([["runs", { "id" => "run-1" }]], activity: [event(store, event_id: "event-first", run_id: "run-1")])
      # Including with an interrupted writer's bytes still on the end: repair is an append's job.
      File.binwrite(path, File.binread(path) + '{"transaction_version":1,"events":[')
      bytes = File.binread(path)

      store.read_activity
      store.read_activity(filters: { run_id: "run-1" }, limit: 1)
      store.fetch_activity("event-first")
      store.fetch_activity("event-absent")

      assert_equal bytes, File.binread(path)
      assert_nil store.fetch_activity("event-absent")
      assert_equal "run-1", store.fetch_activity("event-first").fetch("run_id")
    end
  end

  def test_legacy_records_and_activity_free_transactions_still_load
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      File.write(path, [
        JSON.generate(collection: "jobs", record: { id: "legacy-job" }),
        JSON.generate(transaction_version: 1, events: [{ collection: "jobs", record: { id: "pre-activity-job" } }])
      ].join("\n") + "\n")
      store = build_store(directory)

      assert_equal %w[legacy-job pre-activity-job], store.list("jobs").map { |record| record["id"] }
      assert_empty store.read_activity.fetch("events")

      store.commit([["jobs", { "id" => "new-job" }]], activity: [event(store, job_id: "new-job")])

      assert_equal %w[legacy-job pre-activity-job new-job], store.list("jobs").map { |record| record["id"] }
      assert_equal [1, 2], store.read_activity.fetch("events").map { |committed| committed.fetch("sequence") },
                   "history starts where it starts; nothing is invented for records that predate it"
    end
  end

  def test_a_forged_sequence_in_the_log_is_never_silently_accepted
    in_tmpdir do |directory|
      path = File.join(directory, "state.jsonl")
      store = build_store(directory)
      store.commit([], activity: [event(store, event_id: "event-first")])
      forged = JSON.parse(File.readlines(path).last)
      assert_equal [1, 2], forged.fetch("activity").map { |committed| committed.fetch("sequence") }
      forged["activity"].last["sequence"] = 1

      File.write(path, File.readlines(path)[0..-2].join + JSON.generate(forged) + "\n")

      assert_raises(Backstage::ContractError) { store.read_activity }
      assert_raises(Backstage::ContractError) { store.commit([], activity: [event(store, event_id: "event-next")]) }
    end
  end
end
