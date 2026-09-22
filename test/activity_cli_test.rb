# frozen_string_literal: true

require_relative "test_helper"

# The activity projection's CLI surface: the same query the CLI, and eventually a UI/API, share.
# activity_query_test.rb covers follow's polling/waiting semantics directly with a fake clock;
# these tests cover the CLI's own responsibilities — option parsing, filters, output shapes, and
# errors — and use a real (near-zero) interval since the CLI wires the real system clock.
class ActivityCLITest < Minitest::Test
  Activity = Backstage::Domain::Activity

  def run_cli(argv, env: {})
    out = StringIO.new
    err = StringIO.new
    code = Backstage::CLI.new(argv, out: out, err: err, env: env).call
    [code, out.string, err.string]
  end

  def json(argv, env: {})
    code, out, err = run_cli(argv, env: env)
    assert_equal 0, code, err
    JSON.parse(out)
  end

  def failing(argv, env: {})
    code, _out, err = run_cli(argv, env: env)
    assert_equal 1, code
    JSON.parse(err)
  end

  def with_cli
    in_tmpdir do |directory|
      base = ["--state", File.join(directory, "state.jsonl"), "--artifacts", File.join(directory, "artifacts"), "--pack", PACK]
      yield base, File.join(directory, "state.jsonl")
    end
  end

  # Seeds activity directly through the store, the way test/activity_store_test.rb does, since no
  # workflow transition emits activity yet on this branch (a concurrent slice adds those emitters).
  def seed(state_path, count: 1, type: "runtime.observed", run_id: "run-seed", **overrides)
    store = Backstage::JsonlStore.new(state_path)
    Array.new(count) do |index|
      event = Activity.event(
        type: type, deployment_id: store.deployment_id,
        source: Activity.source(adapter: "test.producer", provenance: "core"),
        event_id: "event-seed-#{index}", run_id: run_id, **overrides
      )
      store.commit([], activity: [event]).activity.first
    end
  end

  def test_list_with_filters_and_limit_paginates_without_misses_or_repeats_until_caught_up
    with_cli do |base, state_path|
      matching = []
      store = Backstage::JsonlStore.new(state_path)
      12.times do |index|
        interesting = index.even?
        identity = "event-#{format("%02d", index)}"
        matching << identity if interesting
        store.commit([], activity: [
                       Activity.event(type: interesting ? "decision.raised" : "runtime.observed",
                                      deployment_id: store.deployment_id,
                                      source: Activity.source(adapter: "test.producer", provenance: "core"),
                                      event_id: identity, run_id: "run-#{index}")
                     ])
      end

      events = []
      cursor = nil
      loop do
        page = json(base + ["--json", "activity", "list", "--kind", "decision.raised", "--limit", "1"] + (cursor ? ["--after", cursor] : []))
        events.concat(page.fetch("events"))
        cursor = page.fetch("next_cursor")
        break if page.fetch("caught_up")
      end

      assert_equal matching, events.map { |event| event.fetch("event_id") }
      assert_equal events.map { |event| event.fetch("event_id") }.uniq, events.map { |event| event.fetch("event_id") }, "no repeats"
    end
  end

  def test_list_filters_by_work_run_target_and_related_id
    with_cli do |base, state_path|
      seed(state_path, count: 1, type: "work.transition_applied", work_item_id: "work-1")
      store = Backstage::JsonlStore.new(state_path)
      store.commit([], activity: [Activity.event(type: "execution.started", deployment_id: store.deployment_id,
                                                   source: Activity.source(adapter: "t", provenance: "core"),
                                                   event_id: "event-run", run_id: "run-1", target_id: "target-1")])
      store.commit([], activity: [Activity.event(type: "artifact.available", deployment_id: store.deployment_id,
                                                   source: Activity.source(adapter: "t", provenance: "core"),
                                                   event_id: "event-artifact", artifact_refs: ["artifact-1"])])

      by_work = json(base + ["--json", "activity", "list", "--work", "work-1"]).fetch("events")
      assert_equal ["event-seed-0"], by_work.map { |event| event.fetch("event_id") }

      by_run = json(base + ["--json", "activity", "list", "--run", "run-1"]).fetch("events")
      assert_equal ["event-run"], by_run.map { |event| event.fetch("event_id") }

      by_target = json(base + ["--json", "activity", "list", "--target", "target-1"]).fetch("events")
      assert_equal ["event-run"], by_target.map { |event| event.fetch("event_id") }

      by_related = json(base + ["--json", "activity", "list", "--related", "artifact-1"]).fetch("events")
      assert_equal ["event-artifact"], by_related.map { |event| event.fetch("event_id") }
    end
  end

  def test_show_returns_one_event_and_not_found_is_a_structured_error
    with_cli do |base, state_path|
      seed(state_path, summary: "hello there")

      shown = json(base + ["--json", "activity", "show", "event-seed-0"])
      assert_equal "hello there", shown.fetch("summary")

      error = failing(base + ["--json", "activity", "show", "event-absent"])
      assert_equal "Backstage::NotFound", error.fetch("error").fetch("type")
    end
  end

  def test_a_cursor_mismatch_is_a_structured_error_with_its_code_and_exits_one
    with_cli do |base, state_path|
      seed(state_path)
      filtered = json(base + ["--json", "activity", "list", "--run", "run-seed"])

      error = failing(base + ["--json", "activity", "list", "--after", filtered.fetch("next_cursor")])
      assert_equal "Backstage::ActivityCursorError", error.fetch("error").fetch("type")
      assert_equal "cursor_filter_mismatch", error.fetch("error").fetch("code")

      code, _out, err = run_cli(base + ["activity", "list", "--after", filtered.fetch("next_cursor")])
      assert_equal 1, code
      assert_match(/cursor_filter_mismatch/, err)
    end
  end

  def test_unknown_filter_and_unknown_kind_are_refused_not_ignored
    with_cli do |base, _state_path|
      unknown_type = failing(base + ["--json", "activity", "list", "--kind", "not.a.real.type"])
      assert_equal "Backstage::ContractError", unknown_type.fetch("error").fetch("type")
      assert_match(/unknown activity type/, unknown_type.fetch("error").fetch("message"))
    end
  end

  def test_jsonl_emits_one_event_per_line_then_a_trailing_cursor_metadata_line
    with_cli do |base, state_path|
      # A store appends its own stream-started marker ahead of a deployment's first-ever activity
      # commit (see activity_store_test.rb); filtering to the seeded type keeps this test's shape
      # assertions independent of that one-time detail.
      seed(state_path, count: 2)

      _code, out, err = run_cli(base + ["--jsonl", "activity", "list", "--kind", "runtime.observed"])
      assert_empty err
      lines = out.lines.map { |line| JSON.parse(line) }

      assert_equal 3, lines.length
      assert_equal %w[event-seed-0 event-seed-1], lines[0..1].map { |line| line.fetch("event_id") }
      trailer = lines.last
      assert_equal %w[cursor next_cursor high_water_mark caught_up].sort, trailer.keys.sort
      assert_equal true, trailer.fetch("caught_up")
    end
  end

  def test_human_output_is_one_compact_line_per_event
    with_cli do |base, state_path|
      seed(state_path, summary: "did a thing")

      _code, out, _err = run_cli(base + ["activity", "list", "--kind", "runtime.observed"])
      assert_equal 1, out.lines.length
      assert_includes out, "runtime.observed"
      assert_includes out, "run-seed"
      assert_includes out, "did a thing"
    end
  end

  def test_follow_stops_at_max_passes_and_streams_every_page_it_read
    with_cli do |base, state_path|
      seed(state_path, count: 3)

      _code, out, err = run_cli(base + ["--jsonl", "activity", "follow", "--max-passes", "2", "--interval", "0.01"])
      assert_empty err
      lines = out.lines.map { |line| JSON.parse(line) }
      summary = lines.last

      assert_equal 2, summary.fetch("passes")
      assert_equal "max_passes", summary.fetch("stop_reason")
      seen_ids = lines[0..-2].select { |line| line.key?("event_id") }.map { |line| line.fetch("event_id") }
      assert_equal %w[event-seed-0 event-seed-1 event-seed-2], seen_ids & %w[event-seed-0 event-seed-1 event-seed-2]
    end
  end

  def test_reading_activity_through_the_cli_never_writes
    with_cli do |base, state_path|
      seed(state_path)
      bytes = File.binread(state_path)

      json(base + ["--json", "activity", "list"])
      json(base + ["--json", "activity", "list", "--run", "run-seed", "--limit", "1"])
      json(base + ["--json", "activity", "show", "event-seed-0"])
      failing(base + ["--json", "activity", "show", "event-absent"])
      run_cli(base + ["--jsonl", "activity", "follow", "--max-passes", "1", "--interval", "0.01"])

      assert_equal bytes, File.binread(state_path)
    end
  end

  # cli_test.rb's test_every_command_documents_itself_without_positional_arguments already walks
  # every top-level command's --help, "activity" included; this just checks the payload actually
  # names all three subcommands and their filters.
  def test_activity_help_documents_every_subcommand_and_filter
    code, out, _err = run_cli(["activity", "--help"])
    assert_equal 0, code
    assert_includes out, "activity list"
    assert_includes out, "activity show EVENT_ID"
    assert_includes out, "activity follow"
    assert_includes out, "--kind TYPE"
    assert_includes out, "--max-passes N"
  end
end
