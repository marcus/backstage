# frozen_string_literal: true

require_relative "test_helper"

# The dispatcher's operator surface. Everything an operator can do here is noninteractive and has a
# structured payload, because the agent using this CLI is the primary user.
class DispatchCLITest < Minitest::Test
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
      work = json(base + ["--json", "submit", "--target", "widgets", "--title", "Durable work", "--idempotency-key", "durable-1", "--workflow", "minimal"])
      yield base, work.fetch("id"), directory
    end
  end

  def test_accept_pass_and_show_carry_the_whole_lifecycle
    with_cli do |base, work_id|
      accepted = json(base + ["--json", "dispatch", "accept", work_id])
      assert_equal "queued", accepted.fetch("status")
      assert_equal "fake", accepted.fetch("mode")
      assert_equal 0, accepted.fetch("retry_policy").fetch("max_retries"), "the pack default is pinned onto the intent"

      repeat = json(base + ["--json", "dispatch", "accept", work_id])
      assert_equal accepted.fetch("id"), repeat.fetch("id"), "acceptance is idempotent"

      report = json(base + ["--json", "dispatch", "pass"])
      assert_equal 1, report.fetch("dispatched")
      assert_equal "completed", report.fetch("intents").first.fetch("status")
      assert_equal Process.pid, report.fetch("owner").fetch("owner_pid")

      described = json(base + ["--json", "dispatch", "show", work_id])
      assert_equal "done", described.fetch("work_state")
      assert_equal "the work item reached a terminal state in its workflow", described.fetch("explanation")
      assert_equal 1, described.fetch("attempts_used")

      assert_empty json(base + ["--json", "dispatch", "list"])
      assert_equal 1, json(base + ["--json", "dispatch", "list", "--all"]).length
    end
  end

  def test_a_human_wait_is_answered_through_the_cli_and_continues_exactly_once
    in_tmpdir do |directory|
      base = ["--state", File.join(directory, "state.jsonl"), "--artifacts", File.join(directory, "artifacts"), "--pack", PACK]
      work_id = json(base + ["--json", "submit", "--target", "widgets", "--title", "Gated work",
        "--idempotency-key", "gated-1", "--workflow", "human-gated-change"]).fetch("id")
      json(base + ["--json", "dispatch", "accept", work_id])

      first = json(base + ["--json", "dispatch", "pass"])
      assert_equal "waiting", first.fetch("intents").first.fetch("status")
      assert_equal "awaiting_approval", json(base + ["--json", "show", work_id]).fetch("state")

      # Restarting the dispatcher repeatedly never answers for the human.
      3.times { assert_equal "waiting", json(base + ["--json", "dispatch", "pass"]).fetch("intents").first.fetch("status") }
      described = json(base + ["--json", "dispatch", "show", work_id])
      assert_equal "a human answer is required before anything else happens", described.fetch("explanation")
      refute_nil described.fetch("open_decision").fetch("question")
      assert_equal 1, described.fetch("attempts_used")

      json(base + ["--json", "decide", work_id, "--choose", "approve", "--reason", "checked by hand"])

      after = json(base + ["--json", "dispatch", "pass"])
      assert_equal "completed", after.fetch("intents").first.fetch("status")
      assert_equal "completed", json(base + ["--json", "show", work_id]).fetch("state")
      assert_equal 1, json(base + ["--json", "dispatch", "show", work_id]).fetch("attempts_used"),
        "answering a decision continued the assignment without a second implementation"
    end
  end

  def test_status_separates_the_current_owner_from_the_last_one
    with_cli do |base, work_id|
      json(base + ["--json", "dispatch", "accept", work_id])

      status = json(base + ["--json", "dispatch", "status"])
      assert_equal 1, status.fetch("counts").fetch("queued")
      assert_equal 1, status.fetch("active")
      assert_equal "fake", status.fetch("authorized_mode")
      refute_nil status.fetch("next_wake_up")
      assert_equal work_id, status.fetch("intents").first.fetch("work_item_id")
      assert_nil status.fetch("owner"), "nothing owns the store before a pass"

      json(base + ["--json", "dispatch", "pass"])
      after = json(base + ["--json", "dispatch", "status"])

      assert_nil after.fetch("owner"), "the pass released the store when it exited"
      assert_equal Process.pid, after.fetch("last_owner").fetch("owner_pid")
      refute_nil after.fetch("last_owner").fetch("released_at")
    end
  end

  def test_uncertain_runtime_is_visible_and_explained_through_the_cli
    with_cli do |base, work_id, directory|
      json(base + ["--json", "dispatch", "accept", work_id])
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      job = Backstage::Application::WorkflowService.new(store: store).request_transition(
        work_item_id: work_id, transition: "start",
        actor: { "role" => "system", "id" => "test", "entry" => "controller" }, request_id: "start-1"
      ).fetch("job")
      store.save("jobs", job.merge("status" => "running", "run_id" => "run-unknown"))
      store.save("runs", Backstage::Domain::Records.run(job_id: job.fetch("id"), work_item_id: work_id, work_revision: job.fetch("work_revision")).merge(
        # A live owner whose container cannot be found: honestly unknown, not provably dead.
        "id" => "run-unknown", "status" => "running", "owner_pid" => Process.pid, "runtime" => { "container_name" => "backstage-unknown" }
      ))

      report = json(base + ["--json", "dispatch", "pass"])

      assert_equal "uncertain", report.fetch("intents").first.fetch("status")
      assert_equal 0, report.fetch("dispatched")
      described = json(base + ["--json", "dispatch", "show", work_id])
      assert_equal "runtime status could not be established; resolve it explicitly", described.fetch("explanation")
      assert_match(/runtime status could not be established/, described.fetch("blocked_reason"))
      assert_includes described.fetch("actions"), "backstage recover #{work_id}"
      assert_equal 1, json(base + ["--json", "dispatch", "status"]).fetch("counts").fetch("uncertain")
    end
  end

  def test_the_documented_acceptance_controls_always_change_something
    with_cli do |base, work_id|
      json(base + ["--json", "dispatch", "accept", work_id])
      # Cancel the acceptance, then take the work back and give it up again: the documented controls
      # must actually change something every time, with no explicit request identity.
      json(base + ["--json", "dispatch", "cancel", work_id])
      reaccepted = json(base + ["--json", "dispatch", "accept", work_id])

      assert_equal 2, reaccepted.fetch("generation"), "re-accepting after a cancellation is not a silent no-op"
      assert_equal false, reaccepted.fetch("deduplicated")
      superseded = json(base + ["--json", "dispatch", "accept", work_id, "--supersede"])
      assert_equal 3, superseded.fetch("generation")
      assert_equal 1, json(base + ["--json", "dispatch", "list"]).length
    end
  end

  def test_run_stops_after_a_bounded_number_of_passes
    with_cli do |base, work_id|
      json(base + ["--json", "dispatch", "accept", work_id])

      result = json(base + ["--json", "dispatch", "run", "--max-passes", "1", "--interval", "0.01"])

      assert_equal 1, result.fetch("passes")
      assert_equal 1, result.fetch("dispatched")
      assert_equal "max_passes", result.fetch("stop_reason")
      assert_equal "done", json(base + ["--json", "show", work_id]).fetch("state")
    end
  end

  def test_direct_processing_is_refused_while_an_intent_owns_the_work
    with_cli do |base, work_id|
      json(base + ["--json", "dispatch", "accept", work_id])

      error = failing(base + ["--json", "process", work_id])
      assert_equal "Backstage::ConflictError", error.fetch("error").fetch("type")
      assert_match(/cancel or supersede/, error.fetch("error").fetch("message"))
      assert_equal "new", json(base + ["--json", "show", work_id]).fetch("state"), "the refusal changed nothing"

      json(base + ["--json", "dispatch", "cancel", work_id, "--reason", "operator took it back"])
      assert_equal "done", json(base + ["--json", "process", work_id]).fetch("state")
    end
  end

  def test_a_fake_dispatcher_refuses_to_consume_work_accepted_for_real_execution
    with_cli do |base, work_id|
      accepted = json(base + ["--json", "dispatch", "accept", work_id, "--publish-draft"])
      assert_equal "publish_draft", accepted.fetch("mode")

      report = json(base + ["--json", "dispatch", "pass"])

      assert_equal 0, report.fetch("dispatched")
      assert_match(/requires publish_draft authorization/, report.fetch("intents").first.fetch("detail"))
      assert_equal "new", json(base + ["--json", "show", work_id]).fetch("state")
    end
  end

  def test_retry_flags_are_pinned_onto_the_acceptance
    with_cli do |base, work_id|
      accepted = json(base + ["--json", "dispatch", "accept", work_id,
        "--max-retries", "2", "--retry-delay", "30", "--retry-backoff", "exponential"])

      policy = accepted.fetch("retry_policy")
      assert_equal({ "max_retries" => 2, "delay_seconds" => 30, "backoff" => "exponential", "max_delay_seconds" => 3600 }, policy)

      conflict = failing(base + ["--json", "dispatch", "accept", work_id, "--max-retries", "5"])
      assert_match(/different request payload/, conflict.fetch("error").fetch("message"))
    end
  end

  def test_cancel_and_supersede_are_the_owning_controls
    with_cli do |base, work_id|
      first = json(base + ["--json", "dispatch", "accept", work_id])

      conflict = failing(base + ["--json", "dispatch", "accept", work_id, "--request-id", "second-try"])
      assert_match(/already accepted/, conflict.fetch("error").fetch("message"))

      second = json(base + ["--json", "dispatch", "accept", work_id, "--request-id", "second-try", "--supersede"])
      assert_equal 2, second.fetch("generation")
      assert_equal 1, json(base + ["--json", "dispatch", "list"]).length

      cancelled = json(base + ["--json", "dispatch", "cancel", second.fetch("id")])
      assert_equal "cancelled", cancelled.fetch("status")
      assert_empty json(base + ["--json", "dispatch", "list"]), "both generations are closed"
      assert_equal "cancelled", json(base + ["--json", "dispatch", "show", first.fetch("id")]).fetch("status")
    end
  end

  def test_human_output_stays_readable_for_every_dispatch_subcommand
    with_cli do |base, work_id|
      run_cli(base + ["dispatch", "accept", work_id])
      code, out, err = run_cli(base + ["dispatch", "status"])
      assert_equal 0, code, err
      refute_empty out

      code, out, err = run_cli(base + ["dispatch", "pass"])
      assert_equal 0, code, err
      refute_empty out
    end
  end

  def test_the_dispatch_command_documents_itself
    code, out, err = run_cli(["dispatch", "--help"])
    assert_equal 0, code, err
    assert_includes out, "dispatch accept"
    assert_includes out, "dispatch run"

    code, _out, err = run_cli(["--json", "dispatch", "nonsense"])
    assert_equal 1, code
    assert_match(/unknown dispatch command/, JSON.parse(err).fetch("error").fetch("message"))
  end

  def test_config_check_reports_the_validated_dispatcher_policy
    policy = json(["--json", "config", "check", "--pack", PACK]).fetch("dispatcher")

    assert_equal 5, policy.fetch("poll_interval_seconds")
    assert_equal 0, policy.fetch("retry").fetch("max_retries")
    assert_equal "fixed", policy.fetch("retry").fetch("backoff")
  end
end
