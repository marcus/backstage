# frozen_string_literal: true

require_relative "test_helper"

class CLITest < Minitest::Test
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

  def with_cli
    in_tmpdir do |directory|
      base = ["--state", File.join(directory, "state.jsonl"), "--artifacts", File.join(directory, "artifacts"), "--pack", PACK]
      yield base, directory
    end
  end

  def test_a_journey_is_inspectable_in_json_and_jsonl
    with_cli do |base|
      work = json(base + ["--json", "submit", "--target", "widgets", "--title", "Steel thread", "--idempotency-key", "thread-1"])
      assert_equal "ready", work.fetch("state")

      code, out, err = run_cli(base + ["--jsonl", "list"])
      assert_equal 0, code, err
      assert_equal work.fetch("id"), JSON.parse(out).fetch("id")

      code, out, = run_cli(base + ["list"])
      assert_equal 0, code
      assert_includes out, "\tready\t", "human output shows workflow position, not execution status"
    end
  end

  def test_every_command_documents_itself_without_positional_arguments
    Backstage::CLI::COMMANDS.each do |command|
      code, out, err = run_cli([command, "--help"])
      assert_equal 0, code, "#{command}: #{err}"
      assert_includes out, "Usage:"
    end
    payload = json(["help", "--json"])
    assert_equal Backstage::CLI::COMMANDS, payload.fetch("commands")
    assert_includes payload.fetch("commands"), "transition"
  end

  def test_workflows_are_discoverable
    with_cli do |base|
      listed = json(base + ["--json", "workflows"])
      assert_equal %w[human-gated-change independent-review minimal], listed.map { |row| row.fetch("name") }.sort

      shown = json(base + ["--json", "workflows", "minimal"])
      assert_equal "new", shown.fetch("initial_state")
      assert_equal %w[start finish reset], shown.fetch("transitions").keys
    end
  end

  def test_submission_selects_a_workflow_and_transitions_are_listed_for_it
    with_cli do |base|
      work = json(base + ["--json", "submit", "--title", "Small", "--idempotency-key", "small-1", "--workflow", "minimal"])
      assert_equal "minimal", work.dig("workflow", "name")
      assert_equal "new", work.fetch("state")

      allowed = json(base + ["--json", "transitions", work.fetch("id")])
      assert_equal %w[start], allowed.map { |row| row.fetch("name") }
      assert_equal "implementation", allowed.first.fetch("dispatches")
      assert_equal true, allowed.first.fetch("available")
    end
  end

  def test_an_operator_transition_is_idempotent_and_appears_in_history
    with_cli do |base|
      work = json(base + ["--json", "submit", "--title", "Small", "--idempotency-key", "small-1", "--workflow", "minimal"])
      id = work.fetch("id")

      first = json(base + ["--json", "transition", id, "--to", "start", "--actor", "system", "--request-id", "op-1", "--reason", "starting"])
      second = json(base + ["--json", "transition", id, "--to", "start", "--actor", "system", "--request-id", "op-1", "--reason", "starting"])

      assert_equal false, first.fetch("deduplicated")
      assert_equal true, second.fetch("deduplicated")
      assert_equal "in_progress", second.dig("work_item", "state")

      history = json(base + ["--json", "history", id])
      assert_equal 1, history.length
      assert_equal "operator_cli", history.first.dig("actor", "entry")
      assert_equal "starting", history.first.fetch("reason")
    end
  end

  def test_a_stale_operator_expectation_fails_with_a_structured_error
    with_cli do |base|
      work = json(base + ["--json", "submit", "--title", "Small", "--idempotency-key", "small-1", "--workflow", "minimal"])
      id = work.fetch("id")
      json(base + ["--json", "transition", id, "--to", "start", "--actor", "system", "--request-id", "op-1"])

      code, _out, err = run_cli(base + ["--json", "transition", id, "--to", "finish", "--actor", "system", "--request-id", "op-2", "--expect-revision", "0"])

      assert_equal 1, code
      payload = JSON.parse(err)
      assert_equal "Backstage::ConflictError", payload.dig("error", "type")
      assert_match(/revision 1, not 0/, payload.dig("error", "message"))
    end
  end

  def test_an_operator_cannot_claim_to_be_an_agent
    with_cli do |base|
      work = json(base + ["--json", "submit", "--title", "Small", "--idempotency-key", "small-1", "--workflow", "minimal"])
      code, _out, err = run_cli(base + ["--json", "transition", work.fetch("id"), "--to", "start", "--actor", "agent", "--request-id", "op-1"])

      assert_equal 1, code
      assert_equal "Backstage::AuthorityError", JSON.parse(err).dig("error", "type")
    end
  end

  def test_decide_answers_the_open_decision_by_name
    with_cli do |base, directory|
      work = json(base + ["--json", "submit", "--target", "widgets", "--title", "Gated", "--idempotency-key", "gated-1", "--workflow", "human-gated-change"])
      id = work.fetch("id")
      json(base + ["--json", "transition", id, "--to", "start", "--actor", "system", "--request-id", "op-1"])

      engine = Backstage::Engine.new(
        store: Backstage::JsonlStore.new(File.join(directory, "state.jsonl")),
        artifact_store: Backstage::ArtifactStore.new(File.join(directory, "artifacts"))
      )
      patch = write_artifact(engine, work_item_id: id, kind: "binary_patch")
      build_workflows(engine).request_transition(
        work_item_id: id, transition: "request_approval",
        actor: operator("system"),
        request_id: "ask-1", evidence: [patch.fetch("id")]
      )

      shown = json(base + ["--json", "show", id])
      decision = shown.fetch("decisions").last
      assert_equal "open", decision.fetch("status")

      answered = json(base + ["--json", "decide", id, "--choose", "approve", "--reason", "reviewed by hand"])
      assert_equal "completed", answered.dig("work_item", "state")
      assert_equal "human", answered.dig("transition", "actor", "role")

      repeat = json(base + ["--json", "decide", id, "--choose", "approve", "--decision", decision.fetch("id"), "--reason", "reviewed by hand"])
      assert_equal true, repeat.fetch("deduplicated")
    end
  end

  # The full local journey through the shipped pack, as an operator would run it.
  def test_the_fake_journey_shows_agent_progress_and_a_completed_assignment
    with_cli do |base|
      work = json(base + ["--json", "submit", "--target", "widgets", "--title", "Journey", "--idempotency-key", "journey-1"])
      result = json(base + ["--json", "process", work.fetch("id")])
      assert_equal "completed", result.fetch("state")

      shown = json(base + ["--json", "show", work.fetch("id")])
      request = shown.fetch("agent_requests").find { |row| row.fetch("status") == "applied" }
      assert_equal "report_progress", request.fetch("transition")
      assert_equal "agent", request.dig("actor", "role")
      assert_equal "running", request.fetch("resulting_state")
      assert_equal %w[start report_progress submit_for_review approve], shown.fetch("transitions").map { |row| row.fetch("transition") }
      assert_empty json(base + ["--json", "transitions", work.fetch("id")]).reject { |row| row.fetch("name") == "reactivate" },
                   "a completed assignment offers only explicit human reactivation"
    end
  end

  def test_recover_reports_structured_findings
    with_cli do |base|
      work = json(base + ["--json", "submit", "--target", "widgets", "--title", "Recover", "--idempotency-key", "rec-1"])
      json(base + ["--json", "transition", work.fetch("id"), "--to", "start", "--actor", "system", "--request-id", "op-1"])

      report = json(base + ["--json", "recover"])
      finding = report.fetch("work_items").first.fetch("findings").first

      assert_equal "dispatch_pending", finding.fetch("kind")
      assert_equal report.fetch("work_items").first.fetch("work_item_id"), work.fetch("id")
    end
  end

  def test_cancel_records_the_request_against_a_run
    with_cli do |base, directory|
      engine = build_engine(directory)
      work = submit_work(engine, key: "cancel-cli")
      run = engine.store.save("runs", Backstage::Domain::Records.run(job_id: "job-x", phase: "implementation", work_item_id: work.fetch("id")))
      base = ["--state", File.join(directory, "state.jsonl"), "--artifacts", File.join(directory, "artifacts"), "--pack", PACK]

      assert json(base + ["--json", "cancel", run.fetch("id")]).fetch("cancellation_requested_at")
    end
  end
end
