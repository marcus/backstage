# frozen_string_literal: true

require_relative "test_helper"

class JourneyTest < Minitest::Test
  class OutcomeRunner
    def initialize(outcome)
      @outcome = outcome
    end

    def run(**) = @outcome
  end

  class PublicationRuntime
    attr_reader :calls

    def initialize(review_verdicts:, reconciled:)
      @review_verdicts = Array(review_verdicts)
      @reconciled = reconciled
      @calls = []
    end

    def run(bundle:, secrets:, cancellation:, capture: nil)
      @calls << { bundle: bundle, secrets: secrets, cancellation: cancellation }
      command = bundle.dig("execution", "command")
      if command[0] == "backstage-container-repository" && command[1] == "finalize"
        File.binwrite(File.join(bundle.dig("execution", "mounts", 0, "source"), "change.patch"), "")
      end
      logs = case command.first
             when "backstage-container-repository"
               repository_logs(command.fetch(1), bundle)
             when "pi"
               pi_logs(bundle.dig("policy", "role") == "independent_review")
             else
               raise "unexpected container command #{command.inspect}"
             end
      # Output reaches the runner and the harness through the capture stream, exactly as a
      # container's would, so this journey exercises the real framing and interpretation path.
      capture&.write(logs)
      capture&.close(reason: "close")
      { "schema_version" => 1, "status" => "succeeded", "summary" => "container succeeded", "process" => { "exit_code" => 0, "signal" => nil }, "cancellation" => { "requested" => false, "timed_out" => false }, "logs" => logs }
    end

    private

    def repository_logs(action, bundle)
      branch = bundle.dig("repository", "branch")
      case action
      when "prepare"
        JSON.generate("type" => "repository_prepared", "resolved_revision" => "abc", "branch" => branch, "reused_remote_branch" => @reconciled) << "\n"
      when "context"
        %({"type":"context_materialized","kind":"repo","name":"td","origin":"https://github.com/example/tracker.git","resolved_revision":"def","mount":"refs/td","read_only":true}\n)
      when "finalize"
        JSON.generate("type" => "repository_materialized", "branch" => branch, "base_revision" => "abc", "patch_sha256" => "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "patch_size" => 0) << "\n"
      when "publish"
        JSON.generate("type" => "repository_published", "url" => "https://github.com/example/widgets/pull/9", "number" => 9, "repository" => "example/widgets", "branch" => branch, "base" => "main", "draft" => true, "reconciled" => @reconciled) << "\n"
      else
        raise "unexpected repository action #{action.inspect}"
      end
    end

    def pi_logs(review)
      events = File.readlines(File.expand_path("fixtures/pi_success.jsonl", __dir__), chomp: true).map { |line| JSON.parse(line) }
      if review
        verdict = @review_verdicts.shift or raise "reviewer ran more times than the test scripted"
        message = events.find { |event| event["type"] == "message_end" }.dig("message", "content", 0)
        message["text"] = JSON.generate("verdict" => verdict, "summary" => "review #{verdict}")
      end
      events.map { |event| JSON.generate(event) }.join("\n") << "\n"
    end
  end

  class FakeTdClient
    attr_reader :handoff_calls, :review_calls

    def initialize
      @issue = { "id" => "td-retry", "status" => "open", "review_history" => [] }
      @handoff_calls = []
      @review_calls = []
    end

    def workspace = "/tmp/tasks"
    def show(_) = @issue

    def handoff(_id, **payload)
      @handoff_calls << payload
      @issue = @issue.merge("handoff" => { "done" => payload[:done], "remaining" => payload[:remaining], "decisions" => payload[:decisions] })
      { "action" => "handoff_recorded" }
    end

    def review(_id, reason:)
      @review_calls << reason
      @issue = @issue.merge("status" => "in_review")
      { "action" => "review_requested", "status" => "in_review" }
    end
  end

  def test_fake_real_cli_journey_compiles_runs_reviews_and_persists_artifacts
    in_tmpdir do |directory|
      state = File.join(directory, "state.jsonl")
      artifacts = File.join(directory, "artifacts")
      submit_out = StringIO.new
      submit = Backstage::CLI.new(
        ["--state", state, "--artifacts", artifacts, "--json", "submit", "--pack", File.expand_path("../packs/example", __dir__), "--target", "widgets", "--title", "Steel thread", "--description", "Prove the journey", "--source", "td", "--source-ref", "td-proof", "--idempotency-key", "work:v1:td:widgets-example:td-proof"],
        out: submit_out, err: StringIO.new, env: {}
      )
      assert_equal 0, submit.call
      work = JSON.parse(submit_out.string)

      process_out = StringIO.new
      process_cli = Backstage::CLI.new(
        ["--state", state, "--artifacts", artifacts, "process", work.fetch("id"), "--pack", File.expand_path("../packs/example", __dir__), "--json"],
        out: process_out, err: StringIO.new, env: {}
      )
      assert_equal 0, process_cli.call
      result = JSON.parse(process_out.string)

      assert_equal "fake", result["mode"]
      assert_equal true, result["handoff_allowed"]
      assert_equal "approved", result["verdict"]
      assert_equal "completed", result["state"]
      store = Backstage::JsonlStore.new(state)
      assert_equal %w[implementation review], store.list("runs").map { |run| run["phase"] }
      assert_equal "completed", store.fetch("work_items", work["id"])["state"]
      kinds = store.list("artifacts").map { |artifact| artifact["kind"] }
      assert_includes kinds, "binary_patch", "the fake journey produces a real candidate"
      assert_includes kinds, "review_verdict"
      history = store.list("work_transitions").sort_by { |row| row["revision"] }
      assert_equal %w[start report_progress submit_for_review approve], history.map { |row| row["transition"] }
      assert_equal %w[system agent agent reviewer], history.map { |row| row.dig("actor", "role") }
      assert_equal 1, store.list("agent_requests").count { |row| row["status"] == "applied" },
                   "a worker request is visible while the run is still going"
    end
  end

  def test_publish_draft_is_never_the_default
    help = StringIO.new
    assert_equal 0, Backstage::CLI.new(["help", "--json"], out: help, err: StringIO.new, env: {}).call
    assert_includes JSON.parse(help.string)["commands"], "process"
    refute_includes JSON.parse(help.string)["usage"]["process"], "--publish-draft WORK_ID"
  end

  def test_returned_findings_revise_the_same_published_change_and_reuse_its_draft_pr
    in_tmpdir do |directory|
      engine = build_engine(directory)
      config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
      work = submit_work(engine, key: "retry-published", title: "Retry", source: "td", source_ref: { "issue_id" => "td-retry", "source_instance" => "widgets-example" }, **config.binding_for("widgets"))
      broker = example_broker( { "GITHUB_TOKEN" => "secret", "OPENROUTER_API_KEY" => "model-secret" })
      system = Backstage::Bootstrap::System.new(
        engine: engine,
        credential_broker: broker,
        secret_guard: Backstage::SecretGuard.new(secret_values: ["secret", "model-secret"]),
        runtime_factory: ->(secret_guard:) { raise "unexpected runtime-factory call with #{secret_guard.class}" }
      )
      runtime = PublicationRuntime.new(review_verdicts: %w[changes_requested approved], reconciled: true)
      host_command_calls = []
      trace = TracePoint.new(:call) do |event|
        host_command_calls << event.method_id if event.defined_class == Backstage::CommandRunner && event.method_id == :run
      end
      result = trace.enable do
        system.controller(configuration: config, runtime: runtime, workspace_root: File.join(directory, "workspaces")).process(work_item_id: work["id"])
      end

      assert_equal true, result["handoff_allowed"]
      assert_equal "completed", engine.show_work(work["id"])["state"]
      assert_equal 1, engine.show_work(work["id"])["revisions_used"], "the revision continued the same assignment"
      steps = result.fetch("steps").filter_map { |step| step.dig("transition", "transition") }
      assert_equal %w[start submit_for_review request_changes revise submit_for_review approve], steps

      urls = engine.store.list("runs").filter_map { |run| run.dig("outcome", "review_change", "url") }.uniq
      assert_equal 1, urls.length, "the revision reuses the draft it already published"
      revised_bundle = runtime.calls.select { |call| call[:bundle].dig("execution", "command", 1) == "prepare" }.last[:bundle]
      assert_equal revised_bundle.dig("repository", "branch"), revised_bundle.dig("repository", "revision")
      assert_equal true, revised_bundle.dig("repository", "resume_existing_change")
      assert runtime.calls.select { |call| call[:bundle].dig("execution", "command", 1) == "finalize" }.all? { |call| call[:secrets].empty? }
      assert runtime.calls.select { |call| call[:bundle].dig("execution", "command", 1) == "publish" }.all? { |call| call[:secrets].keys == ["GH_TOKEN"] }
      assert_empty host_command_calls
      assert_equal 1, engine.store.list("external_actions").count { |action| action["kind"] == "github_draft_pr" }
      assert_equal 2, engine.store.list("artifacts").count { |artifact| artifact["kind"] == "binary_patch" }
      assert_equal 2, engine.store.list("runs").count { |run| run["phase"] == "review" }

      td_client = FakeTdClient.new
      td_source = Backstage::TdWorkSource.new(client: td_client, store: engine.store, engine: engine, source_instance: "widgets-example", target_name: "tasks", workflow: workflow("independent-review"))
      handoff = result.fetch("handoff")
      2.times do
        td_source.post_handoff(work_item: engine.show_work(work["id"]), done: handoff.fetch("done"), remaining: handoff.fetch("remaining"), decisions: handoff.fetch("decisions"))
        td_source.request_review(work_item: engine.show_work(work["id"]), reason: "ready")
      end
      assert_equal 1, td_client.handoff_calls.length
      assert_equal 1, td_client.review_calls.length
    end
  end

end
