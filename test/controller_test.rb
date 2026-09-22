# frozen_string_literal: true

require_relative "test_helper"

# Drives the controller against the shipped pack with local runners, so the configured graph, the
# evidence rules, and the dispatch vocabulary are all exercised without a container or a model call.
class ControllerTest < Minitest::Test
  class ScriptedRunner
    attr_reader :calls

    def initialize(outcomes, channel_path: nil, change_root: nil)
      @outcomes = outcomes
      @channel_path = channel_path
      @change_root = change_root
      @calls = []
    end

    def run(bundle:, secrets: {}, cancellation: nil, capture: nil)
      @calls << bundle
      yield({ "type" => "run_started" }) if block_given?
      @outcomes.shift or raise "runner ran more times than the test scripted"
    end

    def adapter_identifier = "Test::ScriptedRunner"
  end

  def build(directory, workflow_name: "independent-review", runner_factory:, key: "controller-work", target: nil)
    engine = build_engine(directory)
    work = submit_work(engine, workflow_name: workflow_name, key: key, target: target)
    controller = Backstage::Application::Controller.new(
      engine: engine,
      workflows: build_workflows(engine),
      configuration: FakeConfiguration.new(work),
      runner_factory: runner_factory,
      mode: "fake",
      channel_factory: lambda do |bundle, phase|
        Backstage::Adapters::LocalFiles::AgentRequestChannel.new(
          File.join(directory, "channels", "#{bundle.fetch("id")}-#{phase}"),
          workflow_service: build_workflows(engine), store: engine.store
        )
      end
    )
    [engine, controller, work]
  end

  # A pack compiler stand-in: the controller only needs a bundle shaped enough to run.
  class FakeConfiguration
    def initialize(work) = @work = work

    def compile(work_item:)
      {
        "schema_version" => 1, "id" => Backstage::Domain::Records.id("bundle"),
        "work_item" => work_item.slice("id", "title", "description", "source"),
        "repository" => { "url" => "https://github.com/example/widgets", "revision" => "main", "default_branch" => "main", "branch" => "backstage/#{work_item.fetch("id")}", "designated_repository" => "example/widgets" },
        "harness" => { "adapter" => "pi", "provider" => "openrouter", "model" => "test", "prompt" => "do the work" },
        "execution" => { "image" => "test", "command" => ["true"], "timeout_seconds" => 1, "credential_refs" => [], "mounts" => [] },
        "policy" => { "name" => "test", "allowed_actions" => ["clone"] },
        "context_grants" => []
      }
    end
  end

  def implementation(directory, summary: "implemented", status: "succeeded")
    outcome = { "schema_version" => 1, "status" => status, "summary" => summary, "process" => { "exit_code" => 0, "signal" => nil } }
    return outcome unless status == "succeeded"

    path = File.join(directory, "patches", "#{summary.gsub(/\W+/, "-")}.patch")
    FileUtils.mkdir_p(File.dirname(path))
    content = "diff for #{summary}\n"
    File.binwrite(path, content)
    outcome.merge("change_artifact" => {
      "source_path" => path, "branch" => "backstage/x", "base_revision" => "abc",
      "patch_sha256" => Digest::SHA256.hexdigest(content), "patch_size" => content.bytesize
    })
  end

  def review(verdict, summary: "review #{verdict}", reviewer: "reviewer-#{verdict}")
    {
      "schema_version" => 1, "status" => "succeeded", "summary" => summary,
      "process" => { "exit_code" => 0, "signal" => nil },
      "review" => { "verdict" => verdict, "summary" => summary, "independent" => true, "reviewer_session_id" => reviewer }
    }
  end

  def states(result) = result.fetch("steps").filter_map { |step| step.dig("transition", "transition") }

  def test_the_default_journey_completes_with_reviewer_provenance
    in_tmpdir do |directory|
      runners = [implementation(directory), review("approved")]
      engine, controller, work = build(directory, runner_factory: ->(_phase, _bundle, _work) { ScriptedRunner.new([runners.shift]) })

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "completed", result.fetch("state")
      assert_equal %w[start submit_for_review approve], states(result)
      assert_equal true, result.fetch("handoff_allowed")
      assert_match(/Independent review approved by reviewer-approved/, result.dig("handoff", "decisions").first)

      history = build_workflows(engine).history(work.fetch("id"))
      assert_equal %w[system agent reviewer], history.map { |row| row.dig("actor", "role") }
      approval = history.last
      candidate = engine.store.fetch("work_items", work.fetch("id")).fetch("candidate")
      verdict_artifact = engine.store.fetch("artifacts", approval.fetch("evidence").first)
      assert_equal candidate.fetch("sha256"), verdict_artifact.dig("provenance", "candidate_sha256")
      assert_equal %w[implementation review], engine.store.list("runs").map { |run| run.fetch("phase") }
    end
  end

  def test_returned_findings_drive_a_revised_candidate_and_a_fresh_review
    in_tmpdir do |directory|
      runners = [
        implementation(directory, summary: "first pass"),
        review("changes_requested", summary: "tighten the error path"),
        implementation(directory, summary: "second pass"),
        review("approved", summary: "now correct")
      ]
      seen_prompts = []
      engine, controller, work = build(directory, runner_factory: lambda do |_phase, bundle, _work|
        seen_prompts << bundle.dig("harness", "prompt")
        ScriptedRunner.new([runners.shift])
      end)

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "completed", result.fetch("state")
      assert_equal %w[start submit_for_review request_changes revise submit_for_review approve], states(result)
      assert_equal 1, engine.store.fetch("work_items", work.fetch("id")).fetch("revisions_used")
      assert_equal 1, engine.store.list("work_items").length, "a revision continues the same assignment"
      assert_match(/tighten the error path/, seen_prompts[2], "the revised bundle carries the findings")
      assert_equal 2, engine.store.list("artifacts").count { |row| row.fetch("kind") == "binary_patch" }
      assert_equal 2, engine.store.list("runs").count { |run| run.fetch("phase") == "review" }
    end
  end

  def test_an_exhausted_revision_budget_stops_visibly_with_the_findings
    in_tmpdir do |directory|
      runners = 3.times.flat_map { |n| [implementation(directory, summary: "pass #{n}"), review("changes_requested", summary: "still wrong #{n}")] }
      engine, controller, work = build(directory, runner_factory: ->(_phase, _bundle, _work) { ScriptedRunner.new([runners.shift]) })

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "changes_requested", result.fetch("state")
      assert_equal 2, engine.store.fetch("work_items", work.fetch("id")).fetch("revisions_used")
      assert_equal false, result.fetch("handoff_allowed")
      assert_equal "changes_requested", result.fetch("verdict")
      assert_equal 3, engine.store.list("runs").count { |run| run.fetch("phase") == "review" }

      blocked = build_workflows(engine).allowed_transitions(work.fetch("id")).find { |row| row.fetch("name") == "revise" }
      assert_equal false, blocked.fetch("available")
    end
  end

  def test_a_blocked_review_waits_for_a_human_and_resumes_only_through_a_recorded_choice
    in_tmpdir do |directory|
      runners = [implementation(directory), review("blocked", summary: "cannot judge this safely")]
      engine, controller, work = build(directory, runner_factory: ->(_phase, _bundle, _work) { ScriptedRunner.new([runners.shift]) })

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "needs_decision", result.fetch("state")
      assert_equal false, result.fetch("handoff_allowed")
      decision = engine.store.list("decisions").last
      assert_equal "open", decision.fetch("status")
      assert_equal %w[resume_implementation cancel], decision.fetch("choices")

      workflows = build_workflows(engine)
      assert_raises(Backstage::InvalidTransition) do
        workflows.request_transition(work_item_id: work.fetch("id"), transition: "approve", actor: operator, request_id: "sneak", decision_id: decision.fetch("id"))
      end

      workflows.request_transition(
        work_item_id: work.fetch("id"), transition: "resume_implementation",
        actor: operator, request_id: "answer", decision_id: decision.fetch("id"), reason: "reviewed by hand"
      )
      completed = engine.show_work(work.fetch("id"))
      assert_equal "running", completed.fetch("state")
      assert_nil controller.handoff_payload(completed)
    end
  end

  def test_the_human_gated_lifecycle_completes_without_any_reviewer
    in_tmpdir do |directory|
      engine, controller, work = build(directory, workflow_name: "human-gated-change",
                                       runner_factory: ->(_phase, _bundle, _work) { ScriptedRunner.new([implementation(directory)]) })

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "awaiting_approval", result.fetch("state")
      assert_equal %w[start request_approval], states(result)
      assert_equal 0, engine.store.list("runs").count { |run| run.fetch("phase") == "review" }
      decision = engine.store.list("decisions").last
      assert_equal "A candidate change is prepared. Approve it as complete, or decline it?", decision.fetch("question")

      build_workflows(engine).request_transition(
        work_item_id: work.fetch("id"), transition: "approve", actor: operator,
        request_id: "approve-1", decision_id: decision.fetch("id")
      )
      assert_equal "completed", engine.store.fetch("work_items", work.fetch("id")).fetch("state")
    end
  end

  def test_the_minimal_lifecycle_needs_no_evidence_or_decisions
    in_tmpdir do |directory|
      engine, controller, work = build(directory, workflow_name: "minimal",
                                       runner_factory: ->(_phase, _bundle, _work) { ScriptedRunner.new([{ "schema_version" => 1, "status" => "succeeded", "summary" => "done", "process" => { "exit_code" => 0, "signal" => nil } }]) })

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "done", result.fetch("state")
      assert_equal %w[start finish], states(result)
      assert_empty engine.store.list("decisions")
      assert_equal 0, engine.store.list("artifacts").count { |row| row.fetch("kind") == "binary_patch" }
    end
  end

  def test_a_failed_run_is_recorded_on_the_run_and_leaves_the_work_eligible
    in_tmpdir do |directory|
      engine, controller, work = build(directory, runner_factory: ->(_phase, _bundle, _work) { ScriptedRunner.new([implementation(directory, status: "failed", summary: "container exploded")]) })

      result = controller.process(work_item_id: work.fetch("id"))

      assert_equal "ready", result.fetch("state")
      assert_equal %w[start execution_failed], states(result)
      assert_equal "failed", engine.store.list("runs").last.fetch("status")
      assert_equal "system", build_workflows(engine).history(work.fetch("id")).last.dig("actor", "role")
      assert_equal false, result.fetch("handoff_allowed")
    end
  end

  def test_a_worker_may_report_progress_mid_run_but_may_not_invent_a_transition
    in_tmpdir do |directory|
      channel_paths = []
      outcomes = [implementation(directory), review("approved")]
      engine, controller, work = build(directory, runner_factory: lambda do |phase, bundle, _work|
        outcome = outcomes.shift
        next ScriptedRunner.new([outcome]) if phase == "review"

        path = bundle.dig("agent_request_channel", "path")
        channel_paths << path
        runner = Object.new
        runner.define_singleton_method(:adapter_identifier) { "Test::ChannelRunner" }
        runner.define_singleton_method(:run) do |bundle:, secrets: {}, cancellation: nil, capture: nil|
          File.open(path, "a") do |file|
            file.puts(JSON.generate("transition" => "report_progress", "reason" => "halfway through"))
            file.puts(JSON.generate("transition" => "approve", "reason" => "I approve my own work"))
            file.puts(JSON.generate("transition" => "report_progress", "actor" => { "role" => "human" }))
            file.puts("not json at all")
          end
          yield({ "type" => "worker_event" }) if block_given?
          outcome
        end
        runner
      end)

      controller.process(work_item_id: work.fetch("id"))
      requests = engine.store.list("agent_requests")

      refute_empty channel_paths.compact
      assert_equal %w[applied rejected rejected rejected], requests.map { |row| row.fetch("status") }
      assert_equal "report_progress", requests.first.fetch("transition")
      assert_equal "agent", requests.first.dig("actor", "role"), "the role comes from the run, not the request"
      assert_match(/no transition "approve" from running/, requests[1].fetch("error"))
      assert_match(/\$\.actor is not allowed/, requests[2].fetch("error"), "a worker cannot even express a role claim")
      assert_match(/was not valid JSON/, requests[3].fetch("error"))
      progress = build_workflows(engine).history(work.fetch("id")).find { |row| row.fetch("transition") == "report_progress" }
      assert_equal "running", progress.fetch("to"), "an informational transition starts nothing"
    end
  end
end
