# frozen_string_literal: true

require_relative "test_helper"

class TdAdaptersTest < Minitest::Test
  class FakeRunner
    attr_reader :calls

    def initialize(outputs)
      @outputs = outputs
      @calls = []
    end

    def run(argv, chdir:)
      @calls << { argv: argv, chdir: chdir }
      Backstage::CommandResult.new(JSON.generate(@outputs.shift), "", 0)
    end
  end

  class FakeClient
    attr_accessor :issue
    attr_reader :handoff_calls, :review_calls

    def initialize(issue)
      @issue = issue
      @handoff_calls = []
      @review_calls = []
    end

    def ready_issues = [issue]
    def approval_candidates = [issue]
    def show(_) = issue

    def handoff(_id, **payload)
      @handoff_calls << payload
      @issue = issue.merge("handoff" => { "done" => payload[:done], "remaining" => payload[:remaining], "decisions" => payload[:decisions] })
      { "action" => "handoff_recorded" }
    end

    def review(_id, reason:)
      @review_calls << reason
      @issue = issue.merge("status" => "in_review")
      { "action" => "review_requested", "status" => "in_review" }
    end
  end

  def issue
    {
      "id" => "td-ABC123",
      "title" => "Authorized work",
      "description" => "Do it",
      "acceptance" => "It works",
      "status" => "open",
      "labels" => ["agent-ready"],
      "review_history" => []
    }
  end

  def test_td_client_uses_exact_unbounded_readiness_and_approval_queries
    runner = FakeRunner.new([[], []])
    client = Backstage::TdClient.new(workspace: "/tmp/workspace", runner: runner)

    client.ready_issues
    client.approval_candidates

    assert_equal ["td", "query", "status = open AND labels = agent-ready", "--output", "json", "--limit", "0"], runner.calls[0][:argv]
    assert_equal ["td", "query", "(status = in_review OR status = closed) AND has(reviewer)", "--output", "json", "--limit", "0"], runner.calls[1][:argv]
  end

  def test_manual_and_poll_authorization_share_key_and_deduplicate
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      client = FakeClient.new(issue)
      trigger = Backstage::TdTrigger.new(client: client, store: store, source_instance: "widgets-example")

      manual = trigger.manual("td-abc123")
      polled = trigger.poll

      assert_equal "trigger:v1:work:v1:td:widgets-example:td-abc123:authorized", manual["id"]
      assert_empty polled
      assert_equal 1, store.list("triggers").length
    end
  end

  def test_latest_unsuperseded_approval_reenters_once
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      approved = issue.merge(
        "status" => "closed",
        "labels" => [],
        "review_history" => [
          { "id" => "rv-old", "decision" => "approved", "created_at" => "2026-01-01", "superseded" => true },
          { "id" => "rv-current", "decision" => "approved", "created_at" => "2026-01-02", "superseded" => false, "reviewer_session" => "reviewer" }
        ]
      )
      trigger = Backstage::TdTrigger.new(client: FakeClient.new(approved), store: store, source_instance: "widgets-example")

      first = trigger.poll
      second = trigger.poll

      assert_equal 1, first.length
      assert_equal "trigger:v1:work:v1:td:widgets-example:td-abc123:approval:rv-current", first.first["id"]
      assert_empty second
    end
  end

  def test_work_reconciliation_and_writebacks_are_idempotent_with_preflight_and_ledger
    in_tmpdir do |directory|
      engine = build_engine(directory)
      client = FakeClient.new(issue)
      source = Backstage::TdWorkSource.new(client: client, store: engine.store, engine: engine, source_instance: "widgets-example", target_name: "tasks", source_identity: "/tmp/tasks", workflow: workflow("independent-review"))
      work = source.reconcile("td-abc123")
      assert_equal work["id"], source.reconcile("td-abc123")["id"]

      handoff_args = { work_item: work, done: "implemented", remaining: "review", decisions: ["bounded authority"] }
      assert_equal source.post_handoff(**handoff_args), source.post_handoff(**handoff_args)
      assert_equal 1, client.handoff_calls.length

      assert_equal source.request_review(work_item: work, reason: "ready"), source.request_review(work_item: work, reason: "ready")
      assert_equal 1, client.review_calls.length
      assert_equal 2, engine.store.list("external_actions").length
    end
  end
end
