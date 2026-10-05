# frozen_string_literal: true

require_relative "test_helper"

class TdAdaptersTest < Minitest::Test
  class FakeRunner
    attr_reader :calls
    def initialize(outputs)
      @outputs, @calls = outputs, []
    end
    def run(argv, chdir:)
      @calls << { argv: argv, chdir: chdir }
      Backstage::CommandResult.new(JSON.generate(@outputs.shift), "", 0)
    end
  end

  class FakeClient
    attr_accessor :issue, :selected
    attr_reader :handoff_calls, :review_calls, :show_calls
    def initialize(issue)
      @issue, @selected = issue, [issue]
      @handoff_calls, @review_calls, @show_calls = [], [], []
    end
    def ready_issues = selected
    def show(ref)
      @show_calls << ref
      issue
    end
    def handoff(ref, **payload)
      @handoff_calls << [ref, payload]
      @issue = issue.merge("handoff" => payload.transform_keys(&:to_s))
      { "action" => "handoff_recorded" }
    end
    def review(ref, reason:)
      @review_calls << [ref, reason]
      @issue = issue.merge("status" => "in_review")
      { "action" => "review_requested" }
    end
  end

  def issue
    { "id" => "td-ABC123", "title" => "Native work", "description" => "Do it",
      "acceptance" => "It works", "status" => "open", "labels" => ["agent-ready"],
      "updated_at" => "native-version", "native_extension" => { "links" => [1, 2] },
      "review_history" => [{ "decision" => "approved", "reviewer_session" => "source-person" }] }
  end

  def result
    { "summary" => "Draft created", "candidate" => { "sha256" => "a" * 64 },
      "review" => { "verdict" => "approved", "summary" => "No findings" } }
  end

  def test_client_queries_native_ready_candidates_only_and_transports_explicit_refs
    runner = FakeRunner.new([[], issue])
    client = Backstage::Adapters::Td::Client.new(workspace: "/tmp/workspace", runner: runner)
    client.ready_issues
    client.show("td-native")
    assert_equal ["td", "query", "status = open AND labels = agent-ready", "--output", "json", "--limit", "0"], runner.calls[0][:argv]
    assert_equal ["td", "show", "td-native", "--json"], runner.calls[1][:argv]
    refute_respond_to client, :approval_candidates
  end

  def test_handoff_sends_worker_summary_through_literal_note_without_host_input_expansion
    ["@/host/private/document", "-"].each do |summary|
      runner = FakeRunner.new([{ "action" => "handoff_recorded" }])
      client = Backstage::Adapters::Td::Client.new(workspace: "/tmp/workspace", runner: runner)
      source = Backstage::Adapters::Td::Source.new(client: client)
      payload = source.prepare(operation: "record_result", ref: "td-abc123",
                               result: result.merge("summary" => summary), operation_id: "host-operation")
      client.handoff("td-abc123", done: payload.fetch("done"), remaining: payload.fetch("remaining"), decisions: payload.fetch("decisions"))
      argv = runner.calls.fetch(0).fetch(:argv)
      assert_equal ["td", "handoff", "td-abc123", "--note", summary, "--remaining", payload.fetch("remaining")], argv.first(7)
      refute_includes argv, "--done"
      assert_equal payload.fetch("decisions"), argv.each_cons(2).filter_map { |flag, value| value if flag == "--decision" }
      assert payload.fetch("remaining").start_with?("Human disposition")
      assert payload.fetch("decisions").all? { |value| !value.start_with?("@") && value != "-" }
    end
  end

  def test_snapshot_preserves_every_native_field_and_only_adapter_canonicalizes_td_case
    client = FakeClient.new(issue)
    source = Backstage::Adapters::Td::Source.new(client: client)
    snapshot = source.snapshot("TD-AbC123")
    assert_equal "td-abc123", snapshot["ref"]
    assert_equal ["td-abc123"], client.show_calls
    assert_equal issue, JSON.parse(snapshot["content"])
    assert_equal "application/json", snapshot["media_type"]
    assert_equal "native-version", snapshot["version"]
    assert_raises(Backstage::ContractError) { source.snapshot("--unsafe") }
  end

  def test_discovery_repeats_ready_refs_without_persisting_admission_or_source_approvals
    client = FakeClient.new(issue)
    client.selected += [issue.merge("id" => "td-other", "status" => "closed"),
                        issue.merge("id" => "td-third", "labels" => [])]
    source = Backstage::Adapters::Td::Source.new(client: client)
    2.times { assert_equal ["td-abc123"], source.discover }
    assert_empty client.show_calls
    assert_equal %w[record_result request_review], source.capabilities
  end

  def test_handoff_payload_is_native_and_reconciles_by_operation_marker
    client = FakeClient.new(issue)
    source = Backstage::Adapters::Td::Source.new(client: client)
    args = { operation: "record_result", ref: "TD-AbC123", operation_id: "host-operation" }
    payload = source.prepare(**args, result: result)
    assert_equal "Draft created", payload["done"]
    assert_includes payload["decisions"], "Backstage operation: host-operation"
    assert_equal "not_applied", source.reconcile(**args, payload: payload, attempted: false)["status"]
    assert_equal "unknown", source.reconcile(**args, payload: payload, attempted: true)["status"]
    execution = source.execute(**args, payload: payload)
    assert_equal "applied", execution["status"]
    assert_equal "issue_and_td_hierarchy", execution.dig("receipt", "native_scope")
    assert_equal "applied", source.reconcile(**args, payload: payload, attempted: true)["status"]
    assert_equal 1, client.handoff_calls.length
    # Native systems can replace a current handoff. No duplicate-free claim after it disappears.
    client.issue = issue
    assert_equal "unknown", source.reconcile(**args, payload: payload, attempted: true)["status"]
  end

  def test_review_status_is_source_specific_and_missing_attempted_effect_stays_ambiguous
    client = FakeClient.new(issue)
    source = Backstage::Adapters::Td::Source.new(client: client)
    args = { operation: "request_review", ref: "td-abc123", operation_id: "host-operation" }
    payload = source.prepare(**args, result: result)
    assert_equal "not_applied", source.reconcile(**args, payload: payload, attempted: false)["status"]
    assert_equal "unknown", source.reconcile(**args, payload: payload, attempted: true)["status"]
    assert_equal "applied", source.execute(**args, payload: payload)["status"]
    client.issue = issue.merge("status" => "closed")
    assert_equal "applied", source.reconcile(**args, payload: payload, attempted: true)["status"]
    assert_raises(Backstage::ContractError) { source.prepare(**args.merge(operation: "approve"), result: result) }
  end

  def test_fake_source_uses_case_sensitive_plain_documents_and_durable_idempotent_receipts
    in_tmpdir do |directory|
      path = File.join(directory, "native.json")
      jobs = [{ "key" => "Case/Item", "display" => "Custom label", "body" => "# Native\n\nDo this.\n",
                "format" => "text/markdown", "revision" => "v1", "selected" => true }]
      File.write(path, JSON.generate("jobs" => jobs))
      source = Backstage::Adapters::Fake::WorkSource.new(path: path)
      assert_equal ["Case/Item"], source.discover
      assert_equal jobs[0]["body"], source.snapshot("Case/Item")["content"]
      assert_raises(Backstage::NotFound) { source.snapshot("case/item") }
      assert_equal %w[record_result mark_ready], source.capabilities
      args = { operation: "mark_ready", ref: "Case/Item", operation_id: "host-operation" }
      payload = source.prepare(**args, result: result)
      assert_equal "ready_for_human", payload["event"]
      assert_equal "not_applied", source.reconcile(**args, payload: payload, attempted: true)["status"]
      2.times { assert_equal "applied", source.execute(**args, payload: payload)["status"] }
      restarted = Backstage::Adapters::Fake::WorkSource.new(path: path)
      assert_equal "applied", restarted.reconcile(**args, payload: payload, attempted: true)["status"]
      assert_equal 1, JSON.parse(File.read("#{path}.receipts.json")).length
      assert_equal jobs, JSON.parse(File.read(path))["jobs"]
      assert_raises(Backstage::ContractError) { source.execute(**args, payload: payload.merge("result" => {})) }
    end
  end
end
