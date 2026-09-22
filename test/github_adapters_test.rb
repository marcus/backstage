# frozen_string_literal: true

require_relative "test_helper"

class GitHubAdaptersTest < Minitest::Test
  class FakeRunner
    attr_reader :calls

    def initialize(results = [])
      @results = results
      @calls = []
    end

    def run(argv, env: {}, chdir: nil, **)
      @calls << { argv: argv, env: env, chdir: chdir }
      @results.shift || Backstage::CommandResult.new("", "", 0)
    end
  end

  def authority
    Backstage::RepositoryAuthority.new(designated_repository: "example/widgets", default_branch: "main")
  end

  def broker
    Backstage::CredentialBroker.new(env: { "TASKS_TOKEN" => "secret-repo-token" }, mapping: { "github" => { "source_env" => "TASKS_TOKEN", "runtime_env" => "GH_TOKEN" } })
  end

  def publication(overrides = {})
    {
      "url" => "https://github.com/example/widgets/pull/7",
      "number" => 7,
      "repository" => "example/widgets",
      "branch" => "backstage/td-123",
      "base" => "main",
      "draft" => true,
      "reconciled" => false
    }.merge(overrides)
  end

  def test_default_branch_non_designated_repository_and_host_mutation_are_rejected_before_commands
    runner = FakeRunner.new
    adapter = Backstage::GitHubRepository.new(authority: authority, credential_broker: broker, runner: runner)

    assert_raises(Backstage::AuthorityError) { adapter.push(workspace: "/tmp/nope", branch: "main", credential_ref: "github") }
    assert_raises(Backstage::AuthorityError) { adapter.clone(origin: "https://github.com/other/repo.git", revision: "main", destination: "/tmp/nope", credential_ref: "github") }
    assert_raises(Backstage::AuthorityError) { adapter.commit(workspace: "/agent-controlled-checkout", message: "unsafe") }
    assert_raises(Backstage::AuthorityError) { adapter.push(workspace: "/agent-controlled-checkout", branch: "backstage/work", credential_ref: "github") }
    assert_empty runner.calls
  end

  def test_read_only_clone_injects_credential_via_environment_not_argv
    in_tmpdir do |directory|
      destination = File.join(directory, "repo")
      results = [
        Backstage::CommandResult.new("", "", 0),
        Backstage::CommandResult.new("", "", 0),
        Backstage::CommandResult.new("", "", 0),
        Backstage::CommandResult.new("abc123\n", "", 0),
        Backstage::CommandResult.new("", "", 0)
      ]
      runner = FakeRunner.new(results)
      adapter = Backstage::GitHubRepository.new(authority: authority, credential_broker: broker, runner: runner)

      adapter.clone(origin: "https://github.com/example/widgets.git", revision: "main", destination: destination, credential_ref: "github")

      serialized_argv = runner.calls.flat_map { |call| call[:argv] }.join(" ")
      refute_includes serialized_argv, "secret-repo-token"
      assert runner.calls.any? { |call| call[:env].values.any? { |value| value.include?(Base64.strict_encode64("x-access-token:secret-repo-token")) } }
      assert_equal ["git", "-C", destination, "checkout", "--detach", "abc123"], runner.calls.last[:argv]
    end
  end

  def test_container_publication_response_and_ledger_make_retry_idempotent
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: Backstage::SecretGuard.new(secret_values: ["secret-repo-token"]))
      adapter = Backstage::GitHubReviewChange.new(store: store, authority: authority)
      args = { work_item_id: "work-1", repository: "example/widgets", branch: "backstage/td-123", base: "main", idempotency_key: "pr:v1:work-1" }

      first_action = adapter.begin_publication(**args)
      first = adapter.complete_publication(**args, response: publication)
      second_action = adapter.begin_publication(**args)
      second = adapter.complete_publication(**args, response: publication("reconciled" => true))

      assert_equal first_action["id"], second_action["id"]
      assert_equal false, first["reconciled"]
      assert_equal true, second["reconciled"]
      assert_equal 1, store.list("external_actions").length
      assert_equal "succeeded", store.list("external_actions").first["status"]
      refute_includes File.read(store.path), "secret-repo-token"
    end
  end

  def test_draft_pr_rejects_repository_and_base_mismatch_before_preflight
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      adapter = Backstage::GitHubReviewChange.new(store: store, authority: authority)
      common = { work_item_id: "work", branch: "backstage/work", idempotency_key: "pr:work" }

      assert_raises(Backstage::AuthorityError) { adapter.begin_publication(**common, repository: "other/repo", base: "main") }
      assert_raises(Backstage::AuthorityError) { adapter.begin_publication(**common, repository: "example/widgets", base: "release") }
      assert_empty store.list("external_actions")
    end
  end

  def test_preflight_result_rejects_release_base_when_main_was_requested
    in_tmpdir do |directory|
      store = Backstage::JsonlStore.new(File.join(directory, "state.jsonl"))
      adapter = Backstage::GitHubReviewChange.new(store: store, authority: authority)
      args = { work_item_id: "work-1", repository: "example/widgets", branch: "backstage/td-123", base: "main", idempotency_key: "pr:v1:work-1" }
      adapter.begin_publication(**args)

      error = assert_raises(Backstage::AuthorityError) do
        adapter.complete_publication(**args, response: publication("base" => "release", "reconciled" => true))
      end

      assert_includes error.message, "base branch"
      assert_equal "pending", store.list("external_actions").first["status"]
    end
  end

  def test_preflight_result_rejects_returned_head_repository_and_draft_mismatches
    in_tmpdir do |directory|
      mismatches = [
        publication("branch" => "backstage/other"),
        publication("repository" => "other/tasks"),
        publication("draft" => false)
      ]
      mismatches.each_with_index do |response, index|
        store = Backstage::JsonlStore.new(File.join(directory, "state-#{index}.jsonl"))
        adapter = Backstage::GitHubReviewChange.new(store: store, authority: authority)
        args = { work_item_id: "work-#{index}", repository: "example/widgets", branch: "backstage/td-123", base: "main", idempotency_key: "pr:v1:work-#{index}" }
        adapter.begin_publication(**args)
        assert_raises(Backstage::AuthorityError) { adapter.complete_publication(**args, response: response) }
        assert_equal "pending", store.list("external_actions").first["status"]
      end
    end
  end
end
