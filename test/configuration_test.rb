# frozen_string_literal: true

require_relative "test_helper"

class ConfigurationTest < Minitest::Test
  def test_reference_pack_checks_routes_and_compiles_secret_free_bundle
    config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
    work = bound_work("id" => "td-ABC123", "title" => "Do the work", "description" => "Keep it narrow", "source" => "td", "source_ref" => { "issue_id" => "td-abc123", "source_instance" => "widgets-example" })

    assert_equal "widgets", config.route("/projects/widgets")
    bundle = config.compile(work_item: work)

    assert_equal "widgets", bundle["target"]
    assert_equal "https://github.com/example/widgets.git", bundle.dig("repository", "url")
    assert_equal "backstage/td-abc123", bundle.dig("repository", "branch")
    assert_equal "z-ai/glm-5.3-flash", bundle.dig("harness", "model")
    assert_equal ["github", "openrouter"], bundle.dig("execution", "credential_refs").sort
    assert_equal "GITHUB_TOKEN", config.credential_mapping.dig("github", "source_env")
    assert_equal "GH_TOKEN", config.credential_mapping.dig("github", "runtime_env")
    assert_equal true, bundle.dig("context_grants", 0, "read_only")
    refute_includes JSON.generate(bundle), "OPENROUTER_API_KEY="
  end

  def test_one_pack_routes_multiple_targets_deterministically
    in_tmpdir do |directory|
      FileUtils.mkdir_p(File.join(directory, "targets"))
      write_minimal_workflow(directory)
      File.write(File.join(directory, "backstage.yml"), <<~YAML)
        adapters:
          state_store: { kind: jsonl }
          artifact_store: { kind: local }
          worker_runtime: { kind: docker, image: image }
          harness: { kind: pi }
        harness_defaults: { provider: openrouter, model: model }
      YAML
      %w[alpha beta].each do |name|
        File.write(File.join(directory, "targets", "#{name}.yml"), <<~YAML)
          repo: { origin: "https://github.com/example/#{name}.git" }
          trigger: { td_workspace: "#{directory}/#{name}", source_instance: "#{name}-source" }
          authority: { review_change: draft_pr }
        YAML
      end

      config = Backstage::Configuration.new(directory)
      assert_equal "alpha", config.route(File.join(directory, "alpha"))
      assert_equal "beta", config.route(File.join(directory, "beta"))
    end
  end

  def test_bundle_is_persisted_as_an_audit_artifact_with_grants_on_run
    in_tmpdir do |directory|
      config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
      engine = build_engine(directory)
      work = submit_work(engine, key: "config-run", title: "Compile", description: "Audit", source: "td", source_ref: { "issue_id" => "td-x", "source_instance" => "widgets-example" }, **config.binding_for("widgets"))
      bundle = config.compile(work_item: work)

      engine.execute(dispatch(engine, work), runner: Backstage::FakeRunner.new, bundle: bundle)
      shown = engine.show_work(work["id"])
      bundle_artifact = shown["artifacts"].find { |artifact| artifact["kind"] == "job_bundle" }
      persisted = JSON.parse(File.read(bundle_artifact.fetch("path")))
      run = engine.store.list("runs").first

      assert_equal bundle["id"], persisted["id"]
      assert_equal bundle["context_grants"], run["context_grants"]
    end
  end

  def test_compile_rejects_cross_target_source_rebinding
    config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
    work = bound_work("id" => "td-x", "title" => "Wrong source", "description" => "", "source" => "td", "source_ref" => { "issue_id" => "td-x", "source_instance" => "different-target" })

    assert_raises(Backstage::ContractError) { config.compile(work_item: work) }
    assert_raises(Backstage::ContractError) { config.compile(work_item: work.merge("source_identity" => "/tmp/other")) }
  end

  def test_config_rejects_raw_secret_fields_and_path_unsafe_context
    in_tmpdir do |directory|
      write_pack(directory, context_name: "safe", context_mount: "refs/td", extra_pack: "harness_defaults:\n  provider: openrouter\n  model: model\n  api_key: raw-secret\n")
      assert_raises(Backstage::ContractError) { Backstage::Configuration.new(directory) }

      write_pack(directory, context_name: "../../escape", context_mount: "refs/td")
      assert_raises(Backstage::ContractError) { Backstage::Configuration.new(directory) }

      write_pack(directory, context_name: "safe", context_mount: "../escape")
      assert_raises(Backstage::ContractError) { Backstage::Configuration.new(directory) }
    end
  end

  def test_a_credential_reference_must_be_declared_in_the_broker
    in_tmpdir do |directory|
      write_pack(directory, context_name: "safe", context_mount: "refs/td", extra_pack: "harness_defaults: { provider: openrouter, model: model, credentials: openrouter }\n")
      error = assert_raises(Backstage::ContractError) { Backstage::Configuration.new(directory) }
      assert_includes error.message, "credentials.broker"
    end
  end

  def test_pack_paths_expand_home
    config = Backstage::Configuration.new(File.expand_path("../packs/example", __dir__))
    assert_equal File.expand_path("~/.backstage/state.jsonl"), config.state_path
    assert_equal File.expand_path("~/.backstage/artifacts"), config.artifact_path
  end

  def test_repo_context_materializer_clones_and_records_read_only_mount
    in_tmpdir do |directory|
      calls = []
      adapter = Object.new
      adapter.define_singleton_method(:clone) do |**args|
        calls << args
        FileUtils.mkdir_p(args.fetch(:destination))
        File.write(File.join(args.fetch(:destination), "README.md"), "reference")
      end
      grant = { "kind" => "repo", "name" => "td", "origin" => "https://github.com/example/tracker.git", "revision" => "main", "mount" => "refs/td", "read_only" => true, "credential_ref" => "github" }

      record = Backstage::RepositoryContextMaterializer.new(repository_adapter: adapter, workspace_root: directory).materialize([grant]).first

      assert_equal true, calls.first[:read_only]
      assert_equal true, record["read_only"]
      refute File.writable?(File.join(directory, "refs/td", "README.md"))
      refute record.key?("credential_ref")
    ensure
      FileUtils.chmod_R("u+w", File.join(directory, "refs")) if directory && File.exist?(File.join(directory, "refs"))
    end
  end


  private

  def bound_work(fields)
    fields.merge("target" => "widgets", "source_instance" => "widgets-example", "source_identity" => "/projects/widgets")
  end

  def write_pack(directory, context_name:, context_mount:, extra_pack: nil)
    FileUtils.mkdir_p(File.join(directory, "targets"))
    write_minimal_workflow(directory)
    pack = extra_pack || "harness_defaults: { provider: openrouter, model: model }\n"
    File.write(File.join(directory, "backstage.yml"), <<~YAML)
      adapters:
        state_store: { kind: jsonl, path: ~/.backstage/state.jsonl }
        artifact_store: { kind: local, path: ~/.backstage/artifacts }
        worker_runtime: { kind: docker, image: image }
        harness: { kind: pi }
      #{pack}
    YAML
    File.write(File.join(directory, "targets", "target.yml"), <<~YAML)
      repo: { origin: "https://github.com/example/repo.git" }
      trigger: { td_workspace: "#{directory}/target", source_instance: test-source }
      authority: { review_change: draft_pr }
      context:
        repos:
          - { name: "#{context_name}", origin: "https://github.com/example/context.git", mount: "#{context_mount}" }
    YAML
  end
end
