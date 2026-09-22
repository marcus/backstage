# frozen_string_literal: true

require "minitest/autorun"
require "tmpdir"
require "stringio"
require "yaml"
require "fileutils"
require_relative "../lib/backstage"

module BackstageTestHelpers
  PACK = File.expand_path("../packs/example", __dir__)

  def in_tmpdir
    Dir.mktmpdir("backstage-test") { |directory| yield directory }
  end

  def build_engine(directory, secrets: [], clock: nil, capture_options: {})
    guard = Backstage::SecretGuard.new(secret_values: secrets)
    Backstage::Engine.new(
      store: Backstage::JsonlStore.new(File.join(directory, "state.jsonl"), secret_guard: guard),
      artifact_store: Backstage::ArtifactStore.new(File.join(directory, "artifacts"), secret_guard: guard),
      secret_guard: guard,
      clock: clock,
      capture_options: capture_options
    )
  end

  def pack
    @pack ||= Backstage::Configuration.new(PACK)
  end

  def example_broker(env = {})
    Backstage::CredentialBroker.new(env: env, mapping: {
      "github" => { "source_env" => "GITHUB_TOKEN", "runtime_env" => "GH_TOKEN" },
      "openrouter" => { "source_env" => "OPENROUTER_API_KEY", "runtime_env" => "OPENROUTER_API_KEY" }
    })
  end

  def workflow(name)
    pack.workflow(name)
  end

  def build_workflows(engine)
    Backstage::Application::WorkflowService.new(store: engine.store)
  end

  # Admits work under a named workflow without going through a source adapter.
  def submit_work(engine, workflow_name: "independent-review", key: "test-work", **overrides)
    engine.submit(
      idempotency_key: key,
      title: overrides.delete(:title) || "Test work",
      description: overrides.delete(:description) || "",
      workflow: workflow(workflow_name),
      **overrides
    )
  end

  # Ad-hoc packs built in tests still need a workflow to admit work with.
  def write_minimal_workflow(directory, name: "minimal")
    FileUtils.mkdir_p(File.join(directory, "workflows"))
    FileUtils.cp(File.join(PACK, "workflows", "#{name}.yml"), File.join(directory, "workflows", "#{name}.yml"))
  end

  # Takes a dispatching transition and returns the queued job the engine will run.
  def dispatch(engine, work, transition: "start", request_id: "dispatch-1", actor: nil)
    build_workflows(engine).request_transition(
      work_item_id: work.fetch("id"), transition: transition,
      actor: actor || operator("system"), request_id: request_id
    ).fetch("job")
  end

  def operator(role = "human")
    { "role" => role, "id" => "tester", "entry" => "operator_cli" }
  end

  def write_artifact(engine, work_item_id:, kind:, run_id: "run-fixture", content: "fixture", provenance: {})
    artifact = engine.artifact_store.write(
      work_item_id: work_item_id,
      run_id: run_id,
      name: "#{kind}.txt",
      content: content,
      kind: kind,
      provenance: provenance
    )
    engine.store.save("artifacts", artifact)
  end
end

class Minitest::Test
  include BackstageTestHelpers
end
