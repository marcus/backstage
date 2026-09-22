# frozen_string_literal: true

require_relative "test_helper"

class BootstrapSystemTest < Minitest::Test
  include BackstageTestHelpers

  def test_system_composes_namespaced_adapters_behind_legacy_api
    in_tmpdir do |directory|
      system = Backstage::Bootstrap::System.build(
        state: File.join(directory, "state.jsonl"),
        artifacts: File.join(directory, "artifacts"),
        env: { "BACKSTAGE_TOKEN" => "secret" }
      )

      assert_instance_of Backstage::Application::Engine, system.engine
      assert_instance_of Backstage::Adapters::Environment::CredentialBroker, system.credential_broker
      assert_equal Backstage::Application::Engine, Backstage::Engine
      assert_equal Backstage::Surfaces::CLI, Backstage::CLI
      assert_equal Backstage::Adapters::Td::Client, Backstage::TdClient

      assert_match(/\Adeployment-[0-9a-f]{12}\z/, system.deployment_id)
      assert_equal system.deployment_id, system.engine.store.deployment_id, "one composed system is one activity stream"
    end
  end

  def test_legacy_provenance_identifier_survives_namespaced_fake_runner
    in_tmpdir do |directory|
      engine = Backstage::Bootstrap::System.build(
        state: File.join(directory, "state.jsonl"),
        artifacts: File.join(directory, "artifacts"),
        env: {}
      ).engine
      work = submit_work(engine, key: "bootstrap-provenance", title: "Provenance")

      result = engine.execute(dispatch(engine, work), runner: Backstage::FakeRunner.new)

      assert_equal "Backstage::FakeRunner", result.fetch("artifact").dig("provenance", "adapter")
    end
  end

  def test_system_owns_publish_runtime_construction
    in_tmpdir do |directory|
      observed_guard = nil
      runtime = Object.new
      system = Backstage::Bootstrap::System.build(
        state: File.join(directory, "state.jsonl"),
        artifacts: File.join(directory, "artifacts"),
        env: {},
        runtime_factory: ->(secret_guard:) { observed_guard = secret_guard; runtime }
      )

      assert_same runtime, system.publish_runtime
      assert_same system.secret_guard, observed_guard
      assert_instance_of Backstage::Application::WorkflowService, system.workflows
      assert_instance_of Backstage::Application::Recovery, system.recovery
      assert_instance_of Backstage::Application::ActivityQuery, system.activity_query
      assert_same system.activity_query, system.activity_query, "composed once, like every other application-layer accessor here"
    end
  end

  def test_application_and_surface_layers_do_not_name_concrete_adapters
    root = File.expand_path("../lib/backstage", __dir__)
    files = Dir[File.join(root, "application", "**", "*.rb")] + Dir[File.join(root, "surfaces", "**", "*.rb")]

    files.each do |path|
      refute_includes File.read(path), "Backstage::Adapters", path
    end
  end
end
