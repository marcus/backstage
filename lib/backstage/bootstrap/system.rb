# frozen_string_literal: true

module Backstage
  module Bootstrap
    # The composition root shared by local surfaces and future process hosts.
    # It assembles concrete adapters but keeps application behavior in Engine.
    class System
      attr_reader :engine, :credential_broker, :secret_guard

      # `pack` supplies capture bounds and credential nicknames. Everything else about a pack reaches
      # the system per command. A pack that cannot be read here falls back to empty settings rather
      # than refusing to build a system, because `config check` is the command that reports a broken
      # pack and it needs a system to run in.
      def self.build(state:, artifacts:, env: ENV, runtime_factory: nil, pack: nil, capture_options: nil)
        secret_guard = Backstage::Support::SecretGuard.new(secret_values: env.select { |key, _| key.match?(Backstage::Support::SecretGuard::SECRET_KEY) }.values)
        pack_capture, credential_mapping = pack_runtime(pack)
        new(
          engine: Backstage::Application::Engine.new(
            store: Backstage::Adapters::Jsonl::Store.new(state, secret_guard: secret_guard),
            artifact_store: Backstage::Adapters::LocalFiles::ArtifactStore.new(artifacts, secret_guard: secret_guard),
            secret_guard: secret_guard,
            clock: Backstage::Adapters::Environment::SystemClock.new,
            capture_options: capture_options || pack_capture
          ),
          credential_broker: Backstage::Adapters::Environment::CredentialBroker.new(env: env, mapping: credential_mapping),
          secret_guard: secret_guard,
          runtime_factory: runtime_factory || ->(secret_guard:) { Backstage::Adapters::Docker::Runtime.new(secret_guard: secret_guard) },
          state_path: state
        )
      end

      def self.pack_runtime(pack)
        return [{}, {}] if pack.to_s.empty?

        compiled = Backstage::Configuration::DeploymentPack.new(pack)
        [compiled.capture_policy, compiled.credential_mapping]
      rescue Backstage::Error, SystemCallError
        [{}, {}]
      end

      def initialize(engine:, credential_broker:, secret_guard:, runtime_factory:, state_path: nil)
        @engine = engine
        @credential_broker = credential_broker
        @secret_guard = secret_guard
        @runtime_factory = runtime_factory
        @state_path = state_path
      end

      def workflows
        @workflows ||= Backstage::Application::WorkflowService.new(store: engine.store)
      end

      # The read-only activity projection. The CLI and any future UI/API compose this same class
      # over the same store rather than each re-deriving cursor/paging behavior of their own.
      def activity_query
        @activity_query ||= Backstage::Application::ActivityQuery.new(store: engine.store)
      end

      # The identity every activity event and cursor in this deployment is bound to. Surfaces ask
      # the composed system rather than reaching into the store, so a later store swap is invisible
      # to them. Asking mints the identity if this deployment has never recorded anything.
      def deployment_id
        engine.store.deployment_id
      end

      def recovery
        @recovery ||= Backstage::Application::Recovery.new(engine: engine, workflows: workflows, presence: presence)
      end

      def presence
        @presence ||= Backstage::Adapters::Docker::Presence.new
      end

      def clock
        @clock ||= Backstage::Adapters::Environment::SystemClock.new
      end

      # Ownership lives beside the state log so one store has exactly one local dispatcher. The path
      # comes from what this system was composed with, not from asking the store where it keeps its
      # bytes — a store that is not a file still gets a lock.
      def dispatch_ownership(path = nil)
        @dispatch_ownership ||= Backstage::Adapters::LocalFiles::DispatchOwnership.new(
          path || "#{@state_path || engine.store.path}.dispatcher.lock"
        )
      end

      # The durable dispatcher. `authorized_mode` is the authority the calling process was started
      # with; an intent that was accepted for real execution is never consumed by a fake dispatcher,
      # and a real dispatcher still runs a fake intent as fake.
      def dispatcher(configuration:, authorized_mode: "fake", workspace_root: ".backstage/workspaces")
        Backstage::Application::Dispatcher.new(
          engine: engine,
          workflows: workflows,
          recovery: recovery,
          clock: clock,
          authorized_mode: authorized_mode,
          default_retry_policy: dispatcher_retry_policy(configuration),
          controller_factory: lambda do |mode|
            controller(
              configuration: configuration,
              runtime: mode == "publish_draft" ? publish_runtime : nil,
              workspace_root: workspace_root
            )
          end
        )
      end

      def worker(configuration:, authorized_mode: "fake", interval: Backstage::Application::Worker::DEFAULT_INTERVAL, workspace_root: ".backstage/workspaces")
        Backstage::Application::Worker.new(
          dispatcher: dispatcher(configuration: configuration, authorized_mode: authorized_mode, workspace_root: workspace_root),
          ownership: dispatch_ownership,
          clock: clock,
          interval: interval
        )
      end

      def configuration(pack_path)
        Backstage::Configuration::DeploymentPack.new(pack_path)
      end

      def td_source(target:, target_name:, workflow:)
        client = Backstage::Adapters::Td::Client.new(workspace: target.dig("trigger", "td_workspace"))
        [
          Backstage::Adapters::Td::Trigger.new(client: client, store: engine.store, source_instance: target.dig("trigger", "source_instance")),
          Backstage::Adapters::Td::WorkSource.new(
            client: client,
            store: engine.store,
            engine: engine,
            source_instance: target.dig("trigger", "source_instance"),
            target_name: target_name,
            source_identity: target.dig("trigger", "td_workspace"),
            workflow: workflow
          )
        ]
      end

      def publish_runtime
        @runtime_factory.call(secret_guard: secret_guard)
      end

      def controller(configuration:, runtime: nil, workspace_root: ".backstage/workspaces")
        runner_factory, mode = if runtime
                                 [publication_runner_factory(runtime, workspace_root), "publish_draft"]
                               else
                                 [fake_runner_factory(workspace_root), "fake"]
                               end
        Backstage::Application::Controller.new(
          engine: engine,
          workflows: workflows,
          configuration: configuration,
          runner_factory: runner_factory,
          mode: mode,
          channel_factory: channel_factory(workspace_root)
        )
      end

      private

      # The pack's retry default, where the pack knows one. Anything else composing this system gets
      # the domain default rather than a surface's idea of one.
      def dispatcher_retry_policy(configuration)
        return Backstage::Domain::RetryPolicy::DEFAULT unless configuration.respond_to?(:dispatcher_policy)

        configuration.dispatcher_policy.fetch("retry")
      end

      def channel_factory(workspace_root)
        lambda do |bundle, phase|
          Backstage::Adapters::LocalFiles::AgentRequestChannel.new(
            File.join(File.expand_path(workspace_root), "#{bundle.fetch("id")}-#{phase}", "requests"),
            workflow_service: workflows,
            store: engine.store
          )
        end
      end

      # The fake journey produces a real candidate patch, a real reviewer verdict and now a real
      # captured stream, so evidence, live agent visibility and output capture are all exercised
      # without a paid model call or a container.
      def fake_runner_factory(workspace_root)
        lambda do |phase, bundle, _work|
          next Backstage::Adapters::Fake::Runner.new(outcome: fake_review, script: fake_script("review")) if phase == "review"

          Backstage::Adapters::Fake::Runner.new(
            outcome: fake_implementation,
            script: fake_script(phase),
            change_root: File.join(File.expand_path(workspace_root), "#{bundle&.fetch("id") || "fake"}-#{phase}"),
            branch: bundle&.dig("repository", "branch"),
            request_channel_path: bundle&.dig("agent_request_channel", "path")
          )
        end
      end

      # Two records, so the fake journey drives real framing, redaction, chunking and commits rather
      # than a stand-in for them. The offsets are scripted seconds, not elapsed ones: `Fake::Runtime`
      # advances a clock it was given, and this factory gives it none — a composed system runs on
      # the real `SystemClock`, which the fake journey must not be allowed to move. So the two
      # records arrive microseconds apart and close in a single chunk. The 250 ms flush boundary is
      # proven where a clock can be injected without lying about the time, in runtime_capture_test.
      def fake_script(phase)
        [[0.0, "fake #{phase} worker started\n"], [0.3, "fake #{phase} worker finished\n"]]
      end

      def publication_runner_factory(runtime, workspace_root)
        lambda do |phase, bundle, work|
          harness = Backstage::Adapters::Pi::Harness.new(runtime: runtime, credential_broker: credential_broker)
          if phase == "review"
            Backstage::Application::Runners::ContainerPhaseRunner.new(
              phase: phase,
              runtime: runtime,
              harness: harness,
              credential_broker: credential_broker,
              workspace_root: workspace_root,
              review: true
            )
          else
            repository = bundle.fetch("repository")
            authority = Backstage::Domain::RepositoryAuthority.new(
              designated_repository: repository.fetch("designated_repository"),
              default_branch: repository.fetch("default_branch")
            )
            review_change = Backstage::Adapters::GitHub::ReviewChange.new(store: engine.store, authority: authority)
            Backstage::Application::Runners::ContainerPhaseRunner.new(
              phase: phase,
              runtime: runtime,
              harness: harness,
              credential_broker: credential_broker,
              workspace_root: workspace_root,
              review_change: review_change,
              work_item_id: work.fetch("id")
            )
          end
        end
      end

      def fake_implementation
        {
          "schema_version" => 2,
          "status" => "succeeded",
          "summary" => "fake implementation completed",
          "assistant_text" => "Implemented fixture change",
          "process" => { "exit_code" => 0, "signal" => nil },
          "usage" => { "input_tokens" => 0, "output_tokens" => 0, "cost" => nil, "cost_trusted" => false }
        }
      end

      def fake_review
        {
          "schema_version" => 2,
          "status" => "succeeded",
          "summary" => "fake independent review approved",
          "process" => { "exit_code" => 0, "signal" => nil },
          "review" => { "verdict" => "approved", "summary" => "fixture contract is clean", "independent" => true, "reviewer_session_id" => "fake-reviewer" },
          "usage" => { "input_tokens" => 0, "output_tokens" => 0, "cost" => nil, "cost_trusted" => false }
        }
      end
    end
  end
end
