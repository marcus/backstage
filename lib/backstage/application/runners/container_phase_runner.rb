# frozen_string_literal: true

require "fileutils"
require "digest"
require "json"

module Backstage::Application::Runners
  class ContainerPhaseRunner
    AuthorityError = Backstage::AuthorityError
    ContractError = Backstage::ContractError
    ExternalCommandError = Backstage::ExternalCommandError
    IndependentReviewRunner = Backstage::Application::Runners::IndependentReviewRunner
    CaptureDefaults = Backstage::Application::CaptureDefaults
    Outcome = Backstage::Domain::Outcome
    SentinelInterpreter = Backstage::Application::Runners::SentinelInterpreter
    CHANNEL_MOUNT = "/backstage/requests"
    # The only coverages an authorized action may follow. `gap` and `failed` mean output was
    # produced that nothing acknowledged; finalizing or publishing on top of that would be acting
    # on an unaudited run, which is precisely what this slice exists to stop.
    AUDITED = %w[complete truncated].freeze

    def initialize(phase:, runtime:, harness:, credential_broker:, workspace_root:, review: false, review_change: nil, work_item_id: nil)
      @phase = phase
      @runtime = runtime
      @harness = harness
      @credential_broker = credential_broker
      @workspace_root = File.expand_path(workspace_root)
      @review = review
      @review_change = review_change
      @work_item_id = work_item_id
    end

    # Recovery may infer a prelaunch interruption only if both execution paths provide identity.
    def runtime_identity_before_launch?
      [@runtime, @harness].all? { |adapter| adapter.respond_to?(:runtime_identity_before_launch?) && adapter.runtime_identity_before_launch? }
    end

    # `capture` is a `Ports::RuntimeCapture` — the component. This runner owns which streams a phase
    # has, so it opens one per repository step with a `SentinelInterpreter`, and hands the component
    # itself to the harness, which opens the stream its own protocol needs.
    def run(bundle:, secrets: {}, cancellation: nil, capture: nil, &events)
      capture ||= CaptureDefaults.null(run: bundle.fetch("id"), phase: @phase)
      workspace = File.join(@workspace_root, "#{bundle.fetch("id")}-#{@phase}")
      FileUtils.mkdir_p(workspace)
      resolved_secrets = secrets.empty? ? @credential_broker.runtime_environment(bundle.dig("execution", "credential_refs")) : secrets
      github_secret = { "GH_TOKEN" => resolved_secrets.fetch("GH_TOKEN") }
      prepare = repository_command(bundle, workspace, "prepare", github_secret, cancellation, capture, &events)
      return phase_terminal(prepare, "repository_prepare") if terminal_runtime_status?(prepare)
      raise ExternalCommandError.new("container repository preparation failed", argv: ["backstage-container-repository", "prepare"], status: prepare.dig("process", "exit_code"), stdout: prepare["logs"], stderr: "") unless prepare["status"] == "succeeded"

      materialized = Array(bundle["context_grants"]).each_index.map do |index|
        context = repository_command(bundle, workspace, "context", github_secret, cancellation, capture, extra_args: [index.to_s], &events)
        return phase_terminal(context, "context_materialization", materialized) if terminal_runtime_status?(context)
        raise ExternalCommandError.new("repository context preparation failed", argv: ["backstage-container-repository", "context", index.to_s], status: context.dig("process", "exit_code"), stdout: context["logs"], stderr: "") unless context["status"] == "succeeded"

        context.dig("sentinels", "context_materialized")
      end
      harness_bundle = JSON.parse(JSON.generate(bundle))
      harness_bundle["execution"]["mounts"] = [{ "source" => File.join(workspace, "repo"), "target" => "/workspace", "read_only" => @review }]
      harness_bundle["execution"]["mounts"].concat(materialized.map do |grant|
        { "source" => File.join(workspace, "contexts", grant.fetch("name")), "target" => "/workspace/#{grant.fetch("mount")}", "read_only" => true }
      end)
      attach_request_channel!(harness_bundle, bundle)
      harness_secrets = { "OPENROUTER_API_KEY" => resolved_secrets.fetch("OPENROUTER_API_KEY") }
      outcome = if @review
                  IndependentReviewRunner.new(harness: @harness).run(bundle: harness_bundle, secrets: harness_secrets, cancellation: cancellation, capture: capture) { |event| events.call(event.merge("phase" => @phase, "step" => "harness")) if events }
                else
                  @harness.run(bundle: harness_bundle, secrets: harness_secrets, cancellation: cancellation, capture: capture) { |event| events.call(event.merge("phase" => @phase, "step" => "harness")) if events }
                end
      outcome = outcome.merge("materialized_context_grants" => materialized)
      # A verdict authorizes the next phase, so it is gated exactly as finalize and publish are. An
      # `approved` read out of a run whose output nothing acknowledged is not an independent review
      # of anything — the evidence it claims to have read is the part that went missing.
      if @review && outcome["status"] == "succeeded" && !audited?(capture)
        return unaudited(outcome, "accept the review verdict", capture)
      end
      return outcome unless outcome["status"] == "succeeded" && !@review
      return unaudited(outcome, "finalize", capture) unless audited?(capture)

      finalized = repository_command(bundle, workspace, "finalize", {}, cancellation, capture, &events)
      return phase_terminal(finalized, "repository_finalize", materialized) if terminal_runtime_status?(finalized)
      raise ExternalCommandError.new("container repository finalization failed", argv: ["backstage-container-repository", "finalize"], status: finalized.dig("process", "exit_code"), stdout: finalized["logs"], stderr: "") unless finalized["status"] == "succeeded"

      change_artifact = finalized.dig("sentinels", "repository_materialized")
      raise ContractError, "sealed worker did not materialize a repository change" unless change_artifact
      repository = bundle.fetch("repository")
      validate_materialized_change!(change_artifact, repository, workspace)
      publication_workspace = "#{workspace}-publication"
      raise ContractError, "publication workspace already exists" if File.exist?(publication_workspace)

      FileUtils.mkdir_p(publication_workspace)
      FileUtils.cp(File.join(workspace, "change.patch"), File.join(publication_workspace, "change.patch"))
      File.write(File.join(publication_workspace, "change.json"), JSON.generate(change_artifact))

      raise ContractError, "publication ledger adapter and work item are required" unless @review_change && @work_item_id
      return unaudited(outcome, "publish", capture) unless audited?(capture)

      publication_key = "github-pr:v1:#{bundle.dig("work_item", "id")}"
      @review_change.begin_publication(
        work_item_id: @work_item_id,
        repository: repository.fetch("designated_repository"),
        branch: repository.fetch("branch"),
        base: repository.fetch("default_branch"),
        idempotency_key: publication_key
      )
      published = repository_command(bundle, publication_workspace, "publish", github_secret, cancellation, capture, &events)
      return phase_terminal(published, "repository_publish", materialized) if terminal_runtime_status?(published)
      raise ExternalCommandError.new("container repository publication failed", argv: ["backstage-container-repository", "publish"], status: published.dig("process", "exit_code"), stdout: published["logs"], stderr: "") unless published["status"] == "succeeded"

      publication = published.dig("sentinels", "repository_published")
      raise ContractError, "sealed worker did not return a repository publication" unless publication
      change = @review_change.complete_publication(
        work_item_id: @work_item_id,
        repository: repository.fetch("designated_repository"),
        branch: repository.fetch("branch"),
        base: repository.fetch("default_branch"),
        idempotency_key: publication_key,
        response: publication
      )
      outcome.merge("review_change" => change, "change_artifact" => change_artifact.merge("source_path" => File.join(workspace, "change.patch")), "repository" => { "workspace" => workspace, "prepared" => true }, "capture" => capture_block(capture))
    end

    private

    # The worker gets a writable channel and the container-side path to it — never the host path,
    # and never anything else that would let it reach the authoritative store.
    def attach_request_channel!(harness_bundle, bundle)
      host_path = bundle.dig("agent_request_channel", "path")
      return if host_path.to_s.empty?

      harness_bundle["execution"]["mounts"] << { "source" => File.dirname(host_path), "target" => CHANNEL_MOUNT, "read_only" => false }
      harness_bundle["agent_request_channel"] = { "path" => File.join(CHANNEL_MOUNT, File.basename(host_path)) }
    end

    def terminal_runtime_status?(outcome)
      %w[cancelled timed_out].include?(outcome["status"])
    end

    def phase_terminal(outcome, phase, materialized = [])
      outcome.merge(
        "summary" => "#{phase} #{outcome.fetch("status")}",
        "phase" => phase,
        "materialized_context_grants" => materialized
      )
    end

    def validate_materialized_change!(change, repository, workspace)
      raise AuthorityError, "materialized change branch is outside authority" unless change["branch"] == repository.fetch("branch")
      raise ContractError, "materialized change base is required" if change["base_revision"].to_s.empty?
      size = change["patch_size"]
      raise ContractError, "materialized change size is invalid" unless size.is_a?(Integer) && size.between?(0, 10 * 1024 * 1024)

      patch_path = File.join(workspace, "change.patch")
      raise ContractError, "materialized change patch is missing" unless File.file?(patch_path)
      raise ContractError, "materialized change size mismatch" unless File.size(patch_path) == size
      raise ContractError, "materialized change digest mismatch" unless Digest::SHA256.file(patch_path).hexdigest == change["patch_sha256"]
    end

    # One step, one stream. The sentinels the worker announced come back from that stream's close
    # summary, already parsed, so nothing re-scans a log string looking for them a second time.
    def repository_command(bundle, workspace, action, secrets, cancellation, capture, extra_args: [])
      command_bundle = JSON.parse(JSON.generate(bundle))
      command_bundle["execution"]["command"] = ["backstage-container-repository", action, *extra_args]
      command_bundle["execution"]["credential_refs"] = secrets.keys
      command_bundle["execution"]["mounts"] = [{ "source" => workspace, "target" => "/workspace", "read_only" => false }]
      writer = capture.open(step: action, phase: @phase, interpreter: SentinelInterpreter.new)
      outcome = nil
      begin
        outcome = @runtime.run(bundle: command_bundle, secrets: secrets, cancellation: cancellation, capture: writer) do |event|
          yield(event.merge("phase" => @phase, "step" => action)) if block_given?
        end
      ensure
        summary = close_stream(writer, outcome)
      end
      outcome.merge("sentinels" => (summary && summary["sentinels"]) || {})
    end

    # The runtime closed this stream with the reason it ended for; `close` is idempotent, so this
    # returns that same summary and only does real work when the runtime raised before closing.
    def close_stream(writer, outcome)
      status = outcome && outcome["status"]
      reason = %w[cancelled timed_out].include?(status) ? status : (outcome ? "close" : "failed")
      writer.close(reason: reason)
    rescue Backstage::CaptureError
      nil
    end

    def capture_block(capture)
      Outcome.capture_summary(capture.summaries)
    end

    def audited?(capture)
      AUDITED.include?(capture_block(capture).fetch("status"))
    end

    # Refusing is a result, not an exception: the phase ends with a recorded outcome naming why it
    # stopped, so an operator reads the coverage rather than a backtrace.
    def unaudited(outcome, step, capture)
      block = capture_block(capture)
      outcome.merge(
        "status" => "failed",
        "summary" => "refusing to #{step}: runtime capture coverage is #{block.fetch("status")}",
        "capture" => block
      )
    end

    def adapter_identifier = "Backstage::ContainerPhaseRunner"
  end
end

Backstage::ContainerPhaseRunner = Backstage::Application::Runners::ContainerPhaseRunner unless defined?(Backstage::ContainerPhaseRunner)
