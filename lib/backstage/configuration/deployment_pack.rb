# frozen_string_literal: true

require "json"
require "pathname"
require "yaml"

module Backstage::Configuration
  class DeploymentPack
    ContractError = Backstage::ContractError
    ContractValidator = Backstage::Contracts::Validator
    Records = Backstage::Domain::Records
    SecretGuard = Backstage::Support::SecretGuard
    attr_reader :pack_path, :pack, :targets, :workflows

    def initialize(pack_path, validator: ContractValidator.new)
      @pack_path = File.expand_path(pack_path)
      @validator = validator
      @pack = load_yaml(File.join(@pack_path, "backstage.yml"))
      @targets = Dir[File.join(@pack_path, "targets", "*.yml")].sort.to_h do |path|
        [File.basename(path, ".yml"), load_yaml(path)]
      end
      @workflows = load_workflows
      SecretGuard.new.check!(@pack)
      @targets.each_value { |target| SecretGuard.new.check!(target) }
      validate!
    end

    def check
      {
        "valid" => true,
        "pack" => pack_path,
        "targets" => targets.keys,
        "dispatcher" => dispatcher_policy,
        # The bounds capture actually runs under, defaults included. They were validated and then
        # reported nowhere, so no surface could tell an operator — or an agent — how large a record
        # or a run's output may get on this host before something is cut.
        "capture" => capture_check,
        "source_routes" => source_routes,
        "workflows" => workflows.values.map { |workflow| workflow_summary(workflow) },
        "default_workflow" => default_workflow_name,
        "target_workflows" => targets.keys.to_h { |name| [name, workflow_for_target(name).name] }
      }
    end

    # The dispatcher's own small policy: how often a foreground worker polls, and the retry budget
    # an acceptance is pinned with. Pinning happens at acceptance, so editing this never changes a
    # budget that is already authorized.
    def dispatcher_policy
      section = pack.fetch("dispatcher", {})
      raise ContractError, "pack dispatcher must be an object" unless section.is_a?(Hash)

      unknown = section.keys - %w[poll_interval_seconds retry]
      raise ContractError, "unknown dispatcher keys: #{unknown.sort.join(", ")}" unless unknown.empty?

      interval = section.fetch("poll_interval_seconds", 5)
      unless interval.is_a?(Numeric) && interval.positive?
        raise ContractError, "dispatcher poll_interval_seconds must be a positive number"
      end

      {
        "poll_interval_seconds" => interval,
        "retry" => Backstage::Domain::RetryPolicy.normalize(section.fetch("retry", {}))
      }
    end

    # How much of a runtime's output this deployment buffers before it makes it durable, and how
    # much it will keep at all. It is pack configuration rather than a constant because the answer
    # depends on the host: a machine with cheap fsyncs wants smaller chunks and tighter latency, a
    # busy one wants fewer, larger commits, and a deployment that runs very chatty workers wants a
    # different ceiling. Everything here is a bound, so an unset key is the safe default, never
    # "unbounded".
    # `max_record_bytes` bounds one record of plain output; `max_protocol_record_bytes` bounds one
    # record of a harness's own protocol, which is a different thing and wants a much larger number
    # — see Ports::StreamInterpreter#protocol?.
    CAPTURE_KEYS = %w[flush_bytes flush_millis flush_records max_stream_bytes max_run_bytes
                      max_record_bytes max_protocol_record_bytes].freeze

    # The effective policy: every bound with its value, and which of them the pack set. A reader
    # that only saw the overrides could not tell an unset bound from an unbounded one.
    def capture_check
      policy = capture_policy
      Backstage::Application::RuntimeCapture::POLICY_DEFAULTS
        .each_with_object("configured" => policy.keys.sort) do |(key, value), row|
          row[key] = policy.fetch(key, value)
        end
    end

    def capture_policy
      section = pack.fetch("capture", {})
      raise ContractError, "pack capture must be an object" unless section.is_a?(Hash)

      unknown = section.keys - CAPTURE_KEYS
      raise ContractError, "unknown capture keys: #{unknown.sort.join(", ")}" unless unknown.empty?

      section.each_with_object({}) do |(key, value), policy|
        unless value.is_a?(Integer) && value.positive?
          raise ContractError, "capture #{key} must be a positive integer"
        end

        policy[key] = value
      end
    end

    # The compiled workflow a target's sourced work is admitted with.
    def workflow_for_target(target_name)
      target = targets.fetch(target_name) { raise ContractError, "unknown target #{target_name}" }
      workflow(target["workflow"] || default_workflow_name)
    end

    def workflow(name)
      workflows.fetch(name.to_s) do
        raise ContractError, "unknown workflow #{name.inspect}; pack defines #{workflows.keys.sort.join(", ")}"
      end
    end

    def default_workflow_name
      configured = pack.dig("workflows", "default")
      return configured.to_s if configured

      raise ContractError, "pack must set workflows.default when it defines more than one workflow" if workflows.length > 1

      workflows.keys.first
    end

    def route(source_identity)
      normalized = normalize_source_identity(source_identity)
      matches = source_routes.select { |_name, identity| identity == normalized }.keys
      raise ContractError, "no target claims source #{normalized}" if matches.empty?
      raise ContractError, "multiple targets claim source #{normalized}: #{matches.join(", ")}" if matches.length > 1

      matches.first
    end

    def compile(work_item:)
      selected_name = work_item.fetch("target") { raise ContractError, "work item has no persisted target binding" }
      target = targets.fetch(selected_name) { raise ContractError, "unknown target #{selected_name}" }
      bound_identity = normalize_source_identity(work_item.fetch("source_identity") { raise ContractError, "work item has no persisted source identity" })
      expected_identity = source_routes.fetch(selected_name)
      raise ContractError, "work item source identity does not match its target" unless bound_identity == expected_identity
      expected_instance = target.dig("trigger", "source_instance")
      raise ContractError, "work item source instance does not match its target" unless work_item["source_instance"] == expected_instance
      ref_instance = work_item["source_ref"].is_a?(Hash) ? work_item.dig("source_ref", "source_instance") : nil
      if ref_instance && ref_instance != expected_instance
        raise ContractError, "work item source reference conflicts with its immutable binding"
      end
      repo = target.fetch("repo")
      harness = deep_merge(pack.fetch("harness_defaults", {}), target.fetch("harness", {}))
      repository_credential = repo["credentials"] || pack.dig("credentials", "repository_default")
      credential_refs = [harness["credentials"], repository_credential].compact.uniq
      image = pack.dig("adapters", "worker_runtime", "image")
      raise ContractError, "worker image is required" if image.to_s.empty?

      bundle = {
        "schema_version" => 1,
        "id" => Records.id("bundle"),
        "target" => selected_name,
        "work_item" => normalize_work_item(work_item),
        "repository" => {
          "url" => https_origin(repo.fetch("origin")),
          "revision" => repo.fetch("default_revision", repo.fetch("default_branch", "main")),
          "default_branch" => repo.fetch("default_branch", "main"),
          "branch" => "backstage/#{work_item.fetch("id").downcase}",
          "designated_repository" => repository_identity(repo.fetch("origin")),
          "credential_ref" => repository_credential
        }.compact,
        "harness" => {
          "adapter" => pack.dig("adapters", "harness", "kind"),
          "provider" => harness.fetch("provider"),
          "model" => harness.fetch("model"),
          "prompt" => sealed_prompt(selected_name, target, work_item),
          "credential_ref" => harness["credentials"],
          "options" => harness.fetch("options", {})
        }.compact,
        "execution" => {
          "image" => image,
          "command" => harness_command(pack.dig("adapters", "harness", "kind"), harness),
          "timeout_seconds" => target.fetch("timeout_seconds", pack.fetch("timeout_seconds", 1800)),
          "credential_refs" => credential_refs,
          "mounts" => []
        },
        "policy" => compile_policy(pack.fetch("policy", {}), target.fetch("authority", {})),
        "context_grants" => compile_context(target, repository_credential),
        "source_identity" => bound_identity,
        "compiled_at" => Records.timestamp
      }
      SecretGuard.new.check!(bundle)
      @validator.validate!("job-bundle-v1.json", bundle)
    end

    private

    def load_workflows
      paths = Dir[File.join(@pack_path, "workflows", "*.yml")].sort
      raise ContractError, "pack must define at least one workflow in workflows/" if paths.empty?

      paths.to_h do |path|
        file_name = File.basename(path, ".yml")
        workflow = Backstage::Domain::Workflow.compile(load_yaml(path), name: file_name)
        unless workflow.name == file_name
          raise ContractError, "workflow #{workflow.name} must live in workflows/#{workflow.name}.yml"
        end

        [workflow.name, workflow]
      end
    end

    def workflow_summary(workflow)
      {
        "name" => workflow.name,
        "version" => workflow.version,
        "digest" => workflow.digest,
        "initial_state" => workflow.initial_state,
        "states" => workflow.states.keys,
        "terminal_states" => workflow.states.values.select(&:terminal?).map(&:name),
        "transitions" => workflow.transitions.keys,
        "max_revisions" => workflow.max_revisions
      }
    end

    def validate!
      raise ContractError, "pack must define adapters" unless pack["adapters"].is_a?(Hash)
      %w[state_store artifact_store worker_runtime harness].each do |name|
        raise ContractError, "pack adapter #{name} is required" unless pack.dig("adapters", name, "kind")
      end
      raise ContractError, "pack must include at least one target" if targets.empty?

      dispatcher_policy
      capture_policy
      missing = declared_credential_refs - credential_mapping.keys
      raise ContractError, "credential references missing from credentials.broker: #{missing.sort.join(", ")}" unless missing.empty?

      duplicates = source_routes.group_by { |_name, source| source }.select { |_source, rows| rows.length > 1 }
      raise ContractError, "duplicate source routes: #{duplicates.keys.join(", ")}" unless duplicates.empty?
      targets.each do |name, target|
        raise ContractError, "target #{name} repo.origin is required" unless target.dig("repo", "origin")
        raise ContractError, "target #{name} trigger.td_workspace is required" unless target.dig("trigger", "td_workspace")
        raise ContractError, "target #{name} trigger.source_instance is required" if target.dig("trigger", "source_instance").to_s.empty?
        validate_instruction!(name, target["instructions"]) if target["instructions"]
        compile_context(target, target.dig("repo", "credentials"))
        workflow_for_target(name)
      end
      default_workflow_name
    end

    def declared_credential_refs
      refs = []
      refs << pack.dig("credentials", "repository_default")
      refs << pack.dig("harness_defaults", "credentials")
      targets.each_value do |target|
        refs << target.dig("repo", "credentials")
        refs << target.dig("harness", "credentials")
        Array(target.dig("context", "repos")).each { |repo| refs << repo["credentials"] }
      end
      refs.compact.map(&:to_s).uniq
    end

    def source_routes
      targets.transform_values { |target| normalize_source_identity(target.dig("trigger", "td_workspace")) }
    end

    def normalize_source_identity(identity)
      File.expand_path(identity.to_s)
    end

    def normalize_work_item(work_item)
      %w[id title description source].to_h { |key| [key, work_item.fetch(key)] }.merge("source_ref" => work_item["source_ref"]).compact
    end

    def compile_policy(defaults, authority)
      review_change = authority.fetch("review_change", defaults.fetch("review_change", "draft_pr"))
      raise ContractError, "v1 authority supports only draft_pr" unless review_change == "draft_pr"

      {
        "name" => defaults.fetch("name", "implementation-independent-review-v1"),
        "allowed_actions" => %w[clone push_branch create_draft_pr],
        "require_independent_review" => defaults.fetch("require_independent_review", true),
        "forbidden_actions" => %w[push_default merge deploy]
      }
    end

    def compile_context(target, repository_credential)
      Array(target.dig("context", "repos")).map do |repo|
        name = repo.fetch("name")
        mount = repo.fetch("mount")
        unless name.match?(/\A[a-zA-Z0-9][a-zA-Z0-9._-]*\z/)
          raise ContractError, "context repo name is path-unsafe"
        end
        if Pathname.new(mount).absolute? || mount.split("/").any? { |part| part.empty? || %w[. ..].include?(part) } || !mount.match?(%r{\A[a-zA-Z0-9._/-]+\z})
          raise ContractError, "context repo mount must be a safe relative path"
        end

        {
          "kind" => "repo",
          "name" => name,
          "origin" => https_origin(repo.fetch("origin")),
          "revision" => repo.fetch("revision", "main"),
          "mount" => mount,
          "read_only" => true,
          "credential_ref" => repo["credentials"] || repository_credential
        }.compact
      end
    end

    def sealed_prompt(target_name, target, work_item)
      instructions = target["instructions"] ? File.read(File.join(pack_path, target["instructions"])) : ""
      ["Target: #{target_name}", instructions.strip, work_item.fetch("title"), work_item.fetch("description")].reject(&:empty?).join("\n\n")
    end

    def validate_instruction!(target_name, relative_path)
      path = File.expand_path(relative_path, pack_path)
      unless path.start_with?("#{pack_path}/") && File.file?(path)
        raise ContractError, "target #{target_name} instructions must resolve inside the pack"
      end
    end

    def harness_command(kind, harness)
      raise ContractError, "unsupported harness #{kind}" unless kind == "pi"

      ["pi", "--mode", "json", "--provider", harness.fetch("provider"), "--model", harness.fetch("model"), "--no-session", "--no-extensions", "--no-skills", "--no-context-files", "--no-approve"]
    end

    def repository_identity(origin)
      origin.sub(%r{\Agit@github\.com:}, "").sub(%r{\Ahttps://github\.com/}, "").sub(/\.git\z/, "")
    end

    def https_origin(origin)
      return "https://github.com/#{origin.delete_prefix("git@github.com:")}" if origin.start_with?("git@github.com:")

      origin
    end

    def load_yaml(path)
      value = YAML.safe_load_file(path, permitted_classes: [], aliases: false)
      raise ContractError, "#{path} must contain an object" unless value.is_a?(Hash)

      JSON.parse(JSON.generate(value))
    rescue Errno::ENOENT => error
      raise ContractError, error.message
    rescue Psych::Exception => error
      raise ContractError, "invalid YAML #{path}: #{error.message}"
    end

    def deep_merge(base, override)
      base.merge(override) do |_key, old, new|
        old.is_a?(Hash) && new.is_a?(Hash) ? deep_merge(old, new) : new
      end
    end

    public

    # Names in pack YAML, never secret values. Each reference says which environment variable the
    # operator has set and which name the worker receives.
    def credential_mapping
      section = pack.fetch("credentials", {})
      raise ContractError, "pack credentials must be an object" unless section.is_a?(Hash)

      unknown = section.keys - %w[repository_default broker]
      raise ContractError, "unknown credentials keys: #{unknown.sort.join(", ")}" unless unknown.empty?

      broker = section.fetch("broker", {})
      raise ContractError, "credentials.broker must be an object" unless broker.is_a?(Hash)

      broker.to_h do |name, config|
        raise ContractError, "credential #{name} must be an object" unless config.is_a?(Hash)

        extra = config.keys - %w[source_env runtime_env]
        raise ContractError, "unknown credential #{name} keys: #{extra.sort.join(", ")}" unless extra.empty?

        source = config["source_env"].to_s
        runtime = config["runtime_env"].to_s
        unless source.match?(/\A[A-Z][A-Z0-9_]*\z/) && runtime.match?(/\A[A-Z][A-Z0-9_]*\z/)
          raise ContractError, "credential #{name} env names must be uppercase identifiers"
        end

        [name, { "source_env" => source, "runtime_env" => runtime }]
      end
    end

    def state_path
      expand_config_path(pack.dig("adapters", "state_store", "path"))
    end

    def artifact_path
      expand_config_path(pack.dig("adapters", "artifact_store", "path"))
    end

    def binding_for(target_name)
      target = targets.fetch(target_name) { raise ContractError, "unknown target #{target_name}" }
      {
        target: target_name,
        source_instance: target.dig("trigger", "source_instance"),
        source_identity: normalize_source_identity(target.dig("trigger", "td_workspace"))
      }
    end

    private

    def expand_config_path(path)
      raise ContractError, "configured adapter path is required" if path.to_s.empty?

      File.expand_path(path)
    end
  end
end

module Backstage::Configuration
  def self.new(...)
    DeploymentPack.new(...)
  end
end
