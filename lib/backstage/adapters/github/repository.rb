# frozen_string_literal: true

require "base64"
require "fileutils"

module Backstage::Adapters::GitHub
  class Repository
    AuthorityError = Backstage::AuthorityError
    CommandRunner = Backstage::Support::CommandRunner
    ContractError = Backstage::ContractError
    def initialize(authority:, credential_broker:, runner: CommandRunner.new)
      @authority = authority
      @credential_broker = credential_broker
      @runner = runner
    end

    def clone(origin:, revision:, destination:, credential_ref:, read_only: false)
      @authority.check_action!(:clone)
      @authority.check_origin!(origin)
      raise ContractError, "clone destination already exists" if File.exist?(destination)

      token = @credential_broker.resolve(credential_ref)
      env = git_auth_env(token)
      FileUtils.mkdir_p(File.dirname(destination))
      @runner.run(["git", "init", "--quiet", destination], env: env)
      @runner.run(["git", "-C", destination, "remote", "add", "origin", origin], env: env)
      @runner.run(["git", "-C", destination, "fetch", "--depth", "1", "origin", revision], env: env)
      resolved = @runner.run(["git", "-C", destination, "rev-parse", "FETCH_HEAD"], env: env).stdout.strip
      @runner.run(["git", "-C", destination, "checkout", "--detach", resolved], env: env)
      { "origin" => origin, "requested_revision" => revision, "resolved_revision" => resolved, "path" => destination, "read_only" => read_only }
    end

    def branch(workspace:, branch:)
      raise AuthorityError, "host repository mutation is forbidden; branch inside the sealed worker"
    end

    def commit(workspace:, message:)
      raise AuthorityError, "host repository mutation is forbidden; commit inside the sealed worker"
    end

    def push(workspace:, branch:, credential_ref:)
      raise AuthorityError, "host repository mutation is forbidden; push inside the sealed worker"
    end

    private

    def git_auth_env(token)
      encoded = Base64.strict_encode64("x-access-token:#{token}")
      {
        "GIT_TERMINAL_PROMPT" => "0",
        "GIT_CONFIG_COUNT" => "1",
        "GIT_CONFIG_KEY_0" => "http.https://github.com/.extraheader",
        "GIT_CONFIG_VALUE_0" => "AUTHORIZATION: basic #{encoded}"
      }
    end
  end
end

Backstage::GitHubRepository = Backstage::Adapters::GitHub::Repository unless defined?(Backstage::GitHubRepository)
