# frozen_string_literal: true

require "open3"

module Backstage
  module Support
    CommandResult = Data.define(:stdout, :stderr, :exit_code) do
      def success?
        exit_code.zero?
      end
    end

    class CommandRunner
      ExternalCommandError = Backstage::ExternalCommandError
      SecretGuard = Backstage::Support::SecretGuard
      def initialize(secret_guard: SecretGuard.new)
        @secret_guard = secret_guard
      end

      def run(argv, env: {}, chdir: nil, stdin_data: "", allow_failure: false)
        options = { stdin_data: stdin_data }
        options[:chdir] = chdir if chdir
        stdout, stderr, status = Open3.capture3(env, *argv, **options)
        result = CommandResult.new(@secret_guard.redact(stdout), @secret_guard.redact(stderr), status.exitstatus || 128 + status.termsig.to_i)
        unless result.success? || allow_failure
          raise ExternalCommandError.new(
            "command failed (#{result.exit_code}): #{argv.first}",
            argv: argv,
            status: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr
          )
        end
        result
      end
    end
  end
end

Backstage::CommandResult = Backstage::Support::CommandResult unless defined?(Backstage::CommandResult)
Backstage::CommandRunner = Backstage::Support::CommandRunner unless defined?(Backstage::CommandRunner)
