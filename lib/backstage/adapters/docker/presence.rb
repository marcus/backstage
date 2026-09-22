# frozen_string_literal: true

require "json"

module Backstage::Adapters::Docker
  class Presence < Backstage::Ports::RuntimePresence
    CommandRunner = Backstage::Support::CommandRunner

    def initialize(docker: "docker", runner: CommandRunner.new)
      @docker = docker
      @runner = runner
    end

    def status(runtime)
      name = runtime.is_a?(Hash) ? runtime["container_name"] : nil
      return UNKNOWN if name.to_s.empty?

      result = @runner.run([@docker, "inspect", "--format", "{{.State.Running}}", name], allow_failure: true)
      return GONE if !result.success? && result.stderr.to_s.match?(/No such object/i)
      return UNKNOWN unless result.success?

      result.stdout.strip == "true" ? ALIVE : GONE
    rescue StandardError
      UNKNOWN
    end
  end
end
