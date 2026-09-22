# frozen_string_literal: true

require "json"

module Backstage::Adapters::Td
  class Client
    CommandRunner = Backstage::Support::CommandRunner
    ContractError = Backstage::ContractError
    READY_QUERY = "status = open AND labels = agent-ready"
    APPROVAL_QUERY = "(status = in_review OR status = closed) AND has(reviewer)"

    attr_reader :workspace

    def initialize(workspace:, runner: CommandRunner.new)
      @workspace = File.expand_path(workspace)
      @runner = runner
    end

    def ready_issues
      json(["td", "query", READY_QUERY, "--output", "json", "--limit", "0"])
    end

    def approval_candidates
      json(["td", "query", APPROVAL_QUERY, "--output", "json", "--limit", "0"])
    end

    def show(issue_id)
      json(["td", "show", normalize_issue_id(issue_id), "--json"])
    end

    def handoff(issue_id, done:, remaining:, decisions:)
      argv = ["td", "handoff", normalize_issue_id(issue_id), "--done", done, "--remaining", remaining]
      Array(decisions).each { |decision| argv.concat(["--decision", decision]) }
      argv << "--json"
      json(argv)
    end

    def review(issue_id, reason:)
      json(["td", "review", normalize_issue_id(issue_id), "--reason", reason, "--json"])
    end

    private

    def json(argv)
      result = @runner.run(argv, chdir: workspace)
      payload = JSON.parse(result.stdout)
      if payload.is_a?(Hash) && payload["error"]
        message = payload["error"].is_a?(Hash) ? payload["error"]["message"] || payload["error"].to_json : payload["error"].to_s
        raise ContractError, "td returned an error envelope: #{message}"
      end
      payload
    rescue JSON::ParserError => error
      raise ContractError, "invalid td JSON: #{error.message}"
    end

    def normalize_issue_id(issue_id)
      issue_id.to_s.downcase
    end
  end
end

Backstage::TdClient = Backstage::Adapters::Td::Client unless defined?(Backstage::TdClient)
