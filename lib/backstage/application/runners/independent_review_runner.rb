# frozen_string_literal: true

require "json"

module Backstage::Application::Runners
  class IndependentReviewRunner
    ContractError = Backstage::ContractError
    Records = Backstage::Domain::Records
    VERDICTS = %w[approved changes_requested blocked].freeze

    def initialize(harness:, reviewer_session_id: Records.id("reviewer"))
      @harness = harness
      @reviewer_session_id = reviewer_session_id
    end

    # `capture` passes straight through: a reviewer's output is captured the same way an
    # implementer's is, by the harness that knows how to read it.
    def run(bundle:, secrets: {}, cancellation: nil, capture: nil)
      outcome = @harness.run(bundle: bundle, secrets: secrets, cancellation: cancellation, capture: capture) { |event| yield(event) if block_given? }
      payload = parse_verdict(outcome.fetch("assistant_text", ""))
      outcome.merge(
        "review" => {
          "verdict" => payload.fetch("verdict"),
          "summary" => payload.fetch("summary"),
          "independent" => true,
          "reviewer_session_id" => @reviewer_session_id
        }
      )
    rescue JSON::ParserError, KeyError => error
      raise ContractError, "reviewer did not return the required verdict JSON: #{error.message}"
    end

    private

    def parse_verdict(text)
      payload = JSON.parse(text)
      raise ContractError, "unknown review verdict #{payload["verdict"].inspect}" unless VERDICTS.include?(payload["verdict"])
      raise ContractError, "review summary is required" if payload["summary"].to_s.empty?

      payload
    end
  end
end

Backstage::IndependentReviewRunner = Backstage::Application::Runners::IndependentReviewRunner unless defined?(Backstage::IndependentReviewRunner)
