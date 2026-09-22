# frozen_string_literal: true

module Backstage::Adapters::Environment
  class CredentialBroker
    ContractError = Backstage::ContractError

    def initialize(env: ENV, mapping: {})
      @env = env
      @mapping = mapping
    end

    def resolve(reference)
      config = @mapping.fetch(reference) { raise ContractError, "unknown credential reference #{reference}" }
      value = @env[config.fetch("source_env")]
      raise ContractError, "credential reference #{reference} is unavailable" if value.to_s.empty?

      value
    end

    def runtime_environment(references)
      Array(references).to_h do |reference|
        config = @mapping.fetch(reference) { raise ContractError, "unknown credential reference #{reference}" }
        [config.fetch("runtime_env"), resolve(reference)]
      end
    end

    def describe(references)
      Array(references).map do |reference|
        config = @mapping.fetch(reference) { raise ContractError, "unknown credential reference #{reference}" }
        { "reference" => reference, "runtime_env" => config.fetch("runtime_env") }
      end
    end
  end
end

Backstage::CredentialBroker = Backstage::Adapters::Environment::CredentialBroker unless defined?(Backstage::CredentialBroker)
