# frozen_string_literal: true

module Backstage::Domain
  # The small, declared retry policy for accepted work.
  #
  # Deliberately not an expression language: a bounded number of automatic attempts, a delay, and
  # one of two shapes for growing that delay. A policy is normalized once, pinned onto the intent
  # that was accepted with it, and never re-read from configuration afterwards, so editing pack
  # defaults cannot change a budget that is already authorized.
  module RetryPolicy
    ContractError = Backstage::ContractError
    BACKOFFS = %w[fixed exponential].freeze
    DEFAULT = { "max_retries" => 0, "delay_seconds" => 60, "backoff" => "fixed", "max_delay_seconds" => 3600 }.freeze

    module_function

    def normalize(policy = {}, defaults: DEFAULT)
      policy ||= {}
      raise ContractError, "retry policy must be an object" unless policy.is_a?(Hash)

      merged = defaults.merge(stringify(policy))
      unknown = merged.keys - DEFAULT.keys
      raise ContractError, "unknown retry policy keys: #{unknown.sort.join(", ")}" unless unknown.empty?

      max_retries = integer(merged, "max_retries", minimum: 0)
      delay = integer(merged, "delay_seconds", minimum: 1)
      max_delay = integer(merged, "max_delay_seconds", minimum: 1)
      backoff = merged.fetch("backoff").to_s
      raise ContractError, "retry backoff must be one of #{BACKOFFS.join(", ")}" unless BACKOFFS.include?(backoff)
      raise ContractError, "retry max_delay_seconds must not be below delay_seconds" if max_delay < delay

      { "max_retries" => max_retries, "delay_seconds" => delay, "backoff" => backoff, "max_delay_seconds" => max_delay }
    end

    # `number` is the 1-based retry being scheduled, so the first retry waits `delay_seconds`.
    def delay_for(policy, number)
      policy = normalize(policy)
      raise ContractError, "retry number must be positive" if number < 1

      seconds = case policy.fetch("backoff")
                when "exponential" then policy.fetch("delay_seconds") * (2**(number - 1))
                else policy.fetch("delay_seconds")
                end
      [seconds, policy.fetch("max_delay_seconds")].min
    end

    def retries_remaining(policy, retries_used)
      [normalize(policy).fetch("max_retries") - retries_used.to_i, 0].max
    end

    def stringify(policy)
      policy.to_h { |key, value| [key.to_s, value] }
    end

    def integer(policy, key, minimum:)
      value = policy.fetch(key)
      unless value.is_a?(Integer) || (value.is_a?(String) && value.match?(/\A\d+\z/))
        raise ContractError, "retry #{key} must be a whole number"
      end

      number = Integer(value)
      raise ContractError, "retry #{key} must be at least #{minimum}" if number < minimum

      number
    end
  end
end
