# frozen_string_literal: true

require "json"

module Backstage::Contracts
  class Validator
    ContractError = Backstage::ContractError
    def initialize(schema_root: File.expand_path("../../../schemas", __dir__))
      @schema_root = schema_root
    end

    def validate!(name, value)
      # Contracts are versioned files that never change in place, so parsing one once per process
      # is safe — and every activity event is validated on its way into durable history.
      schema = (@schemas ||= {})[name] ||= JSON.parse(File.read(File.join(@schema_root, name))).freeze
      errors = []
      validate_node(schema, value, "$", errors)
      raise ContractError, "contract #{name} failed: #{errors.join("; ")}" unless errors.empty?

      value
    end

    private

    def validate_node(schema, value, path, errors)
      validate_type(schema["type"], value, path, errors) if schema["type"]
      errors << "#{path} must be one of #{schema["enum"].inspect}" if schema["enum"] && !schema["enum"].include?(value)
      errors << "#{path} must equal #{schema["const"].inspect}" if schema.key?("const") && value != schema["const"]
      return validate_object(schema, value, path, errors) if value.is_a?(Hash)
      return validate_array(schema, value, path, errors) if value.is_a?(Array)

      if value.is_a?(String) && schema["minLength"] && value.length < schema["minLength"]
        errors << "#{path} is shorter than #{schema["minLength"]}"
      end
      if value.is_a?(String) && schema["maxLength"] && value.length > schema["maxLength"]
        errors << "#{path} is longer than #{schema["maxLength"]}"
      end
      if value.is_a?(String) && schema["pattern"] && !Regexp.new(schema["pattern"]).match?(value)
        errors << "#{path} does not match #{schema["pattern"].inspect}"
      end
    end

    def validate_object(schema, value, path, errors)
      Array(schema["required"]).each do |key|
        errors << "#{path}.#{key} is required" unless value.key?(key)
      end
      value.each do |key, nested|
        child = schema.fetch("properties", {})[key]
        if child
          validate_node(child, nested, "#{path}.#{key}", errors)
        elsif schema["additionalProperties"] == false
          errors << "#{path}.#{key} is not allowed"
        end
      end
    end

    def validate_array(schema, value, path, errors)
      return unless schema["items"]

      value.each_with_index { |nested, index| validate_node(schema["items"], nested, "#{path}[#{index}]", errors) }
    end

    def validate_type(type, value, path, errors)
      types = Array(type)
      valid = types.any? do |candidate|
        case candidate
        when "object" then value.is_a?(Hash)
        when "array" then value.is_a?(Array)
        when "string" then value.is_a?(String)
        when "integer" then value.is_a?(Integer)
        when "number" then value.is_a?(Numeric)
        when "boolean" then value == true || value == false
        when "null" then value.nil?
        else false
        end
      end
      errors << "#{path} must be #{types.join(" or ")}" unless valid
    end
  end
end

Backstage::ContractValidator = Backstage::Contracts::Validator unless defined?(Backstage::ContractValidator)
