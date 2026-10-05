# frozen_string_literal: true

require "json"
require "fileutils"

module Backstage::Adapters::Fake
  # An intentionally different native model. Jobs use case-sensitive keys and plain documents;
  # result/ready receipts live in a separate local ledger, never in the frozen assignment input.
  class WorkSource < Backstage::Ports::WorkSource
    CAPABILITIES = %w[record_result mark_ready].freeze

    def initialize(path:, ledger_path: nil)
      @path = File.expand_path(path)
      @ledger_path = File.expand_path(ledger_path || "#{@path}.receipts.json")
    end

    def snapshot(ref)
      job = jobs.find { |item| item.fetch("key") == ref }
      raise Backstage::NotFound, "fake source item not found" unless job

      { "ref" => job.fetch("key"), "title" => job.fetch("display"),
        "content" => job.fetch("body"), "media_type" => job.fetch("format"),
        "version" => job["revision"] }.compact
    end

    def discover = jobs.select { |job| job["selected"] == true }.map { |job| job.fetch("key") }.uniq
    def capabilities = CAPABILITIES

    def prepare(operation:, ref:, result:, operation_id:)
      check_operation!(operation)
      { "job_key" => ref, "event" => operation == "record_result" ? "result_recorded" : "ready_for_human",
        "result" => result, "operation_id" => operation_id }
    end

    def reconcile(operation:, ref:, payload:, operation_id:, attempted:)
      check_operation!(operation)
      with_ledger do |rows|
        receipt = rows[operation_id]
        if receipt
          validate_receipt!(receipt, operation: operation, ref: ref, payload: payload)
          { "status" => "applied", "receipt" => receipt.slice("operation", "ref", "operation_id") }
        else
          { "status" => "not_applied", "receipt" => { "operation_id" => operation_id } }
        end
      end
    end

    def execute(operation:, ref:, payload:, operation_id:)
      check_operation!(operation)
      with_ledger(write: true) do |rows|
        existing = rows[operation_id]
        validate_receipt!(existing, operation: operation, ref: ref, payload: payload) if existing
        rows[operation_id] ||= { "operation" => operation, "ref" => ref, "payload" => payload, "operation_id" => operation_id }
        { "status" => "applied", "receipt" => rows.fetch(operation_id).slice("operation", "ref", "operation_id") }
      end
    end

    private

    def jobs
      document = JSON.parse(File.read(@path))
      document.fetch("jobs")
    rescue JSON::ParserError, KeyError => error
      raise Backstage::ContractError, "invalid fake source document: #{error.class}"
    end

    def with_ledger(write: false)
      FileUtils.mkdir_p(File.dirname(@ledger_path))
      # A separate stable inode guards atomic rename and is also shared by independently-created
      # adapter instances. Reconciliation takes this lock so absence is a trustworthy observation.
      File.open("#{@ledger_path}.lock", File::RDWR | File::CREAT, 0o600) do |lock|
        lock.flock(File::LOCK_EX)
        rows = File.exist?(@ledger_path) ? JSON.parse(File.read(@ledger_path)) : {}
        result = yield rows
        if write
          temp = "#{@ledger_path}.#{Process.pid}.tmp"
          File.open(temp, "w", 0o600) { |file| file.write(JSON.generate(rows)); file.flush; file.fsync }
          File.rename(temp, @ledger_path)
          File.open(File.dirname(@ledger_path), File::RDONLY) { |directory| directory.fsync }
        end
        result
      ensure
        File.delete(temp) if temp && File.exist?(temp)
      end
    rescue JSON::ParserError => error
      raise Backstage::ContractError, "invalid fake source receipt ledger: #{error.class}"
    end

    def validate_receipt!(receipt, operation:, ref:, payload:)
      unless receipt["operation"] == operation && receipt["ref"] == ref && receipt["payload"] == payload
        raise Backstage::ContractError, "fake source operation identity conflict"
      end
    end

    def check_operation!(operation)
      raise Backstage::ContractError, "unsupported fake source operation: #{operation}" unless CAPABILITIES.include?(operation)
    end
  end
end
