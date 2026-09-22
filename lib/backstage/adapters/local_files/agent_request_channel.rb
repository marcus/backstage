# frozen_string_literal: true

require "fileutils"
require "json"

module Backstage::Adapters::LocalFiles
  class AgentRequestChannel < Backstage::Ports::AgentRequestChannel
    Records = Backstage::Domain::Records
    # A worker's channel is deliberately small. These bounds are the whole back-pressure story.
    MAX_REQUESTS = 200
    MAX_LINE_BYTES = 4096
    MAX_FILE_BYTES = 512 * 1024
    # The run's phase decides what the worker on the other end is allowed to be.
    PHASE_ROLES = { "implementation" => "agent", "review" => "reviewer" }.freeze

    attr_reader :path

    def initialize(directory, workflow_service:, store:, validator: Backstage::Contracts::Validator.new)
      @directory = File.expand_path(directory)
      @path = File.join(@directory, "requests.jsonl")
      @workflow_service = workflow_service
      @store = store
      @validator = validator
      @offset = 0
      @count = 0
      FileUtils.mkdir_p(@directory)
      @observed_size = 0
      @closed = false
      File.open(@path, File::RDWR | File::CREAT | File::NOFOLLOW | File::NONBLOCK, 0o600) do |file|
        stat = file.stat
        raise Backstage::ContractError, "agent request channel must be a regular file" unless stat.file?

        file.chmod(0o600)
        @identity = [stat.dev, stat.ino]
      end
    end

    def drain(run:)
      return [] if @closed

      File.open(@path, File::RDONLY | File::NOFOLLOW | File::NONBLOCK) do |file|
        stat = file.stat
        unless stat.file? && [stat.dev, stat.ino] == @identity
          return close_with_rejection(run, "agent request channel was replaced or is not a regular file")
        end
        size = stat.size
        return close_with_rejection(run, "agent request channel was truncated") if size < @observed_size
        @observed_size = size
        return close_with_rejection(run, "agent request channel exceeded #{MAX_FILE_BYTES} bytes") if size > MAX_FILE_BYTES
        return [] if size <= @offset

        results = []
        read_new_lines(file, size).each do |line|
          if @count >= MAX_REQUESTS
            results.concat(close_with_rejection(run, "agent request channel exceeded #{MAX_REQUESTS} requests"))
            break
          end
          if line.bytesize > MAX_LINE_BYTES
            results.concat(close_with_rejection(run, "agent request exceeded #{MAX_LINE_BYTES} bytes"))
            break
          end
          results << apply(run, line)
        end
        results.compact
      end
    rescue Errno::ELOOP, Errno::ENOENT, Errno::ENOTDIR, Errno::EACCES
      close_with_rejection(run, "agent request channel is missing or unsafe to read")
    end

    private

    def close_with_rejection(run, message)
      return [] if @closed

      @closed = true
      [record_rejection(run, nil, message)]
    end

    def read_new_lines(file, size)
      file.seek(@offset)
      chunk = file.read(size - @offset).to_s
      # Leave a partial trailing line for the next drain so a worker mid-write is never misread.
      complete, _, remainder = chunk.rpartition("\n")
      @offset = size - remainder.bytesize
      complete.split("\n").reject { |line| line.strip.empty? }
    end

    def apply(run, line)
      @count += 1
      payload = JSON.parse(line)
      raise Backstage::ContractError, "agent request must be an object" unless payload.is_a?(Hash)

      @validator.validate!("agent-request-v2.json", payload)
      work_item_id = run.fetch("work_item_id")
      role = PHASE_ROLES.fetch(run.fetch("phase")) do
        raise Backstage::ContractError, "run phase #{run["phase"].inspect} has no worker role"
      end
      transition = payload.fetch("transition")
      request_id = payload["request_id"].to_s
      request_id = "agent:#{run.fetch("id")}:#{@count}" if request_id.empty?
      # Scope the worker's own identifier to its run so one run cannot replay another's request.
      request_id = "agent:#{run.fetch("id")}:#{request_id}"

      request = @store.save("agent_requests", Records.agent_request(
        work_item_id: work_item_id,
        run_id: run.fetch("id"),
        actor: { "role" => role, "id" => run.fetch("id"), "entry" => "worker_channel" },
        transition: transition,
        reason: payload["reason"],
        evidence: Array(payload["evidence"]),
        request_id: request_id
      ))

      result = @workflow_service.request_transition(
        work_item_id: work_item_id,
        transition: transition,
        actor: { "role" => role, "id" => run.fetch("id"), "entry" => "worker_channel" },
        request_id: request_id,
        reason: payload["reason"],
        evidence: Array(payload["evidence"]),
        decision: payload["decision"],
        expected_revision: payload["expected_revision"],
        run_id: run.fetch("id")
      )
      @store.save("agent_requests", request.merge(
        "status" => "applied",
        "transition_id" => result.fetch("transition").fetch("id"),
        "resulting_state" => result.fetch("work_item").fetch("state"),
        "updated_at" => Records.timestamp
      ))
    rescue JSON::ParserError
      # Parser diagnostics can contain input fragments. Keep rejected contents out of the audit.
      record_rejection(run, nil, "agent request was not valid JSON")
    rescue Backstage::Error => error
      record_rejection(run, defined?(request) && request, "#{error.class.name.split("::").last}: #{error.message}")
    end

    def record_rejection(run, request, message)
      request ||= Records.agent_request(
        work_item_id: run["work_item_id"],
        run_id: run.fetch("id"),
        actor: { "role" => PHASE_ROLES[run["phase"]] || "agent", "id" => run.fetch("id"), "entry" => "worker_channel" },
        transition: "unknown"
      )
      @store.save("agent_requests", request.merge("status" => "rejected", "error" => message, "updated_at" => Records.timestamp))
    end
  end
end
