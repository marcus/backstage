# frozen_string_literal: true

require "json"
require "fileutils"

module Backstage::Adapters::Jsonl
  class Store < Backstage::Ports::Store
    ContractError = Backstage::ContractError
    NotFound = Backstage::NotFound
    Records = Backstage::Domain::Records
    Activity = Backstage::Domain::Activity
    SecretGuard = Backstage::Support::SecretGuard

    # Deployment identity is a single guarded row rather than one record per deployment: two
    # processes racing to mint would each pass an absence guard on their own random id, but only
    # one can create this row, and the loser adopts what it finds.
    DEPLOYMENT_COLLECTION = "deployments"
    DEPLOYMENT_ROW = "current"

    # How far one filtered page may scan past its own results before it returns what it has and
    # hands back a cursor. This is what keeps a sparse filter over long history bounded, at the
    # cost of pages that are shorter than `limit` — see the port for the consumer contract.
    SCAN_MULTIPLIER = 10

    attr_reader :path

    def initialize(path, secret_guard: SecretGuard.new, validator: Backstage::Contracts::Validator.new)
      @path = File.expand_path(path)
      @secret_guard = secret_guard
      @validator = validator
      directory = File.dirname(@path)
      FileUtils.mkdir_p(directory)
      created = !File.exist?(@path)
      FileUtils.touch(@path)
      # A commit fsyncs the log's own bytes; the directory entry needs its own flush, or a crash
      # right after the first commit could leave an acknowledged transaction in a file that is not
      # yet linked. Filesystems that refuse a directory fsync simply do not need one.
      sync_directory(directory) if created
    end

    def save(collection, record)
      commit([[collection, record]]).first
    end

    # One lock covers guard checks and appending a newline-terminated transaction. Readers
    # replay only complete lines, so even a process dying mid-write cannot expose half a batch.
    # Activity events ride in the same line: state and history are one fsync, never two.
    def commit(writes, expect: [], activity: [])
      incoming = Array(activity)
      # Minting has to happen before the writer lock; flock would deadlock against itself.
      deployment = incoming.empty? ? nil : deployment_id
      normalized = writes.map do |collection, record|
        row = stringify(record)
        raise ContractError, "record id is required" if row["id"].to_s.empty?
        raise ContractError, "the #{Activity::COLLECTION} collection is reserved for activity events" if collection.to_s == Activity::COLLECTION

        @secret_guard.check!(row)
        [collection.to_s, row]
      end
      incoming = incoming.map { |event| normalize_activity_event(event, deployment) }
      identities = incoming.map { |event| event.fetch("event_id") }
      raise ContractError, "one commit cannot carry the same event id twice" unless identities.uniq.length == identities.length

      fingerprints = incoming.map { |event| Activity.canonical_fingerprint(event) }
      recorded_at = Records.timestamp
      events = normalized.map do |collection, row|
        { "collection" => collection, "record" => row, "recorded_at" => recorded_at }
      end
      committed = Array.new(incoming.length)

      File.open(@path, "r+b") do |file|
        file.flock(File::LOCK_EX)
        wanted = identities.each_with_object({}) { |identity, memo| memo[identity] = true }
        existing = {}
        state, _order, complete_bytes, high_water = read_index(file) do |event|
          existing[event.fetch("event_id")] = event if wanted.include?(event["event_id"])
        end
        check_expectations!(expect, state)
        check_deployment!(state, deployment) if deployment

        appended = []
        incoming.each_with_index do |event, position|
          original = existing[event.fetch("event_id")]
          if original.nil?
            appended << [position, event]
            next
          end
          unless Activity.canonical_fingerprint(original) == fingerprints[position]
            # Not a lost race: two different facts were minted under one identity. A retry cannot
            # resolve it, so it is raised as its own type rather than as the stale-guard conflict
            # every retry loop absorbs.
            raise Backstage::ActivityConflictError.new(
              "activity event #{event.fetch("event_id")} already exists with different content",
              event_id: event.fetch("event_id")
            )
          end
          # An exact retry is the same fact told twice. Return the history that already exists.
          committed[position] = original
        end

        # A stream needs a first event that says nothing precedes it, so an empty page is
        # distinguishable from history that was lost or never imported.
        pending = high_water.zero? && !appended.empty? ? [stream_started_event(deployment, recorded_at)] : []
        appended.each { |_position, event| pending << event }
        pending.each do |event|
          event["sequence"] = (high_water += 1)
          event["recorded_at"] = recorded_at
          Activity.validate!(event, validator: @validator)
        end
        appended.each { |position, event| committed[position] = event }

        # Nothing new to say: an exact replay of a whole commit leaves the log byte-identical.
        duplicate = !incoming.empty? && pending.empty? &&
                    normalized.all? { |collection, row| state[collection][row.fetch("id")] == row }
        unless (normalized.empty? && pending.empty?) || duplicate
          line = { "transaction_version" => 1, "events" => events }
          line["activity"] = pending unless pending.empty?
          # Remove only the incomplete final line left by an interrupted writer. Complete
          # malformed lines are rejected by read_index and must never be silently discarded.
          file.truncate(complete_bytes)
          file.seek(complete_bytes)
          file.write(JSON.generate(line) + "\n")
          file.flush
          file.fsync
        end
      ensure
        file.flock(File::LOCK_UN)
      end
      CommitResult.new(normalized.map { |_collection, row| row }, activity: committed)
    end

    def fetch(collection, id)
      index.first.dig(collection.to_s, id.to_s)&.dup
    end

    def fetch!(collection, id)
      fetch(collection, id) || raise(NotFound, "#{collection} #{id} was not found")
    end

    # Records that share a timestamp keep the order the log first saw them, so an operator reading
    # `show` sees what actually happened first rather than an arbitrary tiebreak on a random id.
    def list(collection)
      state, order = index
      (state[collection.to_s] || {}).values.sort_by do |record|
        [record["created_at"].to_s, order.fetch([collection.to_s, record.fetch("id")])]
      end
    end

    def find(collection, **fields)
      list(collection).find do |record|
        fields.all? { |key, value| record[key.to_s] == value }
      end
    end

    # See Backstage::Ports::Store#deployment_id. Minting is guarded, so concurrent first use
    # converges on one identity rather than forking the stream.
    def deployment_id
      @deployment_id ||= fetch(DEPLOYMENT_COLLECTION, DEPLOYMENT_ROW)&.fetch("deployment_id") || mint_deployment_id
    end

    # See Backstage::Ports::Store#read_activity for the contract this implements.
    def read_activity(after: nil, filters: {}, limit: 100)
      limit = Activity.check_read_limit!(limit)
      selected = Activity.normalize_filters(filters)
      fingerprint = Activity.filter_fingerprint(selected)
      position, bound_deployment = decode_cursor(after, fingerprint)
      budget = limit * SCAN_MULTIPLIER
      collected = []
      scanned = 0
      cursor_position = position

      state, _order, _bytes, high_water = scan(collect_state: false) do |event|
        sequence = event.fetch("sequence")
        next if sequence <= position
        next if collected.length >= limit || scanned >= budget

        scanned += 1
        cursor_position = sequence
        collected << event if Activity.matches?(event, selected)
      end
      deployment = state[DEPLOYMENT_COLLECTION][DEPLOYMENT_ROW]&.fetch("deployment_id")
      check_cursor_deployment!(bound_deployment, deployment)
      check_cursor_reachable!(position, high_water, deployment)

      {
        "events" => collected,
        "cursor" => after,
        "next_cursor" => encode_cursor(deployment, fingerprint, cursor_position),
        "high_water_mark" => encode_cursor(deployment, fingerprint, high_water),
        "deployment_id" => deployment
      }
    end

    def fetch_activity(event_id)
      wanted = event_id.to_s
      found = nil
      scan(collect_state: false) { |event| found ||= event if event["event_id"] == wanted }
      found
    end

    private

    def sync_directory(directory)
      File.open(directory) { |handle| handle.fsync }
    rescue SystemCallError, IOError, Errno::EINVAL
      nil
    end

    def mint_deployment_id
      record = {
        "schema_version" => 1,
        "id" => DEPLOYMENT_ROW,
        "deployment_id" => Records.id("deployment"),
        "created_at" => Records.timestamp
      }
      commit([[DEPLOYMENT_COLLECTION, record]],
             expect: [{ collection: DEPLOYMENT_COLLECTION, id: DEPLOYMENT_ROW, revision: nil }])
      record.fetch("deployment_id")
    rescue Backstage::ConflictError
      # Another process minted first. There is exactly one stream per store, so adopt it.
      fetch!(DEPLOYMENT_COLLECTION, DEPLOYMENT_ROW).fetch("deployment_id")
    end

    def normalize_activity_event(event, deployment)
      raise ContractError, "activity event must be an object" unless event.is_a?(Hash)

      row = stringify(event)
      raise ContractError, "activity event_id is required" if row["event_id"].to_s.empty?
      raise ContractError, "activity sequence is assigned by the store, not by a caller" unless row["sequence"].nil?
      raise ContractError, "activity recorded_at is assigned by the store, not by a caller" unless row["recorded_at"].nil?

      row.delete("sequence")
      row.delete("recorded_at")
      row["deployment_id"] ||= deployment
      unless row["deployment_id"] == deployment
        raise ContractError, "activity event belongs to deployment #{row["deployment_id"].inspect}, not #{deployment.inspect}"
      end

      @secret_guard.check!(row)
      row
    end

    def stream_started_event(deployment, occurred_at)
      Activity.event(
        type: Activity::STREAM_STARTED,
        deployment_id: deployment,
        occurred_at: occurred_at,
        source: Activity.source(
          adapter: "backstage.adapters.jsonl.store",
          provenance: "store",
          instance: "#{Records.host_name}:#{Process.pid}"
        ),
        summary: "activity stream started for #{deployment}",
        data: { "prior_history" => "none", "imported" => false }
      )
    end

    def check_deployment!(state, deployment)
      current = state[DEPLOYMENT_COLLECTION][DEPLOYMENT_ROW]
      return if current && current["deployment_id"] == deployment

      raise Backstage::ConflictError,
            "activity was prepared for deployment #{deployment.inspect}, which this log does not hold"
    end

    def check_expectations!(expect, current)
      Array(expect).each do |guard|
        collection = guard.fetch(:collection).to_s
        id = guard.fetch(:id).to_s
        record = current.dig(collection, id)
        unless guard.key?(:revision) || guard.key?(:fields)
          raise ContractError, "expectation requires revision or fields"
        end
        if guard.key?(:revision) && guard[:revision].nil?
          raise Backstage::ConflictError, "#{collection} #{id} already exists" if record
          raise ContractError, "absence expectation cannot include fields" if guard.key?(:fields)
          next
        end
        raise Backstage::ConflictError, "#{collection} #{id} was not found" unless record

        fields = stringify(guard.fetch(:fields, {}))
        fields["revision"] = guard[:revision] if guard.key?(:revision)
        fields.each do |field, expected|
          next if record[field] == expected

          raise Backstage::ConflictError,
                "#{collection} #{id} is at #{field} #{record[field].inspect}, not #{expected.inspect}"
        end
      end
    end

    def index
      scan.first(2)
    end

    # A shared-lock read of the whole log. Reads never truncate, never mint, and never write:
    # repairing an interrupted writer's trailing bytes is the next append's job.
    def scan(collect_state: true, &activity_visitor)
      File.open(@path, "rb") do |file|
        file.flock(File::LOCK_SH)
        read_index(file, collect_state: collect_state, &activity_visitor)
      ensure
        file.flock(File::LOCK_UN)
      end
    end

    # Returns [state, order, complete_bytes, high_water]. `collect_state: false` skips materializing
    # record payloads for a caller that only wants activity — deployment identity still comes back,
    # since a cursor is meaningless without it. Every activity event is handed to the visitor in
    # sequence order; nothing accumulates them here, so a page holds only what it collects.
    def read_index(file, collect_state: true)
      state = Hash.new { |hash, key| hash[key] = {} }
      order = {}
      position = 0
      complete_bytes = 0
      high_water = 0
      file.each_line do |line|
        break unless line.end_with?("\n")

        complete_bytes = file.pos
        next if line.strip.empty?

        entry = JSON.parse(line)
        raise ContractError, "state event must be an object" unless entry.is_a?(Hash)

        activity = []
        events = if entry.key?("transaction_version")
                   raise ContractError, "unsupported state transaction version" unless entry["transaction_version"] == 1
                   # A v1 line without activity is a pre-activity transaction and reads unchanged.
                   activity = entry.fetch("activity", [])
                   entry.fetch("events")
                 else
                   [entry] # Existing single-record JSONL logs remain readable.
                 end
        raise ContractError, "transaction events must be an array" unless events.is_a?(Array)
        raise ContractError, "transaction activity must be an array" unless activity.is_a?(Array)

        events.each do |event|
          raise ContractError, "state event must be an object" unless event.is_a?(Hash)
          record = event.fetch("record")
          unless record.is_a?(Hash) && !record["id"].to_s.empty? && event["collection"].is_a?(String)
            raise ContractError, "state event requires a collection and record id"
          end
          key = [event.fetch("collection"), record.fetch("id")]
          next unless collect_state || key.first == DEPLOYMENT_COLLECTION

          state[key.first][key.last] = record
          order[key] ||= (position += 1)
        end

        activity.each do |event|
          unless event.is_a?(Hash) && event["sequence"].is_a?(Integer) && !event["event_id"].to_s.empty?
            raise ContractError, "activity event requires an event id and an integer sequence"
          end
          sequence = event.fetch("sequence")
          raise ContractError, "activity sequence #{sequence} is out of order" unless sequence > high_water

          high_water = sequence
          yield(event) if block_given?
        end
      rescue JSON::ParserError, KeyError, ContractError => error
        raise ContractError, "invalid state event in #{@path}: #{error.message}"
      end
      [state, order, complete_bytes, high_water]
    end

    # Cursors are opaque on purpose: what they carry is this adapter's business, and binding them
    # to the deployment and the filter set is what stops a consumer from resuming into history it
    # never asked for. Base64 without padding keeps them safe in a URL, a shell, or a JSON field.
    def encode_cursor(deployment, fingerprint, position)
      payload = JSON.generate("v" => 1, "d" => deployment, "f" => fingerprint, "p" => position)
      [payload].pack("m0").tr("+/", "-_")
    end

    def decode_cursor(cursor, fingerprint)
      return [0, nil] if cursor.nil?

      payload = begin
        raise ArgumentError, "cursor must be a string" unless cursor.is_a?(String)

        JSON.parse(cursor.tr("-_", "+/").unpack1("m0").to_s)
      rescue StandardError
        raise Backstage::ActivityCursorError.new("activity cursor is not readable", code: "cursor_malformed")
      end
      unless payload.is_a?(Hash) && payload["v"] == 1 && payload["p"].is_a?(Integer) && payload["p"] >= 0
        raise Backstage::ActivityCursorError.new("activity cursor is not readable", code: "cursor_malformed")
      end
      unless payload["f"] == fingerprint
        raise Backstage::ActivityCursorError.new(
          "activity cursor was issued for a different filter set", code: "cursor_filter_mismatch"
        )
      end

      [payload.fetch("p"), payload["d"]]
    end

    def check_cursor_deployment!(bound, current)
      # A cursor from a stream that held no events addresses no history, so it cannot skip any.
      return if bound.nil? || bound == current

      raise Backstage::ActivityCursorError.new(
        "activity cursor belongs to deployment #{bound.inspect}, not #{current.inspect}",
        code: "cursor_deployment_mismatch", deployment_id: current
      )
    end

    # A cursor past the newest committed sequence cannot be resumed from: every page it yields is
    # empty and its next_cursor never reaches a high-water mark that is behind it, so a follower
    # would poll forever believing it was behind. Say so, with the position the stream reaches.
    def check_cursor_reachable!(position, high_water, deployment)
      return if position <= high_water

      raise Backstage::ActivityCursorError.new(
        "activity cursor is at position #{position}, past this deployment's newest event (#{high_water})",
        code: "cursor_ahead_of_stream", deployment_id: deployment, high_water: high_water
      )
    end

    def stringify(value)
      JSON.parse(JSON.generate(value))
    end
  end
end

Backstage::JsonlStore = Backstage::Adapters::Jsonl::Store unless defined?(Backstage::JsonlStore)
