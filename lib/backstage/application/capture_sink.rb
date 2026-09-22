# frozen_string_literal: true

require "json"

module Backstage::Application
  # Where a flushed chunk goes. Framing, redaction and interpretation always happen; only
  # persistence is optional, which is what keeps the null path honest — a test or a dry run
  # exercises the same parsing and the same bounds as production, and differs only in whether
  # anything is written down.
  #
  # A sink is opened once per run and hands out one Stream per captured stream:
  #
  #   stream = sink.open(stream_id:, step:, kind:, phase:, opened_at:, resume: false)
  #   stream.checkpoint          # => the persisted checkpoint to resume from, or nil
  #   stream.durable_chunks      # => [{ "start_offset", "end_offset", "sha256" }, …] already durable
  #   stream.commit(chunk)       # => { "artifact_id" => ..., "reconciled" => bool }
  #   stream.close(summary)      # => { "artifact_ids", "chunk_artifact_ids", "manifest_artifact_id" }
  module CaptureSink
    Activity = Backstage::Domain::Activity
    Recorder = Backstage::Application::ActivityRecorder
    Records = Backstage::Domain::Records
    STREAM_COLLECTION = Backstage::Ports::RuntimeCapture::STREAM_COLLECTION

    # How much of a malformed record is quoted back on its event. Detail belongs in the chunk
    # artifact; the event carries just enough to recognize what went wrong.
    MALFORMED_PREVIEW_BYTES = 200

    # Frames and interprets, persists nothing. Not a stub: it is the correct sink whenever capture
    # should exercise its parsing and bounds without producing durable state.
    class Null
      def open(stream_id:, step:, kind:, phase: nil, opened_at: nil, resume: false)
        Stream.new(stream_id)
      end

      class Stream
        attr_reader :stream_id, :chunks

        def initialize(stream_id)
          @stream_id = stream_id
          @chunks = []
        end

        def checkpoint
          nil
        end

        # Nothing is durable, so a replay can diverge from nothing.
        def durable_chunks
          []
        end

        def commit(chunk)
          @chunks << chunk
          { "artifact_id" => nil, "reconciled" => false }
        end

        def close(_summary)
          { "artifact_ids" => [], "chunk_artifact_ids" => [], "manifest_artifact_id" => nil }
        end
      end
    end

    # Persists chunks and commits their events.
    #
    # The ordering rule is the whole point of this class: a chunk file is fsynced and renamed
    # *before* the commit that references it. A crash between the two leaves an unreferenced file
    # that a cleaner may remove; it can never leave an acknowledged event pointing at a file that
    # was only half written.
    class Durable
      def initialize(store:, artifact_store:, recorder: nil, work_item_id:, run:, attempt: nil,
                     adapter: "backstage.application.runtime_capture")
        @store = store
        @artifact_store = artifact_store
        @recorder = recorder || Recorder.new(store: store, adapter: adapter)
        @work_item_id = work_item_id.to_s
        @run_id = run.is_a?(Hash) ? run.fetch("id").to_s : run.to_s
        @phase = run.is_a?(Hash) ? run["phase"] : nil
        @attempt_id = attempt.is_a?(Hash) ? attempt["id"] : (attempt && attempt.to_s)
      end

      def open(stream_id:, step:, kind:, phase: nil, opened_at: nil, resume: false)
        Stream.new(
          store: @store, artifact_store: @artifact_store, recorder: @recorder,
          work_item_id: @work_item_id, run_id: @run_id, attempt_id: @attempt_id,
          phase: phase || @phase, stream_id: stream_id, step: step, kind: kind,
          opened_at: opened_at || Records.timestamp, resume: resume
        )
      end

      class Stream
        attr_reader :stream_id, :checkpoint, :directory

        def initialize(store:, artifact_store:, recorder:, work_item_id:, run_id:, attempt_id:,
                       phase:, stream_id:, step:, kind:, opened_at:, resume:)
          @store = store
          @recorder = recorder
          @work_item_id = work_item_id
          @run_id = run_id
          @attempt_id = attempt_id
          @phase = phase
          @stream_id = stream_id
          @step = step
          @kind = kind
          @opened_at = opened_at

          existing = store.fetch(STREAM_COLLECTION, stream_id)
          refuse_reopen!(existing) if existing && !resume
          @checkpoint = resume ? existing : nil
          @revision = @checkpoint ? Integer(@checkpoint.fetch("revision")) : 0
          @handle = artifact_store.open_stream(
            work_item_id: work_item_id, run_id: run_id, stream_id: stream_id, kind: kind,
            provenance: provenance, start_offset: @checkpoint ? @checkpoint.fetch("last_offset") : 0,
            chain_sha256: @checkpoint && @checkpoint["chain_sha256"],
            # Chunks below this index have a committed event naming them and their bytes are
            # history. At or above it is at most the orphan a crash between rename and commit
            # leaves behind, which this writer is entitled to rebuild.
            committed_chunks: @checkpoint ? Integer(@checkpoint.fetch("chunk_index")) : 0
          )
          @directory = @handle.directory
          @row = @checkpoint ? @checkpoint.dup : nil
          # Chunk artifact ids are derived from the stream and the index, so a resumed stream can
          # name every chunk its predecessor committed without reading them back. Without this the
          # manifest a resumed close writes would differ from the one before the crash, and the
          # deterministic event describing it would conflict rather than reconcile.
          @artifact_ids = @checkpoint ? (0...Integer(@checkpoint.fetch("chunk_index"))).map { |index| @handle.class.chunk_artifact_id(stream_id, index) } : []
          register unless @checkpoint
        end

        # The byte ranges and digests of the chunks already durable, oldest first, so a resumed
        # writer can tell a replay of its own stream from a different stream replayed into it.
        # Empty when nothing is durable or the chunk artifacts are not readable, which a caller
        # must read as "cannot verify", never as "verified".
        def durable_chunks
          return [] unless @checkpoint

          (0...Integer(@checkpoint.fetch("chunk_index"))).filter_map do |index|
            artifact = @store.fetch("artifacts", @handle.class.chunk_artifact_id(@stream_id, index))
            next unless artifact && artifact["start_offset"] && artifact["sha256"]

            { "start_offset" => Integer(artifact.fetch("start_offset")),
              "end_offset" => Integer(artifact.fetch("end_offset")),
              "sha256" => artifact.fetch("sha256") }
          end
        end

        # One flush: the chunk becomes durable, then one guarded commit carries its artifact
        # record, the advanced checkpoint, and every event derived from it.
        def commit(chunk)
          payload = chunk["payload"]
          appended = payload && !payload.empty? ? @handle.append(payload, index: chunk.fetch("chunk_index")) : nil
          artifact = appended && @handle.chunk_artifact(appended)

          row = advance(chunk, appended)
          writes = []
          writes << ["artifacts", artifact] if artifact
          writes << [STREAM_COLLECTION, row]
          events = build_events(chunk, appended, artifact)

          reconciled = guarded(row, events) { @store.commit(writes, expect: guard, activity: events) }
          @artifact_ids << artifact.fetch("id") if artifact && !@artifact_ids.include?(artifact.fetch("id"))
          { "artifact_id" => artifact && artifact.fetch("id"), "reconciled" => reconciled,
            "chunk" => appended }
        end

        # Writes the manifest, then commits it with the closed checkpoint. `artifact.available` is
        # the event that makes the manifest findable; without it the stream's index would be a file
        # on disk that no history refers to.
        def close(summary)
          manifest = @handle.finalize(summary.merge("artifact_ids" => @artifact_ids))
          row = closed_row(summary)
          event = @recorder.event(
            type: "artifact.available",
            event_id: Recorder.event_id("artifact.available", @stream_id, "manifest"),
            provenance: "core",
            occurred_at: summary["closed_at"],
            work_item_id: @work_item_id, run_id: @run_id, attempt_id: @attempt_id,
            offset: "#{@stream_id}:manifest",
            summary: "captured stream #{@stream_id} closed #{summary["coverage"]} " \
                     "after #{summary["chunks"]} chunks and #{summary["records"]} records",
            data: {
              "stream_id" => @stream_id, "step" => @step, "phase" => @phase, "kind" => @kind,
              "coverage" => summary["coverage"], "bytes" => summary["bytes"],
              "records" => summary["records"], "chunks" => summary["chunks"],
              "malformed" => summary["malformed"], "last_offset" => summary["last_offset"],
              "path" => manifest.fetch("path"), "sha256" => manifest.fetch("sha256")
            }.compact,
            artifact_refs: [manifest.fetch("id")]
          )
          guarded(row, [event]) do
            @store.commit([["artifacts", manifest], [STREAM_COLLECTION, row]],
                          expect: guard, activity: [event])
          end
          { "artifact_ids" => @artifact_ids + [manifest.fetch("id")],
            "chunk_artifact_ids" => @artifact_ids.dup,
            "manifest_artifact_id" => manifest.fetch("id") }
        end

        private

        # A stream id is a durable position, not a name to be reused. Opening one that already has
        # a row without `resume: true` would hand out a handle at offset zero over chunk files an
        # acknowledged event already names, so the two writers would fork two positions over one
        # set of files. The only tolerated case is a row that is still exactly its registration —
        # nothing durable, nothing acknowledged — which is the harmless race of two writers
        # registering the same stream at once, and adopting that costs nothing.
        def refuse_reopen!(existing)
          return if fresh_registration?(existing)

          raise Backstage::CaptureError.new(
            "stream #{@stream_id} already has durable state at offset #{existing["last_offset"]}; " \
            "open it with resume: true",
            stream_id: @stream_id, offset: existing["last_offset"]
          )
        end

        def fresh_registration?(row)
          row["closed_at"].nil? && Integer(row["last_offset"] || 0).zero? &&
            Integer(row["chunk_index"] || 0).zero? && Integer(row["records"] || 0).zero?
        end

        def provenance
          { "adapter" => "backstage.application.runtime_capture", "stream_id" => @stream_id,
            "step" => @step, "captured_at" => @opened_at }.compact
        end

        # The checkpoint exists before the first byte is captured, so recovery can find a stream
        # that produced output and was never closed rather than inferring it from silence.
        def register
          @row = {
            "schema_version" => 1, "id" => @stream_id, "run_id" => @run_id,
            "attempt_id" => @attempt_id, "work_item_id" => @work_item_id, "step" => @step,
            "phase" => @phase, "kind" => @kind, "revision" => 0, "opened_at" => @opened_at,
            "closed_at" => nil, "coverage" => "complete", "last_offset" => 0, "record_index" => 0,
            "framed_offset" => 0,
            "chunk_index" => 0, "bytes" => 0, "records" => 0, "chunks" => 0, "malformed" => 0,
            "truncated_records" => 0, "truncated_record_offset" => nil,
            "bytes_dropped" => 0, "framer_state" => nil,
            "interpreter_state" => nil, "chunk_sha256" => nil, "chain_sha256" => nil,
            "provider_session_id" => nil, "sentinels" => {}, "artifact_dir" => @handle.directory
          }
          @store.commit([[STREAM_COLLECTION, @row]],
                        expect: [{ collection: STREAM_COLLECTION, id: @stream_id, revision: nil }])
        rescue Backstage::ActivityConflictError
          raise
        rescue Backstage::ConflictError
          # Another writer registered this exact stream id between the fetch above and this write.
          # Adopting is only safe while that row is still nothing but a registration; anything
          # further along is durable state this writer's offsets know nothing about.
          @row = @store.fetch(STREAM_COLLECTION, @stream_id)
          refuse_reopen!(@row) if @row
          @revision = Integer(@row.fetch("revision"))
        end

        def guard
          [{ collection: STREAM_COLLECTION, id: @stream_id, revision: @revision }]
        end

        # Applies a commit, tolerating the one conflict that is not a conflict: an in-process retry
        # whose first attempt actually landed. The store reconciles identical events by id; this
        # reconciles the guarded record write the same way, by checking that what is stored is
        # exactly what this call would have written.
        #
        # Two things it will not do.
        #
        # It never swallows an ActivityConflictError. That one says a producer minted two different
        # facts under one event id; no retry resolves it, and the slice-1 rule in errors.rb binds
        # every retry and swallow site to re-raise it.
        #
        # And it never concludes "this landed" from the record row alone. The store checks record
        # expectations *before* activity fingerprints, so a reused event id can surface as a plain
        # ConflictError raised by a stale guard, with the row equal by coincidence — the checkpoint
        # is derived from the same chunk, so a retry rebuilds it byte for byte. Believing the row
        # would drop every event in the commit silently. So the events are verified against the
        # stream: each must be present under its deterministic id with the same canonical
        # fingerprint, or this is not the retry it looks like.
        def guarded(row, events = [])
          yield
          adopt(row)
          false
        rescue Backstage::ActivityConflictError
          raise
        rescue Backstage::ConflictError => error
          stored = @store.fetch(STREAM_COLLECTION, @stream_id)
          raise unless stored && stored == JSON.parse(JSON.generate(row))

          verify_activity!(events, error)
          adopt(stored)
          true
        end

        # Every event this commit carried must already be in the stream, telling the same fact.
        def verify_activity!(events, error)
          Array(events).each do |event|
            id = event.fetch("event_id")
            stored = @store.fetch_activity(id)
            if stored.nil?
              raise Backstage::CaptureError.new(
                "capture of #{@stream_id} could not reconcile: event #{id} was never committed " \
                "(#{error.class}: #{error.message})",
                stream_id: @stream_id, offset: @row && @row["last_offset"], cause: error
              )
            end
            next if Activity.canonical_fingerprint(stored) == Activity.canonical_fingerprint(event)

            raise Backstage::ActivityConflictError.new(
              "activity event #{id} of stream #{@stream_id} is stored with different content",
              event_id: id
            )
          end
        end

        # Position advances only when a commit has actually landed, so a retry rebuilds exactly
        # the same row rather than stacking a second revision on top of a write that never went in.
        def adopt(row)
          @row = row
          @revision = Integer(row.fetch("revision"))
        end

        def advance(chunk, appended)
          base = @row || @store.fetch(STREAM_COLLECTION, @stream_id)
          base.merge(
            "revision" => Integer(base.fetch("revision")) + 1,
            "coverage" => chunk.fetch("coverage"),
            "last_offset" => appended ? appended.fetch("end_offset") : base.fetch("last_offset"),
            "record_index" => chunk.fetch("record_index"),
            # How far into the *input* records have been committed. It equals last_offset while
            # every record is durable, and runs ahead of it once a byte limit is dropping bytes
            # whose records still commit. A resumed writer replays from last_offset and must not
            # tell those records again.
            "framed_offset" => chunk.fetch("framed_offset"),
            "chunk_index" => appended ? appended.fetch("index") + 1 : base.fetch("chunk_index"),
            "bytes" => base.fetch("bytes") + (appended ? appended.fetch("bytes") : 0),
            "records" => base.fetch("records") + chunk.fetch("records"),
            "chunks" => base.fetch("chunks") + (appended ? 1 : 0),
            "malformed" => base.fetch("malformed") + chunk.fetch("malformed"),
            "truncated_records" => base.fetch("truncated_records") + chunk.fetch("truncated_records"),
            # Where the record bound first cut a record. It is a position in the stream, not a
            # running total, so a resumed close still reports the offset a reader has to go and look
            # at rather than only the count of how many times it happened.
            "truncated_record_offset" => base["truncated_record_offset"] || chunk["truncated_record_offset"],
            "bytes_dropped" => chunk.fetch("bytes_dropped"),
            "framer_state" => chunk["framer_state"],
            "interpreter_state" => chunk["interpreter_state"],
            "chunk_sha256" => appended ? appended.fetch("sha256") : base["chunk_sha256"],
            "chain_sha256" => appended ? appended.fetch("chain_sha256") : base["chain_sha256"],
            "provider_session_id" => chunk["provider_session_id"] || base["provider_session_id"],
            "sentinels" => chunk["sentinels"] || base["sentinels"] || {},
            "updated_at" => chunk["occurred_at"]
          )
        end

        def closed_row(summary)
          base = @row || @store.fetch(STREAM_COLLECTION, @stream_id)
          base.merge(
            "revision" => Integer(base.fetch("revision")) + 1,
            "closed_at" => summary.fetch("closed_at"),
            "coverage" => summary.fetch("coverage"),
            "close_reason" => summary["reason"],
            "provider_session_id" => summary["provider_session_id"] || base["provider_session_id"],
            "updated_at" => summary.fetch("closed_at")
          )
        end

        # Every record counted in the checkpoint is named by an event. A flush that persisted no
        # bytes — past a byte limit, where framing and interpretation carry on — still coalesces
        # into one bounded `runtime.observed` carrying the record counts and zero bytes, because
        # records counted in the row and described by nothing are a hole in the history.
        def build_events(chunk, appended, artifact)
          events = []
          events << observed_event(chunk, appended, artifact) if appended || chunk.fetch("records").positive? || chunk.fetch("reason") == "limit"
          events << record_fault_event(chunk, artifact) if chunk.fetch("malformed").positive? || chunk.fetch("truncated_records").positive?
          Array(chunk["observations"]).each do |observation|
            next if observation["malformed"]
            # Metadata, not history: an observation with no type names a session or carries a
            # sentinel, both of which are already applied to the stream row and the close summary.
            next unless observation["type"]

            events << semantic_event(observation, artifact, chunk)
          end
          events
        end

        def observed_event(chunk, appended, artifact)
          start_offset = appended ? appended.fetch("start_offset") : chunk.fetch("start_offset")
          end_offset = appended ? appended.fetch("end_offset") : chunk.fetch("end_offset")
          first_record = chunk.fetch("record_index") - chunk.fetch("records")
          data = {
            "stream_id" => @stream_id, "step" => @step, "phase" => @phase,
            "chunk_index" => chunk.fetch("chunk_index"),
            "start_offset" => start_offset, "end_offset" => end_offset,
            "bytes" => appended ? appended.fetch("bytes") : 0,
            "records" => chunk.fetch("records"), "record_index" => chunk.fetch("record_index"),
            "malformed" => chunk.fetch("malformed"),
            "truncated_records" => chunk.fetch("truncated_records"),
            "sha256" => appended && appended.fetch("sha256"),
            "coverage" => chunk.fetch("coverage"), "reason" => chunk.fetch("reason")
          }.compact
          data["truncated_from_offset"] = chunk["truncated_from_offset"] if chunk["truncated_from_offset"]
          data["bytes_dropped"] = chunk.fetch("bytes_dropped") if chunk.fetch("bytes_dropped").positive?

          @recorder.event(
            type: "runtime.observed",
            # A byte range identifies a chunk that has bytes. A byte-less flush has only its record
            # range — every one of them starts and ends at the same offset, so a byte-range id
            # would name every post-limit flush of a stream identically.
            event_id: if appended
                        Recorder.event_id("runtime.observed", @stream_id, start_offset, end_offset)
                      else
                        Recorder.event_id("runtime.observed", @stream_id, "records", first_record,
                                          chunk.fetch("record_index"))
                      end,
            provenance: "runtime_reported",
            occurred_at: chunk["occurred_at"],
            work_item_id: @work_item_id, run_id: @run_id, attempt_id: @attempt_id,
            offset: appended ? "#{@stream_id}:#{end_offset}" : "#{@stream_id}:records:#{chunk.fetch("record_index")}",
            summary: observed_summary(chunk, appended),
            data: data,
            artifact_refs: artifact && [artifact.fetch("id")]
          )
        end

        def observed_summary(chunk, appended)
          bytes = appended ? appended.fetch("bytes") : 0
          first = chunk.fetch("record_index") - chunk.fetch("records")
          "captured #{format("%.1f", bytes / 1024.0)} KiB of #{@step} output for #{@run_id} " \
            "(records #{first}-#{chunk.fetch("record_index")}, #{chunk.fetch("reason")})"
        end

        # A record this chunk could not tell whole: unreadable, cut at the record bound, or both.
        # Nothing is dropped — the first affected record is quoted back within a hard bound, the
        # rest are counted, and the bytes are in the chunk artifact regardless.
        #
        # One event per chunk, not one per kind of fault. A record over `max_record_bytes` is
        # usually also unparseable, and two events naming the same record would double-report it as
        # surely as zero events would lose it. So both counts, both offsets and both flags ride on
        # a single report, keyed on the first affected record.
        def record_fault_event(chunk, artifact)
          malformed = chunk.fetch("malformed")
          truncated = chunk.fetch("truncated_records")
          preview = chunk["malformed_preview"] || chunk["truncated_preview"] || {}
          # Keyed on the first affected record, not the chunk index: a chunk index does not advance
          # for a flush that persisted no bytes, so several post-limit flushes would all claim the
          # same id. The `malformed` key is kept for a chunk with unreadable records so ids already
          # committed for one keep matching on a replay.
          mark = preview["record_index"] || "chunk:#{chunk.fetch("chunk_index")}"
          kind = malformed.positive? ? "malformed" : "truncated"
          @recorder.event(
            type: "runtime.observed",
            event_id: Recorder.event_id("runtime.observed", @stream_id, kind, mark),
            provenance: "runtime_reported",
            occurred_at: chunk["occurred_at"],
            work_item_id: @work_item_id, run_id: @run_id, attempt_id: @attempt_id,
            offset: "#{@stream_id}:#{kind}:#{mark}",
            summary: fault_summary(malformed, truncated, chunk),
            data: {
              "stream_id" => @stream_id, "step" => @step, "phase" => @phase,
              "chunk_index" => chunk.fetch("chunk_index"),
              "malformed" => malformed.positive? || nil,
              "malformed_records" => malformed,
              "truncated" => truncated.positive? || nil,
              "truncated_records" => truncated,
              "truncated_record_offset" => chunk["truncated_record_offset"],
              "offset" => preview["offset"], "preview" => preview["preview"],
              "coverage" => chunk.fetch("coverage")
            }.compact,
            artifact_refs: artifact && [artifact.fetch("id")]
          )
        end

        def fault_summary(malformed, truncated, chunk)
          parts = []
          parts << "#{malformed} unreadable record(s)" if malformed.positive?
          if truncated.positive?
            parts << "#{truncated} record(s) cut at the record limit, " \
                     "first at offset #{chunk["truncated_record_offset"]}"
          end
          "#{parts.join(" and ")} in #{@step} output for #{@run_id}"
        end

        # When a fact happened, in order of authority: the provider's own time, then the time the
        # record was framed, and only then the flush it was committed in. The middle one matters —
        # the flush time is a property of chunking, so an interpreter with no provider time (the
        # sentinel reader is the standing example) stamped a repository publication with the moment
        # Backstage happened to flush, and the same output split into different chunks produced a
        # different `occurred_at` and therefore a different fingerprint for the same fact. Framing
        # is a function of the bytes alone. Nothing here reads the wall clock at envelope-build
        # time: two builds of the same chunk must be the same fact, because `occurred_at` is inside
        # the fingerprint that decides whether a retry reconciles or is refused.
        def semantic_event(observation, artifact, chunk)
          type = observation.fetch("type")
          @recorder.event(
            type: type,
            event_id: Recorder.event_id(type, @stream_id, observation.fetch("record_index")),
            provenance: observation["provenance"] || "agent_reported",
            occurred_at: observation["occurred_at"] || observation["observed_at"] || chunk["occurred_at"],
            work_item_id: @work_item_id, run_id: @run_id, attempt_id: @attempt_id,
            offset: "#{@stream_id}:#{observation.fetch("record_index")}",
            provider_event_id: observation["provider_event_id"],
            provider_session_id: observation["provider_session_id"],
            summary: observation["summary"],
            data: (observation["data"] || {}).merge(
              "stream_id" => @stream_id, "step" => @step,
              "record_index" => observation.fetch("record_index")
            ),
            artifact_refs: artifact && [artifact.fetch("id")]
          )
        end
      end
    end
  end
end
