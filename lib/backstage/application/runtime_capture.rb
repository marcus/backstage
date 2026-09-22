# frozen_string_literal: true

require "digest"
require "json"
require "time"

module Backstage::Application
  # Incremental, bounded, durable capture of a runtime's output.
  #
  # The order of operations is the design, not an implementation detail:
  #
  #   bytes -> redact -> frame -> interpret -> buffer -> durable chunk -> one guarded commit
  #
  # Redaction runs first so that a byte offset addresses the bytes that actually get persisted;
  # if it ran later, every offset would index text nobody kept. Framing runs on the redacted
  # stream for the same reason. Interpretation runs on complete records only. And a chunk is on
  # the disk, fsynced and renamed, before the commit that references it — so a crash can leave an
  # unreferenced file, never an acknowledged event pointing at half a file.
  #
  # Nothing here starts a thread and nothing sleeps. A runtime that reads its pipe on a reader
  # thread hands the bytes to whichever thread owns the writer; time-based flushing comes from an
  # injected clock, which is what makes the 250 ms boundary exactly testable.
  class RuntimeCapture < Backstage::Ports::RuntimeCapture
    RecordFramer = Backstage::Support::RecordFramer
    SecretGuard = Backstage::Support::SecretGuard
    Interpreter = Backstage::Ports::StreamInterpreter

    FLUSH_BYTES = 64 * 1024
    FLUSH_MILLIS = 250
    # Records a writer may hold in flight before it flushes them, whether or not any bytes go with
    # them. Past a byte limit nothing fills the buffer and nothing starts the flush timer, so
    # without a record bound the in-flight list would grow one entry per line for as long as the
    # runtime keeps printing.
    FLUSH_RECORDS = 1000
    MAX_STREAM_BYTES = 16 * 1024 * 1024
    MAX_RUN_BYTES = 64 * 1024 * 1024
    # What one record of a *protocol* stream may be before the framer caps it — see
    # Ports::StreamInterpreter#protocol?. It is deliberately generous and still bounded: a coding
    # agent reading a file routinely produces a tool result of hundreds of kilobytes, and a record
    # cut in the middle of that is not a shortened statement but an unreadable one. A writer holds
    # at most this much pending text plus this much retained text, so the memory bound stays a
    # bound; it is only a much larger one than a shell step is allowed.
    MAX_PROTOCOL_RECORD_BYTES = 8 * 1024 * 1024

    # Every bound a deployment may set, with the value it takes when it does not. Configuration
    # reads this so that `config check` reports the policy a run actually gets rather than only the
    # keys someone happened to override.
    POLICY_DEFAULTS = {
      "flush_bytes" => FLUSH_BYTES,
      "flush_millis" => FLUSH_MILLIS,
      "flush_records" => FLUSH_RECORDS,
      "max_stream_bytes" => MAX_STREAM_BYTES,
      "max_run_bytes" => MAX_RUN_BYTES,
      "max_record_bytes" => RecordFramer::DEFAULT_MAX_RECORD_BYTES,
      "max_protocol_record_bytes" => MAX_PROTOCOL_RECORD_BYTES
    }.freeze

    attr_reader :run_id, :run_bytes, :streams, :max_record_bytes, :max_protocol_record_bytes

    def initialize(sink:, clock:, run:, attempt: nil, secret_guard: SecretGuard.new, phase: nil,
                   flush_bytes: FLUSH_BYTES, flush_millis: FLUSH_MILLIS,
                   flush_records: FLUSH_RECORDS,
                   max_stream_bytes: MAX_STREAM_BYTES, max_run_bytes: MAX_RUN_BYTES,
                   max_record_bytes: RecordFramer::DEFAULT_MAX_RECORD_BYTES,
                   max_protocol_record_bytes: MAX_PROTOCOL_RECORD_BYTES)
      @sink = sink
      @clock = clock
      @secret_guard = secret_guard
      @flush_bytes = Integer(flush_bytes)
      @flush_millis = Integer(flush_millis)
      @flush_records = Integer(flush_records)
      raise Backstage::ContractError, "flush_records must be positive" unless @flush_records.positive?
      @max_stream_bytes = Integer(max_stream_bytes)
      @max_run_bytes = Integer(max_run_bytes)
      @max_record_bytes = Integer(max_record_bytes)
      @max_protocol_record_bytes = Integer(max_protocol_record_bytes)
      raise Backstage::ContractError, "flush_bytes must be positive" unless @flush_bytes.positive?

      @run_id = run.is_a?(Hash) ? run.fetch("id").to_s : run.to_s
      @phase = phase || (run.is_a?(Hash) ? run["phase"] : nil)
      @attempt_number = if attempt.is_a?(Hash) then Integer(attempt.fetch("number"))
                        elsif attempt.nil? then 1
                        else Integer(attempt)
                        end
      @index = 0
      @run_bytes = 0
      @streams = []
    end

    # See Backstage::Ports::RuntimeCapture#open. `phase` overrides the capture's own phase for one
    # stream, because a phase runner opens streams across context, implementation, finalize and
    # publish inside a single run and the checkpoint and every observation carry the phase.
    # `max_record_bytes` overrides the record bound for this one stream; nil takes the policy's,
    # which is `max_protocol_record_bytes` when the interpreter says the stream is a protocol.
    def open(step:, phase: nil, kind: "runtime_output", interpreter: nil, resume: false,
             max_record_bytes: nil)
      stream_id = "#{@run_id}:#{@attempt_number}:#{@index}:#{step}"
      @index += 1
      opened_at = timestamp
      stream_phase = phase || @phase
      reader = interpreter || Interpreter.new
      sink_stream = @sink.open(stream_id: stream_id, step: step, kind: kind, phase: stream_phase,
                               opened_at: opened_at, resume: resume)
      writer = StreamWriter.new(
        owner: self, sink_stream: sink_stream, clock: @clock, secret_guard: @secret_guard,
        id: stream_id, step: step, kind: kind, phase: stream_phase, opened_at: opened_at,
        interpreter: reader, flush_bytes: @flush_bytes,
        flush_millis: @flush_millis, flush_records: @flush_records,
        max_record_bytes: max_record_bytes ? Integer(max_record_bytes) : record_bound_for(reader)
      )
      @streams << writer
      writer
    end

    # The record bound this policy gives a stream, decided by what the stream *is*. See
    # Ports::StreamInterpreter#protocol?.
    def record_bound_for(interpreter)
      interpreter.respond_to?(:protocol?) && interpreter.protocol? ? @max_protocol_record_bytes : @max_record_bytes
    end

    # How many of `bytes` this stream may still persist, charging the run's shared budget for what
    # it grants. Returning less than asked is how a limit takes effect: byte persistence stops,
    # framing and interpretation do not, because a decision or an effect is never discarded as
    # debug noise.
    def reserve(bytes, stream_bytes:)
      room = [bytes, @max_stream_bytes - stream_bytes, @max_run_bytes - @run_bytes].min
      room = 0 if room.negative?
      @run_bytes += room
      room
    end

    # A summary of every stream this run opened, for the run's `capture` block.
    #
    # A stream that was never closed is included with coverage `open` rather than dropped. That is
    # exactly the case recovery has to reason about — a worker killed mid-stream leaves a writer
    # that produced output and never said how much of it was durable — and a run block that simply
    # omits it would report the same shape as a run that never opened the stream at all.
    def summaries
      @streams.map { |writer| writer.summary || writer.open_summary }
    end

    def timestamp
      @clock.now.utc.iso8601(6)
    end

    # One stream. See Backstage::Ports::RuntimeCapture::StreamWriter for the contract.
    class StreamWriter < Backstage::Ports::RuntimeCapture::StreamWriter
      MALFORMED_PREVIEW_BYTES = CaptureSink::MALFORMED_PREVIEW_BYTES

      attr_reader :id, :step, :kind, :phase, :coverage, :summary

      def initialize(owner:, sink_stream:, clock:, secret_guard:, id:, step:, kind:, phase:,
                     opened_at:, interpreter:, flush_bytes:, flush_millis:, flush_records:,
                     max_record_bytes:)
        @owner = owner
        @sink = sink_stream
        @clock = clock
        @id = id
        @step = step
        @kind = kind
        @phase = phase
        @opened_at = opened_at
        @flush_bytes = flush_bytes
        @flush_millis = flush_millis
        @flush_records = flush_records
        # What the in-flight record list may hold. A record straddling a chunk boundary keeps its
        # text after its first bytes are flushed, so the ceiling is one flush plus one record —
        # setting it any lower would cut ordinary chunks short to enforce a bound the persisting
        # path reaches anyway.
        @retained_limit = flush_bytes + max_record_bytes

        checkpoint = sink_stream.checkpoint
        @redactor = secret_guard.redactor
        state = checkpoint && checkpoint["framer_state"]
        @framer = RecordFramer.restore(state, max_record_bytes: max_record_bytes)
        # The checkpoint framer. It consumes only the bytes that have actually become durable, so
        # its state describes exactly the prefix `last_offset` names — never a byte more. Framing
        # the whole buffer into one framer and checkpointing that is what shifted record indices
        # across a resume, because the state ran ahead of the offset it was stored beside.
        @durable_framer = RecordFramer.restore(state, max_record_bytes: max_record_bytes)
        @interpreter = if checkpoint && checkpoint["interpreter_state"]
                         interpreter.class.restore(checkpoint["interpreter_state"])
                       else
                         interpreter
                       end

        # Offset skip is the cross-process dedupe: a provider that replays from byte zero
        # contributes nothing already durable.
        @skip_remaining = checkpoint ? Integer(checkpoint.fetch("last_offset")) : 0
        @persisted_offset = @skip_remaining
        @verifier = checkpoint ? PrefixVerifier.new(sink_stream.durable_chunks) : nil
        @chunk_index = checkpoint ? Integer(checkpoint.fetch("chunk_index")) : 0
        @records = checkpoint ? Integer(checkpoint.fetch("records")) : 0
        @record_index = checkpoint ? Integer(checkpoint.fetch("record_index")) : 0
        # How far into the input records have already been committed. Past a byte limit that runs
        # ahead of last_offset, and a replay must not tell those records a second time.
        @framed_offset = checkpoint ? Integer(checkpoint["framed_offset"] || checkpoint.fetch("last_offset")) : 0
        @chunks = checkpoint ? Integer(checkpoint.fetch("chunks")) : 0
        @malformed = checkpoint ? Integer(checkpoint.fetch("malformed")) : 0
        @truncated_records = checkpoint ? Integer(checkpoint.fetch("truncated_records")) : 0
        @bytes_dropped = checkpoint ? Integer(checkpoint.fetch("bytes_dropped")) : 0
        @provider_session_id = checkpoint && checkpoint["provider_session_id"]
        # Sentinels are facts the stream produced, not a running total, so they belong in the
        # checkpoint too: a resumed close that reported none would tell a phase runner the
        # repository was never prepared.
        @sentinels = (checkpoint && checkpoint["sentinels"]) || {}
        @coverage = checkpoint ? checkpoint.fetch("coverage") : "complete"
        # Where the first record too large for `max_record_bytes` began. It is the inspectable half
        # of the record bound: the count says how many were cut, this says where reading the stream
        # first stopped meaning what the runtime printed.
        @truncated_record_offset = checkpoint && checkpoint["truncated_record_offset"]
        # `truncated` coverage has two causes and only one of them stops persisting bytes. A byte
        # limit does, and always leaves `bytes_dropped` behind; a record over the record bound does
        # not — its bytes are all durable, it is only the record that was cut. Reading persistence
        # off the coverage word alone silently stopped a resumed stream from ever writing again.
        @persisting = @coverage == "complete" || (@coverage == "truncated" && @bytes_dropped.zero?)

        @buffer = +"".b
        @flush_started_at = nil
        @pending = []
        @retained_bytes = 0
        @artifact_ids = []
        @limit_pending = false
        @truncated_from_offset = nil
        @failed = false
        @closed = false
        @summary = nil
      end

      # Bytes arrive in whatever size the runtime's pipe hands them over, which is not a bound this
      # writer may inherit: a single multi-megabyte read would otherwise sit in the buffer whole.
      # They are absorbed up to the next size-flush boundary at a time, so the buffer never exceeds
      # one flush and a *size* flush ends at `last_offset + flush_bytes` whatever the reader
      # delivered.
      #
      # That is as far as reproducibility goes, and it is worth being exact about: a semantic,
      # records or time flush cuts wherever the reader's read sizes or the drain loop's poll
      # happened to land, so two runs over the same output can chunk it differently. What does not
      # move is what is derived from the *records* — the framing, the record indices, and the
      # semantic event ids built from them — which is what makes a resumed stream continue one
      # history rather than start a second. A chunk's own event id is its byte range, so a rebuilt
      # chunk simply gets the id of the range it actually covers.
      def write(bytes)
        raise Backstage::CaptureError.new("stream #{@id} already failed", stream_id: @id, offset: @persisted_offset) if @failed
        raise Backstage::CaptureError.new("stream #{@id} is closed", stream_id: @id, offset: @persisted_offset) if @closed

        released = skip(@redactor.push(bytes))
        position = 0
        while position < released.bytesize
          slice = released.byteslice(position, [@flush_bytes - @buffer.bytesize, 1].max)
          position += slice.bytesize
          absorb(slice)
          flush_ready
        end
        flush_ready if released.empty?
        nil
      end

      def buffered_bytes
        @buffer.bytesize
      end

      def pending_bytes
        @framer.pending_bytes
      end

      # Records framed, interpreted, and not yet part of a commit. Bounded by `flush_records`.
      def retained_frames
        @pending.length
      end

      # The text those records hold. Bounded by `flush_bytes` plus the one record that crossed it.
      def retained_bytes
        @retained_bytes
      end

      # Bytes the redactor is withholding so a secret cannot slip through a chunk boundary.
      def held_bytes
        @redactor.held_bytes
      end

      def tick
        return nil if @failed || @closed

        flush("time") if timed_out?
        nil
      end

      def failed?
        @failed
      end

      def closed?
        @closed
      end

      # The summary of a stream that is still open. Its coverage is `open`: what is durable is
      # known exactly, what the runtime may still produce is not, and only a reader who knows the
      # run is over — recovery — may turn that into a `gap`.
      def open_summary
        build_summary(reason: "open", coverage: @failed ? "failed" : "open", closed_at: nil)
      end

      # See the port. Returns the Summary hash, and is safe to call twice.
      def close(reason: "close")
        return @summary if @closed

        if @failed
          @closed = true
          return @summary = build_summary(reason: reason, coverage: "failed")
        end

        begin
          absorb(skip(@redactor.finish))
          @framer.finish.each { |frame| record(frame) }
          flush(reason)
          @closed = true
          summary = build_summary(reason: reason, coverage: @coverage)
          closed = @sink.close(summary)
          @artifact_ids = Array(closed["artifact_ids"])
          @summary = summary.merge(
            "artifact_ids" => @artifact_ids,
            "chunk_artifact_ids" => Array(closed["chunk_artifact_ids"]),
            "manifest_artifact_id" => closed["manifest_artifact_id"]
          ).compact
        rescue Backstage::CaptureError
          @closed = true
          @summary = build_summary(reason: reason, coverage: "failed")
          raise
        rescue StandardError => error
          # The manifest commit failing is the same class of failure as a chunk commit failing, and
          # a caller that rescues CaptureError around capture must not have to also know which
          # store error a particular adapter raises.
          @failed = true
          @closed = true
          @summary = build_summary(reason: reason, coverage: "failed")
          raise Backstage::CaptureError.new(
            "capture of #{@id} failed closing at offset #{@persisted_offset}: #{error.message}",
            stream_id: @id, offset: @persisted_offset, cause: error
          )
        end
      end

      private

      # Discards replayed bytes already durable, checking on the way past that they are the bytes
      # that are durable. Redaction is deterministic, so a genuine replay of this stream is
      # byte-identical to the one the checkpoint counted; anything else is a different stream being
      # fed into this one's offsets, and continuing would splice two runtimes into one artifact.
      def skip(bytes)
        return bytes if @skip_remaining.zero? || bytes.empty?

        drop = [@skip_remaining, bytes.bytesize].min
        verify_replay(bytes.byteslice(0, drop))
        @skip_remaining -= drop
        bytes.byteslice(drop, bytes.bytesize - drop) || +"".b
      end

      def verify_replay(replayed)
        divergent = @verifier&.push(replayed)
        return if divergent.nil?

        @failed = true
        @coverage = "failed"
        raise Backstage::CaptureError.new(
          "replayed bytes for #{@id} diverge from the durable stream at offset #{divergent}",
          stream_id: @id, offset: @persisted_offset
        )
      end

      # Frames and interprets everything; buffers only what the byte budget still allows.
      def absorb(redacted)
        return if redacted.nil? || redacted.empty?

        allowed = @persisting ? @owner.reserve(redacted.bytesize, stream_bytes: persisted_bytes) : 0
        if allowed.positive?
          @flush_started_at ||= @clock.now
          @buffer << redacted.byteslice(0, allowed)
        end
        if allowed < redacted.bytesize
          @bytes_dropped += redacted.bytesize - allowed
          if @persisting
            @persisting = false
            @coverage = "truncated"
            @truncated_from_offset = @persisted_offset + @buffer.bytesize
            @limit_pending = true
          end
        end
        # The bound is enforced inside the loop, not after it: one slice of a stream of very short
        # lines is thousands of records, and a list that is only measured between writes is not
        # bounded at all.
        @framer.push(redacted).each do |frame|
          record(frame)
          flush("records") if overfull?
        end
      end

      # One framed record, with whatever the interpreter made of it. Observations are stamped with
      # the record's durable position so their event ids come from the stream, not from a counter
      # that restarts.
      def record(frame)
        # A record the checkpoint says was already committed. Only reachable on a resume past a
        # byte limit, where records commit without their bytes: the framer must still consume them
        # to keep the numbering, but telling them again would be duplicate history.
        return if frame.fetch("end_offset") <= @framed_offset

        @flush_started_at ||= @clock.now
        note_truncated(frame)
        # When the record was framed, which is a function of the bytes and nothing else. An
        # interpreter that reports no provider time of its own is stamped with this rather than with
        # the commit time of whichever chunk the record landed in, so the same output framed into
        # different chunks produces the same fact.
        observed_at = @owner.timestamp
        observations = Array(@interpreter.observe(frame)).map do |observation|
          row = stringify(observation)
          row["record_index"] = frame.fetch("index")
          row["stream_id"] = @id
          row["observed_at"] = observed_at
          @provider_session_id = row["provider_session_id"] if row["provider_session_id"]
          collect_sentinel(row)
          row
        end
        malformed = frame.fetch("encoding") == "replaced" || observations.any? { |row| row["malformed"] }
        @pending << { "frame" => frame, "observations" => observations, "malformed" => malformed }
        @retained_bytes += frame.fetch("text").bytesize
      end

      # A record the framer had to cut at `max_record_bytes`. Its bytes are durable and counted, but
      # what the runtime meant by that record is not fully known, so the stream stops claiming
      # `complete` coverage and remembers where reading it first went wrong. Coverage is only ever
      # lowered here: a stream already `truncated` by a byte limit, or `failed`, keeps its answer.
      def note_truncated(frame)
        return unless frame.fetch("truncated")

        @truncated_record_offset ||= frame.fetch("start_offset")
        @coverage = "truncated" if @coverage == "complete"
      end

      # Whether the in-flight record list has reached its bound. Past a byte limit this is the only
      # thing that flushes: the buffer never fills because nothing is persisted.
      def overfull?
        @pending.length >= @flush_records || @retained_bytes >= @retained_limit
      end

      def collect_sentinel(observation)
        sentinel = observation["sentinel"]
        return unless sentinel.is_a?(Hash) && sentinel["name"]

        @sentinels[sentinel.fetch("name")] = sentinel["payload"]
      end

      # A flush happens on the first of: a full buffer, a semantic observation, a limit taking
      # effect, or the flush interval elapsing. Semantics go first — a decision waiting on a buffer
      # that may never fill is the failure this whole design exists to avoid.
      def flush_ready
        # Only an observation that becomes an event is worth cutting a chunk short for. Metadata —
        # a session id, a sentinel — is applied to the stream and rides out with the next flush.
        return flush("semantic") if @pending.any? { |entry| entry["observations"].any? { |row| row["type"] } }
        return flush("limit") if @limit_pending

        flush("size") if @buffer.bytesize >= @flush_bytes
        flush("records") if overfull?
        flush("time") if timed_out?
      end

      # The interval runs from the first thing held, byte or record. Measuring it from the buffer
      # alone stopped the clock entirely once a limit made the buffer permanently empty.
      def timed_out?
        return false if @flush_started_at.nil?

        ((@clock.now - @flush_started_at) * 1000) >= @flush_millis
      end

      def flush(reason)
        emitted = false
        while @buffer.bytesize >= @flush_bytes
          emit(@flush_bytes, reason)
          emitted = true
        end
        # A size flush emits whole chunks only. The tail, and the records that end inside it,
        # belong to the next chunk; forcing a short chunk here would fragment every stream.
        return emitted if reason == "size" && !@limit_pending
        return emitted unless @buffer.bytesize.positive? || @pending.any? || @limit_pending

        emit(@buffer.bytesize, reason)
        true
      end

      # One chunk: durable bytes first, then one commit carrying the artifact, the checkpoint and
      # the events. Nothing here is applied to this writer's own position until the sink says the
      # commit landed, so a retry rebuilds the identical chunk rather than a second one.
      def emit(length, reason)
        payload = length.positive? ? @buffer.byteslice(0, length) : nil
        chunk_end = @persisted_offset + length
        # A record belongs to the chunk whose byte range completes it, so a record split across a
        # boundary is counted once. Past a limit the framer runs ahead of the bytes on purpose —
        # records keep committing while their bytes are dropped — and holding those back would
        # leave the interpreter state in the checkpoint describing records the checkpoint never
        # committed, which is the same disagreement between state and offset in the framer.
        taken = payload && !@limit_pending ? @pending.take_while { |entry| entry["frame"].fetch("end_offset") <= chunk_end } : @pending.dup
        reason = "limit" if @limit_pending
        # Before the chunk is described, so the state it carries is the state of a stream that ends
        # exactly here. A commit that fails takes the writer with it, so this cannot advance twice.
        @durable_framer.advance(payload) if payload

        chunk = build_chunk(payload: payload, taken: taken, reason: reason, chunk_end: chunk_end)
        result = commit(chunk)

        @buffer = @buffer.byteslice(length, @buffer.bytesize - length) || +"".b
        @pending = @pending.drop(taken.length)
        @retained_bytes -= taken.sum { |entry| entry["frame"].fetch("text").bytesize }
        @flush_started_at = @buffer.bytesize.positive? || @pending.any? ? @clock.now : nil
        @persisted_offset = chunk_end
        @record_index = chunk.fetch("record_index")
        @framed_offset = chunk.fetch("framed_offset")
        @chunk_index += 1 if payload
        @chunks += 1 if payload
        @records += taken.length
        @malformed += chunk.fetch("malformed")
        @truncated_records += chunk.fetch("truncated_records")
        @limit_pending = false
        @artifact_ids << result["artifact_id"] if result["artifact_id"]
        result
      end

      def commit(chunk)
        @sink.commit(chunk)
      rescue Backstage::CaptureError
        @failed = true
        @coverage = "failed"
        raise
      rescue StandardError => error
        @failed = true
        @coverage = "failed"
        raise Backstage::CaptureError.new(
          "capture of #{@id} failed at offset #{@persisted_offset}: #{error.message}",
          stream_id: @id, offset: @persisted_offset, cause: error
        )
      end

      def build_chunk(payload:, taken:, reason:, chunk_end:)
        malformed = taken.count { |entry| entry["malformed"] }
        first = taken.find { |entry| entry["malformed"] }
        cut = taken.find { |entry| entry["frame"].fetch("truncated") }
        record_index = taken.empty? ? @record_index : taken.last["frame"].fetch("index") + 1
        # How far *records* have been committed — only records, never bytes. It used to take the
        # larger of this and the chunk's end offset, on the assumption that a chunk's bytes and its
        # records end together. A record spanning a chunk boundary breaks that assumption, and an
        # oversized one breaks it exactly: the framer cuts it at start + max_record_bytes, which is
        # the chunk boundary itself whenever flush_bytes <= max_record_bytes, so the record was
        # framed *after* a watermark already standing at its end offset and the guard in `record`
        # read it as one the checkpoint had already committed. It was then counted nowhere, named by
        # no event, and coverage still said complete. Records commit in order, so the end offset of
        # the last one committed is the whole answer.
        framed_offset = taken.empty? ? @framed_offset : [taken.last["frame"].fetch("end_offset"), @framed_offset].max
        {
          "stream_id" => @id, "step" => @step, "phase" => @phase, "kind" => @kind,
          "chunk_index" => @chunk_index,
          "payload" => payload,
          "start_offset" => @persisted_offset,
          "end_offset" => chunk_end,
          "records" => taken.length,
          "record_index" => record_index,
          "framed_offset" => framed_offset,
          "malformed" => malformed,
          "malformed_preview" => first && preview(first.fetch("frame")),
          "truncated_records" => taken.count { |entry| entry["frame"].fetch("truncated") },
          "truncated_preview" => cut && preview(cut.fetch("frame")),
          "reason" => reason,
          "coverage" => @coverage,
          "bytes_dropped" => @bytes_dropped,
          "truncated_from_offset" => @truncated_from_offset,
          "truncated_record_offset" => @truncated_record_offset,
          "observations" => taken.flat_map { |entry| entry["observations"] },
          "provider_session_id" => @provider_session_id,
          "sentinels" => @sentinels,
          # The framer that has seen only durable bytes. `framer_state.byte_offset` plus its
          # pending bytes equals `end_offset` by construction, which is the invariant that makes a
          # resume re-frame the same records with the same indices and the same event ids.
          "framer_state" => @durable_framer.state,
          "interpreter_state" => @interpreter.state,
          "occurred_at" => @owner.timestamp
        }
      end

      # Bounded and already redacted — the text came out of the redacted stream — but scrubbed
      # again here because a preview cut at a byte boundary can end mid-character.
      def preview(frame)
        text = frame.fetch("text").to_s
        clipped = text.byteslice(0, MALFORMED_PREVIEW_BYTES).to_s
        { "offset" => frame.fetch("start_offset"), "record_index" => frame.fetch("index"),
          "preview" => clipped.scrub("�") }
      end

      def build_summary(reason:, coverage:, closed_at: @owner.timestamp)
        {
          "stream_id" => @id, "step" => @step, "phase" => @phase, "kind" => @kind,
          "bytes" => persisted_bytes, "records" => @records, "chunks" => @chunks,
          "malformed" => @malformed, "truncated_records" => @truncated_records,
          "bytes_dropped" => @bytes_dropped, "coverage" => coverage,
          "last_offset" => @persisted_offset, "record_index" => @record_index,
          "reason" => reason, "opened_at" => @opened_at, "closed_at" => closed_at,
          "artifact_ids" => @artifact_ids.dup, "sentinels" => @sentinels,
          "provider_session_id" => @provider_session_id,
          "truncated_from_offset" => @truncated_from_offset,
          "truncated_record_offset" => @truncated_record_offset,
          "max_record_bytes" => @framer.max_record_bytes
        }.compact
      end

      # Bytes this stream has actually persisted, which is what a limit is measured against.
      def persisted_bytes
        @persisted_offset + @buffer.bytesize
      end

      def stringify(value)
        JSON.parse(JSON.generate(value))
      end
    end

    # Checks a replayed prefix against the chunks that are already durable, one chunk at a time.
    #
    # Offset skip assumes the bytes being discarded are the bytes already recorded. That assumption
    # is cheap to state and expensive to be wrong about: a provider that restarts with different
    # arguments, a checkpoint pointed at another run's stream, a fixture replayed into the wrong
    # writer, and the resumed stream reads as one contiguous artifact that no single process ever
    # produced. Nothing here holds more than one chunk, and a stream whose chunk artifacts cannot
    # be read simply verifies nothing rather than guessing.
    class PrefixVerifier
      def initialize(chunks)
        @chunks = Array(chunks).sort_by { |chunk| chunk.fetch("start_offset") }
        @index = 0
        @buffer = +"".b
      end

      # Returns the start offset of the first chunk the replay disagrees with, or nil.
      def push(bytes)
        return nil if @index >= @chunks.length

        @buffer << bytes
        while (chunk = @chunks[@index])
          length = chunk.fetch("end_offset") - chunk.fetch("start_offset")
          break if @buffer.bytesize < length

          slice = @buffer.byteslice(0, length)
          @buffer = @buffer.byteslice(length, @buffer.bytesize - length) || +"".b
          @index += 1
          return chunk.fetch("start_offset") unless Digest::SHA256.hexdigest(slice) == chunk.fetch("sha256")
        end
        nil
      end
    end
  end
end
