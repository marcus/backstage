# frozen_string_literal: true

module Backstage::Ports
  # The seam a runtime writes its output through, instead of accumulating it in a String.
  #
  # A runtime adapter holds a `StreamWriter` and hands it bytes as they arrive. What happens next —
  # redaction, framing, interpretation, durable chunks, committed events — is the implementation's
  # business, and a caller that wants none of it composes a null sink rather than skipping the port.
  # The port exists so that "output was produced" and "output was audited" stop being the same
  # claim: a writer only acknowledges bytes it has made durable, and says so when it cannot.
  #
  # Threading: a writer is used from one thread. `Application::RuntimeCapture` starts none, so a
  # runtime that reads its pipe on a reader thread must hand the bytes to the thread that owns the
  # writer rather than calling `write` from the reader.
  class RuntimeCapture
    # The record collection holding one checkpoint row per stream. It is a record collection, not
    # activity: it is current position, which is mutable, and the events it accompanies are the
    # history, which is not. Recovery reads it to find streams that were never closed.
    #
    # `Application::Engine::COLLECTIONS` enumerates the collections a deployment holds and does not
    # list this one yet; the wiring task adds it there when Engine learns about capture.
    STREAM_COLLECTION = "runtime_streams"

    # Coverage of a stream's bytes, and the only honest answers there are.
    #
    # - `complete`   every byte the host received is durable and referenced by an event.
    # - `truncated`  something configured had to cut, and said so. Two causes: a byte limit stopped
    #                persistence while semantic events kept committing, or a record over
    #                `max_record_bytes` was framed short (`truncated_records`, and
    #                `truncated_record_offset` naming where). The second leaves every byte durable
    #                and only the record cut, so a resumed writer keeps persisting; the first is
    #                what `bytes_dropped` counts, and tells them apart.
    # - `gap`        output may have been produced that was never acknowledged (a crash, a kill).
    #                Only recovery can conclude this; a writer never reports it about itself.
    # - `failed`     a durable append or its commit failed. Nothing downstream may treat the
    #                stream as audited.
    # - `open`       the stream is still being written. It appears only in `summaries`, for a
    #                writer that was never closed: what is durable is known exactly, what the
    #                runtime may still produce is not. A reader that knows the run is over —
    #                recovery — turns this into a `gap`; a writer never reports `gap` about itself.
    COVERAGE = %w[complete truncated gap failed open].freeze

    # Why a chunk was flushed. Carried on the coalesced event so an operator can tell a full
    # buffer from a quiet stream from a limit. `records` is the in-flight record bound taking
    # effect, which past a byte limit is the only thing that flushes at all.
    REASONS = %w[size time semantic close limit records].freeze

    # Two `runtime.observed` shapes share the type and are told apart by two fields:
    #
    # - a *chunk* event has `data.malformed` and `data.truncated_records` as Integer counts;
    # - a *record fault* event has `data.malformed == true` and/or `data.truncated == true`, plus
    #   `data.offset` and a bounded `data.preview` of the first affected record,
    #   `data.malformed_records` and `data.truncated_records` counting them, and
    #   `data.truncated_record_offset` where the record bound first cut.
    #
    # A consumer that wants only the second must compare to `true`, not test for truthiness — a
    # count of zero is truthy in Ruby, and that mistake reads every chunk as a fault report.
    #
    # There is at most *one* fault event per chunk, however many kinds of fault it had. A record too
    # large for the record bound is usually also unparseable, and reporting it once as truncated and
    # again as malformed would double-count it as surely as dropping it would lose it.
    #
    # A chunk event is identified by its byte range. A flush that persisted no bytes — past a byte
    # limit, where records keep committing and bytes do not — has no byte range to be identified
    # by, so its id and its `source.offset` come from its record range instead. Every record
    # counted in a checkpoint is named by one of the two.

    # Opens a stream. `step` names the phase step being captured (`harness`, `context`, …) and
    # rides in the stream id; `phase` overrides the capture's own phase for this one stream, which
    # a phase runner needs because it opens streams across context, implementation, finalize and
    # publish inside one run; `kind` is the artifact kind the chunks are stored under.
    # `interpreter` is a StreamInterpreter — nil means no semantic interpretation, framing and
    # persistence unchanged. `resume: true` continues an existing checkpointed stream, discarding
    # replayed bytes at or before its `last_offset`. `max_record_bytes` overrides the record bound
    # for this one stream; nil takes the policy's, which asks the interpreter whether the stream is
    # a protocol (see Ports::StreamInterpreter#protocol?) and gives it the larger bound if it is.
    #
    # Opening a stream id that already has durable state *without* `resume: true` raises
    # Backstage::CaptureError rather than starting a second writer at offset zero over chunk files
    # an acknowledged event already names.
    #
    # Returns a StreamWriter.
    def open(step:, phase: nil, kind: "runtime_output", interpreter: nil, resume: false,
             max_record_bytes: nil)
      raise NotImplementedError
    end

    # A summary per stream this capture opened, closed or not. A writer that was never closed is
    # reported with coverage `open` rather than omitted: that is exactly the case recovery reasons
    # about, and omitting it would make a run that was killed mid-stream indistinguishable from a
    # run that never opened the stream.
    def summaries
      raise NotImplementedError
    end

    # One stream being written.
    #
    # Deduplication has two mechanisms and they cover different failures.
    #
    # *Offset skip* is the cross-process one: a writer opened with `resume: true` reads its
    # checkpoint once and discards bytes whose range ends at or before `last_offset`, so a provider
    # that replays its stream from the beginning contributes nothing already recorded.
    #
    # *Deterministic event ids* cover an in-process retry: a chunk's event id is derived from the
    # stream id and its byte range, so committing the same chunk twice reconciles against the
    # store's existing event instead of growing history.
    #
    # Known limit, deliberately not papered over: `occurred_at` is inside an event's canonical
    # fingerprint. A *cross-process* replay with a fresh clock therefore produces the same event id
    # with a different fingerprint, which a store refuses with a ConflictError. That is the correct
    # outcome — a refusal, never a silent duplicate — and it is why offset skip is the primary
    # mechanism. Synthesizing a stable timestamp to make ids collide cleanly would be forging the
    # time a fact was observed, which is worse than a conflict.
    #
    # *What a resumed writer restores.* The checkpoint describes exactly the bytes at or before
    # `last_offset`: `framer_state.byte_offset` plus its pending bytes equals `last_offset`, never
    # more. That invariant is what makes a resumed stream re-frame the same records with the same
    # indices — and therefore the same semantic event ids — no matter which flush the crash fell
    # on. `framed_offset` records how far records have been *committed*, which runs ahead of
    # `last_offset` only past a byte limit, where records commit and their bytes do not; a resumed
    # writer replays those bytes to keep the numbering and tells none of their records again.
    #
    # *What a resume does not promise.* A resumed run chunks its output where its own reads and its
    # own poll fall: only a size flush is a function of `last_offset`, while a semantic, records or
    # time flush cuts where the reader happened to stop. So a resumed stream may carry different
    # chunk boundaries — and different `runtime.observed` byte ranges — than the run it continues.
    # Records are the stable coordinate: framing, record indices and the semantic event ids derived
    # from them are identical across any splitting of the same bytes. An adapter that needs a
    # stable identity for a fact must key it on the record, never on the chunk.
    #
    # *Divergent replay is refused.* Offset skip assumes the bytes being discarded are the bytes
    # already recorded. A resumed writer checks that assumption against the digests of the chunks
    # that are durable and raises Backstage::CaptureError when the replay disagrees, rather than
    # splicing a second runtime's output onto this stream's offsets. A stream whose chunk artifact
    # records cannot be read verifies nothing, which is "cannot verify", not "verified".
    class StreamWriter
      # `<run_id>:<attempt_number>:<index>:<step>`, where `index` is a per-run counter of opened
      # streams so two runs of the same step never collide.
      def id
        raise NotImplementedError
      end

      # Hands bytes to the stream. Returns nil. Raises Backstage::CaptureError when a chunk could
      # not be made durable or its commit was refused, when a resumed replay diverges from the
      # durable bytes, and when the writer has already failed or been closed; after any of those
      # the writer will not accept more bytes. Every failure a caller has to handle around capture
      # is that one type — a runtime's drain loop rescues CaptureError and nothing else.
      def write(_bytes)
        raise NotImplementedError
      end

      # Redacted bytes buffered but not yet part of a durable chunk. Bounded by the flush size.
      def buffered_bytes
        raise NotImplementedError
      end

      # Bytes held for a record that has not ended yet. Bounded by the framer's record limit.
      def pending_bytes
        raise NotImplementedError
      end

      # Records framed and interpreted but not yet part of a commit, and the text they hold.
      # Bounded by `flush_records` records and by `flush_bytes + max_record_bytes` of text. These
      # are the third place bytes live in flight — past a byte limit, where the buffer is
      # permanently empty and the flush timer never starts, they are the only place.
      def retained_frames
        raise NotImplementedError
      end

      def retained_bytes
        raise NotImplementedError
      end

      # Gives the writer a chance to flush on elapsed time when no bytes have arrived. A runtime
      # draining a quiet pipe calls this on its poll; it is not required for correctness, only for
      # promptness. Returns nil.
      def tick
        raise NotImplementedError
      end

      # Ends the stream, flushing what remains, writing the manifest, and marking the checkpoint
      # row closed. `reason` is why the stream ended (`"close"`, `"cancelled"`, `"failed"`, …).
      #
      # Returns a Summary hash:
      #
      #   { "stream_id", "step", "phase", "kind", "bytes", "records", "chunks", "malformed",
      #     "truncated_records", "bytes_dropped", "coverage", "last_offset", "record_index",
      #     "reason", "opened_at", "closed_at",
      #     "max_record_bytes" => Integer,           # the record bound this stream ran under
      #     "truncated_record_offset" => Integer or nil,  # where it first cut a record
      #     "artifact_ids" => [...],          # chunk artifacts, then the manifest
      #     "chunk_artifact_ids" => [...],    # the chunks alone, in order
      #     "manifest_artifact_id" => String, # the stream manifest, for outcome stream_refs
      #     "sentinels" => { name => payload },
      #     "provider_session_id" => String or nil }
      def close(reason: "close")
        raise NotImplementedError
      end
    end
  end
end
