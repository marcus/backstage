# Runtime capture: implementation design

Part of the [controlling plan](README.md); the design for slice 2 ("Runtime activity") in
[activity](activity.md), now implemented (`td-dfe850`, `td-99aea5`, `td-e90467`, `td-bce761`,
proof `td-095cd0`). Kept as the reference for slices 3 to 5. Deviations taken during
implementation and review: `StreamHandle#append` is idempotent by chunk index and takes
`committed_chunks:` so an orphaned chunk below no acknowledged event can be rewritten; chunk and
manifest artifact ids are derived, not minted; the checkpoint's framer state comes from a lagging
framer that has consumed exactly the durable bytes (`framed_offset` on the row); `summaries`
reports never-closed writers as coverage `open`; record bounds are per stream (`max_record_bytes`
for plain output, `max_protocol_record_bytes` for interpreters that declare `protocol?`); a record
over its bound lowers coverage to `truncated` and is named by the chunk's single record-fault
event together with any malformed count; the Docker log tail is trimmed after UTF-8 scrubbing;
`Docker::Runtime.new` takes `grace_seconds:`; sentinel `occurred_at` comes from the worker's own
timestamp or the record's framing time, never the chunk's commit time.

## 1. Stream identity, offsets, and the chunk hand-off

**Stream id** is `<run_id>:<attempt_number>:<index>:<step>` (for example `run-a1b2:1:2:harness`).
`index` is a per-run monotonic counter of opened streams so two `context` steps never collide. It
is assigned by `Application::RuntimeCapture#open`, the only object holding run/attempt identity,
and persisted immediately in a `runtime_streams` record (id = stream id) so recovery can find it.

**Offset unit is bytes of the redacted stream**, counted from 0 at stream open. Redaction is
length-changing, so raw-byte offsets would not address the persisted bytes; redacted offsets index
the chunk artifacts exactly. A record index (count of complete frames) rides alongside for the
interpreter's own dedupe.

**Docker's reader thread hands bytes, not lines.** Replace `output.each_line` with
`readpartial` chunking and a bounded `SizedQueue` (real backpressure to the pipe). The reader only
enqueues raw bytes; redaction, framing, persistence and commits all happen on the controller thread
inside `drain`, so no new threads exist and the "callbacks run on the controller thread" guarantee
in `runtime_progress_test.rb` is preserved.

Teardown must become `drain.call until reader.join(0.01)` followed by a final `drain.call`; the
current `ensure_process_exit; reader.value; drain.call` deadlocks against a bounded queue.

## 2. The capture component

Port `lib/backstage/ports/runtime_capture.rb`, the seam adapters see:

```ruby
class Backstage::Ports::RuntimeCapture
  def open(step:, kind: "runtime_output", interpreter: nil) # => StreamWriter
  class StreamWriter
    def id                  # String
    def write(bytes)        # nil; raises Backstage::CaptureError
    def buffered_bytes      # Integer, for bounds tests
    def close(reason:)      # => Summary hash
  end
  # Summary: {"stream_id","step","bytes","records","chunks","malformed","coverage","last_offset",
  #           "artifact_ids"=>[..], "sentinels"=>{..}, "provider_session_id"}
end
```

`lib/backstage/application/runtime_capture.rb` implements it with a pluggable sink so framing and
interpretation always run and only persistence is optional:

```ruby
RuntimeCapture.new(sink:, clock:, flush_bytes: 64 * 1024, flush_millis: 250,
                   max_stream_bytes: 16 * 1024 * 1024, max_run_bytes: 64 * 1024 * 1024,
                   secret_guard:, run:, attempt:)
Application::CaptureSink::Null.new     # frames + interprets, persists nothing
Application::CaptureSink::Durable.new(store:, artifact_store:, recorder:, work_item_id:, run:, attempt:)
```

**Flush and commit ordering.** A flush happens on the first of: 64 KiB buffered, 250 ms since the
first buffered byte (injected clock), a semantic observation from the interpreter, or `close`.
One flush = one chunk = one `store.commit`:

1. `handle.append(bytes, index:)` writes `<index>.log.part`, fsyncs, renames to `<index>.log`,
   fsyncs the directory, returns `{"index","path","sha256","bytes","start_offset","end_offset"}`.
2. Only then the sink calls `store.commit(writes, expect:, activity:)`: writes = the chunk's
   `artifacts` record + the updated `runtime_streams` checkpoint; activity = the coalesced
   `runtime.observed` plus every semantic event derived from records in this chunk.

A crash between 1 and 2 leaves an unreferenced chunk file, never an acknowledged event pointing
at an incomplete file.

**Checkpoint record** (`runtime_streams`, one row per stream, revision-guarded):

```json
{"schema_version":1,"id":"run-a1b2:1:2:harness","run_id":"run-a1b2","attempt_id":"attempt-…",
 "work_item_id":"work-…","step":"harness","phase":"implementation","revision":7,
 "opened_at":"…","closed_at":null,"coverage":"complete",
 "last_offset":524288,"record_index":3532,"chunk_index":7,"bytes_dropped":0,
 "framer_state":{"partial_b64":"4Y","record_index":3532,"byte_offset":524288,"truncating":false},
 "interpreter_state":{"settled":false,"provider_session_id":"…","open_tools":2},
 "chunk_sha256":"…","chain_sha256":"…","artifact_dir":"…/streams/2-harness"}
```

**Deduplication**, in order: primary, offset skip (a writer opened in resume mode reads its
`runtime_streams` row once and discards bytes whose range ends at or before `last_offset`);
secondary, deterministic event ids (`ActivityRecorder.event_id("runtime.observed", stream_id,
start_offset, end_offset)`, `("agent.tool_observed", stream_id, record_index)`) so an in-process
retry of a failed commit reconciles instead of appending.

Known limit: `occurred_at` is inside the canonical fingerprint, so a cross-process replay with a
fresh clock yields the same id with a different fingerprint, a `ConflictError`, never a silent
duplicate. Offset skip is the cross-process mechanism; deterministic ids cover in-process retry.
Document this in the port rather than synthesizing timestamps.

**ArtifactStore additions** (`adapters/local_files/artifact_store.rb`):

```ruby
def open_stream(work_item_id:, run_id:, stream_id:, kind:, provenance:) # => StreamHandle
class StreamHandle
  def append(bytes, index:)   # => chunk hash, fsynced+renamed before it returns
  def finalize(summary)       # => Records.artifact of the stream manifest (stream.json)
  def directory
end
```

Layout: `<artifacts>/<work_item_id>/<run_id>/streams/<index>-<step>/{0.log,1.log,…,stream.json}`.
Per-chunk files rather than one appended file: a torn append is a real failure mode, a
create-fsync-rename is not.

## 3. UTF-8 and record framing

`lib/backstage/support/record_framer.rb`, pure (no IO, no clock):

```ruby
framer = RecordFramer.new(max_record_bytes: 64 * 1024)
frames = framer.push(bytes)     # => [Frame,…] complete records only
tail   = framer.finish          # => [Frame,…] final partial line, if any
state  = framer.state           # => Hash, JSON-safe (partial bytes base64)
RecordFramer.restore(state)
framer.pending_bytes
```

`Frame`: `{"index","start_offset","end_offset","text","encoding"=>"utf-8"|"replaced","truncated"=>bool}`.

Bytes accumulate in an ASCII-8BIT buffer split on `"\n"`. A partial multibyte sequence at a chunk
boundary is part of the pending line and resolves with the next chunk. A complete line is forced
to UTF-8; invalid bytes are scrubbed to U+FFFD with `encoding: "replaced"`. A line exceeding
`max_record_bytes` with no newline is emitted `truncated: true` and the framer discards until the
next newline, which is what stops a process printing a gigabyte without a newline from exhausting
RAM.

**Streaming redaction** (`support/secret_guard.rb`, additive) so a secret split across chunks is
still caught: `guard.redactor` returns a `Redactor` whose `push(bytes)` returns safe output while
withholding the last `longest_secret - 1` bytes, and `finish` returns the remainder. Redaction runs
before framing so offsets and persisted bytes agree.

**Malformed handling.** Nothing is dropped. Per flushed chunk the sink emits one extra bounded
`runtime.observed` for the first malformed record with `data.malformed = true`, `data.offset`, and
a ≤200-byte scrubbed preview; the rest are counted in `data.malformed_records`. Invalid JSON lines
take the same path.

## 4. Semantic events

Interpreter port `lib/backstage/ports/stream_interpreter.rb`: `observe(frame) => [observation]`,
`state => bounded JSON-safe hash`, `self.restore(state)`. Observation:
`{"type","summary","data","provenance","occurred_at","provider_event_id","provider_session_id","sentinel"}`.

- `Adapters::Pi::StreamInterpreter` wraps today's `EventParser` normalization. `message_end`
  (assistant) becomes `agent.message_observed`, provenance `agent_reported`, data
  `{role, stop_reason, text_bytes, text_sha256, usage, preview_truncated}`, summary = first 200
  redacted chars. `tool_execution_start/end` become `agent.tool_observed` with
  `{tool_name, tool_call_id, phase, is_error, args_bytes, args_sha256, args_preview, result_bytes,
  result_sha256, result_preview, truncation}`; previews capped at 512 bytes. `session` sets
  `provider_session_id` on the stream record. `agent_start`, `agent_end`,
  `tool_execution_update`, `agent_settled` produce no events (settled/stop_reason surface on
  `runtime.reported_completion`).
- `Application::Runners::SentinelInterpreter` for repository steps: `repository_prepared`,
  `context_materialized`, `repository_materialized`, `repository_published` become
  `artifact.available` with bounded data, and parsed payloads return in the close summary's
  `sentinels` so `ContainerPhaseRunner` stops re-parsing `logs`.

Coalesced output event, one per flushed chunk:

```json
{"type":"runtime.observed","provenance":"runtime_reported","artifact_refs":["artifact-…"],
 "summary":"captured 64.0 KiB of harness output for run-a1b2 (records 3120-3532)",
 "data":{"stream_id":"run-a1b2:1:2:harness","step":"harness","phase":"implementation",
         "chunk_index":7,"start_offset":458752,"end_offset":524288,"bytes":65536,
         "records":412,"malformed":0,"truncated_records":0,"sha256":"…",
         "coverage":"complete","reason":"size|time|semantic|close|limit"}}
```

`runtime.reported_completion` and `execution.verified_completion` stay as Engine emits them.

## 5. Failure semantics, coverage, and limits

`Backstage::CaptureError < Backstage::Error` carries `stream_id`, `offset`, and the cause class.
`StreamWriter#write` raises it when the chunk append or the commit fails. Propagation:

1. Docker's `drain` rescues `CaptureError`, records it, sets cancellation, calls `stop_container`,
   and breaks the loop: controlled cancellation through the owning runtime.
2. The runtime returns `status: "failed"` with `"capture" => {"status":"failed","stream_id","last_offset","error"}`.
3. `ContainerPhaseRunner` refuses to advance to `finalize` or `publish` when capture is not
   `complete`/`truncated`. No authorized action runs on an unaudited stream.
4. Engine surfaces `"capture"` on the execute result and run record; the controller carries it so
   `backstage process --json` shows it. If the store itself is unavailable, `finish_execution` fails
   and the exception reaches the CLI (exit 1). A failed audit append is never audited into the
   storage that failed.

**Coverage on the run** (guarded `update_run`) at each stream close:

```json
"capture":{"status":"complete|truncated|gap|failed","bytes":…,"limit_bytes":…,
           "streams":[{"stream_id":…,"step":…,"bytes":…,"records":…,"chunks":…,
                       "coverage":"complete","last_offset":…}],"updated_at":"…"}
```

**Recovery#interrupt** stops fabricating a bare failure. It reads `run["capture"]` and the
`runtime_streams` rows; any row with `closed_at == nil` means output may have been produced but
never acknowledged, so coverage becomes `gap`. The synthetic outcome gains `"capture"` and stream
artifact references; the `worker_interrupted` finding gains a `capture` block naming the
unobserved interval. That finding is already committed as `reconciliation.finding` activity.

**Limits.** `max_stream_bytes` (16 MiB) and `max_run_bytes` (64 MiB) defaults, adapter-configurable.
On exceeding, byte persistence stops but framing and interpretation continue: semantic observations
still commit because decisions and effects are never discarded as debug noise. Coverage becomes
`truncated` and one `runtime.observed` with `reason: "limit"` records `truncated_from_offset`.

## 6. Replacing the accumulators

| Today | Replacement |
| --- | --- |
| `Engine#execute` `events = []` | Deleted. The result's `"events"` key becomes `"capture"`. Nothing outside tests reads it. |
| Docker `logs = +""` | A bounded tail (`tail_bytes`, default 64 KiB) for diagnostics only, plus `logs_truncated` and `log_tail_bytes`. |
| Pi `@raw_events` | Deleted. The parser keeps `@authoritative_message`, `@settled`, and counters. |
| `ContainerPhaseRunner` re-parse of `logs` | `SentinelInterpreter` output in the close summary, exposed as `outcome["sentinels"]`. One parsing path. |

**Compatibility projection.** Removing `raw.events` from a document still stamped
`schema_version: 1` would silently change what a produced v1 document means, which the
controlling plan forbids. Add `schemas/outcome-v2.json`: `raw` becomes
`{vendor, model, authoritative_message, stream_refs:[{stream_id, artifact_id, records, bytes, coverage}]}`,
`logs` is a documented bounded tail with `logs_truncated`, `capture` is first-class. Add
`Domain::Outcome` with `validate!` (accepts 1 and 2), `upgrade(v1)`, `project_v1(v2)` (asserted
against `outcome-v1.json`), and `failure(...)` replacing the literal hashes in Engine, Recovery and
the fake runner. Engine validates and persists v2; legacy v1 outcomes load through `upgrade`.

## 7. Test doubles and measurement

- `Adapters::Fake::Runtime` (new): scripted `[[offset_seconds, bytes], …]` driven against a fake
  clock through `capture.open` / `writer.write`. Chunk boundaries split a UTF-8 character and a
  secret value; the fake clock makes the 250 ms boundary exact. `Fake::Runner` gains an optional
  `script:`.
- **Kill mid-stream with a real isolated child**, no Docker: reuse `runtime_progress_test.rb`'s
  stub-docker pattern with a Ruby child that streams forever; the controller runs inside
  `Process.fork`, the parent kills it mid-stream and asserts from the store that acknowledged
  `runtime.observed` events and their hash-matching chunk files exist, and that
  `Recovery#reconcile` reports `worker_interrupted` with `capture.status == "gap"`.
- **Large-output bounds, portable.** Structural assertions: after a 64 MiB fixture,
  `buffered_bytes <= 64 KiB`, `framer.pending_bytes <= 64 KiB`, `outcome["logs"].bytesize <= 64 KiB`,
  queue size `<= 64`, chunk count `== ceil(bytes / 64 KiB)`. Secondary: `GC.stat(:heap_live_slots)`
  delta around `GC.start` with a generous threshold. Report elapsed time, allocations and state-log
  size as the local baseline in the plan. `ObjectSpace.memsize_of_all` is too unstable for a hard
  threshold on macOS.

## 8. Ordered steps and task split

Two sequential tasks; file sets are disjoint apart from `lib/backstage.rb`.

**Task A, foundation in isolation.** New: `support/record_framer.rb`, `ports/runtime_capture.rb`,
`ports/stream_interpreter.rb`, `application/runtime_capture.rb`, `application/capture_sink.rb`.
Edit: `support/secret_guard.rb`, `adapters/local_files/artifact_store.rb`, `errors.rb`,
`lib/backstage.rb`. Tests: `record_framer_test.rb`, `runtime_capture_test.rb`,
`artifact_stream_test.rb`. Order: framer → streaming redactor → artifact stream handle → capture
+ null sink → durable sink and checkpoint.

**Task B, wiring and compatibility projection.** New: `schemas/outcome-v2.json`,
`domain/outcome.rb`, `adapters/pi/stream_interpreter.rb`, `application/runners/sentinel_interpreter.rb`,
`adapters/fake/runtime.rb`. Edit: `adapters/docker/runtime.rb`, `adapters/pi/harness.rb`,
`application/runners/container_phase_runner.rb`, `application/engine.rb`, `application/recovery.rb`,
`adapters/fake/runner.rb`, `bootstrap/system.rb`, `surfaces/cli.rb`, `lib/backstage.rb`. Order:
outcome v2 + projection → interpreters → Docker chunked reader and `capture:` keyword → Pi harness
→ ContainerPhaseRunner sentinels → Engine wiring and result shape → Recovery coverage → CLI.
`run(bundle:, secrets:, cancellation:, capture:)` reaches about nine test doubles; pass it
explicitly and update the doubles rather than `respond_to?` sniffing.

## Proof map

| Proof bullet | Test |
| --- | --- |
| Partial/multibyte lines reconstruct | `record_framer_test`: split multibyte, split CRLF, no trailing newline, oversized line, state round-trip |
| Secrets split across chunks | `runtime_capture_test`: a secret straddling a chunk boundary never reaches a chunk file or an event |
| Duplicate replay deduplicates | `runtime_capture_test`: resume from checkpoint skips by offset; in-process commit retry reconciles by id |
| Acknowledged activity survives a kill mid-stream | `runtime_capture_kill_test` (forked controller, stub child process) |
| Recovery reports uncertain capture; outcome not fabricated | `recovery_test`: `worker_interrupted` carries `capture.status == "gap"` and stream refs |
| Large output stays bounded | `runtime_capture_bounds_test`: 64 MiB fixture, structural bounds, GC delta, reported baseline |
| Persistence failure is explicit | failing artifact store → container stopped, no publish, `capture.status == "failed"` on the command result |
| Oversized tool args/results, invalid lines, malformed text | `pi_stream_interpreter_test` + `runtime_capture_test` (`data.malformed = true`) |
| Existing state and schemas remain readable | `outcome_projection_test`: v1 outcomes upgrade; `project_v1` validates against `outcome-v1.json` |

## Risks and recommendations

1. `occurred_at` in the fingerprint blocks cross-process id dedupe: offset skip is primary; document it.
2. `SizedQueue` deadlock in Docker teardown: `drain.call until reader.join(0.01)`, covered by a fixture that outproduces the drain.
3. outcome v2 rather than an additive v1 field, because `raw.events` disappearing from a v1 document is a silent meaning change.
4. The bounded `logs` tail changes Docker test assertions: keep the tail, assert on it, add one >64 KiB truncation test.
5. `capture:` keyword churn across doubles: explicit, not sniffed. An implicit seam is how the second log-parsing path got in.
6. Chunk-per-file inode pressure (1,024 files per 64 MiB stream): accept; `flush_bytes` is configurable.
