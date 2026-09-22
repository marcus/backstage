# Durable activity and event correlation

Part of the [controlling plan](README.md). Slices 1 (durable lifecycle activity) and 2 (runtime
activity) are implemented on `main`; the sections below describe what exists, then the contract
they were built to. Implementation design for slice 2 is in
[runtime-capture-design](runtime-capture-design.md).

## What exists now

`Backstage::Domain::Activity` owns the envelope builder, the one frozen type vocabulary,
provenance classes, the canonical fingerprint, and the filter/cursor-fingerprint rules every store
adapter answers with. `schemas/activity-event-v1.json` is generated from that vocabulary and every
event is validated in the store before it becomes durable. `Backstage::Ports::Store` gained
`commit(writes, expect:, activity:)` returning a `CommitResult` (an Array of written records, so
existing callers are unchanged, plus `#activity`), `read_activity(after:, filters:, limit:)`,
`fetch_activity(event_id)` and `deployment_id`; the port comments are the contract.

The JSONL adapter carries events in the same fsynced `transaction_version` 1 line as the writes,
assigns contiguous sequences under the writer lock, reconciles exact retries, refuses conflicting
reuse of an id, and mints deployment identity once behind an absence guard with an
`activity.stream_started` marker. Reads never write and never mint.

The application half landed with it (`td-f71f59`, `td-cf98d0`): `ActivityRecorder` builds envelopes
for emitters in Engine, WorkflowService, Dispatcher, Controller, Recovery and the td trigger, each
committing its event in the same transaction as the state change it explains; `ActivityQuery` is
the shared read projection; and `backstage activity list/show/follow` is the first surface over it.

Bounds landed with the review of slice 1: `data` is refused above 16 KiB (large content belongs in
artifacts), `read_activity` refuses a `limit` above 1000, a cursor positioned past the stream's
newest event is a structured `cursor_ahead_of_stream` error rather than a follower that can never
catch up, and an event id reused for a different fact raises `ActivityConflictError` (a subclass of
`ConflictError`) so retry loops and swallow sites re-raise it instead of spinning or losing the
event. The td trigger records `source.checked` only when a poll's answer changes, keyed on a
monotonic `check_sequence` on its receipt row, so recoveries are recorded and exact retries
reconcile.

Runtime capture landed as designed (`td-dfe850`, `td-99aea5`, `td-e90467`, `td-bce761`,
proof `td-095cd0`). Each runtime step opens one stream (`<run>:<attempt>:<index>:<step>`) through
`Application::RuntimeCapture`; the Docker runtime hands raw bytes from a bounded queue to the
controller thread, where a streaming redactor, `Support::RecordFramer` and the step's interpreter
run. Output is persisted in fsynced chunk files under `<artifacts>/<work>/<run>/streams/`, each
chunk committed with its `runtime_streams` checkpoint row and a coalesced `runtime.observed` event
in one store commit, after the file is durable. Semantic observations (`agent.message_observed`,
`agent.tool_observed`, `artifact.available` for repository sentinels) carry bounded previews and
deterministic ids keyed on the record index, never on chunk boundaries. Pi records are read
through `Adapters::Pi::StreamInterpreter`; the run's outcome is `outcome-v2` (bounded log tail,
`capture` block, `raw.stream_refs`) with a v1 projection for existing readers.

Coverage words on a stream and on the run's `capture` block: `complete` (every byte durable and
every record read whole); `truncated` (either bytes past `max_stream_bytes`/`max_run_bytes` were
not persisted while records kept being interpreted, or a record exceeded its record bound and was
cut, with `truncated_record_offset`); `open` (a stream still being written); `gap` (Recovery found
an unclosed stream after a worker died: output after `last_offset` may exist that was never
acknowledged); `failed` (a chunk or its commit could not be made durable, the runtime was told to
stop, and no finalize, publish or review verdict is accepted from that run). Record bounds are per
stream: 64 KiB for plain output, 8 MiB for a protocol stream such as Pi; a Pi record past that bound
yields an explicit `incomplete` outcome with `incomplete_reason: record_truncated`, never a bare
failure with empty fields.

Known limits, stated rather than hidden: chunk byte ranges are reproducible only for size flushes,
so anything that must be stable across a resume is keyed on the record index; cross-process replay
deduplicates by offset skip from the checkpoint, and an in-process retry by deterministic event id;
an orphaned chunk left by a crash between append and commit is rewritten on resume and is
cleanup-eligible, but no cleaner exists yet; two broad rescues (the dispatcher's `block` path and
one in Recovery) would swallow an `ActivityConflictError` if their callees ever emitted activity,
which today they do not.

Measured on a 50,000-event, 22.9 MiB log: an activity page costs about 165 ms wherever it reads
from, because the finite-page watermark needs a scan to the end. That is cheaper than the state
reads the same log already required (`list` 286 ms, one guarded commit 239 ms), so no index file
was built. An adapter-internal, rebuildable sequence index is the first thing to add if the
large-history proof in slice 2 wants faster follow.

## Contract

Every acknowledged lifecycle change has a corresponding immutable activity event. An operator
can resume reading after a cursor without missing committed events or seeing a partial commit.
Every received runtime observation that is acknowledged as durable survives a process restart.
An agent's self-reported observation is labeled as such and never masquerades as verified success.

Use one logical activity stream per deployment/store, exposed through an application query.
Existing operational records remain authoritative for current state; events are authoritative
historical facts. Reading or rebuilding a view must never execute actions or replay transitions.

### Event envelope

| Field | Meaning |
| --- | --- |
| `schema_version`, `event_id`, `type` | Versioned contract, immutable identity, named semantic event |
| `deployment_id` | Stable instance identity, not hostname or filesystem path |
| `sequence` | Store-assigned monotonic commit order within this deployment; never supplied by a worker |
| `occurred_at`, `recorded_at` | Producer time when known and durable ingestion time; sequence defines reading order |
| `source` | Trusted adapter/producer identity, producer instance, event ID or durable offset, provenance/authority class |
| `work_item_id`, `target_id` | Owning assignment and target when applicable; system-health events may lack work |
| `job_id`, `run_id`, `attempt_id` | Core execution identities when applicable |
| `request_id`, `transition_id`, `decision_id`, `action_id`, `delivery_id` | Relevant durable relationship IDs, omitted when not applicable |
| `causation_event_id`, `correlation_id`, `links` | Direct cause, grouping, and typed related entity/event references; no claim that correlation grants authority |
| `provider_session_id`, `provider_event_id` | Vendor identities kept separate from Backstage run IDs |
| `summary`, `data`, `artifact_refs` | Bounded redacted description, typed event-specific payload, durable references for detail |

Default work correlation to its work ID; use request identity for system events without work.
Permit optional linked work IDs for future dependencies without creating coordination behavior.
A later distributed trace exporter may attach W3C trace/span IDs; do not force business IDs into
that format or require a telemetry service. External correlation values are untrusted metadata.
Envelope ownership fields come from the entry/run context, never from raw model output.

Initial vocabulary: work admitted; transition applied; decision raised/answered/cancelled;
execution accepted/started/finished; retry scheduled; cancellation requested; runtime observed;
agent message/tool observation; artifact available; effect proposed/approved/attempted/verified/
failed/uncertain; delivery queued/sent/failed/uncertain; reconciliation finding; source check and
dispatcher-health observation. Runtime-reported completion and core-verified completion have
distinct types. No lifecycle transition for each token or heartbeat.

## Atomicity and append-only storage

Extend the Store contract with an atomic activity append accompanying its current guarded batch,
for example `commit(writes, expect: ..., activity: ...)`, preserving existing call/return behavior.
Add bounded `read_activity(after:, filters:, limit:)` and event lookup semantics. The application
chooses event meaning and identities; the store assigns sequence/order and enforces immutability
within the same lock/transaction as record writes. This is a generic ordered-record contract, not
a JSONL byte-offset contract. No separate audit-file append after a successful state commit.

The JSONL adapter can add activity records in its existing fsynced transaction line. Same-commit
events have contiguous ordering assigned under the writer lock. Duplicate event identities with
identical canonical content are reconciled; conflicting reuse is refused without partial writes.
An exact retry returns its original events/cursors rather than appending new history. Guard failure
commits neither state nor activity. Do not allow `save` to overwrite an existing activity event.

For artifact-backed details, persist and fsync the bounded artifact before committing its event
reference. A crash may leave an unreferenced artifact eligible for cleanup; it must not leave an
acknowledged event pointing to an incomplete file. Artifact cleanup never runs ahead of durable
reference/consumer retention rules. Put storage layout and indexing optimizations in adapters.

Activity may share the physical state log initially, but do not repeatedly materialize all
transcript payloads to answer a queue query. Keep event payloads small, large content in artifacts,
and add a rebuildable activity index/cache inside the adapter if the large-history proof requires
it. Measure read/write cost before selecting SQLite. No application SQL or filesystem offsets.

## Runtime capture

Replace unbounded `Engine#execute` event arrays, Docker's accumulated `logs`, and Pi's `raw_events`
with incremental capture and bounded parser state. Preserve final outcome fields through versioned
compatibility projections; the final outcome references captured detail instead of duplicating it.

The runtime/harness boundary provides a stable stream identity and offsets/sequences. Capture
redacted output in durable bounded chunks; parse complete UTF-8 records across arbitrary chunk
and newline boundaries. Persist the normalization checkpoint with emitted events so replay of
the same source chunk deduplicates. A random ID on each reconnect is insufficient. If the
provider supplies no replay, explicitly report the unobserved interval after a crash; never invent
events or claim complete capture of output the host did not receive.

Synchronously commit semantic lifecycle events. Coalesce verbose runtime output using a bounded
size/time buffer (initial proposal: 64 KiB or 250 ms, configurable at the adapter), acknowledging
only flushed data as durable. Force flush on normal completion/cancel. A hard crash can lose an
unacknowledged tail; record capture coverage and a gap on recovery where completeness is unknown.
Do not market this as token-perfect capture or a resumable agent session.

Durable append failure must reach the runner/controller: stop further authorized actions and
attempt controlled cancellation through the owning runtime. Do not silently fall back to an
in-memory success trail. If storage is unavailable, report the failure on the command surface;
recovery records it when storage is usable. A failed audit append cannot itself be audited into
unavailable storage. Add bounded backpressure/output limits so a noisy process cannot exhaust RAM
or disk; capture truncation/gap status explicitly. Never discard decisions/effects as debug noise.

Sanitize before durable storage or delivery. Tests include secrets split across chunks, oversized
tool arguments/results, invalid lines, and malformed text. Default activity stores metadata and
bounded safe summaries; raw content stays under explicit artifact access/retention policy. No
automatic forwarding of transcripts to channels. Repeated health checks need bounded sampling or
change/coalescing policy; preserve failures/recovery and an inspectable latest check receipt.

## Query and consumer semantics

`activity list`, `activity show EVENT`, and `activity follow --after CURSOR` are proposed CLI
commands with JSON/JSONL and filters for work, run, target, kind, and related IDs. Follow uses the
same bounded query repeatedly; it needs no event-bus server. Return an opaque cursor, next cursor,
and high-water mark. Filtered pages advance over scanned positions, including pages with no
matches. Bind cursors to deployment and query filters; reject mismatches rather than silently
skipping history. Clients persist a cursor only after processing a page/event successfully.

Concurrent commits appear after the last acknowledged cursor; wall-clock changes do not reorder
history. Define finite-page snapshot watermark versus continuing follow explicitly. Delivery
consumers keep durable checkpoints and create intents with guards; a crash before checkpoint
advancement replays safely. A UI may be behind, and should show freshness. Query methods never
drain effects or advance workflow state.

No automatic activity pruning in the first slice. Later retention must respect consumer
checkpoints, approval/effect audit retention, and artifact references. An expired cursor returns
a structured `cursor_expired` with the oldest available position; never quietly skip a gap.
Deployment restore/clone needs preserved identity or an explicit new stream generation so old
cursors cannot silently address unrelated history.

## Existing state

Settled while implementing the store half: this is a new system with no history worth preserving,
so no import or migration machinery is built. What is owed is readability — legacy single-record
lines and `transaction_version` 1 transactions without activity still load unchanged — and an
honest starting point. The `activity.stream_started` marker the store appends on first use records
that nothing precedes it, which is what lets a reader tell an empty history from a lost one.
Records that predate the stream keep their own timestamps and are not fabricated into events.

If a future deployment ever does need imported history, it needs deterministic import identities,
a resumable guarded import, effect routing disabled for imported events, and no claim that import
order was historical causality. None of that exists and none of it should be built speculatively.

## Proof

- State/activity atomicity under process kill, partial trailing write, guard conflict and replay.
- Two writers, identical timestamps, clock skew, multi-event transaction and duplicate producer
  input yield stable identities/order; filtered list/follow resumes without misses.
- Kill a real isolated test runtime during output. Committed observations and artifact chunks
  remain inspectable; recovery reports uncertain capture; outcome is not fabricated.
- Large-output fixture demonstrates bounded memory and responsive bounded queries, with measured
  history size and latency. Set concrete acceptance thresholds from the local baseline before
  the slice lands, rather than asserting unlimited scale.
- Existing state and schemas remain readable; bootstrap repeats safely and never routes effects.
- CLI and future UI query the same state-independent projection; reading history performs no writes.

### Slice 2 results, measured 2026-09-09

Runtime capture proved against `4f7bb0f` and re-run on `main` at `08f5c0e` after the wiring
fixes (`td-bce761`).
Machine: Apple M4 Pro, macOS 26.6.2, ruby 4.0.6 (arm64-darwin23). No Docker: every runtime test
drives a real child process through a Ruby stub standing in for the `docker` binary, so the pipe,
the reader thread, the bounded queue and the teardown are the real ones.

| Claim | Test | Result |
| --- | --- | --- |
| Acknowledged activity survives a kill mid-stream | `capture_kill_mid_stream_test`: `…leaves_every_acknowledged_event_backed_by_real_bytes` | Pass. Forked worker running `Engine#execute` (Pi harness over the Docker runtime, stub child streaming pi JSON lines forever), SIGKILLed after ≥3 `runtime.observed` and ≥2 `agent.tool_observed` commits. Every survived event's chunk exists, digest and size match; no `.part` is referenced; the `runtime_streams` row is open with `last_offset` > 0 |
| Recovery reports the gap and fabricates nothing | `capture_kill_mid_stream_test`: `…calls_the_unobserved_interval_a_gap_and_fabricates_no_success` | Pass. `worker_interrupted` with `capture.status == "gap"`, one unobserved interval, synthetic outcome `failed`/`interrupted` carrying `capture` and `raw.stream_refs`; no run or attempt reports success |
| Operator surfaces show what survived | `capture_kill_mid_stream_test`: `…shows_what_survived_and_the_capture_it_could_not_finish` | Pass. `activity list --work ID --json`, `activity show EVENT`, `show WORK_ID` |
| Large output stays bounded | `capture_bounds_proof_test`: `…stays_within_every_stated_bound` | Pass, see table below |
| Bounded queries at that history size | `capture_bounds_proof_test`: `…activity_queries_stay_bounded_at_the_history_the_fixture_produced` | Pass |
| Truncation past a byte limit keeps decisions | `capture_bounds_proof_test`: `…is_truncated_and_still_says_what_the_agent_did` | Pass. 64 KiB limit against ~840 KiB of pi output: coverage `truncated`, all 200 `agent.tool_observed` still committed, byte-less flushes carry `reason: "limit"` and their record counts sum to the checkpoint's total |
| Duplicate replay deduplicates | `capture_replay_dedupe_test`: `…appends_no_history_and_repeats_no_identity` | Pass. Interrupted pass, then a full replay from byte zero through `Fake::Runtime`: identical `(type, record_index, event_id)` sequence to an uninterrupted run, no identity twice |
| Divergent replay refused | `capture_replay_dedupe_test`: `…is_refused_rather_than_spliced`, `…reports_a_failed_capture_rather_than_succeeding` | Pass. `CaptureError`, no history written, durable bytes untouched; through a runtime it surfaces as a failed capture |
| Partial/multibyte lines reconstruct | `record_framer_test` (5 cases), `docker_capture_test` (split across pipe reads), `pi_stream_interpreter_test` (split across writes), plus `capture_replay_dedupe_test`: `…cut_by_a_chunk_boundary_survive_a_resume` for the persisted-boundary case | Pass |
| Persistence failure is explicit | `capture_persistence_failure_test` (3 tests) | Pass. Failing chunk append: child stopped (bounded elapsed against a child that would otherwise sleep 30 s), `capture.status == "failed"` on the Engine result, the run record and `show`; no event names a missing or mismatched file; a restart's `Recovery` reconciles it to `execution_failed`; no external action authorized |
| Publish/finalize refused on an unaudited stream | `container_phase_runner_test`: `test_publication_is_refused_when_a_stream_could_not_be_audited` (pre-existing) | Pass |

**Bounded-output measurements.** Fixture: irregular lines 8–3000 bytes, multibyte, invalid UTF-8,
one record larger than `max_record_bytes`, two secrets, streamed from a child process through
`Adapters::Docker::Runtime` (`read_bytes` 64 KiB, `queue_limit` 8) into `CaptureSink::Durable`.
Peaks are sampled on the controller thread from the `runtime_progress` callback.

| | 4 MiB (default run) | 64 MiB (`BACKSTAGE_CAPTURE_PROOF=1`) | Acceptance threshold |
| --- | --- | --- | --- |
| Fixture / captured bytes | 4,268,925 / 4,263,781 | 67,249,167 / 67,166,977 | — |
| Records / chunks / chunk files | 2,764 / 66 / 66 | 44,235 / 1,025 / 1,025 | chunks == `ceil(bytes / 64 KiB)`, exactly |
| Elapsed | 0.43–0.54 s | 8.49–8.65 s | ≤ 18 s at 64 MiB |
| `total_allocated_objects` delta | ~313,000 | ~53.2 M | ≤ 110 M at 64 MiB |
| `heap_live_slots` delta after `GC.start` | 258–952 | 1,217–1,248 | ≤ 200,000 (asserted) |
| Peak `buffered_bytes` | 3,911 | 58,083 | ≤ `flush_bytes` (65,536), asserted |
| Peak `pending_bytes` | 1,489 | 1,909 | ≤ `max_record_bytes` (65,536), asserted |
| Peak `retained_frames` | 2 | 44 | ≤ `flush_records` (1,000), asserted |
| Peak `retained_bytes` | 3,003 | 57,752 | ≤ `flush_bytes + 2 × max_record_bytes`, asserted |
| `state.jsonl` | 436,723 B | 5,464,702 B | ≤ 12 MB at 64 MiB |
| Activity events | 132 | 2,051 | — |
| `activity list --limit 100` | 0.004 s | 0.015 s | ≤ 0.03 s (2× measured) |
| `activity follow --max-passes 3` | 0.006 s | 0.042 s | ≤ 0.09 s (2× measured) |

The latency thresholds above are the review gate. The automated assertions are deliberately looser
(1 s and 2 s): a wall-clock bound at 2× is flaky on a machine running anything else, and what an
assertion has to catch is a page that starts materializing the transcript, which at this history
size is orders of magnitude away rather than a factor of two. Allocation growth is superlinear in
history because `CaptureSink::Durable` commits once per chunk and the JSONL adapter rescans the
whole log per guarded commit — the rebuildable sequence index this document already names as the
first optimization is what removes it, and elapsed time is still near-linear (16× data, 17× time).

Commands, from the worktree root:

```
bundle exec ruby -Ilib -Itest test/capture_kill_mid_stream_test.rb      # 3 runs, 139 assertions, 0F 0E (1.0–1.3 s)
bundle exec ruby -Ilib -Itest test/capture_bounds_proof_test.rb         # 4 runs,  64 assertions, 0F 0E, 1 skip (1.5 s)
BACKSTAGE_CAPTURE_PROOF=1 bundle exec ruby -Ilib -Itest test/capture_bounds_proof_test.rb   # same, 17.98 s
bundle exec ruby -Ilib -Itest test/capture_replay_dedupe_test.rb        # 4 runs,  54 assertions, 0F 0E (0.04 s)
bundle exec ruby -Ilib -Itest test/capture_persistence_failure_test.rb  # 3 runs,  33 assertions, 0F 0E (3.9 s)
bundle exec rake test                                                   # 395 runs, 19,017 assertions, 0F 0E, 7 skips (25.6 s)
```

`BACKSTAGE_CAPTURE_REPORT=1` prints the measurement lines. The chunk-boundary defect the proof run
found (an oversized record ending exactly on a chunk boundary was dropped) was fixed in
`td-bce761`; its reproduction now runs ungated as a regression, and the full suite at `08f5c0e` is
413 runs, 19,237 assertions, 0 failures, 6 Docker skips (0 skips with the worker image present).

**Two findings, recorded rather than fixed** (routed on `td-095cd0`):

1. *An oversized record whose end offset lands on a chunk boundary is silently discarded.*
   `RecordFramer` emits a truncated record only once one byte past `max_record_bytes` arrives, but
   `StreamWriter#absorb` buffers before it frames, so the chunk covering that record's bytes is
   emitted first and `build_chunk` advances `framed_offset` past it; the frame that arrives one
   byte later is dropped by `record`'s resume-dedupe guard. The record is not counted in `records`
   or `truncated_records`, no `runtime.observed` names it, and the interpreter never sees it — so a
   pi `message_end` or tool result longer than 64 KiB would produce no `agent.*` event at all. Not
   a corner case under the defaults, where `flush_bytes == max_record_bytes == 64 KiB`. Reproduced
   by `capture_bounds_proof_test#test_an_oversized_record_ending_on_a_chunk_boundary_is_silently_discarded`
   (gated behind `BACKSTAGE_CAPTURE_DEFECT=1` so the suite stays green). This contradicts the
   port's "Nothing is dropped" and "Every record counted in a checkpoint is named by one of the
   two"; the byte trail itself is unaffected.
2. *The bounded log tail can exceed its stated size.* `Docker::Runtime::Tail` trims its binary
   buffer to exactly `log_tail_bytes`, then `#text` scrubs to UTF-8 and each invalid byte becomes a
   3-byte U+FFFD, so `outcome["logs"]` measured 65,560 bytes against a stated 65,536, and could
   reach 3× in the worst case. Still bounded — the memory claim holds — but the stated number is
   imprecise. `Adapters::Fake::Runtime` has the same pattern.

**Not done:** no Docker variant of the kill test. Killing a real container mid-stream and asserting
the same properties costs a container per assertion and risks leaving one behind on failure, for no
claim the stub child does not already make about this code — the pipe, the reader thread, the
bounded queue and the teardown are identical. The Docker contract proof stays where it is, behind
`BACKSTAGE_DOCKER_TEST=1` in `docker_runtime_test`.
