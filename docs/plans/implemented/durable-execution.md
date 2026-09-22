# Durable local execution

Status: implemented. Landed on `main` from the `durable-execution` worktree. Owner: coordinating Codex
session; implementation and independent review ran in separate contexts.

## Outcome and scope

An operator can durably enqueue a work item, run a local dispatcher, stop it, and restart it without losing accepted work, human waits, delayed retries, or recorded results. The dispatcher uses the existing workflow controller, execution claims, recovery, and external-action ledger. This is a small durable interpreter of declared workflows, with checkpoints at agent-run and effect boundaries. Harness adapters retain responsibility for fine-grained session/tool continuation.

Keep Ruby standard/default libraries, JSONL through Ports::Store, and the shared composition root. Do not build a general replay SDK or a separate orchestration framework. The existing configurable workflow and independent-review safety contracts remain controlling. Storage transactions must be durable on successful acknowledgement, not merely visible to another process; inspect flush/fsync behavior and close that gap if needed.

Included:

- Explicit durable acceptance of selected work, with inspectable dispatch intent and execution authorization.
- A bounded one-pass dispatcher command and a foreground worker loop suitable for an external process supervisor.
- Startup reconciliation and periodic reconciliation of accepted work.
- Persisted due times and bounded retry accounting, independent of workflow revision limits.
- Recovery of completed-but-unapplied outcomes, conservative handling of uncertain execution, durable human waits and resumption after an authorized answer.
- Structured operator inspection and deterministic noninteractive controls through the shared application core.

Excluded: installing or activating a service, scheduled prompts, Tasks/td/health source watchers, intelligent routing, execution-profile redesign, distributed workers/leases, arbitrary parallel graphs, event-sourcing every internal call, and paid/live publication proof. A foreground worker is sufficient for this slice; document how an external supervisor invokes it without installing one.

## Contract

### Acceptance and authorization

Use a small persisted execution-intent record (or equivalent extension of existing records) for explicitly accepted work. Do not scan all existing work and silently adopt it. Persist the work identity, start selection if any, execution mode, status, due time, attempt/retry accounting, and operator-visible stop/error reason. Repeated acceptance with the same request identity deduplicates; conflicting payloads fail. Define whether a later explicit acceptance resumes or creates a new generation, and fence the old generation.

Fake remains the default. Real execution requires conspicuous --publish-draft authorization at acceptance and at the consuming worker; a fake worker must not consume a real intent or silently execute it as fake. A worker flag alone must not upgrade fake intents. Persist authorization without secrets; credentials resolve only through existing boundaries. Keep direct process/recover commands compatible and prevent direct processing from bypassing or corrupting an active intent.

### Dispatch and ownership

A one-pass command finds eligible accepted work, reconciles it, and performs bounded progress. A foreground worker repeats that operation with an interruptible polling delay. Use one local dispatcher owner per store for this slice, with crash-released ownership through an adapter; a second owner exits clearly. Existing per-job atomic claims and stale-result fencing remain authoritative, including against direct process callers. Do not hold the JSONL transaction lock while running an agent.

Inject clock/wait behavior for deterministic tests. Persist wall-clock due times; polling only discovers due work. A restart never resets deadlines or retry budgets. Handle SIGINT/SIGTERM by stopping new dispatches and preserving enough runtime identity for reconciliation; document whether an active run drains or is cancelled. No default tmux manipulation.

### Reconciliation and retries

Run existing recovery before startup dispatch and on subsequent passes as needed. Recorded terminal run results must be applied without rerunning the agent. Known-live runtime is left alone; unknown authority remains visibly blocked/uncertain, not retried. Dead runtime with sufficient evidence can be interrupted and reconciled. Human waiting states remain waits across restart and resume only after the existing authorized decision operation; no synthetic answers.

Retries are opt-in, bounded, and delayed, with a small configurable policy rather than arbitrary expressions. Default to no automatic retries. Distinguish execution attempts from workflow revision cycles. Retry only eligible known failures after recovery has applied the declared workflow outcome, and only where the workflow permits a new dispatch. Never automatically retry cancellations, human rejection, stale/fenced work, contract/authority failures, unresolved external effects, or unknown runtime status. Preserve the last error, next due time, and exhaustion reason. Persist eligibility and retry scheduling atomically enough that a crash cannot reset the budget or cause immediate unbounded work. Reuse existing ledger reconciliation before any external retry; this does not promise exactly-once external side effects.

### Inspectability and configuration

Expose accepted intent state, current workflow state, current/last execution, next wake-up, attempts remaining, waiting/blocked/exhausted reason, and actionable recovery controls via CLI JSON and human output. Keep lifecycle business rules in application/domain code. Add only the narrow ports actually needed (clock/ownership if appropriate), wired in bootstrap.

Keep configuration small and validate it. Pin retry policy for accepted work so editing defaults does not silently change active authorization or budgets. Workflow snapshots stay immutable. No model/harness policy expansion; preserve that future seam.

## Implementation sequence

1. Inspect existing Engine, Controller, Recovery, WorkflowService, JSONL commit and CLI; record final record/command/policy choices briefly here. Establish passing baseline.
2. Implement acceptance and persisted scheduling state with atomic deduplication and fencing. Prove it through fake work before adding the loop.
3. Implement one-pass dispatch, conservative reconciliation, bounded delayed retries and local ownership; then foreground worker and shutdown behavior.
4. Add operator/config documentation and structured inspection. Update cross-links to this plan without expanding its implementation scope.
5. Obtain independent Claude review of the complete change with a separate context. Repair findings, rerun affected checks, then integrated proof. Move this plan to implemented, record evidence and reviewer, commit and land on main, push.

## Design decisions (recorded during step 1)

Read first: `Application::Engine` (atomic dispatch claim, `finish_execution` merge loop), `Controller#process`
(step budget, `halt_reason`), `Recovery#reconcile` (alive/gone/unknown, outcome reconciliation),
`WorkflowService` (single transition operation, `superseded_dispatch?`, request-id idempotency),
`Adapters::Jsonl::Store#commit` (one exclusive lock, guard check, append, `flush` + `fsync`, complete-line
replay) and `Surfaces::CLI`.

- **Record.** A new `execution_intents` collection holds explicitly accepted work. Its id is derived from
  the acceptance request identity (`intent-<sha256(request_id)>`) so the store's absence guard makes
  deduplication atomic; a repeat with a different payload fails on a recorded request fingerprint.
- **Generations.** One active intent per work item. A later acceptance with a new request identity refuses
  by default and, with `--supersede`, cancels the old intent and creates generation N+1 in one commit,
  requesting cancellation of any run the old generation still owns. The new generation does not launch while
  that run is unresolved, and the old run is fenced where it always was — the engine records a cancelled
  outcome, and `superseded_dispatch?` plus the transition operation's run guards refuse a late transition.
- **Acceptance identity.** Without an explicit `--request-id`, the identity carries the generation, so a
  repeat of the same command deduplicates onto the acceptance it already made while a re-acceptance after a
  cancellation, an exhaustion, or `--supersede` mints a new one. `accept` returns `deduplicated` so a caller
  can tell "already accepted" from "accepted now".
- **Interrupted claims.** A dispatcher that died between claiming an attempt and launching anything produced
  no failure, so the acceptance is requeued with the attempt recorded rather than charged to the retry
  budget. Only a claim that keeps ending that way is stopped.
- **Statuses.** `queued`, `running`, `waiting`, `delayed`, `uncertain`, `blocked`, and the terminal
  `completed`, `cancelled`, `exhausted`. Only a due `queued` intent is ever dispatched, which is what keeps
  a workflow that returns to `ready` after a failure from retrying immediately. `blocked` is a *durable*
  gate rather than a one-pass pause: no later pass steps over it. It holds until a *person* moves the work item —
  an answered decision or an operator transition — or the acceptance is cancelled or superseded. Automatic
  progress never lifts it, and forbidding reasons (unresolved external effects, a repeating crash loop) are
  re-checked before any lift, so workflow progress can never clear work the contract forbids. `uncertain` stays derived from reconciliation each pass, so a runtime that
  becomes resolvable moves on by itself.
- **Ownership of a resolving run.** An acceptance is not retired while an execution is unresolved, even
  when the work item is already terminal, so reconciling that run stays owned rather than orphaned.
- **Accounting.** `attempts_used` counts dispatches; `retries_used` counts automatic retries and is
  incremented when the retry is *scheduled*, so a crash can only lose budget, never reset it. The budget is
  `retry_policy.max_retries` — retries *after* the first failed attempt, so 0 means the first failure stops
  the work — pinned onto the intent at acceptance and never re-read from the pack.
- **Commands.** `backstage dispatch accept|cancel|list|show|status|pass|run`. `pass` is the bounded one-pass
  command; `run` is the foreground worker. `process` refuses outright while an acceptance is active;
  cancelling or superseding it is the only way to take the work back.
- **Ports.** `Ports::Clock` (`now`, `wait`) and `Ports::DispatchOwnership` (`acquire`, `release`, `current`),
  wired in `Bootstrap::System`. Ownership is an `flock` on a lock file beside the state log, so a crash
  releases it; a second owner exits clearly.
- **Policy.** `dispatcher.poll_interval_seconds` and `dispatcher.retry.{max_retries,delay_seconds,backoff,max_delay_seconds}`
  in the pack, validated by `config check`. Default is no automatic retries.
- **Durability.** `commit` already fsyncs the appended transaction inside the exclusive lock; store creation
  now also fsyncs the containing directory so the log's own directory entry survives a crash.
- **Shutdown.** SIGINT/SIGTERM stop new dispatches, checked before every launch inside a pass as well as
  between passes; work the pass declines to start is reported as `deferred`. An in-flight dispatch drains. A hard kill leaves the
  persisted runtime identity and owner pid for recovery.

## Evidence

### Commands, at commit `214362f` (the last code change; the docs commit that follows changes no code)

The worker image was not rebuilt because no image input changed.

| Command | Result |
|---|---|
| `bundle exec rake test` | 219 runs, 2519 assertions, 0 failures, 0 errors, 6 skips (Docker-gated) |
| `BACKSTAGE_DOCKER_TEST=1 bundle exec rake test` | 219 runs, 2558 assertions, 0 failures, 0 errors, 0 skips |
| `bin/backstage config check --pack packs/example --json` | `valid: true`, including the new validated `dispatcher` policy |
| `scripts/scan-secrets lib test packs docs bin scripts schemas` | clean: 103 files, 36 configured values |

Baseline before the slice: 145 runs, 2003 assertions, 0 failures, 6 skips. The suite is stable across
seeds (7, 77, 777, 111, 222, 333 all green).

### What proves what

`test/dispatcher_test.rb` (57 tests) drives acceptance, deduplication, generations, classification,
retries, fencing and inspection against an injected clock and stubbed runtime presence.
`test/dispatch_worker_test.rb` covers ownership, the loop, signals, bounded memory and the polling
delay, including a real child process killed with `SIGKILL` to prove the lock is crash-released.
`test/dispatch_cli_test.rb` covers the operator surface, including a human wait answered through
`backstage decide` and an `uncertain` runtime explained by `dispatch show`.
`test/durable_restart_test.rb` uses the shipped CLI in separate processes: accept then dispatch from
another process; a dispatcher killed mid-attempt whose run is reconciled and retried; four processes
racing one acceptance with no ownership lock between them; a fresh process whose first classification
is a blocked acceptance; and a torn JSONL tail.

Against the plan's minimum list, each bullet is proven, with two stated limits:

- Accept, exit before dispatch, restart, completes once — proven in-process and through separate CLI
  processes, asserting the run count.
- Killed dispatcher during a known runtime — a real child holding ownership and an in-flight run is
  `SIGKILL`ed; while it lives a second dispatcher refuses and nothing is relaunched, and once it is
  provably dead the run is reconciled and the bounded retry completes the work.
- Completion persisted before its transition — applied on restart with the agent never re-run.
- Human wait — survives repeated restarts, resumes only after `decide`, exactly one continuation, and
  no pass ever answers a decision.
- Delayed retry — no early run before the due time, bounded attempts, exhaustion stable across
  restarts and across time.
- Crash around retry accounting and the dispatch claim — the budget is spent when a retry is
  *scheduled*, so it can only shrink; an interrupted claim requeues without spending it, and a
  repeating crash loop stops. Four real processes racing one acceptance produce exactly one run.
- Two workers and a direct `process` caller — the second dispatcher exits naming the holder, and a
  direct caller is refused while an acceptance is active, including against a live dispatcher.
- Cancellation, generations, stale outcomes — a superseded generation's late outcome is refused at
  the transition layer, a cancelled wait never wakes, and a new generation does not launch while the
  old execution is unresolved.
- Fake/real mismatches fail closed in both directions; no flag upgrades an accepted intent.
- Structured inspection explains every status, verified from fresh CLI processes with empty stderr.
- Persisted success survives the supported crash boundaries and JSONL partial-tail recovery.

### Command-level fake journey

One `dispatch pass` carried a work item through the whole configured `independent-review` lifecycle —
`start → report_progress → submit_for_review → approve` — with two runs recorded and the acceptance
closed as `completed`; a second pass considered nothing. A `publish_draft` acceptance was refused by a
fake dispatcher with the work item untouched; `process` was refused while that acceptance was active;
`--supersede` cancelled generation 1 and opened generation 2; and a real `SIGTERM` to `dispatch run`
stopped the worker with `stop_reason: signal:TERM` after its in-flight dispatch drained, releasing the
lock. `--limit 0` waits its poll interval; `--limit 1` drained three acceptances in 5.2s.

### Crash proof

Nine independent chaos rounds: three accepted work items per round, ten real dispatcher processes
`SIGKILL`ed at random points over one store, then passes to drain. Every round ended with all work
items terminal, all acceptances `completed`, and exactly one succeeded run per work item. Rounds that
killed a dispatcher mid-run recorded interrupted runs and still completed each item exactly once.

### Independent review

Reviewed by Claude Opus 5 (`claude-opus-5`) in a separate context with no part in the implementation,
over four rounds against successive repair tips. Quoted verbatim:

> Independent review of the durable local execution and supervised dispatcher slice was performed at tip
> `fc4a432` by Claude Opus 5 (`claude-opus-5`) in a separate context with no part in the implementation,
> over four rounds against five successive repair tips. The reviewer read the controlling plan,
> `AGENTS.md`, and the pre-existing Engine, Controller, Recovery, WorkflowService and JSONL store before
> judging, and verified behaviour by reproduction rather than by inspection alone: fifteen throwaway
> probes driving the shipped CLI and the library over temporary JSONL stores, including real killed child
> processes, a four-process claim race, mid-pass signal delivery, torn state-log tails, and forced crash
> windows between the dispatch claim and the run record. Three critical defects were found and confirmed
> by reproduction — a deduplication short-circuit that made every documented recovery command a silent
> no-op, an interrupted-claim rule that retired accepted work as exhausted under the shipped default
> policy, and three helper methods spliced into the body of `classify` that crashed the whole pass with an
> unhandled `NoMethodError` on the first restart after a block while 214 tests stayed green — together
> with four major and eleven minor findings. All were repaired and each repair was independently
> re-verified at the final tip. At `fc4a432` the reviewer confirmed: no nested method definitions anywhere
> in `lib/` by an independent Ripper scan and a clean `ruby -w` load; accepted work completing exactly
> once across restarts with no duplicate agent launch under crash or concurrency; four concurrent
> dispatcher processes with no ownership lock producing exactly one run; retry budget that can only
> shrink; unknown runtime never retried; human waits never answered synthetically and cancelled waits
> never woken; fake/real authorization failing closed in both directions with no flag upgrading an
> accepted intent; durable blocks that automatic progress cannot lift but a person's transition can; and
> JSONL partial-tail recovery preserving every complete transaction. Suites at that tip: 217 runs / 2512
> assertions and 217 runs / 2551 assertions under `BACKSTAGE_DOCKER_TEST=1`, both with zero failures and
> zero errors; `config check` valid; secret scan clean. Verdict: **approved**, with three non-blocking
> follow-ups recorded — a `--supersede` that silently deduplicates when it reuses the identity of the
> acceptance it means to replace (fix: include `supersede` in the request fingerprint), a
> `dispatch run --limit 0` hot loop (fix: wake immediately only when the pass also dispatched something),
> and an ownership probe that can briefly refuse a starting dispatcher. Durability on acknowledgement is
> verified by code inspection of the fsync path only, which is the honest limit of a test harness without
> a power-loss rig, and the per-pass whole-log read cost is recorded as the pressure signal for the
> JSONL→SQLite step rather than addressed here.
>
> — Claude Opus 5 (`claude-opus-5`), independent reviewing context, no involvement in the implementation.

All three follow-ups were fixed in `214362f` rather than deferred, each with a regression: superseding
is part of the acceptance fingerprint, a pass that started nothing never wakes the worker instantly,
and taking ownership retries briefly past a concurrent probe.

The review's own process observation is worth keeping: five repair commits landed during one review,
and the last of them broke `dispatch pass` on restart while the whole suite stayed green. The lesson is
recorded in the tests — a blocked acceptance is now exercised in a fresh process, because that is the
case a single test process structurally cannot see.

### Limitations

- Durability on acknowledgement is verified by inspecting the fsync path — `commit` writes, flushes
  and `fsync`s inside the exclusive lock, and store creation `fsync`s the containing directory — not
  against real power loss, which needs a harness this slice does not have. The artifact store's own
  file writes are not fsynced; that is pre-existing.
- No paid model call, no live `td` or GitHub publication, and no service installation or activation
  was performed. The real journey remains proven only through the fake path here.
- A pass reads the whole JSONL log several times per accepted item. That is fine at this scale and is
  the concrete pressure signal to name for the JSONL → SQLite step; the fix when it arrives is a
  read-through snapshot behind `Ports::Store`, not a schema change.
- Three follow-ups the reviewer raised were fixed rather than deferred, so nothing is outstanding.

## Acceptance evidence

Use isolated temporary stores, fake/injected runtimes and clocks, and real child-process termination where crash behavior matters. At minimum prove:

- Accept, exit before dispatch, restart: work completes once.
- Kill dispatcher during a known runtime: no duplicate launch while it is alive; dead runtime is conservatively reconciled. Unknown runtime blocks visibly.
- Persist completion then crash before workflow transition: restart applies the saved result without executing again.
- Wait for a human, restart repeatedly, answer through CLI, restart: exactly one eligible continuation, no approval bypass.
- Schedule retry, restart before due time, advance clock: no early run, bounded attempts, stable exhaustion across restart.
- Crash around retry accounting and dispatch claim: budget never resets; duplicate callers cannot run the same accepted execution twice.
- Two workers and a concurrent direct process caller: ownership/claim behavior is explicit and safe.
- Cancellation, new intent generation, and stale outcome fence old work; a cancelled wait does not wake.
- Fake/real mode mismatches fail closed; no implicit adoption or authorization escalation.
- Structured inspection explains queued, running, waiting, delayed, uncertain, exhausted, cancelled and completed work.
- Persisted success survives the supported process-crash boundaries and JSONL partial-tail recovery.

Run bundle exec rake test, BACKSTAGE_DOCKER_TEST=1 bundle exec rake test, bin/backstage config check --pack packs/example --json, and scripts/scan-secrets. Rebuild the worker image if its inputs changed. Document actual counts, independent review provenance, command-level fake journey evidence and limitations. Do not call paid models or perform live td/GitHub publication as product proof. Developer git commits/push/merge are authorized by repository convention.

## Handoff

Implementation agent must use a distinct TD_CONTEXT_ID and comms identity, report progress/blockers periodically, and keep this plan current. Preserve unrelated working tree edits. Review must be independent of implementation, and its findings and final disposition must be recorded. The coordinating session monitors until reviewed implementation is verified and landed.
