# Backstage operator guide

## Current execution scope and planned authority

The current live path produces draft PRs; `--publish-draft` does not authorize other external
actions. Deployment and operational repair are intended product capabilities, initially requiring
human approval of an exact action, resource/environment, and immutable inputs. They will execute
through scoped adapters with a durable action ledger, preflight checks, reconciliation, and result
verification. They are not available through current commands or a new YAML permission alone.
See the [activity and effects plan](../../plans/active/activity-and-effects/README.md).

## Configuration

`packs/example/backstage.yml` selects adapters, the worker image, model defaults, policy, credential references, and the default work lifecycle. Each `packs/example/targets/*.yml` file claims one trigger source and defines one repository's authority, its optional read-only context repositories, and optionally its own workflow. Each `packs/example/workflows/*.yml` file defines one lifecycle; see [the configuration model](config-model.md#workflows).

Run this after every configuration edit:

```sh
bin/backstage config check --pack packs/example --json
```

The example pack maps these references:

| Reference | Source environment | Worker environment |
|---|---|---|
| `github` | `GITHUB_TOKEN` | `GH_TOKEN` |
| `openrouter` | `OPENROUTER_API_KEY` | `OPENROUTER_API_KEY` |

Use a fine-grained GitHub token restricted to the designated repository with Contents read/write and Pull requests read/write. Do not use a broad administrative token for proof. Backstage cannot verify repository selection from a token value, so that scope is an operator precondition.

## Work lifecycles

Every work item follows a workflow chosen from the pack when it is admitted. Inspect what an
instance offers and where a particular assignment stands:

```sh
bin/backstage workflows --pack packs/example --json                 # every lifecycle this pack offers
bin/backstage workflows independent-review --pack packs/example --json  # one, in full
bin/backstage transitions WORK_ID --pack packs/example --json       # what is available right now
bin/backstage history WORK_ID --pack packs/example --json           # every transition already taken
```

`transitions` reports each available transition with the actors who may take it, the evidence it
requires, whether it dispatches work, whether it answers an open decision, and whether a decision
or revision budget blocks it. Evidence and actor checks still run when a transition is requested.

The shipped pack contains three:

| Workflow | Shape |
|---|---|
| `independent-review` | The default. Implementation, then a fresh independent review, ending in a draft ready for human disposition. Returned findings revise the same assignment, bounded by `max_revisions`. |
| `minimal` | `new -> in_progress -> done`. No reviewer, no evidence, no decisions. |
| `human-gated-change` | An agent prepares a change and a person approves or declines it. No reviewer participates at all. |

`bin/backstage process WORK_ID` advances an assignment as far as its workflow allows: it takes the
starting transition, runs each dispatched job, applies the transition the workflow names for that
outcome, and follows a state's `continue`. It stops at anything terminal, anything waiting on a
person, and an exhausted revision budget. Nothing requires a keypress, and a state change never
implicitly starts a worker — an informational transition such as `report_progress` is only a
report.

To move an assignment by hand:

```sh
bin/backstage transition WORK_ID --to cancel --reason "superseded" --pack packs/example --json
bin/backstage transition WORK_ID --to start --actor system --request-id nightly-42 --pack packs/example --json
```

`--request-id` makes the call idempotent: a repeat returns the recorded result, and the effect
runs once. Retries must retain the same work, actor, reason, evidence, and decision payload;
reusing an id for a different request is refused. `--expect-state` and
`--expect-revision` refuse the call if the work moved since you looked, which is how two operators
or an operator and a running controller avoid overwriting each other.

## Human decisions

When work enters a state that awaits a decision, Backstage records the question, the choices, and
the candidate it was asked about. `show --json` carries the open decision, and answering it is one
command:

```sh
bin/backstage show WORK_ID --pack packs/example --json          # read the question and its choices
bin/backstage decide WORK_ID --choose approve --reason "checked by hand" --pack packs/example --json
```

An answer is idempotent, and a decision raised against one candidate cannot authorize a different
one or a later work revision. Only the trusted human entry can answer a decision. Recovery never
clears a wait on a person, whatever happened to the worker.

The default independent-review workflow can resume or cancel after a blocked review; it cannot
complete by bypassing the review. The human-gated example demonstrates local workflow completion,
but cannot authorize GitHub/td handoff: external writeback still requires an approved independent
verdict bound to the completion transition and its candidate.

## Actors and the local trust boundary

Four authorities are distinct, and which one a request carries is decided by where it entered
Backstage — never by a field in the request:

| Entry | May act as |
|---|---|
| Operator CLI (`transition`, `decide`) | `human`, `system` |
| Backstage's own controller and recovery | `system` |
| A running worker's request channel | `agent`, `reviewer`, scoped to the run that owns the channel |
| A completed run's persisted outcome | `agent`, `reviewer`, with identity read off the run |

So `--actor human` from the trusted local CLI is a human decision, while a worker claiming to be
one is refused, and a worker cannot approve its own change: `approve` is reviewer-only and requires
an independent verdict artifact bound to the exact candidate digest under review. New candidate
content invalidates an earlier approval.

Workers reach that channel through a bounded, size-limited file the controller drains. Inside the
container, `backstage-agent-request TRANSITION [REASON] [ARTIFACT_ID...]` files one request. The
worker holds no store, no host credentials, and no CLI authority; a request naming a transition it
may not take is recorded as rejected and changes nothing. The bundle's `workflow_context` lists
role-appropriate transitions, state, revision, and candidate. The controller handles candidate and
review completion evidence after the run. A worker can supply a contextual question with:

```sh
backstage-agent-request --json '{"transition":"escalate","request_id":"question-1","decision":{"question":"Which protocol must remain compatible?","context":"Two clients still use v1"}}'
```

The v2 request contract also accepts `expected_revision`. Human identity and other work/run ids are
never accepted from the payload. Same-run informational state changes retain the worker's authority;
a human decision, cancellation, or newer dispatch fences it. Channel replacement, truncation, or
bounds violations close the channel with a bounded rejection record. Runtime events and heartbeats
make requests visible during quiet runs, without waiting for the final transcript.

`process --json` includes `halt_reason`, distinguishing an active execution, a human wait, exhausted
revision budget, stopped execution, and a terminal state. Repeated `process` calls do not duplicate
an active dispatch. Failed or cancelled runs stop that invocation; explicitly process again after
inspecting the failure if continuing is appropriate.

## Durable execution and the dispatcher

`process` runs one work item in the foreground of the process you typed it into. The dispatcher is
the durable version of the same journey: you accept a work item once, and a local dispatcher keeps
making progress across restarts, crashes, human waits, and delayed retries.

Only explicitly accepted work is ever dispatched. Backstage does not scan the store for things to
pick up, so a work item that nobody accepted stays exactly where it is.

```sh
bin/backstage dispatch accept WORK_ID --pack packs/example --json           # accept for the fake journey
bin/backstage dispatch accept WORK_ID --publish-draft --json              # accept for the real path
bin/backstage dispatch pass --pack packs/example --json                     # one bounded pass, then exit
bin/backstage dispatch pass --limit 1 --work WORK_ID --json               # bound the pass further
bin/backstage dispatch run --pack packs/example --json                      # foreground worker until signalled
bin/backstage dispatch status --json                                      # queue summary, current and last owner
bin/backstage dispatch show WORK_ID --json                                # why this item is where it is
bin/backstage dispatch list --all --json                                  # every acceptance, including closed ones
bin/backstage dispatch cancel WORK_ID --reason "no longer wanted" --json  # stop this acceptance
```

Acceptance is idempotent: repeating `dispatch accept WORK_ID` returns the acceptance it already
made, marked `deduplicated`. The same `--request-id` with a different payload is refused.
Re-accepting work whose acceptance was cancelled or exhausted starts a fresh generation.

One work item has one active acceptance. To replace it, `--supersede` cancels the old one and starts
generation N+1 in a single commit; the superseded generation's run is cancellation-requested, and the
new generation does not launch while that execution is still resolving. Passing both `--request-id`
and `--supersede` with an identity that is already recorded is refused — supersede asks for a new
generation, so it needs a new identity.

`--publish-draft` is required at acceptance *and* at the consuming dispatcher. A dispatcher started
without it reports real work as unauthorized and leaves it queued; a dispatcher started with it
still runs work accepted as fake as fake. No flag anywhere upgrades an existing acceptance.

While an acceptance is active, `process WORK_ID` refuses. Cancelling or superseding the acceptance
is how you take the work back — that keeps authorization and attempt accounting honest.

### What each status means

| Status | Meaning |
|---|---|
| `queued` | Accepted and eligible. The next pass runs it — unless it was accepted for real execution and the dispatcher running is a fake one, in which case it stays queued with that reason on it. |
| `running` | An execution is in flight, or a live dispatcher holds the claim. Nothing is launched beside it. |
| `waiting` | A human answer is required. Answer it with `decide`; no pass ever answers for you. |
| `delayed` | A bounded retry is scheduled. `next_wake_up` is the persisted due time. |
| `uncertain` | Runtime liveness could not be established. Never retried; resolve it with `recover` or `cancel`. |
| `blocked` | The dispatcher stopped and will not act without you — a cancelled run, an unresolved external effect, a workflow that offers no automatic dispatch, a contract or authority refusal, or repeated dispatch attempts that never produced a run. A block is durable: no later pass steps over it. It lifts only when *you* move it — an answered decision or a transition you take yourself — or when you cancel the acceptance or accept a new generation with `--supersede`. Backstage's own automatic progress never lifts it, and a reason that forbids the work outright (an unresolved external effect, a dispatcher that keeps dying before it can record a run) keeps it blocked even after you move the work item. |
| `completed` | The work item reached a terminal state in its workflow. |
| `cancelled` | You stopped this acceptance. |
| `exhausted` | No automatic attempts remain. `stop_reason` says why. |

An acceptance also stays open while an execution is still resolving, even if the work item is
already in a terminal state — the dispatcher that must reconcile that run keeps owning it, and the
acceptance closes on the pass after the run finishes.

Each intent in a `dispatch pass` report carries an `action` saying what the pass did with it:
`dispatched`, `deferred` (eligible, but the pass hit its `--limit` or the dispatcher is stopping),
`observed` (its status was re-read, nothing to start), `skipped` (terminal, or durably blocked, or
needing authorization this dispatcher lacks), `scheduled_retry`, `exhausted`, `completed`, `blocked`,
and `fenced` (something moved underneath it; the next pass re-reads it).

`--limit` bounds how much a single pass starts, not throughput: a worker whose pass deferred work
only because of its limit begins the next pass immediately.
A pass that deferred work for any other reason — an authorization this process lacks, a durable
block — waits normally, because nothing it can do would change that.

`dispatch show` adds the current and last execution, the open decision, the last error, attempts and
retries remaining, and an `actions` list naming the commands that would move this item along.

### Retries

Retries are off by default: the first failure stops the work with a reason. `dispatcher.retry` in
the pack sets the default, and `--max-retries`, `--retry-delay`, `--retry-backoff` and
`--max-retry-delay` set it per acceptance. Whatever is chosen is *pinned onto the acceptance*, so
editing the pack later never changes a budget that is already authorized.

A dispatcher that dies between claiming an attempt and launching anything did not fail at anything,
so the acceptance is simply queued again with the attempt recorded; only a claim that keeps ending
that way is stopped as `blocked`. The stop leaves the retry budget untouched.

`max_retries` counts automatic retries *after* the first failed attempt: 0 means one attempt,
2 means up to three executions. `attempts_used` counts every dispatch, `retries_used` counts only
automatic retries, and neither is related to the workflow's own revision budget. A retry's budget is
spent when it is *scheduled*, so a crash can lose an attempt but never reclaim one, and a restart
never resets a deadline or a budget.

Only known failures are retried, and only after recovery has applied the workflow's declared
failure transition. Cancellations, human rejections, fenced or stale work, contract and authority
failures, unresolved external effects, and unknown runtime status are never retried automatically.

### Running it under a supervisor

`dispatch run` is a foreground process on purpose: it installs nothing, daemonizes nothing, and
touches no tmux server. An external supervisor runs it and restarts it, and a restart is just the
recovery path every pass already takes.

```sh
# launchd, systemd, or anything else that keeps a foreground process alive
exec /path/to/backstage dispatch run --pack /path/to/pack --interval 5
```

One store has one local dispatcher. Ownership is an exclusive lock on `<state>.dispatcher.lock`,
which the operating system releases when the process dies, so a crash never leaves the queue owned.
A second dispatcher exits immediately naming the pid and host that hold it. `dispatch status` reports
`owner` only while the lock is genuinely held, and `last_owner` for whoever held it most recently.

SIGINT and SIGTERM stop new dispatches. The check runs before every launch inside a pass.
Remaining work is reported as `deferred`. The dispatch already in
flight drains, and the process then exits reporting `stop_reason: signal:TERM`. A hard kill during a run leaves the persisted runtime
identity and owner pid behind, which is exactly what recovery reads on the next pass: a provably
dead runtime is reconciled, a live one is left alone, and an unresolvable one stays `uncertain`.

## Recovering interrupted work

```sh
bin/backstage recover --pack packs/example --json            # every unfinished assignment
bin/backstage recover WORK_ID --pack packs/example --json    # one
```

Recovery inspects what was persisted and what is actually alive before deciding anything:

| Finding | What it means and what recovery does |
|---|---|
| `worker_active` | The container is still running. Nothing is launched to replace it. |
| `runtime_unknown` | Liveness could not be established. It stays unknown and actionable; nothing is guessed. |
| `worker_interrupted` | The worker is provably gone with no outcome. The run is marked interrupted, artifacts are preserved, and the workflow's failure transition makes continuation available. |
| `outcome_recorded` | A run finished before its transition was written. The recorded outcome is authoritative and the transition is applied from it. |
| `dispatch_pending` | An execution was requested but never started. `process` will pick it up. |
| `awaiting_decision` | A person owes an answer. Recovery reports it and touches nothing. |
| action `fenced` | The result belongs to a run that was cancelled or replaced. It stays on the run and cannot overwrite the later decision. |

Neither silence, an elapsed timeout, a clean exit, nor a model's own claim of approval satisfies a
review or a human requirement. Reconciliation is idempotent, so running it twice changes nothing
twice.

## Activity history

Every state change lands beside an immutable, append-only activity event in the same commit. The
CLI reads that history through the same query a future UI/API will share; reading it never writes:

```sh
bin/backstage activity list --json                                        # the whole stream, oldest first
bin/backstage activity list --work WORK_ID --json                         # scoped to one work item
bin/backstage activity list --run RUN_ID --kind execution.started --json  # by run and event type (repeatable)
bin/backstage activity list --related ARTIFACT_ID --json                  # anything touching this id
bin/backstage activity show EVENT_ID --json                               # one event
bin/backstage activity follow --interval 2 --json                         # foreground; polls until signalled
```

`list` returns one bounded page: its events, the cursor it was read from, `next_cursor` to resume
from, `high_water_mark` (the latest committed position at read time), and `caught_up`
(`next_cursor == high_water_mark`). A filtered page can be short or empty while more history
remains. Keep passing `next_cursor` back as `--after` until `caught_up` is true. A cursor is bound to the exact filters it was issued
for and to this deployment. Resuming with different filters, or against a different deployment,
raises `Backstage::ActivityCursorError` (its `code`, e.g. `cursor_filter_mismatch`, is
visible under `--json`). The stored history stays as it was.

`follow` is a foreground process, like `dispatch run`: it prints each page as it reads it and
blocks between passes, waiting `--interval` seconds once it is caught up or when a pass read
nothing and the cursor did not move, until `--max-passes` reads have happened or it is signalled
(SIGINT/SIGTERM). `--jsonl` output prints one line per event followed by one trailing
cursor-metadata line per page. A cursor that points past the stream's newest event (for example
after restoring an older copy of the state log) is refused with code `cursor_ahead_of_stream`.

### Reading a run's capture

Every runtime step's output is captured into chunk files under
`<artifacts>/<work_item_id>/<run_id>/streams/<index>-<step>/` (`0.log`, `1.log`, ... and a
`stream.json` manifest), and each chunk is acknowledged by a `runtime.observed` event. Agent
messages and tool calls appear as `agent.message_observed` and `agent.tool_observed` with bounded
previews; the full text stays in the chunk files. `show WORK_ID --json` carries a `capture` block
per run and `process --json` carries it per step:

| `capture.status` | Meaning | What to do |
| --- | --- | --- |
| `complete` | Every byte durable, every record read whole | Nothing |
| `truncated` | Output past a byte limit was not persisted, or one record exceeded its record bound (`truncated_record_offset`) | Inspect the chunk files; raise the `capture` limits in the pack if this is routine |
| `open` | A stream is still being written | Wait, or check the worker is alive |
| `gap` | The worker died with a stream open; output after `last_offset` may never have been received | Run `backstage recover`; treat the run as interrupted, not failed by the agent |
| `failed` | A chunk or its commit could not be made durable; the runtime was stopped and no publish, finalize or review verdict is accepted | Fix storage, then `backstage recover` and re-dispatch |

A Pi record larger than the protocol record bound produces an `incomplete` outcome with
`incomplete_reason: record_truncated`.

## Safe local proof

The broad local proof is network-free after the image exists and does not touch `td`, GitHub, a model provider, or tmux:

```sh
ruby -Itest -e 'Dir["test/**/*_test.rb"].sort.each { |file| require_relative file }'
BACKSTAGE_DOCKER_TEST=1 bundle exec rake test
bin/backstage config check --pack packs/example --json
```

The Docker contract launches a trivial job, redacts an injected canary secret, proves timeout and controller cancellation, checks that containers are removed, and regression-tests the default-branch refusal before any network or credential access.

The agent checkout is finalized without repository credentials into a size-bounded binary patch that records its base, branch, and digest, including untracked files and deletions. Publication runs in a separate credentialed worker that mounts only that artifact, creates a fresh checkout from the authorized HTTPS repository/remote branch, verifies the base, and applies the patch. The agent never sees that publication checkout. Immediately before effects, the worker loads the same state-free Ruby authority core as the host and checks the fresh checkout's actual HTTPS origin, generated branch, configured base, and draft-only action. Hooks, credential helpers, SSH, ambient Git/`gh` configuration, fsmonitor, and checkout-controlled URL rewrites are excluded. The host performs no repository mutation or `gh` command; it validates the worker's structured response and updates the external-action ledger.

## Real proof checklist

A real run mutates the target repository and calls a model. Use your own pack for this, not the fictional paths in `packs/example`. Before running it:

1. Point a target at one repository and one td workspace, and mark a small, reversible issue `agent-ready`.
2. Confirm `GITHUB_TOKEN` is limited to that repository (contents and pull requests). Confirm the target's remote and default branch. Backstage cannot check which repositories a token covers.
3. Export `OPENROUTER_API_KEY` without printing it. Treat the provider account as the spend ceiling. pi's own cost field is not authoritative for the model named in the pack.
4. Poll and note the returned work ID:

   ```sh
   bin/backstage td poll --pack /path/to/your-pack --target your-target --json
   ```

5. Run the only publishing command:

   ```sh
   bin/backstage process WORK_ID --pack /path/to/your-pack --publish-draft --writeback --json
   ```

6. Confirm the result reaches `completed` with a draft PR URL and an approved independent-review verdict bound to the published candidate. Post the returned handoff through the `td` adapter, request review, approve it in an independent session, then poll again and confirm one new `approval:rv-*` trigger.
7. If a run is cancelled or fails after an external write, run `bin/backstage recover WORK_ID --pack /path/to/your-pack --json`, then repeat the process command and confirm it fetches the recorded remote branch, reconciles the same draft PR, and does not duplicate identical `td` handoff/review writes.
8. Scan state and artifacts using canaries derived from the in-memory credential values without printing those values:

   ```sh
   ruby scripts/scan-secrets .backstage/state.jsonl .backstage/artifacts
   ```

The command exits nonzero if a configured credential value occurs in a file. It reports only the credential's environment name and matching path, never the value.

## Cancellation

`DockerRuntime` owns cancellation independently of pi's event stream. It asks Docker to stop the named job container, waits a bounded grace period, then kills it if necessary. A controller-requested cancellation is recorded as `cancelled` even when pi exits without `message_end` or `agent_settled`. The default tmux server is never involved.

Use `bin/backstage show WORK_ID --pack packs/example --json` to find the active run ID, then request cancellation from another process:

```sh
bin/backstage cancel RUN_ID --pack packs/example --json
```

The cancellation request is persisted in the pack's state file. The running engine polls that record through its normal cancellation callback, so repository preparation, context materialization, implementation, and finalization all end in a `cancelled` run. The outcome stays cancellation.

Cancelling a run is not cancelling the assignment. A cancelled run takes the workflow's failure
transition — in the default lifecycle, back to `ready` — leaving the work eligible to continue.
To stop the assignment itself, take the `cancel` transition.

## State, inspection, and overrides

The deployment pack owns the default `state.path` and `artifacts.path`; both expand `~`. Use `--state` and `--artifacts` only for deliberate one-command overrides, or their `BACKSTAGE_STATE` and `BACKSTAGE_ARTIFACTS` equivalents. `show --json` returns the work item with its workflow binding and current `state`, plus its jobs, runs, attempts, artifacts, external actions, transition history, decisions, and agent requests. The work item's `state` is its position in its workflow; `status` on a job, run, or attempt describes execution only, and a failed run does not by itself mean the work failed.

The target, source instance, and source identity are bound when a manual submission or `td poll` ingests work. Processing never accepts a replacement source identity and an idempotent resubmission with a different binding is rejected. Packs contain credential references only; `config check` recursively rejects secret-like raw fields and unsafe context names or mount paths.

Recovery also inspects unfinished execution belonging to cancelled/terminal work, without reopening
it. For a runner that guarantees identity is recorded before launch, a dead controller with no
identity proves launch did not occur and permits interruption recovery. An uninstrumented runner
with missing identity remains unknown. A missing container while its controller is alive can be a
normal gap between phases and does not establish interruption.

State uses newline-terminated JSONL transaction envelopes. An incomplete trailing transaction is
ignored and removed before the next write; malformed complete records remain an explicit error.
Shared reads and guarded batch commits keep transitions, decisions, and dispatch records consistent.
