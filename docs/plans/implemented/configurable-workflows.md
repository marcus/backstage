# Configurable work lifecycles

Status: implemented. Tracking: `td-bd9135`.
Implemented by Claude Opus, with Codex review repairs and independent follow-up review on 2026-09-08.

## Outcome and scope

Backstage lets different kinds of work follow small, configurable workflows. Agents can report progress, attach evidence, request review or a human decision, and continue returned work. The same application operation handles CLI requests, normalized agent requests, reviewer outcomes, and human decisions. Work remains recoverable when an agent forgets a transition or execution ends unexpectedly.

Simplicity is a controlling design constraint. Reshape confusing internals rather than preserving accidental abstractions or building compatibility scaffolding. Preserve recorded artifacts and existing safety boundaries.

This plan is the decision record for configurable work lifecycles. Repository safety boundaries in AGENTS.md still apply.

## Code ownership

- `domain/workflow.rb` owns immutable workflow compilation and graph validation.
- `application/workflow_service.rb` owns transitions, request identity, authority, evidence, and decisions.
- `application/controller.rb`, `engine.rb`, and `recovery.rb` join configured progress to execution and reconciliation.
- `adapters/jsonl`, `adapters/local_files`, `adapters/docker`, and `adapters/pi` own durable storage, bounded worker requests, runtime observation, and harness translation.

The work item owns workflow position; jobs, runs, and attempts describe execution. No additional
assignment entity or parallel legacy workflow engine was introduced.

## Settled design

### Small declarative definitions

A deployment pack defines named workflows with an initial state, terminal states, named transitions, permitted actors, optional required evidence, and optional next-step behavior. Prefer ordinary YAML compiled to validated immutable records. No embedded Ruby, arbitrary shell hooks, expression language, visual editor, workflow framework, nested graphs, parallel joins, or new runtime dependency.

Support a pack default and explicit workflow selection on manual submission. Allow target configuration to select a workflow for sourced work; a second example must demonstrate a genuinely different lifecycle. Do not build a rule engine for job-type routing. Bind the selected workflow identity, version, and resolved definition or digest-backed snapshot when work is admitted. Later pack edits must not silently reinterpret in-flight work.

Validate definitions at `config check`: missing references, ambiguous transitions, unknown actors/behaviors, invalid requirements, and obvious unreachable/unterminated definitions. Cycles are valid for returned work. Keep diagnostics concrete; no general graph-verification machinery.

A three-state `new -> in_progress -> done` workflow is sufficient. Optional features must not require boilerplate there. Status changes may be informational only: changing state does not implicitly start a worker.

### Default workflow

Keep familiar names where useful. Start with these meanings and document the final graph before writing the execution integration:

| State | Meaning |
| --- | --- |
| `ready` | Work is eligible to begin or continue. |
| `running` | Work is being worked on; this is not a claim that a container is alive. |
| `awaiting_review` | A candidate and required evidence await independent review. |
| `changes_requested` | Review returned actionable findings; continuation preserves the assignment and candidate history. |
| `needs_decision` | Work waits for a human answer or authorization within product policy. |
| `completed` | This workflow's completion criteria are satisfied. |
| `cancelled` | The operator stopped the assignment. |

Expected paths: ready -> running -> awaiting_review -> completed; review -> changes_requested -> running; running or review -> needs_decision -> the explicitly recorded continuation state; applicable nonterminal states -> cancelled. Explicit reactivation may return cancelled work to ready. A decision resumes only a defined transition, never an arbitrary state supplied by an agent.

The default completed meaning remains an independently approved draft ready for human disposition, not merged or deployed. A workflow may instead require a human completion gate; prove this in an example. Do not add a second success state unless it expresses a real wait or decision. Execution failure is recorded on the run/attempt; it does not inherently mean the work is failed. Implementation may refine this graph for a clearer operator journey, documenting material changes in the plan.

### One transition operation

Transitions accept work identity, expected current revision/state, named transition, stable request identity, provenance, reason, and artifact/decision references as appropriate. Application code validates authority and requirements, records the transition, and exposes the result. Repeated identical requests return the recorded result; conflicting reuse and stale requests give actionable structured errors.

History records actor/source, from/to, workflow version, timestamp, reason, evidence references, and related execution or decision. State and transition history must remain consistent across interruption and overlapping callers. Choose the smallest store-level mechanism that proves this with the existing JSONL adapter; do not add a distributed coordinator or move databases without concrete need.

A requested next execution is durable and deduplicated against its transition. A crash between accepting a transition and dispatching must not lose the request or launch duplicate work. Use the existing jobs/runs model where possible. External effects still go through their owning adapters and external-action ledger, with preflight reconciliation.

### Actors and agent access

Working agent, independent reviewer, human/operator, and system are distinct authorities. An untrusted `actor: human` field or CLI flag is not evidence of a human decision. Document the local trust boundary: trusted operator commands and scoped worker requests are different entry contexts feeding the same core.

Provide discoverable noninteractive commands with global JSON/JSONL support to inspect workflows and allowed transitions, request a transition with evidence, supply a human decision, inspect history, and recover interrupted work. Names are implementation choices. No UI or HTTP server is necessary.

Provide a real agent-facing path within the existing isolated execution architecture. A bounded file/event request channel processed by the controller is acceptable and preferable to giving workers the authoritative store, host credentials, or operator CLI authority. Derive worker identity/role/scope from its run, not model text. Demonstrate at least one informational transition while a fake/local worker is still active; terminal-outcome-only handling is insufficient for kanban visibility. Keep the harness translation replaceable and avoid requiring proprietary transcript/session formats.

### Evidence and decisions

Reuse artifacts. Requirements name a small set of artifact/evidence kinds; reference existing persisted artifacts rather than embedding large bodies. An artifact must belong to the relevant work and candidate/run; presence alone is not proof that its contents are correct. Required validation and reviewer provenance remain explicit.

Bind approvals and review findings to the candidate revision/digest being reviewed. New candidate content invalidates prior approval for completion. Reuse unchanged context without approving a different patch by accident. A `proof_provided` state is optional; entering review can require proof without introducing another column.

A human decision records the question, permitted choices or expected answer, related work revision, and resulting transition. Duplicate decisions are idempotent; stale decisions must not authorize a newer candidate. Requests to expand authority cannot exceed Backstage's hard product limits.

### Recovery without pretending

The controller handles ordinary execution-derived transitions so progress does not rely solely on an agent remembering bookkeeping. Explicit agent requests remain useful for visibility, evidence, and decisions. Both paths use the same idempotent operation.

On recovery, inspect persisted run outcomes and runtime identity before deciding what happened. Reconcile trustworthy completion that was recorded before an interrupted transition. If execution is still alive, do not launch a replacement. If execution has ended without a valid outcome, preserve artifacts, record interruption, and make an explicit continuation available. Unknown runtime status must remain unknown/actionable rather than guessing death or success.

Neither silence, an elapsed timeout, a clean process exit alone, nor a model's self-asserted approval satisfies review or human requirements. Do not clear `needs_decision` because a worker died. Distinguish cancelling a run from cancelling the assignment, and fence late results so cancelled/replaced runs cannot overwrite later decisions.

Continuation uses persisted artifacts, findings, and decisions to compile a fresh bundle. Native session resume is optional. A new worker can take over without the original harness session. Bound automatic revision cycles with a small configured limit; stop visibly with remaining findings when exhausted. No infinite retry loop or new daemon is required; explicit CLI reconciliation/processing is enough initially.

## Refined graph and ownership (implementation lead, 2026-09-08)

Read of `domain/lifecycle.rb`, `application/engine.rb`, the review policy, and the steel thread
against the settled design. Three things in the current code are the real obstacles, and the
refinements below resolve them without adding machinery.

**Ownership.** The work item owns workflow position in a field named `state`, plus an integer
`revision` bumped by every transition, plus its immutable `workflow` binding. Jobs, runs, and
attempts keep `status` and describe execution only. Renaming the work item's `status` to `state`
is the honest split the plan asks for; it is a small pre-deploy rename and removes the standing
confusion between "where the work is" and "what the container did". No assignment entity is added.

**`succeeded` is dropped.** The current graph carries both `succeeded` and `completed`. The default
workflow keeps only `completed`, per the settled design's rule against a second success state.

**Execution failure is not work failure.** There is no `failed` work state. A failed or interrupted
run is recorded on the run and attempt; the workflow takes `execution_failed` back to `ready`, which
is honest about the work still being eligible. When the configured revision budget is exhausted the
controller escalates to `needs_decision` instead, so exhaustion stops visibly rather than looping.

**Decisions resume a named transition, not a state.** A state marked `awaits_decision` may only be
entered with a decision payload naming a question and choices, where every choice is a transition
defined out of that state with a `human` actor. Answering is the same transition operation with the
chosen transition name, so `decide` is a thin wrapper and no arbitrary state can be supplied.

**Declarative dispatch, no rule engine.** The transition that starts execution carries a `dispatch`
block with a phase, `on_success` (or `on_verdict` for a review phase, a fixed verdict-to-transition
map), and `on_failure`. A non-terminal state may carry `continue:` naming the transition the
controller takes automatically. This is the whole vocabulary — no expressions, no hooks.

The default workflow graph, `independent-review`:

| Transition | From | To | Actors | Requires |
| --- | --- | --- | --- | --- |
| `start` | `ready` | `running` | system, human | — (dispatches implementation) |
| `report_progress` | `running` | `running` | agent, system | — (informational) |
| `submit_for_review` | `running` | `awaiting_review` | agent, system | `change_candidate` (dispatches review) |
| `approve` | `awaiting_review` | `completed` | reviewer | `review_verdict` bound to the current candidate |
| `request_changes` | `awaiting_review` | `changes_requested` | reviewer | `review_verdict` |
| `revise` | `changes_requested` | `running` | system, human | — (dispatches implementation, counts a revision) |
| `escalate` | `running`, `awaiting_review`, `changes_requested` | `needs_decision` | system, agent, reviewer | a decision question and choices |
| `resume_implementation` | `needs_decision` | `running` | human | — (dispatches implementation) |
| `execution_failed` | `running` | `ready` | system | — |
| `cancel` | every nonterminal state | `cancelled` | human | — |
| `reactivate` | `cancelled` | `ready` | human | — |

Two further examples ship as fixtures: `minimal` (`new -> in_progress -> done`, no reviewer, no
evidence, no decisions) and `human-gated-change` (a genuinely different lifecycle with no reviewer
actor at all and a required human authorization gate before `completed`).

**Concurrency.** `Ports::Store` gains one primitive — a `commit` that applies several records under
a single lock after checking expected revisions — and the JSONL adapter implements it as one
appended, fsynced buffer. State, history, decision, and dispatch writes for a transition are one
commit, so interruption cannot leave a partial transition. The same expected-revision check fences
late results: a run whose work item moved on records its outcome on the run and does not transition
the work item.

**Trust boundary.** Authority comes from the entry context, never from request text. Operator CLI
requests may act as `human` or `system`; worker requests arriving on the bounded file channel have
their role derived from the run that owns the channel (`agent` for implementation, `reviewer` for
review) and any role in the request body is rejected.

## Implementation sequence

1. Inspect current code/tests; refine the graph and ownership model in this plan. Preserve unrelated `.gitignore` edits. Capture the existing focused baseline without running paid/live proof.
2. Implement pure workflow definition validation and transition rules, immutable binding, durable history, and minimal/simple plus default workflow fixtures. Establish store consistency and request deduplication at this boundary.
3. Expose workflow inspection, transitions, evidence, and human decisions through the shared CLI/application path. Prove actor checks and stale requests. Add the bounded agent request path and live progress visibility.
4. Adapt the existing steel thread to configured steps; implement review return/continuation and required human waits. Retain draft publication reconciliation and independent fresh review. Avoid introducing a parallel legacy workflow engine.
5. Implement explicit recovery/reconciliation and prove interruption windows, forgotten transitions, active-worker protection, cancellation fencing, and bounded revisions.
6. Update README, configuration and operator guides, schemas, examples, and this plan. Obtain independent Claude review, fix findings, run integrated local gates, and land per repository rules.

Keep cohesive slices in one branch/worktree. Add td children only if they improve execution clarity; do not create a ticket for every method. Version any produced schema contract instead of editing an established `*-v1.json` in place. For existing local records choose explicit, tested compatibility/migration behavior; do not reset user's state. Avoid speculative migration frameworks.

## Acceptance evidence

- Simple workflow completes with no review artifacts or human-decision boilerplate and no implicit worker launch on informational transitions.
- Default fixture journey produces a draft candidate, requires a fresh independent review, and completes with correct provenance; returned findings drive a revised candidate and fresh review within one work item.
- Human-gated example waits with its question and context; an authorized answer resumes exactly once. Agent spoofing, missing evidence, cross-work artifacts, stale decisions, and old-candidate approvals are rejected.
- Agent progress requests are visible during a run through `show --json`; a forgotten request is reconciled from authoritative outcomes without fabricating completion.
- Workflow edits do not change an existing assignment's definition; a newly submitted assignment uses the new definition.
- Duplicate and conflicting transitions, concurrent stale updates, and interruption between state/history/dispatch writes produce a consistent recoverable record without duplicate execution or external effects.
- Recovery covers active worker, dead worker without outcome, persisted outcome before transition, cancelled/replaced worker with late output, and a human wait with no worker. A fresh harness session can continue from recorded evidence.
- Existing repository authority, secret isolation, draft-only behavior, cancellation, review independence, and GitHub/td write reconciliation remain proven.

Use focused tests and fake CLI journeys in isolated temporary state while implementing. On the integrated candidate run `bundle exec rake test`, `BACKSTAGE_DOCKER_TEST=1 bundle exec rake test` with a current worker image, `bin/backstage config check --pack packs/example --json`, and `scripts/scan-secrets` as supported by its local interface. Record commands, results, and commit/tree identity in td. Broad tests run once per relevant candidate; reviewer reuses valid evidence and adds targeted checks for uncovered risks.

No paid Backstage model calls, `--publish-draft` proof, real GitHub/td adapter test writes, or changes to the default tmux server. Repository development commits/push/landing and td tracking remain authorized. Docker tests must use isolated artifacts/containers.

## What shipped

Behavior is documented in [the configuration model](../../guides/active/config-model.md#workflows)
and [the operator guide](../../guides/active/operator-guide.md#work-lifecycles); this section is
only the record of what the plan turned into.

- `domain/workflow.rb` compiles a definition hash into an immutable validated record. Packs declare
  workflows in `workflows/*.yml`; `backstage.yml` names the default and a target may override it.
  Three fixtures ship: `independent-review`, `minimal`, `human-gated-change`.
- `application/workflow_service.rb` is the one transition operation. `application/controller.rb`
  replaced `steel_thread.rb` and `independent_review_policy.rb`; `application/recovery.rb` is the
  explicit reconciliation path. `application/engine.rb` now does execution only.
- `ports/store.rb` gained `commit(writes, expect:)`; `ports/agent_request_channel.rb` and
  `ports/runtime_presence.rb` are the two new seams, with local-file and Docker adapters.
- `schemas/agent-request-v2.json` is the worker-side contract (v1 is retained as a historical contract); `scripts/backstage-agent-request`
  files one request from inside a sealed container.
- CLI gained `workflows`, `transitions`, `transition`, `decide`, `history`, and `recover`, and
  `submit` gained `--workflow`.

### Decisions taken during implementation

- The work item's `status` became `state`, with an integer `revision` and a `revisions_used`
  counter, splitting workflow position from execution status. The `succeeded` state was dropped and
  there is no `failed` work state.
- Definitions are stored once in `workflow_snapshots` keyed by digest rather than inlined on every
  work item, so pack edits cannot reinterpret work in flight and `show` output stays readable.
- Actor authority is keyed on entry context: `operator_cli`, `controller`, `worker_channel`, and
  `run_outcome`. A review approval therefore records the reviewer as the actor.
- No escalation rule engine was built. An exhausted revision budget simply stops in
  `changes_requested` with the findings recorded; `needs_decision` is reached by a blocked verdict
  or a failed review run.
- Two defects surfaced while testing and were fixed: the secret guard rejected a legitimately named
  `awaiting_authorization` state (workflow names are now validated where a pack author reads the
  diagnostic, and the fixture reads `awaiting_approval`), and the JSONL store's `list` tiebroke
  same-timestamp records on a random id (it now keeps the log's own order).

### Review repairs and proof

Independent Claude review of `eb0bce4..a8c5cfd` found controller/recovery, request replay,
human-decision validation, and evidence/graph gaps despite a green baseline. The reviewed repair
adds atomic execution claims and terminal outcome reconciliation, single-line crash-safe JSONL
transactions, exact request replay, persisted-run authority, claim-time review-candidate binding,
human-only decision gates, and configuration diagnostics for impossible outcomes and unbounded loops.

Additional proof exercises real mid-run state changes and quiet-runtime delivery, cancelled and
prelaunch recovery, artifact retention and resumed decision context, file-channel replacement and
flooding, and concurrent callers. A minimal/human-gated local completion does not authorize external
handoff; independent approved evidence is still required. The default has no human bypass of review.

Final command results and reviewer verdicts are recorded in `td-bd9135`. No paid model calls,
`--publish-draft` proof, real GitHub/td adapter writes, or default tmux server changes are part of this
verification. Repository development commits/pushes and td tracking are separate authorized actions.

Final integrated proof: Docker-enabled suite passed 145 tests and 2,042 assertions with no failures,
errors, or skips against the rebuilt worker image. Configuration validation and credential scans
passed. CLI proof completed minimal, independent-review, and human-gated workflows in isolated
state; recovery reported no outstanding work. The image's JSON request form carried a contextual
question successfully. Read-only follow-up reviewers `store_repair` and `execution_repair` approved
the repaired boundaries after their findings were fixed. Exact commands and proof location are in td.
