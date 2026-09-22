# Configuration model

How a Backstage instance is configured. This document is the configuration model: the layers a pack uses and the file shapes `config check` compiles.

## Three layers

1. **Deployment pack** — one per environment. Selects adapters and their settings: state store, artifact directory, worker image, harness, credential broker, model defaults, plus role instructions, skills, and the work lifecycles the instance offers. A pack is a directory.
2. **Target** — one per repository the instance works on. Owns the repo remote and revision rules, which trigger source routes to it, its authority grants, harness overrides, target-specific instructions, its context grants, and which workflow its sourced work is admitted with. One instance serves many targets.
3. **Job bundle** — compiled per run: pack defaults ← target overrides ← the specific work item, validated against the versioned bundle schema. The compiled bundle is persisted as a run artifact, so the audit trail answers "what was this agent given" per run, not per config version.

Routing binds triggers to targets: a trigger event carries its source identity (for `td`, the workspace/repo it came from), and each target declares which sources it claims. A work item routed to a target executes with that target's bundle inputs; targets that grant nothing extra add nothing.

## File shapes

Packs are directories of hand-editable YAML, validated by `backstage config check`, compiled to JSON at bundle time. The committed example lives at `packs/example/` in this repository. Another deployment keeps its pack in its own directory and passes that path to `--pack`. Packs are the deployment boundary and are not shared across environments.

```yaml
# packs/example/backstage.yml — the deployment pack entry
adapters:
  state_store:    { kind: jsonl,  path: ~/.backstage/state }
  artifact_store: { kind: local,  path: ~/.backstage/artifacts }
  worker_runtime: { kind: docker, image: backstage-worker:1 }
  harness:        { kind: pi }
harness_defaults:
  provider: openrouter
  model: z-ai/glm-5.3-flash
  credentials: openrouter           # broker reference — never the secret itself
credentials:
  repository_default: github
  broker:
    github:
      source_env: GITHUB_TOKEN
      runtime_env: GH_TOKEN
    openrouter:
      source_env: OPENROUTER_API_KEY
      runtime_env: OPENROUTER_API_KEY
workflows:
  default: independent-review       # a file in workflows/; a target may override it
```

```yaml
# packs/example/targets/widgets.yml
repo:
  origin: https://github.com/example/widgets.git
  default_revision: main
  credentials: github
trigger:
  td_workspace: /projects/widgets
  source_instance: widgets-example
authority:
  review_change: draft_pr
context:
  repos:
    - { name: tracker, origin: https://github.com/example/tracker.git, mount: refs/tracker, credentials: github }
instructions: instructions/widgets.md
```

```yaml
# a second target that waits for a person instead of a reviewer
repo:    { origin: https://github.com/example/notes.git, credentials: github }
trigger: { td_workspace: /projects/notes, source_instance: notes-example }
authority: { review_change: draft_pr }
workflow: human-gated-change
# no context block — this agent gets only its own clone
```

## Dispatcher policy

```yaml
# packs/example/backstage.yml
dispatcher:
  poll_interval_seconds: 5     # how long the foreground worker waits between passes
  retry:
    max_retries: 0             # automatic retries AFTER the first failed attempt; 0 disables them
    delay_seconds: 60          # delay before the first retry
    backoff: fixed             # fixed or exponential
    max_delay_seconds: 3600    # ceiling for exponential backoff
```

The whole section is optional and defaults to the values above. `config check` validates it, and the
retry policy is copied onto each acceptance, so changing it here affects only work accepted
afterwards. There is deliberately no expression language: a bounded count, a delay, and one of two
shapes for growing it.

## Capture policy

An optional top-level `capture:` block bounds runtime output capture. Every key is a positive
integer; `bin/backstage config check --json` reports the effective values under `capture` with
`configured` listing the keys the pack set.

| Key | Default | Meaning |
| --- | --- | --- |
| `flush_bytes` | 65536 | Coalesced output is committed as one chunk at this size |
| `flush_millis` | 250 | ... or after this long since the first buffered byte |
| `flush_records` | 1000 | ... or after this many framed records are held in flight |
| `max_stream_bytes` | 16777216 | Bytes persisted per stream before coverage becomes `truncated` |
| `max_run_bytes` | 67108864 | Bytes persisted per run across all streams |
| `max_record_bytes` | 65536 | Longest line read whole on a plain output stream |
| `max_protocol_record_bytes` | 8388608 | Longest record read whole on a protocol stream (Pi) |

## Workflows

Each `packs/example/workflows/*.yml` file defines one work lifecycle: an initial state, the states
(any of which may be terminal or may await a human decision), and the named transitions between
them. The file name is the workflow name. `backstage.yml` names the pack default under
`workflows.default`, and a target may override it with `workflow: NAME`.

```yaml
# packs/example/workflows/minimal.yml — the smallest useful lifecycle
name: minimal
version: 1
initial_state: new

states:
  new: {}
  in_progress: {}
  done: { terminal: true }

transitions:
  start:
    from: [new]
    to: in_progress
    actors: [system, human]
    dispatch: { phase: implementation, on_success: finish, on_failure: reset }
  finish:  { from: [in_progress], to: done, actors: [system, agent, human] }
  reset:   { from: [in_progress], to: new,  actors: [system, human] }
```

Nothing above is boilerplate: review, evidence, decisions, and revision budgets are all optional.
The complete vocabulary is small and deliberately not a programming language — no embedded Ruby,
shell hooks, expressions, nested graphs, or parallel joins.

**On a state:** `description`, `terminal` (this workflow ended here), `awaits_decision` (a human
answer is required to leave), and `continue` (the one transition the controller may take
automatically from here).

**On a transition:** `description`, `from` (one or more states), `to`, `actors`, `requires`,
`counts_revision`, `dispatch`, and `decision`.

- `actors` lists which of `system`, `human`, `agent`, and `reviewer` may take it. Authority is
  decided by where a request entered Backstage, never by what the request claims to be — see
  [the operator guide](operator-guide.md#actors-and-the-local-trust-boundary).
- `requires` names evidence that must accompany the request: `change_candidate` (a materialized
  patch artifact) or `review_verdict` (an independent verdict artifact bound to the candidate
  digest under review).
- `dispatch` requests an execution when the transition is taken. `phase` is `implementation` or
  `review`; `on_success` names the transition to take when the run succeeds, `on_verdict` maps a
  reviewer's `approved`/`changes_requested`/`blocked` to transitions, and `on_failure` names what
  to take when the run fails, is cancelled, or times out.
- `decision` is required on any transition entering an `awaits_decision` state. It carries the
  `question` a human is asked and the `choices` they may pick — each of which must be a transition
  defined out of that state that a human may take.
- `max_revisions` on the workflow bounds how many times a `counts_revision` transition may cycle.
  When the budget is spent the controller stops rather than looping, leaving the work in its
  current state with the findings recorded.

`backstage config check` compiles every workflow and refuses missing state or transition
references, unknown actors, unknown evidence kinds, unknown keys, interchangeable transitions out
of one state, unreachable states, states that cannot terminate, decision states nothing can enter
or a human cannot answer, dispatch outcomes that are not available where the dispatch lands, and
names that read as credential fields. Cycles are valid — returned work continues through one.

A work item is bound to its workflow's name, version, and definition digest when it is admitted,
and the resolved definition is stored once under that digest. Editing a pack workflow therefore
never reinterprets work already in flight; newly submitted work picks up the new definition.

## Context grants

Context grants are the target's declaration of what an implementing agent may reach live during a run, per the context-provisioning seam in the spec. Grant kinds:

- `repo` — a read-only reference checkout fetched at run time and mounted at a declared path (`refs/tracker`). It reuses the clone machinery and scoped-token pattern the review slice already builds.
- `mcp` — an MCP server entry (logs, read-only data) with broker-resolved scoped credentials. Not implemented yet.
- `cli` — a tool expected on PATH in the worker image.
- `file` — mounted reference material.

Reference repos are bundle inputs, never image contents. The image stays boring and stable (tools only); baking repos in would mean per-target images, rebuilds on upstream changes, and stale references between rebuilds. If clone time hurts, the worker runtime may cache checkouts host-side and bind-mount them read-only — an adapter optimization, invisible to the contract.

Every grant materialized for a run is recorded with the run, so audit covers what the agent could reach as well as what it did.

## Rules

The [activity and effects plan](../../plans/active/activity-and-effects/README.md) defines future
notification routing and human-gated deployment/repair grants. These capabilities are not yet
accepted configuration vocabulary. Current draft-only enforcement remains scoped to the existing
publisher; it is not a permanent product prohibition on operational actions. A future grant needs
an implemented executor and approval path, not just an added allowlist entry.

- Config files carry credential **references**; the credential broker resolves them at run time. Secrets never appear in packs, bundles-as-stored, state, or artifacts.
- The bundle schema is versioned JSON Schema; additive fields are the expected evolution path (context grants arrived this way).
- `backstage config check` validates a pack, its targets, and its workflows non-interactively and exits non-zero on any error, so config is CI-checkable.
- Workflow definitions are compiled to immutable records and bound to work by digest. Changing a definition is a new digest, never a reinterpretation of work already admitted.
