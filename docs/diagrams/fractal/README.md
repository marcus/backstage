# Backstage Fractal model

This directory is the project-owned Backstage model. Keep `model.c4` and `fractal.json` together
in this checkout so a branch or worktree describes and reviews its own architecture claims.
Provenance records the bounded source-and-document review behind current claims and keeps
proposals explicitly separate.

Reconcile every changed claim against evidence in your branch or worktree, and make the model
changes there. Run commands from that checkout's root so repository paths resolve to the checkout
being modeled. Preserve existing element and relationship `uid` values and scene IDs so saved
links remain stable.

With the `fractal` CLI available, validate, inspect, and export from the repository root:

```sh
CHECKOUT_ROOT="$(git rev-parse --show-toplevel)"
MODEL_DIR="$CHECKOUT_ROOT/docs/diagrams/fractal"
EXPORT_DIR="$(mktemp -d)"

fractal validate --directory "$MODEL_DIR" --json
fractal inspect --directory "$MODEL_DIR" --json
fractal project --directory "$MODEL_DIR" --scene overview --json
fractal layout --directory "$MODEL_DIR" --scene overview --json
fractal export --directory "$MODEL_DIR" --scene overview \
  --format png --output "$EXPORT_DIR/backstage-overview.png"
```

Fractal's persistent project catalog lives at `~/.config/fractal/catalog.json`. Register this
checkout's `docs/diagrams/fractal/` directory there rather than copying the model into Fractal's
repository. The normal local studio is `http://127.0.0.1:5199`; once the catalog entry and service
are available, open `http://127.0.0.1:5199/?model=backstage&scene=overview`.

For an isolated review that does not depend on the persistent service, use a free task-specific
port and point Fractal at this checkout's diagram root:

```sh
CHECKOUT_ROOT="$(git rev-parse --show-toplevel)"
PORT=5283

cd /path/to/fractal
FRACTAL_MODELS_DIR="$CHECKOUT_ROOT/docs/diagrams" \
  npm run dev -- --host 127.0.0.1 --port "$PORT"
```

The directory name is the isolated server's model key, so open
`http://127.0.0.1:5283/?model=fractal&scene=overview`.

## Sequence journey

`sequences.json` owns the authored `draft-review` successful path. It starts at explicit dispatch
acceptance, publishes the finalized draft through scoped authority, then reviews it independently.
It omits admission, retries and revision cycles; it is not a runtime trace. Keep message and phase
IDs stable when correcting the story, and recheck chronology against the runner and journey tests.

```sh
fractal journeys --directory docs/diagrams/fractal --json
fractal sequence --directory docs/diagrams/fractal --journey draft-review --json
```

Open `http://127.0.0.1:5199/sequence?model=backstage&journey=draft-review` after the catalog model is
reloaded. The optional file remains with this repository alongside its architecture model.

## Activity component detail

Open [How history stays trustworthy](http://127.0.0.1:5199/?model=backstage&scene=activity-detail)
for a component-level discussion of the durable activity log. This is a C3-style responsibility
view within the existing model, not a claim that the whole hierarchy follows strict C4 levels.
Expand **Durable activity log** in another perspective to reveal the same four components.

The recorder builds facts, the event contract defines their shape, the shared Store port commits
state and history atomically, and the query service reads them through list, show and follow.
Relationship arrows show dependencies, not a temporal sequence. The recorder does not append
history: its caller, such as the workflow service, supplies both state writes and built events to
`Store#commit`. Obtaining a deployment identity can initialize that identity in the store;
building an event does not commit its history. The JSONL implementation remains represented by **State & artifacts** outside this
focused view. The Store node here describes its activity-facing contract, not a separate store.
Operational records remain authoritative for current state; activity explains what happened.

The scoped scene deliberately omits producer and storage context to keep the four responsibilities
readable. Use **Inside the engine** for that surrounding context. Notification delivery and other
planned effects have not been added to this current-only detail.

Evidence was read on 2026-09-10: `application/activity_recorder.rb`, `application/activity_query.rb`,
`domain/activity.rb`, `ports/store.rb`, `adapters/jsonl/store.rb`, `application/workflow_service.rb`,
`bootstrap/system.rb` and `surfaces/cli.rb` under `lib/backstage/`, plus the event schema. Node
inspectors link to those files and the corresponding activity tests. These are authored source
claims, not runtime trace output or automatic freshness checks.

```sh
fractal validate --directory docs/diagrams/fractal --json
fractal project --directory docs/diagrams/fractal --scene activity-detail --json
fractal export --directory docs/diagrams/fractal --scene activity-detail \
  --format png --output /tmp/backstage-activity-detail.png
```

## Additional current drilldowns

Two additional focused scenes make the durable execution seams inspectable without multiplying
perspectives for every class. **How work keeps moving safely** opens the queue's accepted intent,
revisioned claim, bounded pass, recovery and external-effect reconciliation responsibilities.
It is grounded in the dispatcher and recovery code, not a promise that every future effect type is
implemented. **How runtime evidence stays usable** follows the capture path: redaction before
framing, typed observation, fsynced chunk-before-event persistence, and the coverage gate that
refuses finalization, review acceptance or publication after a gap or failed capture.

These scenes deliberately leave provider command construction, every workflow state and every
JSONL collection shallow. The overview retains those as shared responsibilities, while node
inspectors point to the relevant implementation and focused tests. Current code review covered
the dispatcher, recovery, engine, workflow service, repository authority, deployment-pack
compiler, container phase runner, independent reviewer, runtime capture and sink, stream
interpreters, JSONL store and local artifact store on 2026-09-10. Notifications, replies,
deployments and repair remain proposals.

```sh
fractal project --directory docs/diagrams/fractal --scene dispatch-detail --json
fractal project --directory docs/diagrams/fractal --scene capture-detail --json
fractal export --directory docs/diagrams/fractal --scene capture-detail \
  --format png --output /tmp/backstage-capture-detail.png
```
