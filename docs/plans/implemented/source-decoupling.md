# Native sources and governed execution

Tracking: td-4837da. Marcus authorized replacement of breaking contracts; no production compatibility layer is required. Existing user state and unrelated files must remain intact.

## Contract

Native content is an opaque UTF-8 document (`content`, `media_type`, core-computed `sha256`). A title is a display label, not a normalized task. Work retains a frozen input and optional source provenance (`connection`, adapter `kind`, configured stable `identity`, native `ref`, optional native `version`). Core revisions, execution modes, workflow, target authority and decisions remain separate.

Deployment packs define named `sources` separately from repository targets. Sources declare kind, stable identity, adapter settings, permitted `targets`, optional `default_target`, and allowed `operations`. Manual-only packs define no sources. Native refs are interpreted only by their adapter. Target selection comes from trusted config/CLI, not documents. Admission and refreshed assignments must deduplicate atomically; refresh creates new work, never changes accepted content, and refuses a still-active predecessor.

`Ports::WorkSource` supplies `snapshot(ref)` (ref/title/media_type/content/version), `discover` (native refs), `capabilities`, `prepare(operation:, ref:, result:, operation_id:)`, `reconcile(operation:, ref:, payload:, operation_id:, attempted:)` and `execute(operation:, ref:, payload:, operation_id:)`. Reconciliation reports applied, not_applied, or unknown. This is an execution envelope and operation contract, not a task taxonomy. The composition root explicitly wires td and a structurally different file-backed fake source.

Host-side result delivery requires the existing independent approval bound to candidate and completion. It owns operation identity, immutable payload, credential boundary, concurrency ownership, ledger and reconciliation. Partial success retries only unfinished operations; an ambiguous attempted write cannot be repeated without trustworthy reconciliation or an explicit operator resolution. Native prose never grants authority or chooses endpoints. Draft publication still precedes independent review.

## Work

1. Replace source-shaped domain/config/job-bundle contracts and implement shared admission, frozen snapshots and refresh.
2. Implement td and fake adapters and reliable host-side result delivery.
3. Integrate generic CLI/composition and native input into implementation/reviewer prompts; update tests.
4. Update examples, README, guides and owned architecture model; prove executable journeys and full suite.
5. Independent review, fixes, scoped commits, normal main push, exact remote SHA and CI verification.

## Evidence

Manual pack without td fields; native payload fidelity; source ref case and namespaces; explicit repository routing; discovery retry after failed admission; frozen source edits and refresh; unsupported operations; partial, ambiguous and concurrent delivery; task content cannot confer authority; publication/review order; full suite, config checks, secret scan, prose scan and executable fake examples. No test may mutate an external tracker.

## Implemented — 2026-10-05

The native document/envelope contract, named connections, source-free manual pack, td/fake adapters, admission and refresh, generic CLI, governed delivery, guides, Mermaid flow and Fractal source scene are implemented. The live draft-before-independent-review journey remains covered.

Independent review corrected literal td summary transport (`--note`), added delivery reservations and current-evidence checks against concurrent refresh/reopen, and made result attribution reuse the exact approved candidate verdict. Native td hierarchy cascades are explicit in documentation and receipts; no single-issue effect guarantee is claimed.

Local acceptance evidence: full suite 453 tests / 19,551 assertions, zero failures or errors, six opt-in Docker skips; focused review fixes 49 tests / 354 assertions; both pack checks; native fake CLI import → accepted queue → independent review → repeatable delivery; refreshed completion denial and explicit replacement acceptance; source-free native-file journey; secret scan; prose gates; Markdown/evidence links; Fractal validation (54 elements, 54 relationships, 8 scenes, 1 journey) and rendered source scene; README Mermaid rendering. Independent reviewer approved with no remaining material findings. No real tracker mutations or live Docker runs were used.

Final normal push, exact remote SHA and CI evidence are recorded with td-4837da at handoff. Existing user state, models and unrelated files remain intact.
