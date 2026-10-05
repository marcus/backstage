# Work sources

Backstage accepts a native task document and governs its execution. A source adapter translates source-specific reads and writes at that boundary; it does not translate every task into a shared tracker schema.

The included adapters are td and a file-backed fake. Other sources require adapter code and explicit composition; their APIs and permissions depend on the source.

## Separate native content from execution

A snapshot supplies a display title, UTF-8 content, media type, native reference, and optional native version. Input is bounded to 1 MiB. Backstage preserves content and computes its digest. Description structure, status names, priority, acceptance criteria, comments, and attachments remain source-specific. An adapter can serialize its native document as JSON or render an agent-readable document; the core does not interpret those fields.

The execution envelope identifies the source and assignment, records the repository target and immutable workflow binding, and supports idempotency. For example:

```yaml
title: Repair the export
input:
  content: 'Repair the export using the attached native requirements.'
  media_type: text/markdown
  sha256: CORE_COMPUTED_DIGEST
source:
  connection: native
  kind: fake
  identity: native-jobs
  ref: Widget/Case-17
  version: OPTIONAL_NATIVE_VERSION
target: widgets
workflow: independent-review
```

This example illustrates the execution boundary. The content can instead be a source-native JSON document without requiring the core to parse it. Source identity comes from trusted connection settings. Native references preserve adapter-specific case and syntax. Target selection comes from the source's allowed/default targets or an explicit trusted CLI argument.

Accepted content stays frozen for implementation, review, and retries. Importing an edited source again returns the existing assignment. `source refresh` creates a new assignment when content, media type, title, or version changed, returns the existing item otherwise, and refuses an active predecessor; explicit acceptance authorizes execution of the replacement.

## Connection configuration

Sources live separately from targets in `sources/NAME.yml`:

```yaml
kind: td
identity: widgets-tasks
targets: [widgets]
default_target: widgets
operations: [record_result, request_review]
workspace: /projects/widgets
```

The composition root explicitly recognizes `td` and `fake`. Add an adapter and factory branch to support another source; writing a new YAML `kind` does not install one. The [configuration model](config-model.md#source-connections) documents pack validation, routing, and the fake connection.

Connection settings own adapter endpoints and credential references. Native content cannot supply endpoint authority, credential names, operation permissions, repository routing, or human decisions. A source comment saying “approved” remains task content.

## Extension contract

Implement [`Ports::WorkSource`](../../../lib/backstage/ports/work_source.rb) and wire it through [`bootstrap/system.rb`](../../../lib/backstage/bootstrap/system.rb). Keep provider commands, native selection, status vocabulary, and result formatting in the adapter.

| Method | Responsibility |
|---|---|
| `snapshot(ref)` | Fetch one native assignment and return its `ref`, `title`, `content`, `media_type`, and optional `version`. Preserve native content and interpret the reference inside the adapter. |
| `discover` | Return native references eligible for admission according to this source's selection rules. Discovery does not grant execution acceptance. |
| `capabilities` | List implemented operation names. Pack `operations` can permit a subset, never create an implementation. |
| `prepare(operation:, ref:, result:, operation_id:)` | Format the source-specific payload from verified result evidence. Preparation performs no external write; its payload is persisted before execution. |
| `reconcile(operation:, ref:, payload:, operation_id:, attempted:)` | Return an object with `status` (`applied`, `not_applied`, or `unknown`) and a bounded JSON-object `receipt` from trustworthy source evidence. Distinguish an initial absence from an ambiguous attempted write. |
| `execute(operation:, ref:, payload:, operation_id:)` | Perform the prepared operation through the host-side adapter and return `applied` or `unknown` with an inspectable receipt. |

Use the port and its implementations as the exact Ruby contract. `operation_id` is the durable delivery identity; use a native idempotency key or recognizable receipt where the source supports it. Native versions remain opaque strings. Core work revisions and workflow transitions are separate from those versions.

The td adapter owns issue selection and native ID rules, reads the native issue document, and formats `record_result` as a handoff plus `request_review` as a td review command. The fake source uses a different file structure and implements `record_result` plus `mark_ready`. Source states never become Backstage workflow states. Native td handoffs and reviews can cascade to descendants; review can also advance a parent when its children are ready. Configuring these operations permits those native command effects. The receipt observes the referenced issue and does not enumerate all hierarchy changes. An adapter needing narrower effects must use a source API that supports that scope.

## Delivery and failure behavior

[`Application::ResultDelivery`](../../../lib/backstage/application/result_delivery.rb) checks independent completion authority, configured source identity, adapter capabilities, and permitted operations. The host owns local delivery locking and durable operation records. Workers never call arbitrary source APIs with host credentials.

Each selected operation has an immutable prepared payload and independent delivery state. Repeating a delivery reuses applied operations, reconciles unfinished ones, and only retries a write known not to have applied. Partial success does not cause a completed operation to repeat.

If a write was attempted and its result is unknown, return `unknown` unless source evidence resolves it. Do not infer failure from a timeout, missing response, or stale read. Backstage stops until reconciliation establishes the result or a trusted operator records `deliver resolve --applied` or `--not-applied` with a reason. A source without reliable receipts or reconciliation cannot provide a general duplicate-free retry guarantee.

Unsupported operations fail explicitly. Queue execution does not implicitly request any delivery. `deliver WORK_ID --operation NAME` and direct `process --deliver NAME` invoke the same host service. See the [operator guide](operator-guide.md#delivering-results) for commands.

## Proving another adapter

Use fixtures and fake writes before touching a live tracker. Prove the real admission, acceptance, execution, and explicit delivery journey, including:

- Native payload fidelity, reference case, configured source namespaces, and allowed target routing.
- Idempotent admission and retry after discovery succeeds but admission fails.
- Frozen source edits and explicit refresh without copied acceptance.
- Unsupported operations and source content that claims authority.
- Applied results, partial success, ambiguous writes, and concurrent delivery.
- Independent approval of the exact candidate before result delivery.

Keep tracker mutation tests local unless the operator separately authorizes a live integration test. A new source should require adapter and composition changes, not changes to the lifecycle engine or the task content model.
