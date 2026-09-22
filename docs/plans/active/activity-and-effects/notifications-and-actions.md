# Notification channels and human-gated external actions

Part of the [controlling plan](README.md). Proposed behavior; current live publication remains
draft-only until these capabilities have an implemented executor and explicit deployment grants.

## One source, multiple deliveries

Support both sources: a validated agent-session output/request and an applied workflow transition.
An agent proposes a named notification intent with bounded content/artifact references through
the run-scoped channel or versioned outcome. The core checks allowed intent, destinations, source
run, revision, and policy; it never scans arbitrary transcript prose for instructions to send.
An agent may propose recipients from permitted aliases, not create new destinations/authority.

An applied transition can declare named notification hooks. Configuration uses a small catalog
of message purposes and destination aliases. Initial precedence: deployment defaults, target
overrides, then workflow hook selection within those grants. Hooks reference stable names, not
arbitrary code/expressions. Compile/validate aliases and restrictions, snapshot resolved routing
and template versions, and record why each destination was chosen. Pending deliveries keep
their snapshot; current revocations can stop a send. Config edits never silently add recipients
to an already admitted obligation.

The originating commit includes the accepted notification request, its source activity, and a
durable routing obligation with resolved policy snapshot. A restart-safe router materializes
one intent per destination and commits its progress atomically. Core records retain source event,
request, decision, work, and effect-slot identity. If a session output and its completing
transition represent the same notification, the configured slot/dedup scope maps them to one
request. Separate intended notifications use separate slots; retries do not create new ones.

Example: `decision.raised` routes to the originating td comment and Slack. Both deliveries point
to the same decision. Slack may be sent while the comment is pending; each can retry independently.
Another hook can send the final result to both when work completes. Routine successful runs can
remain quiet by policy while still recording activity. Notification hooks subscribe to explicit
source kinds, not their own delivery events, preventing feedback loops.

### Owned seams and records

| Boundary | Responsibility |
| --- | --- |
| Application routing | Decide what should be sent, to whom, why, and whether policy allows it |
| Work-source adapter | Own writes to its originating ticket/record, including a comment when supported |
| Delivery adapter | Render/send/reconcile a bounded message on other channels; report provider references and capabilities |
| Inbound adapter | Verify transport authenticity, map identity, normalize reply, deduplicate ingress |
| Application decision service | Authorize the answer against the current decision and approval subject |
| Effect coordinator | Claim and execute/reconcile authorized effects; persist attempt/outcome activity |

Keep one external-effect identity and ledger for each send. Delivery records add channel/thread,
recipient and message metadata; they reference the effect ledger rather than maintaining a
second competing send state. Extend existing effect code carefully; td handoff/review and GitHub
publication keep their owning adapters and established reconciliation behavior.

Derive delivery/effect IDs from deployment, accepted notification request, stable destination,
and delivery slot/version. Guard creation atomically and store a separate content/routing
fingerprint so conflicting reuse is rejected. Do not derive a new effect ID on each send attempt.

Suggested request state: accepted/routing/routed/cancelled. Delivery status is derived from its
ledger: pending, in_flight, sent, failed_retryable, failed_terminal, uncertain, cancelled. Persist
attempts, last error, due time, provider reference and reconciliation evidence. Each claimed
effect has a revision/owner token; stale completions cannot overwrite newer resolution. No
external write happens before the durable intent/claim exists.

A stored token alone cannot stop a paused executor from later calling a remote API. Use
provider-enforced fencing/idempotency where available. Otherwise hold local execution ownership
that cannot be replaced while the executor remains live, and never reclaim a claim merely because
a timeout elapsed when executor liveness is unknown. Mark it uncertain and reconcile before any
replacement is permitted. The ownership guard covers the launch and remote operation, not just
recording its result. A future remote executor needs equivalent enforceable launch ownership.

The adapter declares send reconciliation capabilities. After a timeout, look up the provider
result by supported request identity/reference/marker. If absence or success cannot be proven,
record `uncertain` and require an explicit resolution; do not blindly resend. Exactly-once
external delivery cannot be promised for arbitrary email/webhook providers. Retry known failures
with bounded backoff; a manual resend creates an explicit linked new delivery where duplication
is a known possibility. Cancellation after sending cannot unsend the message.

Optional delivery failure affects attention and its own retries, not implementation retries or
work success. Required delivery gates, if declared, wait for the named delivery group explicitly.
Recovery/Dispatcher currently treat unresolved external effects broadly; teach them effect scope
and criticality so a failed Slack send does not freeze unrelated work, while an uncertain deploy
blocks any dependent operation. Never weaken existing publication uncertainty checks globally.

Delivery needs service time while agents run or wait. A serial dispatcher blocked in `runner.run`
cannot be its only drainer. Start a separately supervised local delivery loop (or bounded CLI
pass) with guarded per-effect claims, using the same shared application core/store. This is not
an agent execution pool. Long agent work must not delay human questions, replies, or retries.

## Decisions and replies

Add decision kinds `question` and `action_approval`, preserving existing decisions. A question
has context, permitted choices and a bounded answer schema (start with free text plus choice).
An action approval includes an immutable action subject. Notification content is a rendering of
that decision; receiving or reading a message is not approval.

Store message mappings: deployment, adapter account/workspace, conversation/thread/message,
delivery, decision ID, revision and approved subject when applicable. The outbound rendering
includes a stable decision reference and canonical answer instructions. The CLI is sufficient
initially; add browser links only when a real authenticated answer surface exists.

Inbound receipt is durable before acknowledging the provider. Use provider event identity plus
account scope for replay protection; process with a durable checkpoint. Verify webhook signature
or equivalent authentication before accepting an asserted sender. Account/conversation identity
is not itself permission: map the verified sender to an allowed human principal for this target
and decision. Reject ambiguous replies instead of guessing the latest question in a channel.
First reply adapter uses explicit choice/decision identity, not an LLM interpretation of approval.

Replace the literal `operator_cli` check with a trusted entry-context abstraction issued by
the composition/authentication boundary. CLI local trust, authenticated UI, and verified channel
input all call the same core command. Workers cannot supply that context or approve their own
request. Never add a user-controlled `actor=human` override.

Answer acceptance checks decision ID, current status, expected work revision, allowed choice,
principal, expiry/revocation, and approval subject digest. Atomically record the answer, work
transition, downstream dispatch/action request, and activity. Exact duplicate answers reconcile;
competing answers yield one winner and a structured conflict. Edits/late replies cannot change
an answered decision. On cancellation/supersession, obsolete pending deliveries are cancelled
and later replies report that the decision is no longer open.

The resumed run receives the structured answer and provenance. Fresh-run continuation is the
first behavior; live harness-session suspension/restoration is not part of this plan. Optional
message updates marking a decision answered are ledgered effects too, never privileged writes.

## Deploy and repair as scoped actions

Model these as core-authorized external actions, not shell hooks hidden in workflow YAML and
not a broader allowlist in the existing draft publisher. A workflow transition or agent output
may propose an action. The action is inert until policy and, initially for deploy/repair, a
human decision authorize the exact subject.

Action proposal includes capability ID, owning target/environment/resource, immutable input
artifact/version/digest, normalized parameters, preconditions, intended effect, verification
criteria, reconciliation key, and rollback/compensation information when available. The human
sees those fields in a reviewable preview. Approval binds the action digest, policy/grant version,
decision/work revision, principal and expiry. Changed inputs/target invalidate approval; approval
of a code candidate does not approve its deployment. Freeze nondeterministic inputs before
approval (for example image digest instead of `latest`).

Initial state path: proposed -> awaiting_approval -> authorized -> executing -> succeeded /
failed / uncertain, with declined/cancelled/expired alternatives before execution. Record
intermediate provider submission separately from verified success. Current workflow YAML needs
a versioned, typed action reference/dispatch vocabulary and explicit result transitions; do not
pretend `phase: implementation` and candidate/review evidence are already general operations.

Introduce a narrow external-action executor contract with preview/validate, execute, reconcile,
and verify operations, implemented per capability/provider. Reuse credentials/store/artifact
seams. An action execution can be recorded without manufacturing a repository checkout or an
agent run. If an agent is needed to investigate a non-code issue, its generalized bundle/phase
is a separate extension, not a prerequisite for executing a typed deploy request.

Immediately before effect execution, revalidate the grant, approval digest, environment identity,
current resource preconditions, cancellation and expiry; claim durably with revision fencing.
Bind policy authority and claim to one atomic authorization step. Race semantics: cancellation
before this step prevents launch; afterward it is a stop request, not a promise the remote effect
did not happen. Reconcile and report the observed result. Do not allow a stale process to start
an operation after its claim has been replaced.

Apply the same enforceable launch/reclaim rule as delivery above: provider fencing/idempotency
or non-replaceable live local execution ownership. If a paused process might still perform the
effect, a database revision check cannot justify launching its replacement. Uncertain ownership
requires reconciliation and resolution, not automatic expiry-based reassignment.

Credentials reach only the scoped credentialed executor, never the proposing agent's checkout.
An approval cannot manufacture an unavailable adapter or undeclared grant. Effective permission
is the intersection of implemented capability, deployment/target policy, entry authority, and
current approval. Default-deny unknown actions. Keep the existing repository publisher's branch,
origin/base and draft checks for that capability. Do not grant merge/default-branch push merely
because deploy support was added; each would require its own declared capability and review.

Persist the attempt before remote mutation; on restart reconcile before any retry. Verify the
actual deployment/repair target after the provider accepts the request. Uncertain or partial
results stay visible and block dependent operations. Marking a workflow terminal must not stop
reconciliation of an in-flight action. Compensation is a separate authorized action; rollback
is not assumed possible or automatically authorized by approving the forward action.

Pack grants select allowed capabilities/resources and human-gate policy. Initial deploy/repair
capabilities require human approval even if a future policy vocabulary can support bounded
automation. Moving a capability to automatic policy is an explicit future policy change, not
something an agent or notification reply can grant itself.

## Surfaces and validation

Proposed CLI: `notifications list/show`, `deliveries list/show/retry`, bounded delivery `pass/run`,
`decisions list/show/answer`, `actions propose/show/approve/cancel/reconcile`, and an `attention`
query. Reuse/alias existing `decide` where semantics match; no duplicate approval implementation.
All offer JSON/JSONL, deterministic noninteractive arguments, documented refusal reasons, and
shared application results. An explicit live-effects entry mode is needed for delivery/actions;
fake remains default. It must not be inferred from `--publish-draft`.

Configuration validation rejects unknown adapters/capabilities, missing recipients, unsupported
reply routes, notification cycles, incomplete action result mappings, incompatible evidence,
and deployment grants lacking the initial human gate. New schemas/definitions are versioned;
in-flight work remains bound to its admitted definition and approval subject.

Proof fixtures cover: comment succeeds/Slack fails; send succeeds then host dies before receipt;
agent output plus completion hook deduplicates; configuration changes during retry; routing
worker races including a paused owner and attempted claim replacement; two channel answers;
forged sender; stale revision; expiring approval; edited image
digest; cancellation before/after claim; provider accepts but target verification fails; uncertain
action at terminal work; failed optional notification alongside valid ongoing work. Existing
GitHub/td reconciliation and credential-isolation regression tests must remain green.
