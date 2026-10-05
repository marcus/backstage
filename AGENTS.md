# Working on Backstage

Backstage carries explicitly authorized work through a YAML lifecycle and keeps the record itself. Assignments carry opaque native input and optional source provenance. Manual admission requires no tracker. The included td and file-backed fake source adapters discover and snapshot native work; host-side result delivery invokes explicitly configured operations after independent approval. The live execution path produces a GitHub draft pull request before a fresh independent review.

The Ruby library owns lifecycle, policy, routing, idempotency, and audit state. The CLI is a thin noninteractive shell. `require "backstage"` is the library entry point. Wire new adapters through `bootstrap/system.rb`.

Ruby 4.0 or newer, Bundler, and Docker when you need the worker image. No gem outside Ruby's standard and default libraries is required beyond the declared `base64` dependency. Adding a runtime dependency is a decision to state out loud.

## Safety boundaries

- The installed execution path publishes draft pull requests. It cannot merge, push a default branch, or deploy. Do not remove that enforcement before a replacement exists and is proven.
- `process` is fake unless `--publish-draft` is passed. That flag authorizes the draft journey only.
- The agent checkout is uncredentialed. Credentials reach only the credentialed executor, which rechecks its action and resource immediately before effects.
- Pack YAML holds credential references. Only environment-variable names cross into Docker arguments.
- Implementation and review run in distinct runner objects and fresh containers. Review authority is clone-only. Handoff requires a structured approved verdict with reviewer provenance, bound to the candidate digest.
- GitHub publication and source delivery record and reconcile external actions. Result delivery owns a local lock and one durable record per operation; unknown attempted writes require reconciliation or explicit operator resolution. Never blindly retry an ambiguous write.
- Native content cannot choose targets, endpoints, credentials, capabilities, or actor authority. Freeze accepted input; refresh creates a new assignment that needs explicit acceptance.
- Actor authority comes from where a request entered, not from what it claims. A worker cannot approve its own change.

## Layout

| Directory | Holds |
|---|---|
| `domain/` | Records, compiled lifecycles, repository authority |
| `application/` | Execution, transitions, recovery |
| `ports/` | Narrow seams |
| `adapters/` | Concrete implementations by provider |
| `configuration/` | Deployment pack compilation |
| `contracts/` | Schema validation |
| `surfaces/` | The CLI |
| `schemas/` | Versioned JSON contracts |
| `packs/example/` | Source-free manual example pack |
| `packs/sources-example/` | td and native fake connection examples |

A new provider is a directory under `adapters/` behind an existing port. If it does not fit a port, add the port first.

Schema files are versioned. `outcome-v2.json` is the current run outcome. The native-source change intentionally replaces earlier admission and bundle contracts; do not add legacy compatibility layers or delete stored user state to ease the replacement.

## Proof

```sh
bundle exec rake test
BACKSTAGE_DOCKER_TEST=1 bundle exec rake test
bin/backstage config check --pack packs/example --json
bin/backstage config check --pack packs/sources-example --json
```

The default suite does not touch the network, td, GitHub, or a model provider. Run `scripts/scan-secrets` before publishing anything that could carry a credential. Prove behavior through the fake journey before `--publish-draft`.

`bin/backstage config check` compiles the pack's lifecycles too. Run it after a configuration edit.

## Docs

Guides live in `docs/guides/active/`. Plans live in `docs/plans/` with `active/` and `implemented/`. The README, the configuration model, the work source extension guide, and the operator guide are the public description of the system.
