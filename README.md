# Backstage

Backstage carries explicitly authorized work through a lifecycle written in YAML. The included path starts with a [td](https://github.com/marcus/td) issue. One container implements it, a second container reviews the result, and Backstage can open a GitHub draft pull request and write the outcome back through td. Every transition is stored, so an interrupted run is recovered from the record.

This project is not the CNCF Backstage developer portal.

What ships today publishes draft pull requests. `--publish-draft` does not merge, push a default branch, deploy, or notify anyone. Deployment and other operational changes are designed and not implemented. `process` with no flag is a fake journey: no model call and no GitHub call.

## Try the fake journey

Ruby 4.0 or newer, and Bundler. Docker, td, and a token are not required for this path.

```sh
bundle install
bin/backstage config check --pack packs/example --json
work_id=$(bin/backstage --json submit --pack packs/example --target widgets --title "Example" --description "Prove it" | jq -r .id)
bin/backstage process "$work_id" --pack packs/example --json
bin/backstage --json show "$work_id"
```

`packs/example` is a full pack aimed at a fictional `example/widgets` repository. Structured state defaults to `~/.backstage/state.jsonl` and artifacts to `~/.backstage/artifacts`. `--state`, `--artifacts`, `BACKSTAGE_STATE`, and `BACKSTAGE_ARTIFACTS` override those paths.

Global `--json` and `--jsonl` work before or after the command. The [operator guide](docs/guides/active/operator-guide.md) covers `transition`, `decide`, `recover`, `dispatch`, and `cancel`. Nothing in the CLI waits for a keypress.

## Publish a draft

A real run needs a local Docker build of the worker, a fine-grained GitHub token for one repository, and `OPENROUTER_API_KEY` for the included pi harness. The example pack names those variables. It does not contain the values.

```sh
docker build --tag backstage-worker:0.1.0 .
export GITHUB_TOKEN          # contents and pull requests on that one repository
export OPENROUTER_API_KEY
bin/backstage process "$work_id" --pack packs/example --publish-draft --json
```

The image pins `node:24-bookworm-slim` by digest, pi at 0.84.3, and Go 1.27.0 with architecture-specific checksums. It also contains `git`, `gh`, and the repository worker. Repositories, prompts, and credentials arrive with the job.

The agent checkout is produced without repository credentials, as a size-bounded patch that records its base, branch, and digest. A separate credentialed worker mounts only that patch, creates a fresh checkout, and rechecks origin, branch, base, and draft-only action before it commits or pushes. Pack YAML holds credential references. Only environment-variable names are passed into Docker.

```yaml
credentials:
  repository_default: github
  broker:
    github:
      source_env: GITHUB_TOKEN
      runtime_env: GH_TOKEN
    openrouter:
      source_env: OPENROUTER_API_KEY
      runtime_env: OPENROUTER_API_KEY
```

Use a token restricted to the designated repository. Backstage cannot read a token and tell which repositories it covers, so that scope is an operator precondition.

## Configuration

A deployment pack selects adapters, the worker image, credential references, and the lifecycles on offer. A target is one repository: its remote, the td workspace that routes to it, and any read-only context checkouts. A job bundle is compiled for each run and stored with that run, which is how the audit trail answers what the agent was given.

`bin/backstage config check --pack packs/example --json` compiles the pack, workflows included. The [configuration model](docs/guides/active/config-model.md) is the file shape. Another deployment keeps its own pack outside this repository and passes that directory to `--pack`.

Routing and the chosen workflow are fixed when work enters. The target, source instance, canonical source identity, and workflow digest are stored on the work item. A later `process`, or an idempotent resubmit, cannot retarget it.

td is the included work source. Another tracker is a new adapter behind that port, not a change to the lifecycle core.

## Boundaries

Actor authority comes from where the request entered. An operator may speak as a person or as the system. A worker's requests are limited to the run that owns its channel, and only as `agent` or `reviewer`. A worker cannot approve its own change.

Implementation and review are separate runner objects and fresh containers. Review authority is clone-only. Handoff needs an approved independent verdict bound to the candidate digest under review, or a person's recorded decision. New candidate content invalidates an earlier approval.

Each transition writes state, history, any decision, and any requested execution in one store commit, guarded by the work item's revision. That guard is also what drops a late result from a cancelled or replaced run.

GitHub and td writes use a preflight check and a persisted external-action ledger. A retry after publication resumes the recorded branch, reconciles the same draft pull request, and does not repeat an identical td handoff.

## Development

```sh
bundle exec rake test
BACKSTAGE_DOCKER_TEST=1 bundle exec rake test
bin/backstage config check --pack packs/example --json
```

The default suite does not use the network, td, GitHub, or a model provider. Docker tests are opt-in and need that image. Run `scripts/scan-secrets` on anything that might have picked up a credential value before publishing it.

Schemas in `schemas/` are versioned (`*-v1.json`, and `outcome-v2.json` for the current run outcome). When a contract changes, add a new file. Do not edit a schema that already has records written against it.

`lib/backstage` is split by boundary:

| Directory | Holds |
|---|---|
| `domain/` | Records, compiled lifecycles, repository authority |
| `application/` | Execution, transitions, recovery |
| `ports/` | Narrow seams |
| `adapters/` | td, GitHub, pi, Docker, JSONL, local files, environment, and the fake journey |
| `configuration/` | Deployment pack compilation |
| `contracts/` | Schema validation |
| `surfaces/` | The CLI |
| `bootstrap/system.rb` | Composition root shared by the CLI and any future host |

`require "backstage"` is the library entry point. No gem outside Ruby's standard and default libraries is required beyond the `base64` dependency declared in the gemspec. Adding another runtime dependency is a decision to make explicitly.

## Status

0.1.0. The store is JSONL. pi and Docker are the included harness and runtime. [td](https://github.com/marcus/td) is the included work source.

## License

[MIT](LICENSE).
