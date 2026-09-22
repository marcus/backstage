# Contributing

Ruby 4.0 or newer, and Bundler. The default test suite does not use the network.

```sh
bundle install
bundle exec rake test
bin/backstage config check --pack packs/example --json
```

Docker tests are opt-in and need a local worker image:

```sh
docker build --tag backstage-worker:0.1.0 .
BACKSTAGE_DOCKER_TEST=1 bundle exec rake test
```

Commands are noninteractive. A change that can only be reached by a keypress is incomplete. `--json` output is part of the interface.

Schemas in `schemas/` are versioned. Add a new schema file when a contract changes. Do not edit a schema that already has records written against it.

`--publish-draft` authorizes a draft pull request on the designated repository. It does not authorize a merge, a push to the default branch, a deploy, or a notification. Keep that boundary unless the replacement capability exists and is proven.

Run `scripts/scan-secrets` on files that might contain a credential value before you push them.
