# AUTO-GENERATED — do not edit manually.
# Regenerate by running: bash ci/mirroring/scripts/sync-mirror-config.sh

# Mirroring between snowflake-eng and snowflakedb

## Source of truth

The mirror config generator lives in
[snowflake-eng/drivers](https://github.com/snowflake-eng/drivers) at
`ci/mirroring/scripts/generate_mirror_config.py`. This repo consumes it
via the local sync script.


## Regenerating mirror config

All mirror infrastructure files (`ci/mirroring/`, `ci/mirroring/scripts/`,
mirror workflow YAMLs) are auto-generated. **Do not edit them manually.**

To regenerate, run from the repo root:

```bash
bash ci/mirroring/scripts/sync-mirror-config.sh
```

The sync script fetches the latest generator from `snowflake-eng/drivers`,
compares its SHA-256 hash against `ci/mirroring/.generator-hash`, and
regenerates only if the upstream generator has changed.

### `--force` flag

If the upstream generator hasn't changed since the last run, the sync
script skips regeneration. To force a regeneration regardless (e.g.,
after changing local parameters in `sync-mirror-config.sh`), pass
`--force`:

```bash
bash ci/mirroring/scripts/sync-mirror-config.sh --force
```

### Staleness detection

The `mirror.yml` workflow includes a `check-generator-staleness` job
that blocks the mirror push if the upstream generator has been updated
but `sync-mirror-config.sh` hasn't been re-run. When this check fails:

1. Run `bash ci/mirroring/scripts/sync-mirror-config.sh`
2. Review and commit the regenerated files
3. Re-trigger the mirror workflow


## How it works

This repo is the internal source of truth. A Copybara-based mirror
keeps the `snowflakedb/` copy in sync with the public-safe subset of
this tree, and lets external contributors land changes through a
labeled-PR import flow.

Two directions, both driven by `ci/mirroring/copy.bara.sky`:

- **Outbound** (`mirror` workflow): every commit on `master` or
  `release/*` is replayed onto the same branch on the mirror.
  Authorship and commit message are preserved; Copybara appends a
  `GitOrigin-RevId: <sha>` trailer for traceability. Runs daily via
  `.github/workflows/mirror.yml`, which also accepts a manual
  `workflow_dispatch` (with a `dry_run` input that previews the push
  without writing to the mirror).

- **Inbound** (`import` workflow): an external contributor opens a PR
  on the mirror. A maintainer applies the `ok-to-import` label, then
  dispatches `.github/workflows/mirror-inbound.yml` with the PR
  number. Copybara reads the labeled PR and creates a matching PR on
  the internal repo with the contributor's authorship preserved, so
  it runs against full internal CI before merge. Once the internal PR
  is merged, the next outbound run pushes the merge back to the
  mirror, closing the loop.

### Filter model: denylist

`ci/mirroring/copy.bara.sky` uses a **denylist** (`EXCLUDED_PATHS`) — new source
files reach the mirror by default; anything matching the denylist does
not.

Safety nets (independent of the denylist):
1. `verify_match` guards FAIL the sync if a mirrored file names any
   `snowflake-eng` repo, an internal hostname, or the internal 1Password vault.
2. The `NOMIRROR/` convention: put any new internal-only material
   under a directory named `NOMIRROR` (at any depth) and it is excluded
   without touching the config.

### Authentication

Two credentials (compromise of one cannot reach the other org):

- **Internal (snowflake-eng):** `SNOWFLAKE_EMU_TOKEN` — a PAT with access
  to the internal org. Configurable via `INTERNAL_TOKEN_NAME` in
  `sync-mirror-config.sh`.
- **Mirror (snowflakedb):** a GitHub App installation token created
  at workflow runtime via `actions/create-github-app-token@v1`.

Required repository secrets:

- `SNOWFLAKE_EMU_TOKEN` — PAT for `snowflake-eng` access.
- `MIRRORING_APP_ID` — the numeric App ID of the GitHub App installed on
  the `snowflakedb` organization.
- `MIRRORING_APP_PRIVATE_KEY` — the PEM private key for that GitHub App.


## How to use it

### Adding internal-only files

Put new internal files under a directory named `NOMIRROR/` (at any
depth). Nothing else is required.

### Adding files that should be mirrored

Add them as normal. The denylist defaults to public, so the file ships
on the next mirror run.

### Importing an external PR (inbound)

1. External contributor opens a PR against `master` on the mirror.
2. A maintainer applies the `ok-to-import` label.
3. Dispatch `.github/workflows/mirror-inbound.yml` with the mirror PR
   number.
4. A matching PR appears on the internal repo. Internal CI runs.
5. On merge, the next outbound run replays the commit back to the
   mirror. `close-imported-pr.yml` auto-closes the original mirror PR.


## Remaining steps

Tracked here so the rollout is visible. None block routine use of the
mirror.

- **Validate the inbound flow end-to-end.** Walk one real PR
  (open on mirror → label → dispatch inbound → run internal CI →
  merge → confirm outbound replays it back → close the mirror PR).
- **Ship `close-imported-pr.yml` to the mirror.** Now that the GitHub
  App token has `workflow` scope, the outbound mirror can push it
  automatically once `.github/**` is unblocked. Until then, deploy
  the file to the mirror manually.
- **Create the `ok-to-import` label on the mirror repo.**
