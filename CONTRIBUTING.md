Purpose: Explain how to propose and validate changes safely.
Audience: Contributors to the fleet dbt warehouse.
[Root README](README.md)

# Contributing

Branch from `main` and keep each branch short-lived and focused on one concern. Allowed prefixes are `feature/*`, `fix/*`, `chore/*`, and `docs/*`. Use Conventional Commits (`type(scope): summary`). The `commit-msg` hook enforces this format and a 72-character subject cap.

Open a pull request and stop. Do not merge it, and do not push to `main`; a maintainer handles review and merge.

`make help` is the authoritative command list. Run these required local checks before opening a PR:

```bash
make lint-fast
make py-test
make contracts-check
```

`make demo` is optional and builds a zero-credential fixture demo in `fleet_dev`; it is not a substitute for the required checks.

Never edit `.env`, commit secrets, or weaken, delete, or skip tests, contracts, or checks to get CI green. Source credentials are read-only; keep write access scoped to the warehouse and publish paths.

A PR that changes a contract must carry the `contract-change` label and a `CHANGELOG.md` entry. Ask first before dependency upgrades, production or Databricks runs, breaking contract changes, schedule/SLO/lookback/threshold changes, deploy workflow edits, or releases.

Documentation must start with a short Purpose line and Audience line, then link back to the root README with the correct relative path. See [SECURITY.md](SECURITY.md) before reporting a vulnerability or changing credential, PII, or publication handling.
