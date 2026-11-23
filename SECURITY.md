# Security policy

**Purpose:** Explain how to report vulnerabilities and which security controls protect this repository and pipeline.

**Audience:** Security researchers, contributors, maintainers, and operators.

[Root README](README.md) · [Architecture](ARCHITECTURE.md) · [Deployment](DEPLOYMENT.md)

## Reporting a vulnerability

Report privately to the maintainer listed in the [README](README.md#author-contact-license), or contact [GitHub @Nitinx12](https://github.com/Nitinx12). Do not open a public issue for an unfixed vulnerability.

Include the affected path or component, impact, reproduction steps, and any safe suggested fix. Do not include real credentials, tokens, or row-level data in the report. Coordinate disclosure with the maintainer before publishing details.

There is no released tag yet; report findings against `main` and include the commit SHA when possible.

## Scope

- Repository source, Makefile targets, scripts, DAGs, SQL, contracts, and generated artifacts.
- CI/CD definitions in `.github/workflows/` and the Airflow image build.
- Local and CI handling of connection settings, secrets, and fixture data.
- Sensitive-data boundaries from bronze through published marts.

Source system infrastructure, Databricks account configuration, and remote host administration are outside this repository's scope, but report issues that expose or mishandle this project's data or credentials.

## Secret handling

- `.env` is ignored and must never be committed or modified without explicit permission; `.env.example` contains placeholders only.
- All hosts, catalogs, tokens, passwords, and the PII salt come from environment variables.
- Source credentials are read-only, and write credentials are scoped to the warehouse and publication paths.
- No credentials are baked into the Airflow image or GitHub Actions artifacts.
- Notifications pass through a redactor that scrubs URL credentials, Bearer tokens, and variables whose names contain `PASSWORD`, `TOKEN`, `SECRET`, `KEY`, `SALT`, or `FERNET`.
- Gitleaks scans full history on pull requests, pushes to `main`, a weekly schedule, and manual dispatch.

Run `bash helpers/protect_env.sh --check`, `make env-check`, and `make contracts-check` before proposing security-sensitive changes. See [contributing](CONTRIBUTING.md) for the required local gates.

## Data protection

| Value | Required handling |
|---|---|
| `drivers.license_number` | Hashed; raw value does not reach marts. |
| `drivers.date_of_birth` | Converted to an age attribute; not present in marts. |
| `drivers.first_name`, `last_name` | Postgres only; the driver scorecard is excluded from publication. |
| `fuel_purchases.fuel_card_number` | Masked to the last four digits. |
| `safety_incidents.description` | Excluded from gold and published marts. |

The publication allowlist contains five non-PII marts. Any new `serve: true` model carrying a PII-tagged column must be blocked from publication.

## Pipeline guardrails

- Least-privilege GitHub Actions permissions, commit-pinned third-party actions, and no `pull_request_target`.
- Immutable `sha-<commit>` images resolved to a digest before deploy; `latest` is never deployed.
- Contract and data-test failures block the run; retries are limited to transient connection errors.
- Tests, contracts, schema checks, and secrets scanning must not be weakened to obtain a green build.
- Production and Databricks runs require explicit approval and are read-only unless performing the approved publish.

## Related documents

- [Architecture guide](docs/ARCHITECTURE.md)
- [Deployment guide](docs/DEPLOYMENT.md)
- [Testing guide](docs/TESTING.md)
- [Contributing](CONTRIBUTING.md)
