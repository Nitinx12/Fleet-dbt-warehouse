# Screenshot plan

**Purpose:** List the visual assets the README needs and how to capture each one reproducibly.

**Audience:** Maintainers preparing a release or refreshing the README.

[Back to README](../README.md) · [Business insights](../README.md#business-insights)

One vector screenshot is embedded in the README; the remaining captures are still TODO. Do not embed placeholders.

| Asset | Target path | How to capture |
|---|---|---|
| Pipeline flow | `assets/screenshots/pipeline_flow.svg` | Vector export of the README architecture Mermaid diagram; no credentials or PII. |
| Airflow DAG graph | `assets/screenshots/airflow_dag.png` | Run `make airflow-up`, open the Airflow UI, unpause `fleet_daily_load`, wait for one successful run, then save the Graph view. |
| dbt lineage | `assets/screenshots/dbt_lineage.png` | Run `make dbt-docs ENV=ci`, open `dbt/target/index.html`, choose Lineage, then save the view. |
| BI dashboard | `assets/screenshots/bi_dashboard.png` | Capture a real dashboard backed by the five published Databricks marts. A BI workspace is not configured in this repo. |
| Sample query output | `assets/screenshots/sample_query.txt` | After `make demo`, query a prefixed `ci_demo_*` fact table with `psql` and save the redacted terminal output. |

Before committing a capture:

- Confirm it contains no credentials, tokens, row-level PII, or private hostnames.
- Use a relative path in Markdown and descriptive alt text.
- Keep every raster under 500 KB; prefer SVG for architecture diagrams.
- Regenerate chart images with `make report`; do not hand-edit `reports/img/*.png`.
