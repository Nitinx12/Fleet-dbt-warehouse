# Project site numbers

`index.html` must not claim a number that the repository does not prove. This
document explains how those numbers are derived, bound and checked.

## Source of truth

`scripts/site_facts.py` reads the repository and writes `site/facts.json`. Every
entry has `value`, `source` and `how`, so a displayed number can be traced back
to the file or command that produced it.

```bash
make site-facts        # regenerate site/facts.json
python scripts/site_facts.py --check-stale
python scripts/site_facts.py --print
```

The collector reads the dbt manifest when present. When it is absent, as in CI,
it parses `dbt_project.yml` and the schema YAML instead. Tests are counted by
`pytest --collect-only -q`; Airflow tests are collected separately with
`PYTHONPATH=airflow`.

## Binding rules

- A real number in visible HTML is wrapped in `data-fact="<key>"`; the text is
  the formatted value from `facts.json` and `scripts/site_numbers_allowlist.yml`.
- JavaScript reads `FACTS` rather than hardcoding a repo fact. Real values come
  from `FACTS[key]` or `num(key)`.
- Presentation geometry, timers, indexes and demo records are not repo facts.
  They are covered by explicit rules in `scripts/site_numbers_allowlist.yml`;
  demo records are also marked `// data-illustrative`.
- Every format (`count`, `percent1`, `usd_m1`, `decimal2`, and so on) is explicit
  in the allowlist. Rounding uses `Decimal` with `ROUND_HALF_UP`.

`index.html` carries both `__SITE_FACTS__` and `__LIVE_DATA__` placeholders.
`.streamlit/streamlit_state.py` injects both at render time. A committed fallback
JSON script lets the file work without the Streamlit host; `FACTS` parses the
injected script and falls back to the committed copy.

## Checks

```bash
make site-check         # strict checker, no render
make site-render        # render build/site.html with both placeholders filled
make site-browser-smoke # render plus a zero-dependency Node DOM check
```

`scripts/check_site_numbers.py` fails on:

- an unbound number in HTML text or JavaScript;
- a `data-fact` value that differs from the generated fact;
- a hardcoded repo fact in JavaScript;
- a stale or missing README/docs claim;
- a mismatch between the Streamlit payload keys and `live_payload`;
- a changed audit view column list;
- an exact-looking number marked as illustrative in strict mode.

`make site-browser-smoke` runs against `index.html` and `site/facts.json`, then
checks the rendered `build/site.html` has no unresolved placeholders. It needs
only Node; no browser package is installed.

CI runs `make site-browser-smoke` in the `lint` job and the pre-push hook runs
`make site-check`. When a repository count changes, regenerate the facts, update
the bound text, fallback JSON and docs in the same commit.
