"""Streamlit host for the project site and a read-only audit panel."""

import base64
import re
import sys
from importlib import import_module
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Engine
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT / "site" / "scripts"))
_state = import_module("streamlit_state")
audit_error = _state.audit_error
inject_facts = _state.inject_facts
inject_payload = _state.inject_payload
live_payload = _state.live_payload

REPO = "https://github.com/Nitinx12/Fleet-dbt-warehouse"
BRANCH = "main"  # change if your default branch differs

# Fixed allowlist: the only objects this app will ever query
VIEWS = {
    "Batch summary": "audit.v_batch_summary",
    "dbt results": "audit.v_dbt_results",
    "Data quality": "audit.v_data_quality_results",
}

st.set_page_config(page_title="Fleet dbt Warehouse", page_icon="🚚", layout="wide")
st.markdown(
    "<style>#MainMenu,header,footer{visibility:hidden}"
    ".block-container{padding:.5rem 1rem 0!important;max-width:100%!important}</style>",
    unsafe_allow_html=True,
)


def load_site(live: dict) -> str:
    html = (ROOT / "index.html").read_text(encoding="utf-8")
    logo = ROOT / "assets/logo/logo_1.png"
    if logo.exists():  # relative image paths don't resolve inside the iframe
        b64 = base64.b64encode(logo.read_bytes()).decode()
        html = html.replace("assets/logo/logo_1.png", f"data:image/png;base64,{b64}")
    html = re.sub(  # send doc/report links to GitHub
        r'href="((?:README|ARCHITECTURE|DEPLOYMENT)\.md|docs/[\w.]+|reports/[\w./]+)"',
        rf'href="{REPO}/blob/{BRANCH}/\1" target="_blank" rel="noreferrer"',
        html,
    )
    return inject_payload(inject_facts(html), live)


def db_configured() -> bool:
    try:
        return "postgres" in st.secrets
    except FileNotFoundError:  # no secrets file at all
        return False


@st.cache_resource
def get_engine() -> Engine:
    cfg = st.secrets["postgres"]
    url = URL.create(
        "postgresql+psycopg2",
        username=cfg["user"],
        password=cfg["password"],
        host=cfg["host"],
        port=int(cfg.get("port", 5432)),
        database=cfg["dbname"],
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10, "sslmode": cfg.get("sslmode", "prefer")},
    )


@st.cache_data(ttl=60)
def read_view(view: str, limit: int = 50) -> pd.DataFrame:
    schema, name = view.split(".")  # view comes from the VIEWS allowlist only
    with get_engine().connect() as conn:
        cols = [
            r[0]
            for r in conn.execute(
                text(
                    "select column_name from information_schema.columns "
                    "where table_schema=:s and table_name=:t order by ordinal_position"
                ),
                {"s": schema, "t": name},
            )
        ]
        order = next((c for c in reversed(cols) if c.endswith(("_at", "_date"))), None)
        sql = f"select * from {schema}.{name}"
        if order:
            order = order.replace('"', '""')
            sql += f' order by "{order}" desc nulls last'
        return pd.read_sql(text(sql + " limit :n"), conn, params={"n": limit})


site_tab, live_tab = st.tabs(["Pipeline site", "Live audit status"])

with site_tab:
    st.iframe(load_site(live_payload(read_view, db_configured)), height=1000)

with live_tab:
    if not db_configured():
        st.info(
            "Live status is off. Add a `[postgres]` block (host, port, dbname, user, password) "
            "under **Settings → Secrets** in Streamlit Cloud, or in `.streamlit/secrets.toml` locally. "
            "Use a read-only user with SELECT on the `audit` schema."
        )
    else:
        left, right = st.columns([4, 1])
        left.caption("Read-only view of the audit schema. Refreshes every 60 seconds.")
        if right.button("Refresh now"):
            st.cache_data.clear()
        tabs = st.tabs(list(VIEWS))
        for tab, (label, view) in zip(tabs, VIEWS.items()):
            with tab:
                try:
                    df = read_view(view)
                    st.dataframe(df, width="stretch", hide_index=True)
                    st.caption(f"{view} · latest {len(df)} rows")
                except (SQLAlchemyError, KeyError, TypeError, ValueError) as exc:
                    st.error(f"Could not read {view}. {audit_error(exc)}")
