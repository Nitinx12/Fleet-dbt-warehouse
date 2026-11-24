import json
from pathlib import Path

from streamlit_state import inject_payload, live_payload


class FakeFrame:
    def __init__(self, rows):
        self.rows = rows

    def to_dict(self, orient):
        assert orient == "records"
        return self.rows


def test_live_payload_empty_without_secrets():
    calls = []

    def read(view, limit):
        calls.append(view)
        return FakeFrame([])

    assert live_payload(read, lambda: False) == {"batches": [], "dbt": [], "dq": []}
    assert calls == []


def test_live_payload_reads_audit_views():
    def read(view, limit):
        return FakeFrame([{"view": view, "limit": limit}])

    payload = live_payload(read, lambda: True)
    assert [payload[k][0]["view"] for k in ("batches", "dbt", "dq")] == [
        "audit.v_batch_summary",
        "audit.v_dbt_results",
        "audit.v_data_quality_results",
    ]
    assert payload["batches"][0]["limit"] == 20


def test_live_payload_fails_closed_on_missing_key():
    def read(view, limit):
        raise KeyError("audit view")

    assert live_payload(read, lambda: True) == {"batches": [], "dbt": [], "dq": []}


def test_inject_payload_replaces_placeholder():
    out = inject_payload('<script id="live-data">__LIVE_DATA__</script>', {"n": 7})
    data = json.loads(out.split(">", 1)[1].split("</script>", 1)[0])
    assert data == {"n": 7}
    assert "__LIVE_DATA__" not in out


def test_inject_payload_emits_strict_json():
    out = inject_payload(
        "__LIVE_DATA__", {"x": float("nan"), "rows": [{"y": float("inf")}]}
    )
    assert "NaN" not in out
    assert "Infinity" not in out
    assert json.loads(out) == {"x": None, "rows": [{"y": None}]}


def test_inject_payload_escapes_script_close():
    out = inject_payload("__LIVE_DATA__", {"x": "</script><b>hi</b>"})
    assert "</script><b>" not in out
    assert "\\u003c/script>" in out


def test_index_expects_live_data_not_simulation():
    html = (Path(__file__).parents[2] / "index.html").read_text(encoding="utf-8")
    assert "__LIVE_DATA__" in html
    assert "simulated run" not in html
