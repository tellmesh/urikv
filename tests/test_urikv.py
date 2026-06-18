import json
import os
import tempfile
from pathlib import Path

from uri_control.edge.runtime import Runtime


def _rt(**config):
    os.environ["URISYS_NODE_DATA"] = config.pop("_data", tempfile.mkdtemp())
    rt = Runtime(config=config)
    import urikv

    urikv.register(rt)
    return rt


def test_kv_health_and_set_get():
    db = Path(tempfile.mkdtemp()) / "test.db"
    rt = _rt(kv={"path": str(db)})
    h = rt.call("kv://runtime/query/health", {}, {})
    assert h["result"]["ok"] is True

    miss = rt.call("kv://session/key/draft/query/get", {}, {})
    assert miss["result"]["exists"] is False

    rt.call(
        "kv://session/key/draft/command/set",
        {"value": {"text": "hello"}},
        {"approved": True},
    )
    get_out = rt.call("kv://session/key/draft/query/get", {}, {})
    assert get_out["result"]["value"]["text"] == "hello"


def test_log_events_read(tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text(
        json.dumps({"operation": "screen.capture", "result": {"ok": True}}) + "\n"
        + json.dumps({"operation": "him.move", "result": {"ok": False}}) + "\n",
        encoding="utf-8",
    )
    rt = _rt(log={"streams": {"events": str(events)}}, _data=str(tmp_path))
    out = rt.call("log://events/query/summarize", {"limit": 10}, {})
    assert out["result"]["matched"] == 2
    assert out["result"]["operations"]["screen.capture"] == 1


def test_discover(tmp_path):
    (tmp_path / "node-identity.json").write_text(json.dumps({"node_id": "test-node"}), encoding="utf-8")
    rt = _rt(_data=str(tmp_path), node_id="test-node")
    out = rt.call("kv://runtime/query/discover", {}, {})
    assert out["result"]["node_id"] == "test-node"
    assert "runtime" in out["result"]
