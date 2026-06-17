# UriPack: urikv

Self-contained Markpact — definitions, full source, run config. Unpack & run: `urisys markpact run urikv/urikv.markpact.md --as service` (writes `.markpact/`).

```yaml markpact:pack
apiVersion: urisys.io/v1
kind: UriPack
metadata:
  id: urikv-pack
  version: 0.1.0
  language: python
description: urikv URI pack.
schemes:
- kv
capabilities:
- id: kv.health
  uri: kv://runtime/query/health
  kind: query
  operation: kv.health
  handler: python://urikv.handlers:health
  side_effects: false
  approval: not_required
- id: kv.list
  uri: kv://runtime/query/list
  kind: query
  operation: kv.list
  handler: python://urikv.handlers:list_items
  side_effects: false
  approval: not_required
- id: kv.discover
  uri: kv://runtime/query/discover
  kind: query
  operation: kv.discover
  handler: python://urikv.handlers:discover
  side_effects: false
  approval: not_required
- id: kv.key.exists
  uri: kv://{namespace}/key/{key}/query/exists
  kind: query
  operation: kv.key.exists
  handler: python://urikv.handlers:key_exists
  side_effects: false
  approval: not_required
- id: kv.key.get
  uri: kv://{namespace}/key/{key}/query/get
  kind: query
  operation: kv.key.get
  handler: python://urikv.handlers:key_get
  side_effects: false
  approval: not_required
- id: kv.prefix.keys
  uri: kv://{namespace}/prefix/{prefix}/query/keys
  kind: query
  operation: kv.prefix.keys
  handler: python://urikv.handlers:list_items
  side_effects: false
  approval: not_required
- id: kv.key.set
  uri: kv://{namespace}/key/{key}/command/set
  kind: command
  operation: kv.key.set
  handler: python://urikv.handlers:key_set
  side_effects: true
  approval: required
- id: kv.key.delete
  uri: kv://{namespace}/key/{key}/command/delete
  kind: command
  operation: kv.key.delete
  handler: python://urikv.handlers:key_delete
  side_effects: true
  approval: required
policy:
  default: deny_mutations_without_approval
runtime:
  default_environment: mock
  supports:
  - mock
  - local
  - docker
```

```yaml markpact:run
modes:
- pack
- service
- flow
- interface
- adapter
default: service
scheme: kv
service:
  port: 8790
  wire: POST /uri/call
flow:
  ids: []
adapter:
  wire: POST /uri/call
  events: GET /events
```

```python markpact:module path=urikv/__init__.py
from .routes import register

__all__ = ["register"]
```

```python markpact:module path=urikv/handlers.py
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _kv_cfg(context: dict[str, Any]) -> dict[str, Any]:
    cfg = dict((context.get("config") or {}).get("kv") or {})
    path = str(
        cfg.get("path")
        or os.environ.get("URISYS_KV_PATH")
        or os.environ.get("URISYS_NODE_DATA", "data")
    )
    if path.endswith((".db", ".sqlite", ".sqlite3")):
        db_path = path
    else:
        db_path = str(Path(path).expanduser() / "store.db")
    cfg.setdefault("driver", os.environ.get("URISYS_KV_DRIVER", "sqlite"))
    cfg["path"] = str(Path(db_path).expanduser())
    return cfg


def _connect(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS kv (
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT,
            value_type TEXT NOT NULL DEFAULT 'json',
            updated_at REAL NOT NULL,
            expires_at REAL,
            PRIMARY KEY (namespace, key)
        )
        """
    )
    con.commit()
    return con


def _now() -> float:
    return time.time()


def _decode_value(row: sqlite3.Row) -> Any:
    raw = row["value"]
    if raw is None:
        return None
    if row["value_type"] == "text":
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _encode_value(value: Any) -> tuple[str, str]:
    if isinstance(value, str):
        return value, "text"
    return json.dumps(value, ensure_ascii=False), "json"


def _purge_expired(con: sqlite3.Connection) -> int:
    cur = con.execute("DELETE FROM kv WHERE expires_at IS NOT NULL AND expires_at <= ?", (_now(),))
    con.commit()
    return cur.rowcount


def health(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    del payload
    cfg = _kv_cfg(context)
    db_path = cfg["path"]
    exists = Path(db_path).exists()
    stats = {"ok": True, "scheme": "kv", "driver": cfg.get("driver", "sqlite"), "path": db_path, "exists": exists}
    if exists:
        con = _connect(db_path)
        try:
            stats["namespaces"] = [
                r["namespace"]
                for r in con.execute("SELECT DISTINCT namespace FROM kv ORDER BY namespace").fetchall()
            ]
            stats["keys"] = int(con.execute("SELECT COUNT(*) AS c FROM kv").fetchone()["c"])
            stats["expired_purged"] = _purge_expired(con)
        finally:
            con.close()
    return stats


def list_items(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    cfg = _kv_cfg(context)
    namespace = str(payload.get("namespace") or context.get("params", {}).get("namespace") or "").strip()
    prefix = str(payload.get("prefix") or context.get("params", {}).get("prefix") or "").strip()
    include_values = bool(payload.get("include_values", False))
    limit = int(payload.get("limit") or 200)
    con = _connect(cfg["path"])
    try:
        _purge_expired(con)
        if namespace:
            sql = "SELECT namespace, key, value, value_type, updated_at, expires_at FROM kv WHERE namespace = ?"
            args: list[Any] = [namespace]
            if prefix:
                sql += " AND key LIKE ?"
                args.append(prefix + "%")
            sql += " ORDER BY key LIMIT ?"
            args.append(limit)
            rows = con.execute(sql, args).fetchall()
        else:
            rows = con.execute(
                """
                SELECT namespace, key, value, value_type, updated_at, expires_at
                FROM kv ORDER BY namespace, key LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items = []
        for row in rows:
            item = {
                "namespace": row["namespace"],
                "key": row["key"],
                "updated_at": row["updated_at"],
                "expires_at": row["expires_at"],
            }
            if include_values:
                item["value"] = _decode_value(row)
            items.append(item)
        namespaces = sorted({r["namespace"] for r in con.execute("SELECT DISTINCT namespace FROM kv").fetchall()})
        return {"items": items, "namespaces": namespaces, "count": len(items)}
    finally:
        con.close()


def discover(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """System snapshot: node paths, runtime state keys, kv stats, recent activity."""
    del payload
    cfg = _kv_cfg(context)
    data_root = Path(os.environ.get("URISYS_NODE_DATA", "data")).expanduser()
    events_path = Path(
        os.environ.get("URISYS_NODE_EVENTS")
        or (context.get("config") or {}).get("log", {}).get("events_path")
        or "data/events.jsonl"
    ).expanduser()
    runtime = context.get("runtime")
    state = context.get("state") or {}
    node_cfg = context.get("config") or {}

    identity_path = data_root / "node-identity.json"
    pairing_path = data_root / "node-pairing.json"
    identity = {}
    pairing = {}
    if identity_path.exists():
        try:
            identity = json.loads(identity_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            identity = {"error": "invalid json"}
    if pairing_path.exists():
        try:
            pairing = json.loads(pairing_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pairing = {"error": "invalid json"}

    data_files: list[dict[str, Any]] = []
    if data_root.exists():
        for path in sorted(data_root.rglob("*")):
            if path.is_file() and path.stat().st_size <= 5_000_000:
                rel = str(path.relative_to(data_root))
                data_files.append({"path": rel, "bytes": path.stat().st_size})

    kv_stats = health({}, context)
    event_count = 0
    if events_path.exists():
        event_count = sum(1 for _ in events_path.open(encoding="utf-8", errors="replace"))

    return {
        "ok": True,
        "hostname": os.uname().nodename,
        "node_id": identity.get("node_id") or node_cfg.get("node_id"),
        "paired": bool(pairing.get("paired")),
        "paths": {
            "URISYS_NODE_DATA": str(data_root),
            "URISYS_NODE_EVENTS": str(events_path),
            "URISYS_KV_PATH": cfg["path"],
            "URISYS_NODE_CONFIG": os.environ.get("URISYS_NODE_CONFIG"),
        },
        "runtime": {
            "loaded_packs": sorted(getattr(runtime, "_loaded_packs", set()) or []),
            "state_keys": sorted(state.keys()),
            "routes_count": len(getattr(runtime, "routes", []) or []),
        },
        "kv": kv_stats,
        "events": {"path": str(events_path), "lines": event_count},
        "data_files": data_files[:100],
        "profile_sections": sorted(node_cfg.keys()),
    }


def key_exists(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    namespace, key = _ns_key(context)
    cfg = _kv_cfg(context)
    con = _connect(cfg["path"])
    try:
        _purge_expired(con)
        row = con.execute(
            "SELECT 1 FROM kv WHERE namespace = ? AND key = ?",
            (namespace, key),
        ).fetchone()
        return {"namespace": namespace, "key": key, "exists": row is not None}
    finally:
        con.close()


def key_get(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    namespace, key = _ns_key(context)
    cfg = _kv_cfg(context)
    con = _connect(cfg["path"])
    try:
        _purge_expired(con)
        row = con.execute(
            "SELECT namespace, key, value, value_type, updated_at, expires_at FROM kv WHERE namespace = ? AND key = ?",
            (namespace, key),
        ).fetchone()
        if row is None:
            return {"namespace": namespace, "key": key, "exists": False, "value": None}
        return {
            "namespace": namespace,
            "key": key,
            "exists": True,
            "value": _decode_value(row),
            "updated_at": row["updated_at"],
            "expires_at": row["expires_at"],
        }
    finally:
        con.close()


def key_set(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    namespace, key = _ns_key(context)
    if "value" not in payload:
        raise ValueError("payload.value is required")
    ttl = payload.get("ttl") or payload.get("ttl_seconds")
    expires_at = _now() + float(ttl) if ttl is not None else None
    encoded, value_type = _encode_value(payload["value"])
    if context.get("dry_run"):
        return {
            "dry_run": True,
            "namespace": namespace,
            "key": key,
            "would_set": payload["value"],
            "ttl": ttl,
        }
    cfg = _kv_cfg(context)
    con = _connect(cfg["path"])
    try:
        con.execute(
            """
            INSERT INTO kv (namespace, key, value, value_type, updated_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value = excluded.value,
                value_type = excluded.value_type,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at
            """,
            (namespace, key, encoded, value_type, _now(), expires_at),
        )
        con.commit()
        return {"namespace": namespace, "key": key, "set": True, "expires_at": expires_at}
    finally:
        con.close()


def key_delete(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    namespace, key = _ns_key(context)
    if context.get("dry_run"):
        return {"dry_run": True, "namespace": namespace, "key": key, "would_delete": True}
    cfg = _kv_cfg(context)
    con = _connect(cfg["path"])
    try:
        cur = con.execute("DELETE FROM kv WHERE namespace = ? AND key = ?", (namespace, key))
        con.commit()
        return {"namespace": namespace, "key": key, "deleted": cur.rowcount > 0}
    finally:
        con.close()


def _ns_key(context: dict[str, Any]) -> tuple[str, str]:
    params = context.get("params") or {}
    namespace = str(params.get("namespace") or "default").strip()
    key = str(params.get("key") or "").strip()
    if not key:
        raise ValueError("URI key param is required")
    return namespace, key
```

```python markpact:module path=urikv/log_handlers.py
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def _log_cfg(context: dict[str, Any]) -> dict[str, Any]:
    cfg = dict((context.get("config") or {}).get("log") or {})
    data_root = Path(os.environ.get("URISYS_NODE_DATA", "data")).expanduser()
    default_events = Path(os.environ.get("URISYS_NODE_EVENTS", str(data_root / "events.jsonl"))).expanduser()
    streams = dict(cfg.get("streams") or {})
    streams.setdefault("events", str(default_events))
    streams.setdefault("node", os.environ.get("URISYS_NODE_LOG", "/tmp/urisys-node.log"))
    cfg["streams"] = {k: str(Path(v).expanduser()) for k, v in streams.items()}
    cfg["data_root"] = str(data_root)
    return cfg


def _parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if value.endswith(("m", "h", "d")):
        unit = value[-1]
        num = float(value[:-1])
        delta = {"m": timedelta(minutes=num), "h": timedelta(hours=num), "d": timedelta(days=num)}[unit]
        return datetime.now(timezone.utc) - delta
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _event_ts(event: dict[str, Any]) -> datetime | None:
    ms = event.get("occurred_at_unix_ms")
    if isinstance(ms, (int, float)):
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    raw = event.get("occurred_at") or event.get("timestamp")
    if isinstance(raw, str):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def log_health(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    del payload
    cfg = _log_cfg(context)
    streams = {}
    for name, path in cfg["streams"].items():
        p = Path(path)
        streams[name] = {"path": path, "exists": p.exists(), "bytes": p.stat().st_size if p.exists() else 0}
    return {"ok": True, "scheme": "log", "streams": streams, "data_root": cfg["data_root"]}


def log_streams(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    out = log_health(payload, context)
    return {"streams": out["streams"]}


def _read_jsonl(path: Path, *, grep: str | None, since: datetime | None, limit: int, tail: bool) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    pattern = re.compile(grep, re.I) if grep else None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        if pattern and not pattern.search(line):
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            event = {"raw": line}
        if since is not None:
            ts = _event_ts(event)
            if ts is not None and ts < since:
                continue
        rows.append(event)
    if tail and limit > 0:
        rows = rows[-limit:]
    elif limit > 0:
        rows = rows[:limit]
    return rows


def _read_text(path: Path, *, grep: str | None, limit: int, tail: bool) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    if grep:
        pattern = re.compile(grep, re.I)
        lines = [ln for ln in lines if pattern.search(ln)]
    if tail and limit > 0:
        lines = lines[-limit:]
    elif limit > 0:
        lines = lines[:limit]
    return [{"line": ln, "index": i} for i, ln in enumerate(lines)]


def log_read(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    cfg = _log_cfg(context)
    params = context.get("params") or {}
    stream = str(params.get("stream") or payload.get("stream") or "events").strip()
    grep = payload.get("grep") or payload.get("q")
    since = _parse_since(str(payload.get("since") or "") or None)
    limit = int(payload.get("limit") or 50)
    tail = bool(payload.get("tail", True))

    if stream == "file":
        rel = str(params.get("path") or payload.get("path") or "").strip()
        if not rel:
            raise ValueError("log file path is required for stream=file")
        path = Path(rel).expanduser()
        if not path.is_absolute():
            path = Path(cfg["data_root"]) / rel
        if not path.exists():
            return {"stream": stream, "path": str(path), "entries": [], "exists": False}
        if path.suffix == ".jsonl":
            entries = _read_jsonl(path, grep=str(grep) if grep else None, since=since, limit=limit, tail=tail)
        else:
            entries = _read_text(path, grep=str(grep) if grep else None, limit=limit, tail=tail)
        return {"stream": stream, "path": str(path), "exists": True, "count": len(entries), "entries": entries}

    path = Path(cfg["streams"].get(stream, cfg["streams"]["events"]))
    if path.suffix == ".jsonl":
        entries = _read_jsonl(path, grep=str(grep) if grep else None, since=since, limit=limit, tail=tail)
    else:
        entries = _read_text(path, grep=str(grep) if grep else None, limit=limit, tail=tail)
    return {"stream": stream, "path": str(path), "exists": path.exists(), "count": len(entries), "entries": entries}


def log_summarize(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    payload = dict(payload)
    payload.setdefault("limit", 200)
    payload.setdefault("tail", True)
    out = log_read(payload, context)
    entries = out.get("entries") or []
    by_operation: dict[str, int] = {}
    by_ok: dict[str, int] = {"true": 0, "false": 0, "unknown": 0}
    for event in entries:
        if not isinstance(event, dict):
            continue
        op = str(event.get("operation") or event.get("event_type") or "unknown")
        by_operation[op] = by_operation.get(op, 0) + 1
        result = event.get("result")
        if isinstance(result, dict) and "ok" in result:
            key = "true" if result.get("ok") else "false"
            by_ok[key] = by_ok.get(key, 0) + 1
        else:
            by_ok["unknown"] += 1
    return {
        "stream": out.get("stream"),
        "path": out.get("path"),
        "matched": len(entries),
        "operations": by_operation,
        "ok_counts": by_ok,
    }
```

```python markpact:module path=urikv/routes.py
from __future__ import annotations

from importlib.resources import files

from urisysedge.manifest import register_manifest_file


def manifest_path():
    return files(__package__).joinpath("manifest.yaml")


def register(runtime):
    register_manifest_file(runtime, manifest_path())
```

```markdown markpact:docs
# urikv

`kv://` and `log://` capability packs for **urisys-node** — shared state and system introspection.

## kv://

| URI | Opis |
|-----|------|
| `kv://runtime/query/health` | driver, path, liczba kluczy |
| `kv://runtime/query/list` | lista kluczy (`payload.namespace`, `prefix`, `include_values`) |
| `kv://runtime/query/discover` | snapshot: node identity, packi, pliki w `URISYS_NODE_DATA`, stan runtime |
| `kv://{ns}/key/{key}/query/get` | odczyt wartości |
| `kv://{ns}/key/{key}/command/set` | zapis (`payload.value`, opcjonalnie `ttl`) |

Backend: SQLite (`config.kv.path` lub `URISYS_KV_PATH` / `URISYS_NODE_DATA/store.db`).

## log://

| URI | Opis |
|-----|------|
| `log://runtime/query/health` | dostępne strumienie logów |
| `log://runtime/query/streams` | mapa ścieżek |
| `log://events/query/read` | tail `events.jsonl` (`grep`, `since`, `limit`, `tail`) |
| `log://events/query/summarize` | agregacja operacji / ok |
| `log://file/{path}/query/read` | plik względem `URISYS_NODE_DATA` |

## Profil node

~~~json
{
  "kv": {"driver": "sqlite", "path": "~/.local/share/urisys/store.db"},
  "log": {
    "streams": {
      "events": "~/.local/share/urisys/events.jsonl",
      "node": "/tmp/urisys-node.log"
    }
  }
}
~~~
```

