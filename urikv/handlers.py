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
