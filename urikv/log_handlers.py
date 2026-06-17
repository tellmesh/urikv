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
