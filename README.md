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

```json
{
  "kv": {"driver": "sqlite", "path": "~/.local/share/urisys/store.db"},
  "log": {
    "streams": {
      "events": "~/.local/share/urisys/events.jsonl",
      "node": "/tmp/urisys-node.log"
    }
  }
}
```

## Ekosystem TellMesh

Orchestrator: **[urisys](https://github.com/tellmesh/urisys)** · Mapa: **[MESH.md](https://github.com/tellmesh/urisys/blob/main/docs/MESH.md)** · Model: **[ECOSYSTEM.md](https://github.com/tellmesh/urisys/blob/main/../docs/ECOSYSTEM.md)**

| Pole | Wartość |
|------|---------|
| **Warstwa** | Capability pack |
| **Scheme** | `kv://` / `log://` |
| **Zależność** | `uricore>=0.1.8` |

Runtime edge: **`uri_control.edge`** w pakiecie **`uricore`** (legacy `urisysedge` usunięty 2026-06).
Router intencji: **`urirouter`** (`uri_router`) — resolve + HTTP/MQTT delegate.

<!-- end-ecosystem -->
