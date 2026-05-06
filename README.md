# lakehouse-rosetta

Same upsert in **Iceberg**, **Delta**, and **Lance** — read side-by-side. Sibling to [icelab](../icelab), which pins format and varies language; this one flips it.

## Layout

```
data/seed.py               — base + stream parquet (ported verbatim from icelab perth-v1)
iceberg/upsert.py          — pyiceberg direct + DLT
delta/upsert.py            — deltalake direct + DLT
lance/upsert.py            — pylance direct + DLT lancedb
{iceberg,delta,lance}/multimodal.py  — TODO stubs
bench-upsert.py            — runs (format × impl) = 6 scenarios, reports time + peak RSS
```

Each `upsert.py` exposes the same two-function contract:

```python
def setup_direct(warehouse: str, base: pa.Table): ...
def upsert_direct(handle, batch: pa.Table) -> None: ...

def setup_dlt(warehouse: str, base: pa.Table): ...
def upsert_dlt(handle, batch: pa.Table) -> None: ...

IMPLS = [("direct", setup_direct, upsert_direct),
         ("dlt",    setup_dlt,    upsert_dlt)]
```

`handle` is whatever the path returns — a pyiceberg `Table`, a `DeltaTable`, a Lance dataset URI, or a DLT pipeline. No wrapping.

## Run

```bash
uv sync

BENCH_SIZE=small  uv run bench-upsert.py
BENCH_SIZE=medium uv run bench-upsert.py
BENCH_SIZE=large  uv run bench-upsert.py
```

The bench seeds `data/base_<size>.parquet` + `data/stream_<size>.parquet` on first run for that size and skips re-seeding thereafter. To regenerate, delete the parquet files (or run `BENCH_SIZE=<size> uv run data/seed.py` directly).

| Preset | Base rows  | Workload (batches × rows)               | Total batches / events |
|--------|------------|------------------------------------------|------------------------|
| small  |    100,000 | 10×10 + 6×30 + 4×80                      | 20 batches,   600      |
| medium |  1,000,000 | 12×30 + 8×100 + 5×300                    | 25 batches, 2,660      |
| large  | 10,000,000 | 15×50 + 10×150 + 5×400                   | 30 batches, 4,250      |

Output (one line per scenario):

```
iceberg:direct     total X.XXX s, N batches, mean Y.Y ms/batch, p99 Z.Z ms/batch, peak AAA MB
iceberg:dlt        ...
delta:direct       ...
delta:dlt          ...
lance:direct       ...
lance:dlt          ...
```

Run at multiple sizes and compare `peak MB` per scenario — flat-with-size means rowid-addressed updates; growing-with-size means copy-on-write rewriting matched files.

## Workload

70% updates / 30% inserts. Three batch shapes per run (many-small, even, few-large). 6-column schema (`id, source_path, content_hash, size_bytes, updated_at, payload`). See `data/seed.py`.

## Why peak RSS, not Δrss

Six scenarios share one process, so RSS is roughly monotonic across them and per-scenario Δrss is misleading. Sampling `psutil.Process().memory_info().rss` after every batch and tracking the max gives a per-scenario peak that doesn't bleed across runs.

## Library pins

```
pyiceberg[sql-sqlite] >= 0.11
deltalake             >= 0.20
pylance               >= 0.20
dlt[pyiceberg,deltalake,lancedb] >= 1.12
pyarrow               >= 18
psutil                >= 5.9
```

## Notes

- SQLite catalogs and local file warehouses. No Docker, no REST, no object store.
- Lance has no catalog; `setup` just returns a dataset URI. That asymmetry is part of the comparison — left visible.
- `pylance` imports as `lance` and would collide with this repo's `lance/` package; the bench loads each `upsert.py` as a flat module (no `__init__.py`) so `import lance` resolves to pylance. Friction noted in `bench-upsert.py:load_upsert`.
- `dlt-lancedb` requires a `_dlt_load_id` column for merge-keyed tables. Toggled via env var at the top of `lance/upsert.py`.
