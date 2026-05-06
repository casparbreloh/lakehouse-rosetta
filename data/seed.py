from __future__ import annotations

import datetime as dt
import hashlib
import os
import random
import string
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent

SIZES = {
    "small":  {"base_rows":    100_000},
    "medium": {"base_rows":  1_000_000},
    "large":  {"base_rows": 10_000_000},
}

SHAPES = {
    "small":  [("many", 10, 10), ("even", 6, 30),  ("few", 4, 80)],
    "medium": [("many", 12, 30), ("even", 8, 100), ("few", 5, 300)],
    "large":  [("many", 15, 50), ("even", 10, 150), ("few", 5, 400)],
}

CHUNK_ROWS = 100_000

EPOCH = dt.datetime(2026, 1, 1)

SCHEMA = pa.schema(
    [
        pa.field("id", pa.string(), nullable=False),
        pa.field("source_path", pa.string(), nullable=False),
        pa.field("content_hash", pa.string(), nullable=False),
        pa.field("size_bytes", pa.int64(), nullable=False),
        pa.field("updated_at", pa.timestamp("us"), nullable=False),
        pa.field("payload", pa.string(), nullable=False),
    ]
)


def _payload(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_letters + string.digits, k=64))


def _row(i: int, rng: random.Random, ts_offset: int):
    return (
        f"id_{i:09d}",
        f"/data/shard_{i % 1024:04d}/file_{i:09d}.bin",
        hashlib.sha256(f"{i}-{rng.random()}".encode()).hexdigest(),
        rng.randint(1024, 16 * 1024 * 1024),
        EPOCH + dt.timedelta(seconds=ts_offset + i),
        _payload(rng),
    )


def _table(indices, rng: random.Random, ts_offset: int) -> pa.Table:
    cols: list[list] = [[], [], [], [], [], []]
    for i in indices:
        for c, v in zip(cols, _row(i, rng, ts_offset)):
            c.append(v)
    return pa.Table.from_arrays(
        [
            pa.array(cols[0], type=pa.string()),
            pa.array(cols[1], type=pa.string()),
            pa.array(cols[2], type=pa.string()),
            pa.array(cols[3], type=pa.int64()),
            pa.array(cols[4], type=pa.timestamp("us")),
            pa.array(cols[5], type=pa.string()),
        ],
        schema=SCHEMA,
    )


def _write_base(path: Path, rows: int, rng: random.Random) -> None:
    with pq.ParquetWriter(path, SCHEMA, compression="zstd") as writer:
        for start in range(0, rows, CHUNK_ROWS):
            end = min(start + CHUNK_ROWS, rows)
            writer.write_table(_table(range(start, end), rng, 0))


def main() -> None:
    size = os.environ.get("BENCH_SIZE")
    if size not in SIZES:
        print(f"BENCH_SIZE must be one of {list(SIZES)}", file=sys.stderr)
        sys.exit(2)

    base_path = HERE / f"base_{size}.parquet"
    stream_path = HERE / f"stream_{size}.parquet"
    if base_path.exists() and stream_path.exists():
        print(f"seed[{size}]: {base_path.name} + {stream_path.name} present, skipping")
        return

    rng = random.Random(42)
    base_rows = SIZES[size]["base_rows"]

    print(f"seed[{size}]: writing {base_path.name} ({base_rows:,} rows)")
    _write_base(base_path, base_rows, rng)

    all_indices: list[int] = []
    insert_cursor = base_rows
    for (_name, n_batches, batch_size) in SHAPES[size]:
        shape_total = n_batches * batch_size
        n_upd = int(shape_total * 0.7)
        n_ins = shape_total - n_upd
        upd = rng.sample(range(base_rows), n_upd)
        ins = list(range(insert_cursor, insert_cursor + n_ins))
        insert_cursor += n_ins
        slice_ = upd + ins
        rng.shuffle(slice_)
        all_indices.extend(slice_)

    print(f"seed[{size}]: writing {stream_path.name} ({len(all_indices):,} rows across {len(SHAPES[size])} shapes)")
    pq.write_table(_table(all_indices, rng, 10_000_000), stream_path, compression="zstd")
    print(f"seed[{size}]: done")


if __name__ == "__main__":
    main()
