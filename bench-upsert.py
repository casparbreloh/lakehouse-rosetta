from __future__ import annotations

import importlib.util
import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent
DATA = REPO / "data"
WAREHOUSE = REPO / "warehouse"

SHAPES = {
    "small":  [("many", 10, 10), ("even", 6, 30),  ("few", 4, 80)],
    "medium": [("many", 12, 30), ("even", 8, 100), ("few", 5, 300)],
    "large":  [("many", 15, 50), ("even", 10, 150), ("few", 5, 400)],
}

FORMATS = ["iceberg", "delta", "lance"]

PROC = psutil.Process()


def rss_mb() -> float:
    return PROC.memory_info().rss / 1024 / 1024


def percentile(values_ms: list[float], p: float) -> float:
    if not values_ms:
        return 0.0
    s = sorted(values_ms)
    k = int(round((p / 100) * (len(s) - 1)))
    return s[k]


def shape_batches(stream, shapes):
    offset = 0
    for _name, n_batches, rows_per in shapes:
        for i in range(n_batches):
            yield stream.slice(offset + i * rows_per, rows_per)
        offset += n_batches * rows_per


def fresh_warehouse(name: str) -> Path:
    wh = WAREHOUSE / name
    if wh.exists():
        shutil.rmtree(wh)
    wh.mkdir(parents=True)
    return wh


def report(scenario: str, total_s: float, batch_ms: list[float], peak_mb: float) -> None:
    mean = statistics.mean(batch_ms) if batch_ms else 0.0
    p99 = percentile(batch_ms, 99)
    print(
        f"{scenario:<18} total {total_s:.3f} s, {len(batch_ms)} batches, mean {mean:.1f} ms/batch, p99 {p99:.1f} ms/batch, peak {peak_mb:.0f} MB",
        flush=True,
    )


def run_one(fmt: str, impl_name: str, setup, upsert, base, stream, shapes) -> None:
    wh = fresh_warehouse(f"{fmt}-{impl_name}")
    handle = setup(str(wh), base)
    batch_ms: list[float] = []
    peak = rss_mb()
    t0 = time.perf_counter()
    for batch in shape_batches(stream, shapes):
        bt = time.perf_counter()
        upsert(handle, batch)
        batch_ms.append((time.perf_counter() - bt) * 1000)
        peak = max(peak, rss_mb())
    report(f"{fmt}:{impl_name}", time.perf_counter() - t0, batch_ms, peak)


def load_upsert(fmt: str):
    # friction: load each upsert.py as a flat module so `import lance` finds pylance, not our `lance/` dir.
    path = REPO / fmt / "upsert.py"
    spec = importlib.util.spec_from_file_location(f"_{fmt}_upsert", str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def run_child(size: str, scenario: str) -> None:
    fmt, impl_name = scenario.split(":")
    base = pq.read_table(DATA / f"base_{size}.parquet")
    stream = pq.read_table(DATA / f"stream_{size}.parquet")
    module = load_upsert(fmt)
    for name, setup, upsert in module.IMPLS:
        if name == impl_name:
            run_one(fmt, name, setup, upsert, base, stream, SHAPES[size])
            return
    print(f"unknown scenario {scenario}", file=sys.stderr)
    sys.exit(2)


def ensure_seed(size: str) -> None:
    base = DATA / f"base_{size}.parquet"
    stream = DATA / f"stream_{size}.parquet"
    if base.exists() and stream.exists():
        return
    spec = importlib.util.spec_from_file_location("_seed", str(DATA / "seed.py"))
    seed_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(seed_mod)
    seed_mod.main()


def main() -> None:
    size = os.environ.get("BENCH_SIZE")
    if size not in SHAPES:
        print(f"BENCH_SIZE must be one of {list(SHAPES)}", file=sys.stderr)
        sys.exit(2)

    scenario = os.environ.get("BENCH_SCENARIO")
    if scenario:
        run_child(size, scenario)
        return

    ensure_seed(size)
    for fmt in FORMATS:
        module = load_upsert(fmt)
        for impl_name, _, _ in module.IMPLS:
            subprocess.run(
                [sys.executable, __file__],
                env={**os.environ, "BENCH_SCENARIO": f"{fmt}:{impl_name}"},
            )


if __name__ == "__main__":
    main()
