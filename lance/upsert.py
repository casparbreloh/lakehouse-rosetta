from __future__ import annotations

import os

# lance Rust crate logs WARN once per table-existence probe during dlt's create-if-not-exists.
# Harmless (probes fail because tables don't exist yet) but spammy. Suppress via Rust log filter.
os.environ.setdefault("LANCE_LOG", "error")

import dlt
import lance
import pyarrow as pa
from dlt.destinations import lance as lance_dest
from dlt.destinations.adapters import lance_adapter


# --- direct ---
def setup_direct(warehouse: str, base: pa.Table):
    uri = f"{warehouse}/records.lance"
    lance.write_dataset(base, uri)
    return uri


def upsert_direct(uri: str, batch: pa.Table) -> None:
    ds = lance.dataset(uri)
    (
        ds.merge_insert("id")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute(batch)
    )


# --- dlt ---
def setup_dlt(warehouse: str, base: pa.Table):
    pipeline = dlt.pipeline(
        pipeline_name="bench",
        destination=lance_dest(storage={"bucket_url": warehouse}),
        dataset_name="bench",
        pipelines_dir=f"{warehouse}/_pipelines",
    )
    pipeline.run(
        base,
        table_name="records",
        primary_key="id",
        write_disposition="append",
    )
    return pipeline


def upsert_dlt(pipeline, batch: pa.Table) -> None:
    pipeline.run(
        lance_adapter(batch, merge_key="id"),
        table_name="records",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )


IMPLS = [
    ("direct", setup_direct, upsert_direct),
    ("dlt",    setup_dlt,    upsert_dlt),
]
