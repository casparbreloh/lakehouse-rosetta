from __future__ import annotations

import os

# friction: dlt-lancedb requires `_dlt_load_id` on merge-keyed tables; this env var asks dlt to add it.
os.environ.setdefault("NORMALIZE__PARQUET_NORMALIZER__ADD_DLT_LOAD_ID", "TRUE")

import dlt
import lance
import pyarrow as pa
from dlt.destinations import lancedb


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
        destination=lancedb(lance_uri=warehouse),
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
        batch,
        table_name="records",
        primary_key="id",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )


IMPLS = [
    ("direct", setup_direct, upsert_direct),
    ("dlt",    setup_dlt,    upsert_dlt),
]
