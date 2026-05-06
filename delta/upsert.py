from __future__ import annotations

import dlt
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from dlt.destinations import filesystem


# --- direct ---
def setup_direct(warehouse: str, base: pa.Table):
    path = f"{warehouse}/records"
    write_deltalake(path, base)
    return DeltaTable(path)


def upsert_direct(table: DeltaTable, batch: pa.Table) -> None:
    (
        table.merge(
            source=batch,
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


# --- dlt ---
def setup_dlt(warehouse: str, base: pa.Table):
    pipeline = dlt.pipeline(
        pipeline_name="bench",
        destination=filesystem(bucket_url=f"file://{warehouse}"),
        dataset_name="bench",
        pipelines_dir=f"{warehouse}/_pipelines",
    )
    pipeline.run(
        base,
        table_name="records",
        primary_key="id",
        table_format="delta",
        write_disposition="append",
    )
    return pipeline


def upsert_dlt(pipeline, batch: pa.Table) -> None:
    pipeline.run(
        batch,
        table_name="records",
        primary_key="id",
        table_format="delta",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )


IMPLS = [
    ("direct", setup_direct, upsert_direct),
    ("dlt",    setup_dlt,    upsert_dlt),
]
