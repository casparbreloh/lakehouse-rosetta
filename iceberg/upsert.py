from __future__ import annotations

import dlt
import pyarrow as pa
from dlt.destinations import filesystem
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestampType

ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", StringType(), required=True),
    NestedField(2, "source_path", StringType(), required=True),
    NestedField(3, "content_hash", StringType(), required=True),
    NestedField(4, "size_bytes", LongType(), required=True),
    NestedField(5, "updated_at", TimestampType(), required=True),
    NestedField(6, "payload", StringType(), required=True),
    identifier_field_ids=[1],
)


# --- direct ---
def setup_direct(warehouse: str, base: pa.Table):
    catalog = SqlCatalog(
        "bench",
        uri=f"sqlite:///{warehouse}/catalog.db",
        warehouse=f"file://{warehouse}",
    )
    catalog.create_namespace("bench")
    table = catalog.create_table("bench.records", schema=ICEBERG_SCHEMA)
    table.append(base)
    return table


def upsert_direct(table, batch: pa.Table) -> None:
    table.upsert(batch)


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
        table_format="iceberg",
        write_disposition="append",
    )
    return pipeline


def upsert_dlt(pipeline, batch: pa.Table) -> None:
    pipeline.run(
        batch,
        table_name="records",
        primary_key="id",
        table_format="iceberg",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
    )


IMPLS = [
    ("direct", setup_direct, upsert_direct),
    ("dlt",    setup_dlt,    upsert_dlt),
]
