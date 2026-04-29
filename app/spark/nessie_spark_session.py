"""Registers Iceberg (Nessie) catalog in Spark jobs for `nessie.schema.table`.

In interactive environments, catalog/FileIO may already be set in the cluster.
In remote submit flows, the driver usually does not inherit those settings.

For `s3a://` warehouses, many Spark images do not include AWS SDK jars required
by S3FileIO, so HadoopFileIO is used as a safe default.

Some Iceberg metadata created by other engines can reference `s3://`; mapping
`fs.s3.impl` to `S3AFileSystem` keeps interoperability with Spark Hadoop IO.
"""

from __future__ import annotations

from app.config import AppConfig


def pyspark_nessie_session_snippet(cfg: AppConfig) -> str:
    cat = cfg.spark_iceberg_catalog_name
    return f"""cat = {repr(cat)}
spark = (
    SparkSession.builder
    .config(
        "spark.sql.catalog." + cat,
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config(
        "spark.sql.catalog." + cat + ".catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    .config("spark.sql.catalog." + cat + ".uri", {repr(cfg.nessie_uri)})
    .config("spark.sql.catalog." + cat + ".ref", {repr(cfg.nessie_ref)})
    .config(
        "spark.sql.catalog." + cat + ".authentication.type",
        {repr(cfg.nessie_authentication_type)},
    )
    .config("spark.sql.catalog." + cat + ".warehouse", {repr(cfg.nessie_warehouse)})
    .config(
        "spark.sql.catalog." + cat + ".io-impl",
        {repr(cfg.nessie_io_impl)},
    )
    .config(
        "spark.hadoop.fs.s3.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )
    .config("spark.sql.defaultCatalog", cat)
    .getOrCreate()
)"""
