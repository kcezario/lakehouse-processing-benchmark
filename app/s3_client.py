import io
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import boto3
import pandas as pd
import pyarrow.parquet as pq


def create_s3_client(cfg):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint,
        aws_access_key_id=cfg.s3_access_key,
        aws_secret_access_key=cfg.s3_secret_key,
        region_name=cfg.s3_region,
    )


def upload_parquet(
    cfg,
    key: str,
    data,
    logger=None,
    metrics=None,
):
    """
    Faz upload de um DataFrame/lista como parquet para S3/MinIO
    """

    start = time.time()

    # garante DataFrame
    if not isinstance(data, pd.DataFrame):
        df = pd.DataFrame(data)
    else:
        df = data

    # in-memory buffer (stable pyarrow behavior for column names)
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Validates serialized Parquet columns (not only in-memory DataFrame columns).
    table_on_disk = pq.read_table(buffer)
    parquet_columns = list(table_on_disk.column_names)
    buffer.seek(0)

    required_meta = ("_batch_id", "_ingested_at")
    missing_parquet = [c for c in required_meta if c not in parquet_columns]
    if missing_parquet:
        raise ValueError(
            f"Parquet missing required columns after to_parquet: {missing_parquet} | "
            f"colunas_no_arquivo={parquet_columns} | colunas_no_df={list(df.columns)}"
        )

    if logger:
        logger.debug(
            f"[PARQUET VERIFY] uri=s3://{cfg.bucket_name}/{key} | "
            f"schema_columns={parquet_columns} | rows={len(df)}"
        )

    client = create_s3_client(cfg)

    client.put_object(
        Bucket=cfg.bucket_name,
        Key=key,
        Body=buffer.getvalue(),
    )

    elapsed = time.time() - start

    if metrics:
        metrics.add_s3_time(elapsed)

    if logger:
        logger.debug(f"Parquet upload completed | key={key} | rows={len(df)}")


def upload_log_file(cfg, local_path: str, logger=None) -> str:
    """
    Uploads a log file to s3_logs_prefix/{date}/{timestamp}_{id}_{name}.
    Returns the created S3 key.
    """
    dt = datetime.now(timezone.utc)
    date_folder = dt.strftime("%Y-%m-%d")
    stamp = dt.strftime("%Y%m%dT%H%M%SZ")
    short = uuid4().hex[:8]
    base = Path(cfg.log_file_name or "logs.log").name
    key = f"{cfg.s3_logs_prefix}/{date_folder}/{stamp}_{short}_{base}"

    client = create_s3_client(cfg)
    with open(local_path, "rb") as f:
        body = f.read()

    client.put_object(
        Bucket=cfg.bucket_name,
        Key=key,
        Body=body,
        ContentType="text/plain; charset=utf-8",
    )

    if logger:
        logger.info(
            f"Log enviado ao MinIO | s3://{cfg.bucket_name}/{key}"
        )

    return key