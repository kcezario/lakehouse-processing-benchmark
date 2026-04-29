import uuid

import pandas as pd

from app.data_generator import generate_data
from app.log_formatting import format_elapsed_sec, wall_clock_start
from app.s3_client import upload_parquet

_STAGING_META_COLS = ("_batch_id", "_ingested_at")


def ingest_incremental(
    cfg,
    batch_id,
    logger=None,
    metrics=None,
    rows=1000,
    batches=5,
    total_rows=None,
):
    """
    Gera dados incrementais e salva no staging (S3/MinIO)

    Retorna:
        list[str] -> lista de arquivos staging gerados
    """
    if total_rows is not None:
        batches = max(1, int(batches))
        total_rows = max(0, int(total_rows))
        base_rows = total_rows // batches
        remainder = total_rows % batches
        batch_sizes = [base_rows + (1 if i < remainder else 0) for i in range(batches)]
    else:
        batch_sizes = [max(0, int(rows)) for _ in range(max(1, int(batches)))]

    staging_files = []

    t0 = wall_clock_start()
    if logger:
        logger.info("Starting raw-layer file generation")

    for i, batch_rows in enumerate(batch_sizes):
        if logger:
            logger.debug(f"START: batch_{i}")

        data = generate_data(batch_rows)
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        # Required metadata for Spark/Trino in raw-layer files (do not remove).
        data["_batch_id"] = batch_id
        data["_ingested_at"] = pd.Timestamp.utcnow()

        missing = [c for c in _STAGING_META_COLS if c not in data.columns]
        if missing:
            raise ValueError(
                f"Invalid raw-layer file: missing required columns {missing} "
                f"(presentes: {list(data.columns)})"
            )

        if logger:
            logger.debug(f"[STAGING] columns={list(data.columns)}")
            if len(data) > 0:
                logger.debug(
                    f"[STAGING] sample_row={data.iloc[0].to_dict()}"
                )
            else:
                logger.debug("[RAW_LAYER] sample_row=(no rows in this batch)")

        file_id = str(uuid.uuid4())
        date_part = f"date={pd.Timestamp.utcnow().date()}"
        file_part = f"{file_id}.parquet"
        root = (cfg.s3_staging_key_root or "").strip().strip("/")
        key_segments = [s for s in (root, cfg.staging_prefix, date_part, file_part) if s]
        key = "/".join(key_segments)

        if logger:
            logger.debug(
                f"[STAGING] destino s3://{cfg.bucket_name}/{key} "
                f"(bucket={cfg.bucket_name}; relative key)"
            )

        upload_parquet(
            cfg=cfg,
            key=key,
            data=data,
            logger=logger,
            metrics=metrics,
        )

        if logger:
            logger.debug(f"Arquivo staging gerado: {key}")
            logger.debug(f"END: batch_{i}")

        staging_files.append(key)

    if logger:
        logger.info(
            f"Raw-layer generation completed | files_created={len(staging_files)} "
            f"{format_elapsed_sec(t0)}"
        )

    return staging_files