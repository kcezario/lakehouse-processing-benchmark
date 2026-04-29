import uuid
import time

from app.config import AppConfig
from app.logger import finalize_log_upload, get_logger
from app.metrics.execution_metrics import ExecutionMetrics

from app.s3_client import create_s3_client
from app.ingestion.incremental_writer import ingest_incremental

from app.trino.trino_client import (
    create_connection,
    ensure_schema_and_tables,
    collect_final_metrics,
)

from app.transformation.factory import get_transformer
from app.cleanup.staging_cleaner import clean_files


def main():
    cfg = AppConfig()
    logger = get_logger(cfg.log_level, cfg)

    try:
        metrics = ExecutionMetrics()

        batch_id = str(uuid.uuid4())

        logger.info(f"Pipeline started | batch_id={batch_id}")

        # =====================================================
        # ENV LOAD TIME
        # =====================================================
        env_start = time.time()

        s3 = create_s3_client(cfg)
        conn = create_connection(cfg)

        env_time = time.time() - env_start
        metrics.set_env_load_time(env_time)

        # =====================================================
        # INGESTION
        # =====================================================
        logger.info("START: incremental_ingestion")

        staging_files = ingest_incremental(
            cfg=cfg,
            batch_id=batch_id,
            logger=logger,
            metrics=metrics,
        )

        logger.info("END: incremental_ingestion")

        # =====================================================
        # TRINO SETUP
        # =====================================================
        ensure_schema_and_tables(conn, cfg, logger, metrics)

        # =====================================================
        # TRANSFORMATION (SELECTED ENGINE)
        # =====================================================
        logger.info(f"START: transformation ({cfg.transformation_engine})")

        transformer = get_transformer(cfg.transformation_engine)

        result = transformer.run(
            conn=conn,
            s3=s3,
            cfg=cfg,
            staging_files=staging_files,
            batch_id=batch_id,
            logger=logger,
            metrics=metrics,
        )

        logger.info(f"END: transformation ({cfg.transformation_engine})")

        if result.status != "success":
            raise RuntimeError(f"Transformation failed: {result.error}")

        # =====================================================
        # FINAL VALIDATION (ALWAYS THROUGH TRINO)
        # =====================================================
        summary = collect_final_metrics(conn, cfg, batch_id, logger)

        # =====================================================
        # RAW LAYER CLEANUP
        # =====================================================
        logger.info("START: raw_layer_cleanup")

        clean_files(
            s3=s3,
            bucket=cfg.bucket_name,
            keys=staging_files,
            logger=logger,
        )

        logger.info("END: raw_layer_cleanup")

        # =====================================================
        # METRICS
        # =====================================================
        metrics.log_summary(logger)

        logger.info("Pipeline finished successfully")
    finally:
        finalize_log_upload(cfg)


if __name__ == "__main__":
    main()