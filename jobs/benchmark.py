import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import uuid
import time

from app.config import AppConfig
from app.logger import finalize_log_upload, get_logger
from app.metrics.execution_metrics import ExecutionMetrics

from app.s3_client import create_s3_client
from app.ingestion.incremental_writer import ingest_incremental
from app.cleanup.staging_cleaner import clean_files

from app.trino.trino_client import (
    create_connection,
    ensure_schema_and_tables,
    collect_final_metrics,
)

from app.transformation.factory import get_transformer
from app.benchmark.timeout import timeout, BenchmarkTimeoutError


def run_single_benchmark(cfg, engine, rows, logger):
    logger.info("===================================================")
    logger.info(f"BENCHMARK START | engine={engine} | rows={rows}")

    batch_id = str(uuid.uuid4())
    metrics = ExecutionMetrics()

    s3 = create_s3_client(cfg)
    conn = create_connection(cfg)

    ensure_schema_and_tables(conn, cfg, logger, metrics)

    # =====================================================
    # INGEST (generate raw-layer files)
    # =====================================================
    staging_files = ingest_incremental(
        cfg=cfg,
        batch_id=batch_id,
        logger=logger,
        metrics=metrics,
        total_rows=rows,
    )

    # =====================================================
    # TRANSFORMATION
    # =====================================================
    transformer = get_transformer(engine)

    start = time.time()

    try:
        if engine == "spark":
            result = transformer.run(
                conn=conn,
                s3=s3,
                cfg=cfg,
                staging_files=staging_files,
                batch_id=batch_id,
                expected_rows=rows,
                logger=logger,
                metrics=metrics,
            )
        else:
            with timeout(cfg.benchmark_timeout_seconds):
                result = transformer.run(
                    conn=conn,
                    s3=s3,
                    cfg=cfg,
                    staging_files=staging_files,
                    batch_id=batch_id,
                    logger=logger,
                    metrics=metrics,
                )

        elapsed = time.time() - start

        summary = collect_final_metrics(conn, cfg, batch_id, logger)

        status = "SUCCESS"
        if engine == "spark":
            inserted = summary.get("rows", 0)
            if inserted < rows:
                status = "PARTIAL"
                logger.warning(
                    f"Spark: rows in final table ({inserted}) < expected ({rows}). "
                    f"SPARK_POLL_TIMEOUT_SECONDS={cfg.spark_poll_timeout_seconds}s — "
                    "increase this if the K8s driver is slower; also inspect "
                    "Iceberg INSERT logs in the Spark pod."
                )

    except BenchmarkTimeoutError:
        elapsed = time.time() - start
        status = "TIMEOUT"
        summary = {"rows": 0}

        logger.warning(f"TIMEOUT | engine={engine} | rows={rows}")

    except Exception as e:
        elapsed = time.time() - start
        status = "ERROR"
        summary = {"rows": 0}

        logger.error(f"ERROR | engine={engine} | {str(e)}")

    # =====================================================
    # CLEANUP
    # =====================================================
    clean_files(
        s3=s3,
        bucket=cfg.bucket_name,
        keys=staging_files,
        logger=logger,
    )

    return {
        "engine": engine,
        "rows": rows,
        "status": status,
        "time": round(elapsed, 2),
        "rows_written": summary.get("rows", 0),
    }


def main():
    cfg = AppConfig()
    logger = get_logger(cfg.log_level, cfg)

    try:
        if not cfg.benchmark_enabled:
            logger.warning("Benchmark disabled (BENCHMARK_ENABLED=false)")
            return

        results = []

        logger.info("STARTING BENCHMARK")

        for rows in cfg.benchmark_rows:
            for engine in cfg.benchmark_engines:
                result = run_single_benchmark(cfg, engine, rows, logger)
                results.append(result)

        # =====================================================
        # FINAL SUMMARY
        # =====================================================
        logger.info("===================================================")
        logger.info("BENCHMARK FINAL RESULT")

        for r in results:
            logger.info(
                f"{r['engine']} | rows={r['rows']} | "
                f"time={r['time']}s | status={r['status']} | "
                f"written={r['rows_written']}"
            )
    finally:
        finalize_log_upload(cfg)


if __name__ == "__main__":
    main()