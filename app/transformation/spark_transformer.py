import json
import threading
import time

from app.log_formatting import format_elapsed_sec
from app.spark.nessie_spark_session import pyspark_nessie_session_snippet
from app.spark.spark_client import submit_spark_code
from app.transformation.base import BaseTransformer, TransformationResult
from app.trino.trino_client import run_query


def _flush_logger(logger):
    if not logger:
        return
    for h in getattr(logger, "handlers", ()):
        try:
            h.flush()
        except Exception:
            pass
    parent = getattr(logger, "parent", None)
    if parent:
        for h in getattr(parent, "handlers", ()):
            try:
                h.flush()
            except Exception:
                pass


def wait_for_spark_result(
    conn,
    cfg,
    batch_id,
    expected_rows,
    logger=None,
    submit_errors=None,
):
    full_table = f"{cfg.trino_catalog}.{cfg.trino_schema}.{cfg.final_table}"
    poll_t0 = time.perf_counter()
    timeout_seconds = max(1, int(cfg.spark_poll_timeout_seconds))
    interval_seconds = max(1, int(cfg.spark_poll_interval_seconds))
    target_rows = max(1, int(expected_rows or 1))
    last_count = 0
    tentativa = 0

    while True:
        if submit_errors:
            raise submit_errors[0]

        tentativa += 1
        count_result = run_query(
            conn,
            f"SELECT COUNT(*) FROM {full_table} WHERE batch_id='{batch_id}'",
        )
        last_count = count_result[0][0] if count_result else 0

        decorrido = time.perf_counter() - poll_t0
        if logger:
            logger.debug(
                f"[Spark Polling] tentativa={tentativa} | batch_id={batch_id} "
                f"| rows_found={last_count} | expected={target_rows} "
                f"| decorrido={decorrido:.1f}s"
            )
            _flush_logger(logger)

        if last_count >= target_rows:
            if logger:
                logger.info(
                    f"[Spark Polling] COMPLETED | rows={last_count} "
                    f"{format_elapsed_sec(poll_t0)}"
                )
            return last_count

        elapsed = time.perf_counter() - poll_t0
        if elapsed >= timeout_seconds:
            if logger:
                logger.warning(
                    f"[Spark Polling] TIMEOUT | rows={last_count} "
                    f"{format_elapsed_sec(poll_t0)} | "
                    f"limite={timeout_seconds}s (SPARK_POLL_TIMEOUT_SECONDS). "
                    f"POST /submit often returns before pod completion; increase "
                    f"polling se o job no Kubernetes demora mais."
                )
            return last_count

        time.sleep(interval_seconds)


class SparkTransformer(BaseTransformer):
    engine_name = "spark"

    def run(
        self,
        *,
        conn,
        s3,
        cfg,
        staging_files,
        batch_id,
        expected_rows=None,
        logger=None,
        metrics=None,
    ):
        del s3, metrics

        if not staging_files:
            raise ValueError(
                "SparkTransformer: empty staging_files; nothing to read from S3."
            )

        # URIs explicitos desta execucao. Ler o prefixo inteiro puxa Parquets antigos
        # (sem _batch_id/_ingested_at) e o Spark funde schema com particao `date=`.
        staging_uris = [
            f"s3a://{cfg.bucket_name}/{key.lstrip('/')}" for key in staging_files
        ]
        paths_literal = repr(staging_uris)

        if logger:
            logger.info(
                f"[Spark] leitura parquet deste batch | arquivos={len(staging_uris)}"
            )
            logger.debug(
                f"[Spark] paths={json.dumps(staging_uris)}"
            )

        if not cfg.nessie_uri:
            raise ValueError(
                "NESSIE_URI is not set; required to register Nessie catalog "
                "in Spark and use nessie.<schema>.<table>."
            )

        full_table_trino = (
            f"{cfg.trino_catalog}.{cfg.trino_schema}.{cfg.final_table}"
        )
        cat = cfg.spark_iceberg_catalog_name
        spark_iceberg_table = f"{cat}.{cfg.trino_schema}.{cfg.final_table}"

        spark_code = f"""
from pyspark.sql import SparkSession

{pyspark_nessie_session_snippet(cfg)}

staging_paths = {paths_literal}
df = spark.read.parquet(*staging_paths)

df = df.filter(df["_batch_id"] == "{batch_id}")

df = df.withColumn(
    "moeda_original",
    df["is_usd"].cast("string")
)

df = df.withColumn(
    "valor_em_real",
    df["amount"]
)

df.createOrReplaceTempView("v")

spark.sql(f'''
INSERT INTO {spark_iceberg_table}
SELECT
    name,
    amount,
    event_date,
    CASE WHEN is_usd THEN 'USD' ELSE 'BRL' END,
    CASE WHEN is_usd THEN amount * {cfg.exchange_rate_usd_brl} ELSE amount END,
    _ingested_at,
    _batch_id
FROM v
''')

count = spark.sql(
    f"SELECT count(*) as c FROM {spark_iceberg_table} "
    f"WHERE batch_id = '{batch_id}'"
).collect()[0]["c"]

print("ROWS_WRITTEN:", count)

spark.stop()
"""

        submit_errors: list[BaseException] = []
        http_response_logged = threading.Event()

        def _submit_job():
            try:
                submit_spark_code(
                    cfg=cfg,
                    name=cfg.spark_payload_name,
                    code=spark_code,
                    logger=logger,
                    response_logged_event=http_response_logged,
                )
            except BaseException as e:
                submit_errors.append(e)
            finally:
                if not http_response_logged.is_set():
                    http_response_logged.set()

        submit_thread = threading.Thread(
            target=_submit_job,
            name="spark-submit",
            daemon=True,
        )
        submit_thread.start()

        if logger:
            logger.debug(
                "Submit Spark em thread em background; aguardando resposta HTTP."
            )
            _flush_logger(logger)

        http_wait = (
            float(cfg.spark_submit_read_timeout_seconds)
            + float(cfg.spark_connect_timeout_seconds)
            + 30.0
        )
        wait_http = http_response_logged.wait(timeout=http_wait)
        if not wait_http and logger:
            logger.warning(
                "Timeout aguardando resposta HTTP do submit Spark "
                f"(>{http_wait:.0f}s); seguindo para join da thread."
            )

        submit_thread.join(timeout=http_wait)

        if submit_errors:
            raise submit_errors[0]

        if logger:
            logger.info(
                f"[Spark] submit accepted; polling Trino up to "
                f"{cfg.spark_poll_timeout_seconds}s"
            )
            _flush_logger(logger)

        total = wait_for_spark_result(
            conn=conn,
            cfg=cfg,
            batch_id=batch_id,
            expected_rows=expected_rows,
            logger=logger,
            submit_errors=submit_errors,
        )

        submit_thread.join(timeout=0.5)

        count = run_query(
            conn,
            f"SELECT COUNT(*) FROM {full_table_trino} WHERE batch_id='{batch_id}'",
            logger=None,
        )
        total = count[0][0] if count else total

        return TransformationResult(
            engine=self.engine_name,
            batch_id=batch_id,
            status="success",
            rows_read=total,
            rows_transformed=total,
            rows_persisted=total,
        )
