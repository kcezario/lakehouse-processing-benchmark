import logging

import trino
from trino.auth import BasicAuthentication

from app.log_formatting import format_elapsed_sec, format_row, wall_clock_start


# =========================================================
# CONNECTION
# =========================================================

def create_connection(cfg):
    auth = None

    if cfg.trino_password:
        auth = BasicAuthentication(
            cfg.trino_user,
            cfg.trino_password,
        )

    if cfg.trino_cert:
        verify = cfg.trino_cert
    else:
        verify = cfg.trino_verify

    return trino.dbapi.connect(
        host=cfg.trino_host,
        port=cfg.trino_port,
        user=cfg.trino_user,
        auth=auth,
        catalog=cfg.trino_catalog,
        schema=cfg.trino_schema,
        http_scheme=cfg.trino_http_scheme,
        verify=verify,
    )


# =========================================================
# QUERY EXECUTION
# =========================================================

def run_query(conn, query, logger=None, metrics=None):
    import time

    start = time.time()

    if logger:
        # evita logar INSERT gigante
        preview = query.strip().split("\n")[0][:120]
        logger.debug(f"Executando query Trino: {preview}...")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"SQL (preview): {query.strip()[:2000]}")

    cursor = conn.cursor()
    cursor.execute(query)

    try:
        result = cursor.fetchall()
    except Exception:
        result = []

    elapsed = time.time() - start

    if metrics:
        metrics.add_trino_time(elapsed)

    return result


# =========================================================
# DDL SETUP (IDEMPOTENTE)
# =========================================================

def ensure_schema_and_tables(conn, cfg, logger=None, metrics=None):
    full_schema = f"{cfg.trino_catalog}.{cfg.trino_schema}"
    full_final = f"{cfg.trino_catalog}.{cfg.trino_schema}.{cfg.final_table}"

    t0 = wall_clock_start()
    if logger:
        logger.info("Checking schema and tables in Trino catalog")

    # SCHEMA
    run_query(
        conn,
        f"CREATE SCHEMA IF NOT EXISTS {full_schema}",
        logger,
        metrics,
    )

    # FINAL TABLE
    run_query(
        conn,
        f"""
        CREATE TABLE IF NOT EXISTS {full_final} (
            nome VARCHAR,
            valor_original DOUBLE,
            data_evento TIMESTAMP,
            moeda_original VARCHAR,
            valor_em_real DOUBLE,
            data_ingestao TIMESTAMP,
            batch_id VARCHAR
        )
        WITH (format = 'PARQUET')
        """,
        logger,
        metrics,
    )

    if logger:
        logger.info(
            f"DDL validation completed | queries_executadas=2 {format_elapsed_sec(t0)}"
        )


# =========================================================
# FINAL VALIDATION (FONTE DA VERDADE)
# =========================================================

def collect_final_metrics(conn, cfg, batch_id, logger=None):
    full_final = f"{cfg.trino_catalog}.{cfg.trino_schema}.{cfg.final_table}"

    t0 = wall_clock_start()
    if logger:
        logger.info("Starting final table validation")

    queries_executadas = 0

    # ROW COUNT
    count = run_query(
        conn,
        f"""
        SELECT COUNT(*)
        FROM {full_final}
        WHERE batch_id = '{batch_id}'
        """,
        logger,
    )
    queries_executadas += 1

    total_rows = count[0][0] if count else 0

    # HEAD
    head = run_query(
        conn,
        f"""
        SELECT *
        FROM {full_final}
        WHERE batch_id = '{batch_id}'
        LIMIT 5
        """,
        logger,
    )
    queries_executadas += 1

    # COLUMNS
    columns = run_query(
        conn,
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{cfg.trino_schema}'
          AND table_name = '{cfg.final_table}'
        """,
        logger,
    )
    queries_executadas += 1

    total_columns = len(columns)

    # TABLE LOCATION (Iceberg) - opcional, pode variar por conector/versao
    try:
        location = run_query(
            conn,
            f"""
            SELECT location
            FROM "{cfg.trino_catalog}".system."metadata.table_properties"
            WHERE table_schema = '{cfg.trino_schema}'
              AND table_name = '{cfg.final_table}'
            """,
            logger,
        )
        queries_executadas += 1
        table_location = location[0][0] if location else "unknown"
    except Exception:
        table_location = "unknown"

    # SIZE (fallback simples)
    try:
        size = run_query(
            conn,
            f"""
            SELECT sum("$file_size")
            FROM {full_final}
            WHERE batch_id = '{batch_id}'
            """,
            logger,
        )
        queries_executadas += 1

        total_size = size[0][0] if size and size[0][0] else 0
    except Exception:
        total_size = 0

    # LOG
    if logger:
        logger.info(
            f"Validation completed | queries_executadas={queries_executadas} "
            f"{format_elapsed_sec(t0)}"
        )
        logger.info(
            f"Final table summary | rows={total_rows} | cols={total_columns} | "
            f"table={full_final} | location={table_location} | "
            f"size_bytes={total_size}"
        )
        logger.debug("Final table HEAD (5 rows):")
        for row in head:
            logger.debug(format_row(row))

    return {
        "rows": total_rows,
        "columns": total_columns,
        "size": total_size,
        "location": table_location,
    }