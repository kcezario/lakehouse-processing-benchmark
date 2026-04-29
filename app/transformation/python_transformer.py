from app.transformation.base import BaseTransformer, TransformationResult
from app.log_formatting import format_elapsed_sec, wall_clock_start
import pyarrow.parquet as pq
import pandas as pd
import io


class PythonTransformer(BaseTransformer):
    engine_name = "python"

    def run(
        self,
        *,
        conn,
        s3,
        cfg,
        staging_files,
        batch_id,
        logger=None,
        metrics=None,
    ):
        total_read = 0
        total_written = 0

        rows_to_insert = []

        t_read = wall_clock_start()
        if logger:
            logger.info("Starting raw-layer read")

        n_staging = len(staging_files or [])

        for path in staging_files:
            if logger:
                logger.debug(f"[Python] Reading raw-layer file: {path}")

            obj = s3.get_object(Bucket=cfg.bucket_name, Key=path)
            body = obj["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            df = table.to_pandas()

            df = df[df["_batch_id"] == batch_id]

            total_read += len(df)

            if df.empty:
                continue

            df["moeda_original"] = df["is_usd"].apply(
                lambda x: "USD" if x else "BRL"
            )

            df["valor_em_real"] = df.apply(
                lambda r: r["amount"] * cfg.exchange_rate_usd_brl
                if r["is_usd"]
                else r["amount"],
                axis=1,
            )

            df["nome"] = df["name"]
            df["valor_original"] = df["amount"]
            df["data_evento"] = df["event_date"]
            df["data_ingestao"] = df["_ingested_at"]

            final_df = df[
                [
                    "nome",
                    "valor_original",
                    "data_evento",
                    "moeda_original",
                    "valor_em_real",
                    "data_ingestao",
                    "_batch_id",
                ]
            ].rename(columns={"_batch_id": "batch_id"})

            rows_to_insert.extend(final_df.values.tolist())

        if logger:
            logger.info(
                f"Raw-layer read completed | files_read={n_staging} "
                f"{format_elapsed_sec(t_read)}"
            )

        if not rows_to_insert:
            return TransformationResult(
                engine=self.engine_name,
                batch_id=batch_id,
                status="success",
                rows_read=total_read,
                rows_transformed=0,
                rows_persisted=0,
            )

        from app.trino.trino_client import run_query

        full_table = f"{cfg.trino_catalog}.{cfg.trino_schema}.{cfg.final_table}"

        chunk_size = 500

        t_persist = wall_clock_start()
        if logger:
            logger.info("Starting final table persistence")

        insert_queries = 0

        for i in range(0, len(rows_to_insert), chunk_size):
            chunk = rows_to_insert[i : i + chunk_size]

            values_sql = ",".join(
                [
                    f"('{r[0]}',{r[1]},TIMESTAMP '{r[2]}','{r[3]}',{r[4]},TIMESTAMP '{r[5]}','{r[6]}')"
                    for r in chunk
                ]
            )

            query = f"""
            INSERT INTO {full_table}
            VALUES {values_sql}
            """

            run_query(conn, query, logger, metrics)
            insert_queries += 1

            total_written += len(chunk)

        if logger:
            logger.info(
                f"Persistence completed | executed_queries={insert_queries} "
                f"{format_elapsed_sec(t_persist)}"
            )

        return TransformationResult(
            engine=self.engine_name,
            batch_id=batch_id,
            status="success",
            rows_read=total_read,
            rows_transformed=total_written,
            rows_persisted=total_written,
        )