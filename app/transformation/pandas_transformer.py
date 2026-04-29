from app.transformation.base import BaseTransformer, TransformationResult
from app.log_formatting import format_elapsed_sec, wall_clock_start
import pyarrow.parquet as pq
import pandas as pd
import io


class PandasTransformer(BaseTransformer):
    engine_name = "pandas"

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
        dfs = []

        t_read = wall_clock_start()
        if logger:
            logger.info("Starting raw-layer read")

        n_staging = len(staging_files or [])

        for path in staging_files:
            if logger:
                logger.debug(f"[Pandas] Reading raw-layer file: {path}")

            obj = s3.get_object(Bucket=cfg.bucket_name, Key=path)
            body = obj["Body"].read()
            table = pq.read_table(io.BytesIO(body))
            df = table.to_pandas()

            dfs.append(df)

        if not dfs:
            if logger:
                logger.info(
                    f"Raw-layer read completed | files_read={n_staging} "
                    f"{format_elapsed_sec(t_read)}"
                )
            return TransformationResult(
                engine=self.engine_name,
                batch_id=batch_id,
                status="success",
                rows_read=0,
                rows_transformed=0,
                rows_persisted=0,
            )

        # Avoid reindex/alignment issues when multiple files
        # arrive with overlapping indices.
        df = pd.concat(dfs, ignore_index=True)
        df = df.loc[:, ~df.columns.duplicated()]
        df = df[df["_batch_id"] == batch_id]

        total = len(df)

        if logger:
            logger.info(
                f"Raw-layer read completed | files_read={n_staging} "
                f"{format_elapsed_sec(t_read)}"
            )

        if df.empty:
            return TransformationResult(
                engine=self.engine_name,
                batch_id=batch_id,
                status="success",
                rows_read=0,
                rows_transformed=0,
                rows_persisted=0,
            )

        df["moeda_original"] = df["is_usd"].map(
            {True: "USD", False: "BRL"}
        )

        df["valor_em_real"] = df["amount"]
        df.loc[df["is_usd"], "valor_em_real"] = (
            df["amount"] * cfg.exchange_rate_usd_brl
        )

        final_df = pd.DataFrame(
            {
                "nome": df["name"],
                "valor_original": df["amount"],
                "data_evento": df["event_date"],
                "moeda_original": df["moeda_original"],
                "valor_em_real": df["valor_em_real"],
                "data_ingestao": df["_ingested_at"],
                "batch_id": df["_batch_id"],
            }
        )

        from app.trino.trino_client import run_query

        full_table = f"{cfg.trino_catalog}.{cfg.trino_schema}.{cfg.final_table}"

        chunk_size = 500
        total_written = 0

        t_persist = wall_clock_start()
        if logger:
            logger.info("Starting final table persistence")

        insert_queries = 0

        for i in range(0, len(final_df), chunk_size):
            chunk = final_df.iloc[i : i + chunk_size]

            values_sql = ",".join(
                [
                    f"('{r.nome}',{r.valor_original},TIMESTAMP '{r.data_evento}','{r.moeda_original}',{r.valor_em_real},TIMESTAMP '{r.data_ingestao}','{r.batch_id}')"
                    for r in chunk.itertuples(index=False)
                ]
            )

            query = f"INSERT INTO {full_table} VALUES {values_sql}"

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
            rows_read=total,
            rows_transformed=total,
            rows_persisted=total_written,
        )