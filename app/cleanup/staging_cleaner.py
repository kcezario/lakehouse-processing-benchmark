from app.log_formatting import format_elapsed_sec, wall_clock_start


def clean_files(
    s3,
    cfg=None,
    staging_files=None,
    logger=None,
    bucket=None,
    keys=None,
):
    # Retrocompatibilidade:
    # - assinatura antiga: clean_files(s3, bucket=..., keys=...)
    # - assinatura nova: clean_files(s3, cfg=..., staging_files=...)
    target_bucket = bucket or (cfg.bucket_name if cfg else None)
    target_keys = staging_files if staging_files is not None else keys

    if not target_bucket:
        raise ValueError("Bucket not provided to clean_files.")
    if target_keys is None:
        target_keys = []

    t0 = wall_clock_start()
    if logger:
        logger.info("Starting raw-layer cleanup")

    for key in target_keys:
        s3.delete_object(
            Bucket=target_bucket,
            Key=key,
        )

        if logger:
            logger.debug(f"Arquivo staging removido: {key}")

    if logger:
        logger.info(
            f"Cleanup completed | files_removed={len(target_keys)} "
            f"{format_elapsed_sec(t0)}"
        )