from __future__ import annotations

import logging
import os
import tempfile
import time
import uuid
from pathlib import Path

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.config import AppConfig


_LOCAL_LOG_PATH: str | None = None


def _s3_logging_ready(cfg: AppConfig) -> bool:
    return bool(
        cfg.log_s3_enabled
        and cfg.s3_endpoint
        and cfg.s3_access_key
        and cfg.s3_secret_key
        and cfg.bucket_name
        and cfg.s3_logs_prefix
    )


def get_logger(level: str = "INFO", cfg: AppConfig | None = None):
    """
    Logger output to stdout; optionally mirrors output to a local file
    and uploads to MinIO/S3 via finalize_log_upload(cfg).
    To enable upload, pass cfg on first call and finalize once per job.
    """
    global _LOCAL_LOG_PATH

    logger = logging.getLogger("lakehouse_benchmark")

    if logger.handlers:
        return logger

    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    logger.setLevel(level_map.get(level.upper(), logging.INFO))

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if cfg is not None and _s3_logging_ready(cfg):
        tmp_root = Path(tempfile.gettempdir()) / "lakehouse_benchmark_logs"
        tmp_root.mkdir(parents=True, exist_ok=True)
        run_part = uuid.uuid4().hex[:12]
        _LOCAL_LOG_PATH = str(tmp_root / f"run-{run_part}.log")
        file_handler = logging.FileHandler(_LOCAL_LOG_PATH, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def finalize_log_upload(cfg: AppConfig | None) -> None:
    """
    Closes FileHandler and uploads the file to logs prefix in bucket,
    then removes the local file. Call once at the end of ingest/benchmark jobs.
    """
    global _LOCAL_LOG_PATH

    log = logging.getLogger("lakehouse_benchmark")

    for h in list(log.handlers):
        if isinstance(h, logging.FileHandler):
            try:
                h.flush()
                log.removeHandler(h)
                h.close()
            except Exception:
                pass

    path = _LOCAL_LOG_PATH
    _LOCAL_LOG_PATH = None

    if not path or not cfg or not _s3_logging_ready(cfg):
        return

    if not os.path.isfile(path):
        return

    try:
        from app.s3_client import upload_log_file

        upload_log_file(cfg, path, logger=log)
    except Exception as e:
        print(f"[logger] Failed to upload log to MinIO/S3: {e}", flush=True)

    try:
        os.unlink(path)
    except OSError:
        pass


class Timer:
    def __init__(self, logger, step_name):
        self.logger = logger
        self.step_name = step_name

    def __enter__(self):
        self.start = time.time()
        self.logger.debug(f"START: {self.step_name}")

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start
        self.logger.debug(f"END: {self.step_name} | duration={elapsed:.2f}s")
