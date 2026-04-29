from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TransformationResult:
    engine: str
    batch_id: str
    status: str
    rows_read: int
    rows_transformed: int
    rows_persisted: int
    error: str | None = None
    metadata: dict[str, Any] | None = None


class BaseTransformer:
    engine_name: str = "base"

    def run(
        self,
        *,
        conn,
        s3,
        cfg,
        staging_files: list[str],
        batch_id: str,
        logger=None,
        metrics=None,
    ) -> TransformationResult:
        raise NotImplementedError