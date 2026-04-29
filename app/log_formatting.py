from __future__ import annotations

import time
from datetime import date, datetime
from decimal import Decimal


def wall_clock_start() -> float:
    return time.perf_counter()


def format_elapsed_sec(start: float) -> str:
    return f"em {time.perf_counter() - start:.2f} sec"


def format_log_value(valor):
    if valor is None:
        return valor
    if isinstance(valor, datetime):
        return valor.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(valor, date):
        return valor.strftime("%Y-%m-%d")
    if isinstance(valor, float):
        return round(valor, 2)
    if isinstance(valor, Decimal):
        return round(float(valor), 2)
    try:
        import numpy as np

        if isinstance(valor, np.floating):
            return round(float(valor), 2)
        if isinstance(valor, np.integer):
            return int(valor)
    except ImportError:
        pass
    return valor


def format_row(row):
    if row is None:
        return row
    return [format_log_value(v) for v in row]
