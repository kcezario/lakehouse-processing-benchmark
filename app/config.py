from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

PRODUCT = os.getenv("PRODUCT", "lakehouse-benchmark").strip()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev").strip()
TENANT = f"{PRODUCT}-{ENVIRONMENT}"

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Fixed bucket naming based on PRODUCT + ENVIRONMENT.
BUCKET_NAME = f"bkt-{TENANT}-mda"
BUCKET_URI = f"s3a://{BUCKET_NAME}"

DATASET = os.getenv("DATASET", "test_data")
STAGING_PREFIX = f"{TENANT}/raw/{DATASET}"

# Optional prefix inside the bucket before STAGING_PREFIX.
# Bucket name is already passed in put_object(Bucket=...); avoid duplicating it
# in object keys.
_s3_staging_root_raw = os.environ.get("S3_STAGING_KEY_ROOT")
if _s3_staging_root_raw is None:
    S3_STAGING_KEY_ROOT = ""
else:
    S3_STAGING_KEY_ROOT = _s3_staging_root_raw.strip().strip("/")

TRINO_HOST = os.getenv("TRINO_HOST")
TRINO_PORT = int(os.getenv("TRINO_PORT", 8080))
TRINO_USER = os.getenv("TRINO_USER", f"svc_{PRODUCT}_{ENVIRONMENT}")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")
TRINO_VERIFY = os.getenv("TRINO_VERIFY", "true").lower() == "true"
TRINO_CERT = os.getenv("TRINO_CERT")

TRINO_CATALOG = os.getenv("TRINO_CATALOG", f"iceberg_{PRODUCT}_{ENVIRONMENT}")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", f"{PRODUCT}_{ENVIRONMENT}")

STAGING_TABLE = f"staging_{DATASET}"
FINAL_TABLE = f"{DATASET}_final"

EXCHANGE_RATE_USD_BRL = float(os.getenv("EXCHANGE_RATE_USD_BRL", "5.03"))

TRANSFORMATION_ENGINE = os.getenv("TRANSFORMATION_ENGINE", "pandas").lower()

SUPPORTED_TRANSFORMATION_ENGINES = (
    "python",
    "pandas",
    "spark",
)

if TRANSFORMATION_ENGINE == "trino":
    raise ValueError("Trino is not supported as a transformation engine")
if TRANSFORMATION_ENGINE not in SUPPORTED_TRANSFORMATION_ENGINES:
    supported = ", ".join(SUPPORTED_TRANSFORMATION_ENGINES)
    raise ValueError(
        f"Invalid transformation engine: {TRANSFORMATION_ENGINE}. "
        f"Supported: {supported}"
    )

def _spark_submit_url() -> str:
    raw = os.getenv("SPARK_CLIENT_URL")
    default = "http://localhost:8080/submit"
    if raw and str(raw).strip():
        return str(raw).strip()
    return default


SPARK_CLIENT_URL = _spark_submit_url()

SPARK_PAYLOAD_NAME = os.getenv(
    "SPARK_PAYLOAD_NAME",
    f"{TENANT}-{DATASET}-spark-transform",
)

SPARK_TIMEOUT_SECONDS = int(os.getenv("SPARK_TIMEOUT_SECONDS", "300"))
SPARK_CONNECT_TIMEOUT_SECONDS = int(
    os.getenv("SPARK_CONNECT_TIMEOUT_SECONDS", "15")
)
# Maximum wait for the first byte of POST /submit HTTP response.
SPARK_SUBMIT_READ_TIMEOUT_SECONDS = int(
    os.getenv("SPARK_SUBMIT_READ_TIMEOUT_SECONDS", "120")
)
SPARK_POLL_INTERVAL_SECONDS = int(os.getenv("SPARK_POLL_INTERVAL_SECONDS", "3"))
# HTTP submit may return early while the Spark job keeps running in the cluster.
SPARK_POLL_TIMEOUT_SECONDS = int(os.getenv("SPARK_POLL_TIMEOUT_SECONDS", "600"))

# Nessie catalog used by generated Spark code.
NESSIE_URI = os.getenv("NESSIE_URI", "").strip()
NESSIE_REF = os.getenv("NESSIE_REF", "main").strip()
NESSIE_AUTHENTICATION_TYPE = os.getenv(
    "NESSIE_AUTHENTICATION_TYPE", "NONE"
).strip()
NESSIE_WAREHOUSE = os.getenv(
    "NESSIE_WAREHOUSE", f"{BUCKET_URI}/iceberg"
).strip()
SPARK_ICEBERG_CATALOG_NAME = os.getenv(
    "SPARK_ICEBERG_CATALOG_NAME", "nessie"
).strip()
# HadoopFileIO uses s3a via Hadoop. S3FileIO requires AWS SDK jars.
NESSIE_IO_IMPL = os.getenv(
    "NESSIE_IO_IMPL", "org.apache.iceberg.hadoop.HadoopFileIO"
).strip()

BENCHMARK_ENABLED = os.getenv("BENCHMARK_ENABLED", "false").lower() == "true"

BENCHMARK_ENGINES = tuple(
    engine.strip().lower()
    for engine in os.getenv(
        "BENCHMARK_ENGINES",
        "python,pandas,spark",
    ).split(",")
    if engine.strip() and engine.strip().lower() in SUPPORTED_TRANSFORMATION_ENGINES
)

BENCHMARK_ROWS = tuple(
    int(value.strip())
    for value in os.getenv(
        "BENCHMARK_ROWS",
        "5000",
        # "5000,10000,50000,100000",
    ).split(",")
    if value.strip()
)

BENCHMARK_TIMEOUT_SECONDS = int(
    os.getenv("BENCHMARK_TIMEOUT_SECONDS", "120")
)

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE_NAME = os.getenv("LOG_FILE_NAME", "logs.log")

# MinIO/S3 logs use a relative key path inside the bucket.
S3_LOGS_PREFIX = os.getenv("S3_LOGS_PREFIX", "logs/lakehouse_benchmark").strip().strip(
    "/"
)
LOG_S3_ENABLED = os.getenv("LOG_S3_ENABLED", "true").lower() == "true"


@dataclass(frozen=True)
class AppConfig:
    product: str = PRODUCT
    environment: str = ENVIRONMENT
    tenant: str = TENANT

    bucket_name: str = BUCKET_NAME
    bucket_uri: str = BUCKET_URI
    s3_endpoint: str = S3_ENDPOINT
    s3_access_key: str = S3_ACCESS_KEY
    s3_secret_key: str = S3_SECRET_KEY
    s3_region: str = S3_REGION

    dataset: str = DATASET
    staging_prefix: str = STAGING_PREFIX
    s3_staging_key_root: str = S3_STAGING_KEY_ROOT

    trino_host: str = TRINO_HOST
    trino_port: int = TRINO_PORT
    trino_user: str = TRINO_USER
    trino_password: str = TRINO_PASSWORD
    trino_http_scheme: str = TRINO_HTTP_SCHEME
    trino_verify: bool = TRINO_VERIFY
    trino_cert: str | None = TRINO_CERT
    trino_catalog: str = TRINO_CATALOG
    trino_schema: str = TRINO_SCHEMA

    staging_table: str = STAGING_TABLE
    final_table: str = FINAL_TABLE
    exchange_rate_usd_brl: float = EXCHANGE_RATE_USD_BRL

    transformation_engine: str = TRANSFORMATION_ENGINE
    supported_transformation_engines: tuple[str, ...] = SUPPORTED_TRANSFORMATION_ENGINES

    spark_client_url: str = SPARK_CLIENT_URL
    spark_payload_name: str = SPARK_PAYLOAD_NAME
    spark_timeout_seconds: int = SPARK_TIMEOUT_SECONDS
    spark_connect_timeout_seconds: int = SPARK_CONNECT_TIMEOUT_SECONDS
    spark_submit_read_timeout_seconds: int = SPARK_SUBMIT_READ_TIMEOUT_SECONDS
    spark_poll_interval_seconds: int = SPARK_POLL_INTERVAL_SECONDS
    spark_poll_timeout_seconds: int = SPARK_POLL_TIMEOUT_SECONDS

    nessie_uri: str = NESSIE_URI
    nessie_ref: str = NESSIE_REF
    nessie_authentication_type: str = NESSIE_AUTHENTICATION_TYPE
    nessie_warehouse: str = NESSIE_WAREHOUSE
    nessie_io_impl: str = NESSIE_IO_IMPL
    spark_iceberg_catalog_name: str = SPARK_ICEBERG_CATALOG_NAME

    benchmark_enabled: bool = BENCHMARK_ENABLED
    benchmark_engines: tuple[str, ...] = BENCHMARK_ENGINES
    benchmark_rows: tuple[int, ...] = BENCHMARK_ROWS
    benchmark_timeout_seconds: int = BENCHMARK_TIMEOUT_SECONDS

    log_level: str = LOG_LEVEL
    log_file_name: str = LOG_FILE_NAME
    log_s3_enabled: bool = LOG_S3_ENABLED
    s3_logs_prefix: str = S3_LOGS_PREFIX