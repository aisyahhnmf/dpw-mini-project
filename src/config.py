"""Konfigurasi terpusat. Semua path relatif terhadap root proyek."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = ROOT / "raw"
PROCESSED_DIR = ROOT / "processed"
REJECTED_DIR = ROOT / "rejected"
STATE_DIR = ROOT / "state"
LOG_DIR = ROOT / "logs"
EVIDENCE_DIR = ROOT / "evidence"
WAREHOUSE_PATH = ROOT / "warehouse" / "warehouse.duckdb"

SOURCE_CSV = DATA_DIR / "orders.csv"            # sumber "REST API" lokal
STREAM_CSV = DATA_DIR / "stream_orders.csv"     # sumber event Kafka

RAW_PARQUET = RAW_DIR / "orders.parquet"
VALID_BATCH_PARQUET = PROCESSED_DIR / "valid_orders_batch.parquet"
REJECTED_BATCH_PARQUET = REJECTED_DIR / "rejected_orders_batch.parquet"
WATERMARK_FILE = STATE_DIR / "watermark.json"

STREAM_VALID_JSONL = PROCESSED_DIR / "stream_valid.jsonl"
STREAM_REJECTED_JSONL = REJECTED_DIR / "stream_rejected.jsonl"
CLEAN_PARQUET = PROCESSED_DIR / "orders_clean.parquet"

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "orders"
KAFKA_GROUP = "orders-etl"

PAGE_SIZE = 10   # orders.csv resmi hanya 36 baris -> 4 halaman (pagination terlihat)
MAX_RETRIES = 5
BACKOFF_BASE_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 8.0

ALLOWED_STATUS = ("paid", "shipped", "cancelled", "refunded")
