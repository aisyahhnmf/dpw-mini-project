"""Batch extractor: pagination + retry/backoff + watermark + Parquet mentah."""
import json
import time
from datetime import datetime, timezone

import pandas as pd

from . import config as C
from .api_adapter import LocalCsvApi, parse_ts
from .contract import validate_batch
from .logging_setup import get_logger
from .retry import retry_with_backoff

log = get_logger("extractor")
BUSINESS_COLS = ["order_id", "customer_id", "order_ts", "product_category", "quantity",
                 "unit_price", "discount_pct", "payment_method", "status"]


def read_watermark() -> str | None:
    if C.WATERMARK_FILE.exists():
        return json.loads(C.WATERMARK_FILE.read_text())["last_order_ts"]
    return None


def write_watermark(ts: str) -> None:
    C.STATE_DIR.mkdir(exist_ok=True)
    C.WATERMARK_FILE.write_text(json.dumps(
        {"last_order_ts": ts, "updated_at": datetime.now(timezone.utc).isoformat()}, indent=2))


def _read_parquet_or_empty(path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def extract_all_pages(api: LocalCsvApi, page_size: int, since: str | None, stats: dict) -> list[dict]:
    """Ambil semua halaman: page=1.. sampai has_next False. Setiap panggilan dibungkus retry."""
    records, page = [], 1
    while True:
        def call(p=page):
            return api.get_orders(page=p, page_size=page_size, updated_since=since)

        def counted():
            try:
                return call()
            except Exception:
                stats["retries"] += 1
                raise

        resp = retry_with_backoff(counted, max_retries=C.MAX_RETRIES, base=C.BACKOFF_BASE_SECONDS,
                                  max_delay=C.BACKOFF_MAX_SECONDS, logger=log, label=f"GET page={page}")
        log.info("page=%d/%s diterima: %d record (total_sumber=%d)", page, resp["total_pages"] or 0,
                 len(resp["data"]), resp["total"])
        records.extend(resp["data"])
        stats["pages"] += 1
        if not resp["has_next"]:
            return records
        page += 1


def run_batch(page_size: int = C.PAGE_SIZE, simulate_failures: int = 0, ignore_watermark: bool = False,
              fail_fast: bool = False) -> dict:
    t0 = time.time()
    for d in (C.RAW_DIR, C.PROCESSED_DIR, C.REJECTED_DIR, C.STATE_DIR):
        d.mkdir(exist_ok=True)
    stats = {"pages": 0, "retries": 0}
    since = None if ignore_watermark else read_watermark()
    log.info("=== RUN BATCH mulai | watermark=%s | page_size=%d | simulasi_gagal=%d ===",
             since, page_size, simulate_failures)

    api = LocalCsvApi(C.SOURCE_CSV, fail_first_n_calls=simulate_failures)
    records = extract_all_pages(api, page_size, since, stats)
    n_read = len(records)

    # 1) RAW: simpan apa adanya (string), buang replay persis (idempotensi level baris)
    now = datetime.now(timezone.utc).isoformat()
    new_raw = pd.DataFrame(records, columns=BUSINESS_COLS).astype(str)
    old_raw = _read_parquet_or_empty(C.RAW_PARQUET)
    before = len(old_raw)
    merged = pd.concat([old_raw, new_raw.assign(_extracted_at=now)], ignore_index=True)
    merged = merged.drop_duplicates(subset=BUSINESS_COLS, keep="first").reset_index(drop=True)
    merged.to_parquet(C.RAW_PARQUET, index=False)
    raw_appended = len(merged) - before
    new_rows = merged.iloc[before:][BUSINESS_COLS].to_dict("records")  # hanya baris yang benar-benar baru

    # 2) DATA CONTRACT: pisahkan valid / rejected
    valid, rejected = validate_batch(new_rows, fail_fast=fail_fast)

    # 3) SINK valid: idempotent berdasarkan order_id (kunci bisnis)
    old_valid = _read_parquet_or_empty(C.VALID_BATCH_PARQUET)
    v_before = len(old_valid)
    new_valid = pd.DataFrame(valid)
    all_valid = pd.concat([old_valid, new_valid], ignore_index=True) if len(new_valid) else old_valid
    if len(all_valid):
        all_valid = all_valid.drop_duplicates(subset=["order_id"], keep="first").reset_index(drop=True)
        all_valid.to_parquet(C.VALID_BATCH_PARQUET, index=False)
    valid_appended = len(all_valid) - v_before

    # 4) REJECTED: simpan beserta alasan
    old_rej = _read_parquet_or_empty(C.REJECTED_BATCH_PARQUET)
    new_rej = pd.DataFrame(rejected)
    if len(new_rej):
        new_rej["rejected_at"] = now
        new_rej["source"] = "batch_api"
        all_rej = pd.concat([old_rej, new_rej], ignore_index=True)
        all_rej.to_parquet(C.REJECTED_BATCH_PARQUET, index=False)
        all_rej.to_csv(C.REJECTED_DIR / "rejected_orders_batch.csv", index=False)

    # 5) Watermark baru HANYA setelah semua penulisan sukses (pola aman)
    parsed = [t for t in (parse_ts(r["order_ts"]) for r in records) if t is not None]
    if parsed:
        write_watermark(max(parsed).strftime("%Y-%m-%dT%H:%M:%S"))

    metrics = {
        "run_at": now, "watermark_in": since, "watermark_out": read_watermark(),
        "pages": stats["pages"], "retries": stats["retries"],
        "records_read": n_read, "raw_appended": raw_appended,
        "duplicates_skipped_raw": n_read - raw_appended,
        "valid_new": len(valid), "valid_appended": valid_appended,
        "rejected": len(rejected), "raw_total": len(merged),
        "valid_total": len(all_valid), "unique_order_id_valid": int(all_valid["order_id"].nunique()) if len(all_valid) else 0,
        "seconds": round(time.time() - t0, 3),
    }
    log.info("RUN SELESAI | %s", json.dumps(metrics))
    with open(C.LOG_DIR / "run_metrics.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(metrics) + "\n")
    return metrics
