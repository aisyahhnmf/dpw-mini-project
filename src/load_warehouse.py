"""Muat data bersih + rejected ke DuckDB (schema raw). Idempotent: CREATE OR REPLACE."""
import json
import duckdb
import pandas as pd
from . import config as C
from .logging_setup import get_logger

log = get_logger("warehouse")


def _rejected_frames() -> pd.DataFrame:
    frames = []
    if C.REJECTED_BATCH_PARQUET.exists():
        frames.append(pd.read_parquet(C.REJECTED_BATCH_PARQUET))
    if C.STREAM_REJECTED_JSONL.exists():
        with open(C.STREAM_REJECTED_JSONL, encoding="utf-8") as f:
            rows = [json.loads(x) for x in f if x.strip()]
        if rows:
            frames.append(pd.DataFrame(rows))
    if not frames:
        return pd.DataFrame(columns=["order_id", "reject_reason", "reject_fields", "source"])
    cols = ["order_id", "customer_id", "order_ts", "product_category", "quantity", "unit_price",
            "discount_pct", "payment_method", "status", "reject_reason", "reject_fields", "source", "rejected_at"]
    df = pd.concat(frames, ignore_index=True)
    for c in cols:
        if c not in df:
            df[c] = None
    return df[cols].astype(str)


def run_load() -> dict:
    C.WAREHOUSE_PATH.parent.mkdir(exist_ok=True)
    con = duckdb.connect(str(C.WAREHOUSE_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"CREATE OR REPLACE TABLE raw.orders AS SELECT * FROM read_parquet('{C.CLEAN_PARQUET.as_posix()}')")
    rej = _rejected_frames()
    con.register("rej_df", rej)
    con.execute("CREATE OR REPLACE TABLE raw.rejected_orders AS SELECT * FROM rej_df")
    n_orders = con.execute("SELECT count(*) FROM raw.orders").fetchone()[0]
    n_uniq = con.execute("SELECT count(DISTINCT order_id) FROM raw.orders").fetchone()[0]
    n_rej = con.execute("SELECT count(*) FROM raw.rejected_orders").fetchone()[0]
    leak = con.execute("SELECT count(*) FROM raw.orders o JOIN raw.rejected_orders r ON o.order_id = r.order_id").fetchone()[0]
    con.close()
    info = {"raw.orders": n_orders, "unique_order_id": n_uniq, "raw.rejected_orders": n_rej, "rejected_leaked_into_orders": leak}
    log.info("warehouse dimuat: %s", info)
    assert leak == 0, "Record rejected bocor ke warehouse!"
    return info


if __name__ == "__main__":
    run_load()
