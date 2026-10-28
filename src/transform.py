"""Transformasi Polars (LAZY): union batch+stream valid -> dedup order_id -> gross/net -> Parquet bersih."""
import polars as pl
from . import config as C
from .logging_setup import get_logger

log = get_logger("transform")

SCHEMA_CAST = {"order_id": pl.Utf8, "customer_id": pl.Utf8, "product_category": pl.Utf8, "quantity": pl.Int64,
               "unit_price": pl.Float64, "discount_pct": pl.Float64, "payment_method": pl.Utf8, "status": pl.Utf8}
COLS = ["order_id", "customer_id", "order_ts", "product_category", "quantity", "unit_price",
        "discount_pct", "payment_method", "status"]


def add_amounts(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Normalisasi tipe + hitung gross_amount & net_amount (dipakai juga oleh benchmark)."""
    return lf.with_columns(
        pl.col("order_ts").cast(pl.Utf8).str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=True),
        *[pl.col(k).cast(v) for k, v in SCHEMA_CAST.items()],
    ).with_columns(
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount")
    ).with_columns(
        (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
        pl.col("order_ts").dt.date().alias("order_date"),
    )


def run_transform() -> dict:
    frames = []
    if C.VALID_BATCH_PARQUET.exists():
        frames.append(pl.scan_parquet(C.VALID_BATCH_PARQUET).select(COLS).with_columns(pl.lit("batch_api").alias("source")))
    if C.STREAM_VALID_JSONL.exists() and C.STREAM_VALID_JSONL.stat().st_size > 0:
        frames.append(pl.scan_ndjson(C.STREAM_VALID_JSONL).select(COLS).with_columns(pl.lit("kafka").alias("source")))
    if not frames:
        raise SystemExit("Tidak ada data valid. Jalankan batch (dan/atau consumer) dulu.")
    # lazy: belum ada eksekusi sampai .collect()
    lf = pl.concat([f.with_columns(pl.col("order_ts").cast(pl.Utf8), *[pl.col(k).cast(v) for k, v in SCHEMA_CAST.items()]) for f in frames])
    lf = add_amounts(lf.unique(subset=["order_id"], keep="first", maintain_order=True))
    log.info("Rencana query lazy:\n%s", lf.explain())
    df = lf.collect()
    df = df.with_columns(pl.col("gross_amount").round(2), pl.col("net_amount").round(2))
    C.PROCESSED_DIR.mkdir(exist_ok=True)
    df.write_parquet(C.CLEAN_PARQUET)
    info = {"rows": df.height, "unique_order_id": df["order_id"].n_unique(), "schema": {k: str(v) for k, v in df.schema.items()}}
    log.info("transform selesai: %s", info)
    return info


if __name__ == "__main__":
    run_transform()
