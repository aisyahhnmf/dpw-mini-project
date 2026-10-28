"""Benchmark Pandas vs Polars (eager & lazy) pada dataset & pekerjaan IDENTIK.

Pekerjaan: baca CSV -> parse timestamp -> gross/net -> group by (order_date, product_category)
           -> sum(net_amount), count.
Dataset sintetis dibuat dengan numpy (seed tetap), ditulis SEKALI ke CSV agar kedua library
membaca file yang sama. Setiap konfigurasi dijalankan --repeats kali; dilaporkan min & median.
Usage: python -m src.benchmark --rows 1000000 --repeats 5
"""
import argparse, csv, os, platform, statistics, time
import numpy as np
import pandas as pd
import polars as pl
from . import config as C

CATS = np.array(["electronics", "fashion", "home", "beauty", "sports", "books"])
PAY = np.array(["credit_card", "bank_transfer", "ewallet", "cod"])
STATUS = np.array(["paid", "shipped", "cancelled", "refunded"])


def make_dataset(path, rows: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ts = pd.Timestamp("2026-01-01") + pd.to_timedelta(rng.integers(0, 270 * 86400, rows), unit="s")
    df = pd.DataFrame({
        "order_id": [f"B{i:09d}" for i in range(rows)],
        "customer_id": [f"C{x:05d}" for x in rng.integers(1, 5001, rows)],
        "order_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "product_category": CATS[rng.integers(0, len(CATS), rows)],
        "quantity": rng.integers(1, 6, rows),
        "unit_price": np.round(rng.uniform(10000, 2500000, rows), 2),
        "discount_pct": rng.choice([0, 0.05, 0.1, 0.15, 0.2, 0.25], rows),
        "payment_method": PAY[rng.integers(0, len(PAY), rows)],
        "status": STATUS[rng.choice(4, rows, p=[0.55, 0.3, 0.1, 0.05])],
    })
    df.to_csv(path, index=False)
    return df


def job_pandas(path):
    df = pd.read_csv(path)
    df["order_ts"] = pd.to_datetime(df["order_ts"], format="%Y-%m-%dT%H:%M:%S")
    df["gross_amount"] = df["quantity"] * df["unit_price"]
    df["net_amount"] = df["gross_amount"] * (1 - df["discount_pct"])
    df["order_date"] = df["order_ts"].dt.date
    return df.groupby(["order_date", "product_category"]).agg(net=("net_amount", "sum"), n=("order_id", "count")).reset_index()


def _pl_pipeline(lf):
    return (lf.with_columns(pl.col("order_ts").str.to_datetime("%Y-%m-%dT%H:%M:%S"))
              .with_columns((pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"))
              .with_columns((pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
                            pl.col("order_ts").dt.date().alias("order_date"))
              .group_by(["order_date", "product_category"])
              .agg(pl.col("net_amount").sum().alias("net"), pl.col("order_id").count().alias("n")))


def job_polars_eager(path):
    return _pl_pipeline_eager(pl.read_csv(path))


def _pl_pipeline_eager(df):
    # eager: setiap langkah langsung dieksekusi & dimaterialisasi
    df = df.with_columns(pl.col("order_ts").str.to_datetime("%Y-%m-%dT%H:%M:%S"))
    df = df.with_columns((pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"))
    df = df.with_columns((pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
                         pl.col("order_ts").dt.date().alias("order_date"))
    return df.group_by(["order_date", "product_category"]).agg(
        pl.col("net_amount").sum().alias("net"), pl.col("order_id").count().alias("n"))


def job_polars_lazy(path):
    return _pl_pipeline(pl.scan_csv(path)).collect()


def timeit(fn, path, repeats):
    times, out = [], None
    for _ in range(repeats):
        t = time.perf_counter(); out = fn(path); times.append(time.perf_counter() - t)
    return times, out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--repeats", type=int, default=5)
    a = p.parse_args()
    C.PROCESSED_DIR.mkdir(exist_ok=True); C.EVIDENCE_DIR.mkdir(exist_ok=True)
    path = C.PROCESSED_DIR / f"bench_{a.rows}.csv"
    make_dataset(path, a.rows)
    size_mb = os.path.getsize(path) / 1e6
    print(f"dataset: {a.rows:,} baris, {size_mb:.1f} MB")

    jobs = [("pandas", "eager", job_pandas), ("polars", "eager", job_polars_eager), ("polars", "lazy (scan_csv+collect)", job_polars_lazy)]
    results, totals = [], {}
    for lib, mode, fn in jobs:
        fn(path)  # warm-up (cache OS) - tidak dihitung
        times, out = timeit(fn, path, a.repeats)
        total_net = float(out["net"].sum())
        totals[(lib, mode)] = total_net
        results.append({"library": lib, "mode": mode, "rows": a.rows, "file_mb": round(size_mb, 1), "repeats": a.repeats,
                        "min_s": round(min(times), 4), "median_s": round(statistics.median(times), 4),
                        "runs_s": ";".join(f"{t:.4f}" for t in times), "sum_net_amount": round(total_net, 2)})
        print(f"{lib:7s} {mode:26s} median={statistics.median(times):.3f}s min={min(times):.3f}s")
    base = results[0]["median_s"]
    for r in results:
        r["speedup_vs_pandas"] = round(base / r["median_s"], 2)
    ref = list(totals.values())[0]
    assert all(abs(v - ref) / ref < 1e-9 for v in totals.values()), "Hasil antar-library berbeda!"
    env = {"python": platform.python_version(), "pandas": pd.__version__, "polars": pl.__version__,
           "os": platform.platform(), "cpu_count": os.cpu_count(), "processor": platform.processor()}
    out = C.EVIDENCE_DIR / "pandas_polars_benchmark.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(results[0]) + list(env)); w.writeheader()
        for r in results: w.writerow({**r, **env})
    plan = _pl_pipeline(pl.scan_csv(path)).explain()
    (C.EVIDENCE_DIR / "lazy_query_plan.txt").write_text(
        "RENCANA QUERY POLARS LAZY (hasil .explain(), sebelum collect)\n\n" + plan +
        "\n\nCatatan: lazy hanya membaca kolom yang dibutuhkan (projection pushdown) dan mengoptimalkan urutan operasi; "
        "eager mengeksekusi & mematerialisasi tiap langkah segera.\n", encoding="utf-8")
    print("tulis", out)


if __name__ == "__main__":
    main()
