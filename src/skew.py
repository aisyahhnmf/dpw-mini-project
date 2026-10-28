"""Eksperimen data skew: satu customer_id dominan.

Cara membuat skew: ambil dataset benchmark, pilih satu customer_id dominan, lalu GANDAKAN baris
milik customer itu sampai porsinya mencapai --dominant-share (mis. 50%) dari total baris.
Diukur: distribusi ukuran grup (sebelum/sesudah) dan waktu group_by(customer_id) Pandas vs Polars,
plus mitigasi (proyeksi kolom + agregasi parsial dua tahap). TIDAK menguji / mengklaim OOM.
Usage: python -m src.skew --rows 1000000 --dominant-share 0.5
"""
import argparse, os, statistics, time
import pandas as pd
import polars as pl
from . import config as C
from .benchmark import make_dataset


def t(fn, repeats=5):
    fn()
    ts = []
    for _ in range(repeats):
        s = time.perf_counter(); r = fn(); ts.append(time.perf_counter() - s)
    return statistics.median(ts), r


def dist(df: pd.DataFrame, label: str) -> dict:
    sizes = df.groupby("customer_id").size().sort_values(ascending=False)
    return {"dataset": label, "rows": len(df), "n_customers": int(sizes.size), "max_group_rows": int(sizes.iloc[0]),
            "top1_share_pct": round(100 * sizes.iloc[0] / len(df), 2), "median_group_rows": float(sizes.median()),
            "max_over_median": round(sizes.iloc[0] / sizes.median(), 1)}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=1_000_000)
    p.add_argument("--dominant-share", type=float, default=0.5)
    p.add_argument("--repeats", type=int, default=5)
    a = p.parse_args()
    C.PROCESSED_DIR.mkdir(exist_ok=True); C.EVIDENCE_DIR.mkdir(exist_ok=True)
    base = make_dataset(C.PROCESSED_DIR / f"bench_{a.rows}.csv", a.rows)
    base["net_amount"] = base["quantity"] * base["unit_price"] * (1 - base["discount_pct"])

    dom_id = base["customer_id"].iloc[0]
    dom_rows = base[base["customer_id"] == dom_id]
    # butuh total dominan D sehingga (D)/(N - d + D) = share  => D = share*(N-d)/(1-share)
    d, N = len(dom_rows), len(base)
    target = int(a.dominant_share * (N - d) / (1 - a.dominant_share))
    reps = max(1, round(target / d))
    skewed = pd.concat([base] + [dom_rows] * (reps - 1), ignore_index=True)

    rows_out = []
    for label, df in (("balanced", base), ("skewed", skewed)):
        info = dist(df, label)
        pl_df = pl.from_pandas(df[["customer_id", "order_date" if "order_date" in df else "order_ts", "net_amount"]])
        t_pd, _ = t(lambda: df.groupby("customer_id").agg(net=("net_amount", "sum"), n=("order_id", "count")), a.repeats)
        t_pl, _ = t(lambda: pl_df.group_by("customer_id").agg(pl.col("net_amount").sum(), pl.len().alias("n")), a.repeats)
        # mitigasi: proyeksi kolom + agregasi parsial (per chunk) lalu gabung
        def partial_agg():
            parts = [c.group_by("customer_id").agg(pl.col("net_amount").sum().alias("s"), pl.len().alias("n"))
                     for c in pl_df.iter_slices(250_000)]
            return pl.concat(parts).group_by("customer_id").agg(pl.col("s").sum(), pl.col("n").sum())
        t_part, _ = t(partial_agg, a.repeats)
        info.update({"pandas_groupby_median_s": round(t_pd, 4), "polars_groupby_median_s": round(t_pl, 4),
                     "polars_partial_agg_chunked_median_s": round(t_part, 4), "dominant_customer": dom_id})
        rows_out.append(info)
        print(info)

    out_df = pd.DataFrame(rows_out)
    out_df.to_csv(C.EVIDENCE_DIR / "skew_experiment.csv", index=False)
    md = ["# Eksperimen Data Skew", "",
          f"**Cara membuat skew:** dataset sintetis {a.rows:,} baris; customer `{dom_id}` dipilih sebagai dominan, "
          f"barisnya digandakan {reps}x hingga porsinya ±{a.dominant_share:.0%} dari total ({len(skewed):,} baris).",
          "", out_df.to_markdown(index=False), "",
          "**Lingkungan:** " + f"{os.cpu_count()} CPU; pandas {pd.__version__}; polars {pl.__version__}; median dari {a.repeats} kali jalan.", "",
          "## Interpretasi (isi setelah melihat angka di atas)",
          "- Bandingkan `max_group_rows`, `top1_share_pct`, dan `max_over_median`: skew membuat satu grup jauh lebih besar dari median.",
          "- Bandingkan waktu `group_by` sebelum vs sesudah. Pada mesin satu node, perbedaan waktu bisa kecil; "
          "skew terutama merugikan pada eksekusi paralel/terdistribusi karena satu worker memegang grup terbesar.",
          "- Kolom `polars_partial_agg_chunked_median_s` menunjukkan mitigasi agregasi parsial per potongan 250 ribu baris.", "",
          "## Keterbatasan",
          "- Satu mesin, dataset sintetis, tanpa uji OOM: **tidak ada klaim OOM** karena memang tidak terjadi/diuji.",
          "- Waktu sangat bergantung pada beban mesin, cache OS, dan jumlah core; ulangi di mesin berbeda untuk generalisasi.",
          "- group_by tunggal pada satu node tidak mereplikasi masalah straggler pada cluster."]
    (C.EVIDENCE_DIR / "skew_experiment.md").write_text("\n".join(md), encoding="utf-8")


if __name__ == "__main__":
    main()
