"""Menjawab 5 pertanyaan analitik minimal dari mart dbt -> evidence/analytics_answers.md"""
import duckdb
from . import config as C

Q = {
    "1. Nilai penjualan bersih per hari (10 hari pertama; net_sales_paid_shipped = status paid/shipped)":
        "SELECT order_date, total_orders, round(net_amount_all_status,2) AS net_all_status, round(net_sales_paid_shipped,2) AS net_paid_shipped FROM marts.agg_daily_sales ORDER BY order_date LIMIT 10",
    "2. Kategori produk dengan penjualan bersih tertinggi":
        "SELECT product_category, total_orders, round(net_amount_all_status,2) AS net_all_status, round(net_sales_paid_shipped,2) AS net_paid_shipped FROM marts.agg_sales_by_category ORDER BY net_all_status DESC",
    "3. Jumlah & nilai pesanan menurut status":
        "SELECT status, total_orders, round(net_amount,2) AS net_amount FROM marts.agg_orders_by_status ORDER BY total_orders DESC",
    "4. Metode pembayaran paling sering digunakan":
        "SELECT payment_method, total_orders, pct_of_orders FROM marts.agg_payment_method_usage ORDER BY total_orders DESC",
    "5a. Jumlah record ditolak (total)":
        "SELECT count(*) AS total_rejected_records FROM staging.stg_rejected_orders",
    "5b. Penyebab penolakan terbesar (per field)":
        "SELECT rejected_field, total_rejected_records FROM marts.agg_rejection_reasons ORDER BY total_rejected_records DESC",
    "Validasi sampel: total net harian dihitung ulang langsung dari raw.orders (harus sama dengan mart)":
        "SELECT a.order_date, round(a.net_amount_all_status,2) AS mart, round(sum(o.net_amount),2) AS dari_raw "
        "FROM marts.agg_daily_sales a JOIN raw.orders o ON cast(o.order_ts AS date)=a.order_date GROUP BY a.order_date, a.net_amount_all_status ORDER BY a.order_date LIMIT 3",
}


def main():
    con = duckdb.connect(str(C.WAREHOUSE_PATH), read_only=True)
    out = ["# Jawaban Pertanyaan Analitik (dari DuckDB + dbt marts)", ""]
    for title, sql in Q.items():
        df = con.execute(sql).df()
        out += [f"## {title}", "", "```sql", sql, "```", "", df.to_markdown(index=False), ""]
    C.EVIDENCE_DIR.mkdir(exist_ok=True)
    (C.EVIDENCE_DIR / "analytics_answers.md").write_text("\n".join(out), encoding="utf-8")
    print("\n".join(out))


if __name__ == "__main__":
    main()
