"""Membuat evidence CSV: validation_cases.csv (aturan kontrak) & transformation_check.csv (hitung manual vs output)."""
import csv
import duckdb
from . import config as C
from .contract import validate_record

CASES = [
    ("valid_normal", {"order_id": "T1", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("order_id_kosong", {"order_id": "", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("customer_id_kosong", {"order_id": "T2", "customer_id": "", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("quantity_nol", {"order_id": "T3", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "0", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("quantity_negatif", {"order_id": "T4", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "-3", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("quantity_desimal", {"order_id": "T5", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2.5", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("unit_price_negatif", {"order_id": "T6", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "-1", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("unit_price_nol_boleh", {"order_id": "T7", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "0", "discount_pct": "0", "payment_method": "cod", "status": "paid"}),
    ("discount_1.5", {"order_id": "T8", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "1.5", "payment_method": "cod", "status": "paid"}),
    ("discount_negatif", {"order_id": "T9", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "-0.1", "payment_method": "cod", "status": "paid"}),
    ("discount_batas_1_boleh", {"order_id": "T10", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "1", "unit_price": "100000", "discount_pct": "1", "payment_method": "cod", "status": "paid"}),
    ("timestamp_rusak", {"order_id": "T11", "customer_id": "C1", "order_ts": "bukan-tanggal", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid"}),
    ("status_pending", {"order_id": "T12", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "pending"}),
    ("status_refunded_boleh", {"order_id": "T13", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "refunded"}),
    ("field_ekstra", {"order_id": "T14", "customer_id": "C1", "order_ts": "2026-08-01T10:00:00", "product_category": "home", "quantity": "2", "unit_price": "100000", "discount_pct": "0.1", "payment_method": "cod", "status": "paid", "coupon": "X"}),
]


def main():
    import sys
    C.EVIDENCE_DIR.mkdir(exist_ok=True)
    with open(C.EVIDENCE_DIR / "validation_cases.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["case", "input", "status", "alasan_penolakan"])
        for name, rec in CASES:
            ok, err = validate_record(rec)
            w.writerow([name, rec, "VALID" if ok else "REJECTED", "" if ok else err["reason"]])
    if "--cases-only" in sys.argv:
        return
    con = duckdb.connect(str(C.WAREHOUSE_PATH), read_only=True)
    rows = con.execute("""SELECT order_id, quantity, unit_price, discount_pct, gross_amount, net_amount,
        round(quantity*unit_price,2) AS gross_manual, round(quantity*unit_price*(1-discount_pct),2) AS net_manual
        FROM raw.orders ORDER BY order_id LIMIT 15""").fetchall()
    with open(C.EVIDENCE_DIR / "transformation_check.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "quantity", "unit_price", "discount_pct", "gross_pipeline", "net_pipeline", "gross_manual", "net_manual", "cocok"])
        for r in rows:
            w.writerow(list(r) + [abs(r[4] - r[6]) < 0.015 and abs(r[5] - r[7]) < 0.015])
    bad = con.execute("SELECT count(*) FROM raw.orders WHERE abs(net_amount - quantity*unit_price*(1-discount_pct)) > 0.015").fetchone()[0]
    print("baris dengan selisih net_amount > 0.015:", bad)
    print("dtypes:", con.execute("DESCRIBE raw.orders").fetchall())


if __name__ == "__main__":
    main()
