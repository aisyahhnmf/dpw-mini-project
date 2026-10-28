"""Membuat dataset SINTETIS (bukan data pelanggan nyata), deterministik (seed tetap).

orders.csv        : 2000 baris, order_ts naik monoton (dipakai sebagai watermark),
                    ~4% baris sengaja cacat, + 10 order_id duplikat persis.
stream_orders.csv : 40 baris untuk Kafka (34 valid, 6 invalid), ts lebih baru.
"""
import csv, random
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parent
random.seed(42)
CATS = ["electronics", "fashion", "home", "beauty", "sports", "books"]
PAY = ["credit_card", "bank_transfer", "ewallet", "cod"]
STATUS = ["paid", "shipped", "cancelled", "refunded"]
STATUS_W = [0.55, 0.30, 0.10, 0.05]
FIELDS = ["order_id", "customer_id", "order_ts", "product_category", "quantity",
          "unit_price", "discount_pct", "payment_method", "status"]


def make_row(order_id, ts):
    return {
        "order_id": order_id,
        "customer_id": f"C{random.randint(1, 150):04d}",
        "order_ts": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "product_category": random.choice(CATS),
        "quantity": str(random.randint(1, 5)),
        "unit_price": f"{random.uniform(10000, 2500000):.2f}",
        "discount_pct": f"{random.choice([0, 0, 0.05, 0.1, 0.15, 0.2, 0.25]):.2f}",
        "payment_method": random.choice(PAY),
        "status": random.choices(STATUS, STATUS_W)[0],
    }


# (nama cacat, fungsi yang merusak baris)
DEFECTS = [
    ("customer_id_kosong", lambda r: r.update(customer_id="")),
    ("quantity_nol", lambda r: r.update(quantity="0")),
    ("quantity_negatif", lambda r: r.update(quantity="-2")),
    ("quantity_desimal", lambda r: r.update(quantity="2.5")),
    ("unit_price_negatif", lambda r: r.update(unit_price="-15000.00")),
    ("discount_di_atas_1", lambda r: r.update(discount_pct="1.5")),
    ("timestamp_tidak_valid", lambda r: r.update(order_ts="31-31-2026 25:61")),
    ("status_tidak_dikenal", lambda r: r.update(status="pending")),
    ("order_id_kosong", lambda r: r.update(order_id="")),
]


def build(prefix, n, start, step_sec, defect_rate, dup_n):
    rows, ts = [], start
    for i in range(1, n + 1):
        ts += timedelta(seconds=step_sec + random.randint(0, 60))
        rows.append(make_row(f"{prefix}{i:05d}", ts))
    for idx in random.sample(range(len(rows)), int(n * defect_rate)):
        random.choice(DEFECTS)[1](rows[idx])
    # duplikat persis (replay) disisipkan sesudah baris aslinya -> urutan ts tetap naik
    for idx in sorted(random.sample(range(len(rows)), dup_n), reverse=True):
        if rows[idx]["order_id"]:
            rows.insert(idx + 1, dict(rows[idx]))
    return rows


def write(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    # CATATAN: data/orders.csv adalah dataset RESMI dari dosen (36 baris) dan TIDAK ditimpa di sini.
    # Hanya data sintetis untuk Kafka (stream_orders.csv) yang dibuat.
    stream = build("ORD-S", 40, datetime(2026, 10, 1), 600, 0.15, 0)
    write(OUT / "stream_orders.csv", stream)
    print("stream_orders.csv dibuat")
