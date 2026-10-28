# Mini Project — End-to-End Event-to-Warehouse Pipeline (E-Commerce)

Pipeline individu: **REST API (simulasi) + Kafka → Data Contract → Polars → DuckDB → dbt**. Sumber batch: `data/orders.csv` resmi (36 baris); stream & benchmark: data **sintetis** (bukan data pelanggan nyata).

## Struktur
```
data/            generate_dataset.py, orders.csv (sumber "API"), stream_orders.csv (sumber Kafka)
src/             config, retry, api_adapter, contract (Pydantic), extractor, kafka_producer/consumer,
                 transform (Polars), load_warehouse (DuckDB), benchmark, skew, analytics, evidence helper
dbt_project/     models/staging, models/marts (dim_*, fct_orders, agg_*), tests, schema.yml, profiles.yml
docker-compose.yml  Kafka single broker (KRaft)
run_all.py       orkestrator + pembuat evidence (log) otomatis
tests/           pytest (retry, pagination, watermark, kontrak, sink idempoten, transformasi)
raw/ processed/ rejected/ state/ logs/ warehouse/ evidence/   <- hasil eksekusi
kafka_modul3/    paket Modul 3 (praktikum + tantangan) + RUN_MODUL3.md
diagrams/        pipeline.png/.puml, star_schema.puml
laporan.md       laporan teknis · evidence/demo_notes.md  agenda demo
```

## Prasyarat
Python 3.10+, Docker Desktop (untuk Kafka), ±1 GB disk. Di folder proyek:
```bash
python -m venv .venv
.venv\Scripts\activate          # Windows  (Linux/macOS: source .venv/bin/activate)
pip install -r requirements.txt
```

## Menjalankan (urut)
| # | Perintah | Hasil / bukti |
|---|---|---|
| 1 | `python run_all.py --steps batch` | run1 (3 error simulasi → retry), run2 (watermark), run3 (baca ulang penuh); `raw/orders.parquet`; evidence `log_batch_run*.txt`, `incremental_run1_run2.png`, `validation_cases.csv`, `pytest_result.txt` |
| 2 | `python run_all.py --steps kafka` (Docker menyala) | broker + topic `orders` (3 partition), producer ≥20 event, consumer, uji duplikasi; `kafka_status.txt`, `kafka_events.txt`, `stream_summary.txt` |
| 3 | `python run_all.py --steps warehouse` | Polars lazy transform → `processed/orders_clean.parquet` → `warehouse/warehouse.duckdb` |
| 4 | `python run_all.py --steps dbt` | `dbt run`, `dbt test`, `dbt docs generate`, `analytics_answers.md`, `transformation_check.csv` |
| 5 | `python run_all.py --steps bench` | `pandas_polars_benchmark.csv`, `lazy_query_plan.txt`, `skew_experiment.md/.csv` |

`python run_all.py --steps all` menjalankan semuanya. Ringkasan PASS/FAIL: `evidence/run_summary.txt`.
Langkah manual setara (mis. batch): `python -m src.run_batch --reset --simulate-failures 3` → `python -m src.run_batch` → `python -m src.run_batch --ignore-watermark`.

Dokumentasi dbt dapat dibuka: `cd dbt_project && dbt docs serve --profiles-dir . ` (lalu screenshot lineage/halaman model → `evidence/dbt_docs.png`).

## Mengganti adapter CSV dengan REST API sungguhan
`src/api_adapter.py::LocalCsvApi.get_orders(page, page_size, updated_since)` cukup diganti kelas yang memanggil
`requests.get(url, params={"page":..,"page_size":..,"updated_since":..}, timeout=10)`; map HTTP 429/5xx/timeout → `TransientError`
(hormati header `Retry-After`), 4xx lain → `PermanentError`. Extractor, retry, watermark, kontrak, dan sink tidak berubah.

## Keputusan desain ringkas
- **Idempotensi:** raw dedup baris identik; valid dedup `order_id`; sink Kafka dedup `order_id`; warehouse `CREATE OR REPLACE`; dbt dari tabel deduplikasi.
- **Watermark** `order_ts` ( `>` watermark), diperbarui **setelah** semua penulisan sukses. Baris dengan `order_ts` rusak tidak dapat dibandingkan sehingga hanya muncul pada full load; tercatat di rejected.
- **Kafka:** key = `order_id`; commit manual **sesudah** sink → **at-least-once**; duplikasi bisnis dicegah sink idempoten. Bukan exactly-once end-to-end.
- **Definisi penjualan bersih** pada mart: `net_sales_paid_shipped` (status paid/shipped) dan `net_amount_all_status` (semua status).

## Batas yang jujur
Benchmark/skew dijalankan pada satu mesin dan data sintetis; tidak ada klaim OOM. Exactly-once tidak diklaim untuk sink file/DuckDB.
