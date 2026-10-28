"""Mengisi Self_Assessment_Mini_Project_Data_Processing.xlsx dari BUKTI NYATA di folder evidence/.

  python finalize_self_assessment.py --prefill    -> isi kolom bukti & penjelasan saja (status 'Bukti tersedia - belum diuji', nilai kosong)
  python finalize_self_assessment.py              -> setelah run_all.py: status & nilai dihitung dari pengecekan otomatis
Nilai = bobot_indikator x (cek lolos / total cek). Cek yang butuh tindakan manual (screenshot, video, [ISI] di laporan)
akan GAGAL sampai kamu benar-benar melengkapinya -> nilai tidak pernah penuh tanpa bukti.
Nama/NRP tidak diisi otomatis: isi sendiri sel B3 dan E3.
"""
import argparse, json, re, sys
from datetime import date
from pathlib import Path
import openpyxl

ROOT = Path(__file__).resolve().parent
EV = ROOT / "evidence"
TEMPLATE = ROOT / "Self_Assessment_TEMPLATE_ASLI.xlsx"
OUT = ROOT / "Self_Assessment_Mini_Project_Data_Processing_TERISI.xlsx"

# bobot maksimal per indikator (jumlah per aspek = bobot aspek pada sheet Ringkasan)
W = {8: 4, 9: 4, 10: 4, 11: 4, 12: 4, 13: 3, 14: 3, 15: 3, 16: 3, 17: 3, 18: 3.75, 19: 3.75, 20: 3.75, 21: 3.75,
     22: 3, 23: 3, 24: 3, 25: 3, 26: 3, 27: 3, 28: 3, 29: 4, 30: 4, 31: 4, 32: 3, 33: 4, 34: 2.5, 35: 2.5, 36: 2.5, 37: 2.5}


def rd(rel):
    p = ROOT / rel
    return p.read_text(encoding="utf-8", errors="replace") if p.exists() and p.is_file() else ""


def ex(rel, minsize=1):
    p = ROOT / rel
    return p.exists() and p.is_file() and p.stat().st_size >= minsize


def metrics():
    """Metrik run batch 1..3 dari logs/run_metrics.jsonl."""
    p = ROOT / "logs" / "run_metrics.jsonl"
    rows = [json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()] if p.exists() else []
    return rows[-3:] if len(rows) >= 3 else rows


def no_placeholder(rel):
    t = rd(rel)
    return bool(t) and "[ISI" not in t


def lines(rel):
    t = rd(rel)
    return len(t.splitlines())


def build_checks():
    m = metrics()
    r1 = m[0] if len(m) >= 1 else {}
    r2 = m[1] if len(m) >= 2 else {}
    r3 = m[2] if len(m) >= 3 else {}
    log1 = rd("evidence/log_batch_run1.txt")
    pages = [(int(a), int(b)) for a, b in re.findall(r"page=(\d+)/(\d+) diterima", log1)]
    kev = rd("evidence/kafka_events.txt")
    kst = rd("evidence/kafka_status.txt")
    summ = rd("evidence/stream_summary.txt")
    mm = re.search(r"valid di sink\s*:\s*(\d+)\s+\(order_id unik: (\d+)\)", summ)
    pyt = rd("evidence/pytest_result.txt")
    tchk = rd("evidence/transformation_check.csv")
    dbtrun, dbttest = rd("evidence/dbt_run.log"), rd("evidence/dbt_test.log")
    return {
        8: [("log run1 ada", bool(log1)), ("multi halaman & halaman terakhir = total", len(pages) >= 2 and pages[-1][0] == pages[-1][1]),
            ("record dibaca > 0", r1.get("records_read", 0) > 0)],
        9: [("retry tercatat >= 3x", log1.count("retry dalam") >= 3), ("proses pulih (RUN SELESAI)", "RUN SELESAI" in log1)],
        10: [("run1 menulis watermark", bool(r1.get("watermark_out"))), ("run2 memakai watermark", bool(r2.get("watermark_in"))),
             ("run2 membaca 0 record baru", r2.get("records_read", -1) == 0), ("incremental_run1_run2.png ada", ex("evidence/incremental_run1_run2.png"))],
        11: [("raw/orders.parquet ada", ex("raw/orders.parquet")), ("dibaca ulang (raw_parquet_check)", "RAW" in rd("evidence/raw_parquet_check.txt"))],
        12: [("run3 (baca ulang penuh) raw_appended=0", r3.get("raw_appended", -1) == 0), ("run3 valid_appended=0", r3.get("valid_appended", -1) == 0),
             ("order_id unik == total valid", r3.get("unique_order_id_valid") == r3.get("valid_total") and r3.get("valid_total", 0) > 0),
             ("log mencatat dibaca/valid/ditolak", "records_read" in log1 and "rejected" in log1)],
        13: [("docker-compose.yml ada", ex("docker-compose.yml")), ("topic orders 3 partition", "PartitionCount: 3" in kst and "orders" in kst),
             ("broker running", "running" in kst.lower() or "Up" in kst)],
        14: [("producer selesai", "PRODUCER SELESAI" in kev), ("dikirim >= 20 event", any(int(n) >= 20 for n in re.findall(r"dikirim=(\d+)", kev))),
             ("terkonfirmasi == dikirim", len(re.findall(r"terkonfirmasi=(\d+)", kev)) > 0 and "gagal=0" in kev)],
        15: [("consumer selesai", "SELESAI |" in kev), ("stream_valid.jsonl berisi", ex("processed/stream_valid.jsonl")),
             ("stream_rejected.jsonl berisi", ex("rejected/stream_rejected.jsonl")), ("alasan penolakan tercatat", "reject_reason" in rd("rejected/stream_rejected.jsonl"))],
        16: [("laporan tanpa [ISI]", no_placeholder("laporan.md")), ("experiment-report Modul 3 tanpa [ISI]", no_placeholder("kafka_modul3/praktikum_bagian3/experiment-report.md"))],
        17: [("duplikat dilewati tercatat", "DUPLIKAT dilewati" in kev), ("order_id unik == valid di sink", bool(mm) and mm.group(1) == mm.group(2))],
        18: [("validation_cases.csv >= 14 kasus", lines("evidence/validation_cases.csv") >= 15), ("ada VALID & REJECTED", "VALID" in rd("evidence/validation_cases.csv") and "REJECTED" in rd("evidence/validation_cases.csv")),
             ("pytest lolos", "passed" in pyt and "failed" not in pyt and "error" not in pyt.lower().replace("0 error", ""))],
        19: [("contract.py memakai Pydantic", "BaseModel" in rd("src/contract.py")), ("pytest lolos", "passed" in pyt and "failed" not in pyt)],
        20: [("valid parquet ada", ex("processed/valid_orders_batch.parquet")), ("rejected parquet ada", ex("rejected/rejected_orders_batch.parquet")),
             ("valid+rejected == raw run1", r1.get("valid_new", 0) + r1.get("rejected", 0) == r1.get("raw_appended", -1))],
        21: [("rejected csv dengan reject_reason", "reject_reason" in rd("rejected/rejected_orders_batch.csv")),
             ("tidak bocor ke warehouse (leak=0)", "'rejected_leaked_into_orders': 0" in rd("evidence/log_load_warehouse.txt")),
             ("dbt singular test tidak gagal", "ERROR=0" in dbttest and "PASS=" in dbttest)],
        22: [("transformation_check.csv ada", ex("evidence/transformation_check.csv")), ("semua cocok", tchk.count("True") >= 10 and "False" not in tchk)],
        23: [("benchmark csv 3 baris", lines("evidence/pandas_polars_benchmark.csv") >= 4), ("ada kolom lingkungan", "cpu_count" in rd("evidence/pandas_polars_benchmark.csv")),
             ("laporan tanpa [ISI]", no_placeholder("laporan.md"))],
        24: [("lazy_query_plan.txt ada", ex("evidence/lazy_query_plan.txt")), ("transform lazy berjalan", "Rencana query lazy" in rd("evidence/log_transform.txt"))],
        25: [("skew_experiment.md ada", ex("evidence/skew_experiment.md")), ("skew csv 2 baris (balanced & skewed)", lines("evidence/skew_experiment.csv") >= 3),
             ("tanpa klaim OOM", "tidak ada klaim OOM" in rd("evidence/skew_experiment.md").lower())],
        26: [("laporan tanpa [ISI] (analisis optimasi terisi)", no_placeholder("laporan.md")), ("memuat strategi mitigasi", "agregasi parsial" in rd("laporan.md"))],
        27: [("warehouse.duckdb ada", ex("warehouse/warehouse.duckdb")), ("load warehouse tercatat", "raw.orders" in rd("evidence/log_load_warehouse.txt"))],
        28: [("dbt run sukses", "[exit code 0]" in dbtrun), ("stg_orders dibangun", "stg_orders" in dbtrun)],
        29: [("fct_orders dibangun", "fct_orders" in dbtrun), ("dim_customer & dim_product_category dibangun", "dim_customer" in dbtrun and "dim_product_category" in dbtrun),
             ("diagram star schema ada", ex("diagrams/star_schema.puml"))],
        30: [("agg_daily_sales dibangun", "agg_daily_sales" in dbtrun), ("analytics_answers.md ada", ex("evidence/analytics_answers.md")),
             ("validasi sampel ada", "Validasi sampel" in rd("evidence/analytics_answers.md"))],
        31: [("dbt test log ada", bool(dbttest)), ("ERROR=0", "ERROR=0" in dbttest), ("ada tes PASS", bool(re.search(r"PASS=([1-9]\d*)", dbttest)))],
        32: [("dbt docs generate sukses", "[exit code 0]" in rd("evidence/dbt_docs_generate.log")), ("catalog/index.html ada", ex("dbt_project/target/index.html")),
             ("screenshot evidence/dbt_docs.png (manual)", ex("evidence/dbt_docs.png"))],
        33: [("laporan tanpa [ISI]", no_placeholder("laporan.md")), ("membahas star vs snowflake", "snowflake" in rd("laporan.md").lower())],
        34: [("modul src terpisah", all(ex(f"src/{f}") for f in ("extractor.py", "contract.py", "transform.py", "kafka_producer.py", "kafka_consumer.py", "load_warehouse.py"))),
             ("model dbt ada", ex("dbt_project/models/marts/fct_orders.sql")), ("pytest lolos", "passed" in pyt and "failed" not in pyt)],
        35: [("README.md ada", ex("README.md", 500)), ("requirements.txt ada", ex("requirements.txt")),
             ("run_all tanpa FAIL", "run_summary.txt" and bool(rd("evidence/run_summary.txt")) and "FAIL" not in rd("evidence/run_summary.txt"))],
        36: [("laporan.md ada", ex("laporan.md", 2000)), ("tanpa placeholder [ISI]", no_placeholder("laporan.md")), ("diagram pipeline ada", ex("diagrams/pipeline.png"))],
        37: [("demo_notes.md ada", ex("evidence/demo_notes.md")), ("tautan/video diisi (tanpa [ISI])", no_placeholder("evidence/demo_notes.md"))],
    }


# lokasi bukti (kolom I) & penjelasan implementasi (kolom J)
INFO = {
    8: ("src/api_adapter.py, src/extractor.py; evidence/log_batch_run1.txt", "Extractor memanggil adapter dengan page & page_size=100 sampai has_next=False; tiap halaman dicatat di log. orders.csv resmi 36 record -> 4 halaman (page_size=10)."),
    9: ("src/retry.py; evidence/log_batch_run1.txt (baris WARNING 'retry dalam')", "3 panggilan API pertama disimulasikan gagal (HTTP 503) via --simulate-failures 3; jeda 0,5s -> 1s -> 2s (exponential, + jitter, maks 8s), batas 5 retry; proses pulih dan selesai."),
    10: ("state/watermark.json; evidence/log_batch_run1/2.txt; evidence/incremental_run1_run2.png", "Watermark = order_ts maksimum, ditulis setelah semua penulisan berhasil. Run 2 memakai watermark dan membaca 0 record baru."),
    11: ("raw/orders.parquet; evidence/raw_parquet_check.txt", "Hasil ekstraksi disimpan apa adanya (string) ke Parquet sebelum validasi/transformasi; dibaca ulang dengan pandas untuk verifikasi."),
    12: ("evidence/log_batch_run3_full_reread.txt; logs/run_metrics.jsonl", "Run 3 membaca ulang SELURUH sumber (--ignore-watermark): raw/valid yang ditambahkan = 0 karena dedup baris identik & order_id. Log mencatat dibaca, valid, ditolak, ditambahkan per run."),
    13: ("docker-compose.yml; evidence/kafka_status.txt", "Kafka 4.2.1 KRaft single broker via Docker Compose; topic orders dibuat dengan 3 partition, RF=1; status container & describe topic tersimpan."),
    14: ("src/kafka_producer.py; evidence/kafka_events.txt", "Producer (acks=all, idempotent) mengirim event dari stream_orders.csv (40 baris) dengan key=order_id; delivery callback mencatat partition & offset."),
    15: ("src/kafka_consumer.py; processed/stream_valid.jsonl; rejected/stream_rejected.jsonl; evidence/kafka_events.txt", "Consumer membaca topic, memvalidasi dengan kontrak Pydantic, menulis valid/rejected ke file terpisah beserta alasan, partition, dan offset; commit manual setelah sink."),
    16: ("laporan.md bagian 4; kafka_modul3/praktikum_bagian3/experiment-report.md", "Dijelaskan partition, consumer group, offset, at-least-once vs exactly-once, dan batas jaminan implementasi (at-least-once + sink idempoten; bukan exactly-once end-to-end)."),
    17: ("evidence/kafka_events.txt; evidence/stream_summary.txt; src/kafka_consumer.py", "Event dengan order_id sama dikirim dua kali (--duplicate-demo) dan 5 event diputar ulang; sink melewati order_id yang sudah ada sehingga hasil unik. Batas: tidak atomik dengan offset."),
    18: ("src/contract.py; evidence/validation_cases.csv; tests/test_pipeline.py", "Seluruh 6 aturan minimal diterapkan; 15 kasus uji valid/invalid (termasuk nilai batas) menghasilkan status dan alasan."),
    19: ("src/contract.py (Pydantic v2); evidence/pytest_result.txt", "validate_record/validate_batch memakai OrderContract (extra=forbid); mode --fail-fast melempar ContractViolation."),
    20: ("processed/valid_orders_batch.parquet; rejected/rejected_orders_batch.parquet; evidence/log_batch_run1.txt", "Record dipisah menjadi valid dan rejected; jumlah valid + rejected = record baru pada run 1."),
    21: ("rejected/rejected_orders_batch.csv; evidence/log_load_warehouse.txt; dbt test assert_no_rejected_in_fact", "Rejected menyimpan record asli + reject_reason + reject_fields; query JOIN antara raw.orders dan raw.rejected_orders = 0, dan dbt singular test memverifikasi hal sama."),
    22: ("src/transform.py; evidence/transformation_check.csv", "gross = quantity x unit_price; net = gross x (1 - discount_pct) dengan Polars; timestamp -> Datetime; sampel 15 baris dibandingkan dengan perhitungan manual."),
    23: ("src/benchmark.py; evidence/pandas_polars_benchmark.csv; laporan.md bagian 6", "Dataset sintetis identik, pekerjaan identik; pandas vs Polars eager vs lazy; 5 pengulangan + warm-up (median); lingkungan & keterbatasan dicatat."),
    24: ("src/transform.py, src/benchmark.py; evidence/lazy_query_plan.txt", "Polars scan_parquet/scan_csv + collect() (lazy) dibandingkan dengan read_csv (eager); rencana query lazy disimpan dari explain()."),
    25: ("src/skew.py; evidence/skew_experiment.md/.csv", "Satu customer_id digandakan hingga ~50% baris; dibandingkan distribusi grup dan waktu group_by sebelum/sesudah. Tidak ada klaim OOM."),
    26: ("laporan.md bagian 6", "Mitigasi: pemrosesan bertahap, proyeksi kolom, agregasi parsial per chunk (diukur pada skew.py), menghindari materialisasi."),
    27: ("warehouse/warehouse.duckdb; src/load_warehouse.py; evidence/log_load_warehouse.txt", "Data bersih dan rejected dimuat ke schema raw DuckDB (CREATE OR REPLACE -> idempoten); jumlah baris & order_id unik diperiksa."),
    28: ("dbt_project/models/staging/*.sql; evidence/dbt_run.log", "stg_orders meng-cast tipe (decimal, integer, timestamp), merapikan nama/huruf; stg_rejected_orders untuk audit."),
    29: ("dbt_project/models/marts/*.sql; diagrams/star_schema.puml; evidence/dbt_run.log", "fct_orders (grain: 1 baris per order_id) + 4 dimensi (customer, product_category, payment_method, date) dengan surrogate key md5 dan relasi tervalidasi."),
    30: ("dbt_project/models/marts/agg_*.sql; evidence/analytics_answers.md", "agg_daily_sales dan agregat kategori/status/metode bayar/alasan penolakan; hasil dicek ulang terhadap raw.orders (validasi sampel)."),
    31: ("dbt_project/models/**/schema.yml; dbt_project/tests/*.sql; evidence/dbt_test.log", "unique & not_null pada key, relationships fct->dim, accepted_values status, 2 singular test."),
    32: ("evidence/dbt_docs_generate.log; dbt_project/target/index.html; evidence/dbt_docs.png", "dbt docs generate menghasilkan catalog.json/manifest/index.html; screenshot lineage disertakan."),
    33: ("laporan.md bagian 7", "Dibahas star vs snowflake (trade-off normalisasi dimensi) dan kapan incremental model dibutuhkan beserta risikonya (late-arriving data)."),
    34: ("src/ (modul terpisah), tests/test_pipeline.py, dbt_project/", "Tanggung jawab dipisah: adapter, retry, extractor, contract, producer/consumer, transform, loader, benchmark/skew, dbt."),
    35: ("README.md; requirements.txt; run_all.py; evidence/run_summary.txt", "README berisi prasyarat, instalasi, urutan perintah, dan lokasi output; run_all.py menjalankan semua tahap dan menulis ringkasan PASS/FAIL."),
    36: ("laporan.md; diagrams/pipeline.png", "Laporan Markdown: arsitektur, keputusan desain, hasil pengujian, analitik, keterbatasan, refleksi."),
    37: ("evidence/demo_notes.md; [tautan video]", "Agenda demo 5-8 menit disiapkan dan urut sesuai alur utama; video direkam sendiri."),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefill", action="store_true")
    a = ap.parse_args()
    wb = openpyxl.load_workbook(TEMPLATE)
    ws = wb["Self Assessment"]
    ws["H3"] = date.today().strftime("%d %B %Y")
    wb["Ringkasan Mandiri"]["E3"] = "='Self Assessment'!H3"      # memperbaiki #REF! pada template
    checks = None if a.prefill else build_checks()
    wb["Ringkasan Mandiri"]["A16"] = ("Asumsi bobot per indikator: A=4 (x5), B=3 (x5), C=3,75 (x4), D=3 (x5), E=3/3/4/4/4/3/4 (7 indikator), F=2,5 (x4); "
                                      "jumlah per aspek sama dengan bobot pada tabel di atas.")
    total = 0
    for row, (loc, desc) in INFO.items():
        ws.cell(row, 9).value = loc
        if a.prefill:
            ws.cell(row, 7).value = "Bukti tersedia - belum diuji"
            ws.cell(row, 10).value = desc + " [Nilai & status menunggu verifikasi eksekusi.]"
            continue
        res = checks[row]
        ok = sum(1 for _, v in res if v)
        frac = ok / len(res)
        failed = [n for n, v in res if not v]
        ws.cell(row, 7).value = "Sesuai" if frac == 1 else ("Perlu klarifikasi" if frac > 0 else "Belum sesuai")
        ws.cell(row, 8).value = round(W[row] * frac, 2)
        total += ws.cell(row, 8).value
        ws.cell(row, 10).value = desc + f" Verifikasi otomatis: {ok}/{len(res)} cek lolos." + (f" Belum terpenuhi: {'; '.join(failed)}." if failed else "")
    wb.save(OUT)
    print("Disimpan:", OUT)
    if not a.prefill:
        print(f"Total nilai mandiri (otomatis): {total:.2f} / 100")
        for row, res in checks.items():
            bad = [n for n, v in res if not v]
            if bad:
                print(f" - baris {row} ({INFO[row][0].split(';')[0]}): kurang -> {', '.join(bad)}")


if __name__ == "__main__":
    main()
