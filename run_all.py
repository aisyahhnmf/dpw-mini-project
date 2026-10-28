"""Orkestrator: menjalankan tiap tahap & menyimpan log ke evidence/ (bukti untuk self-assessment).

  python run_all.py --steps batch          # A, C (tanpa Docker)
  python run_all.py --steps kafka          # B (butuh Docker Desktop menyala)
  python run_all.py --steps warehouse      # D transform + DuckDB
  python run_all.py --steps dbt            # E (butuh dbt-duckdb)
  python run_all.py --steps bench          # D benchmark + skew
  python run_all.py --steps all
Gagal di satu langkah tidak menghentikan langkah lain; ringkasan PASS/FAIL ada di evidence/run_summary.txt.
"""
import argparse, json, os, shutil, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
EV = ROOT / "evidence"
EV.mkdir(exist_ok=True)
PY = sys.executable
RESULTS = []


def run(name, cmd, logfile, cwd=ROOT, check_text=None):
    print(f"\n>>> [{name}] {' '.join(map(str, cmd))}")
    t = time.time()
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=900,
                           env={**os.environ, "PYTHONUTF8": "1"})
        out = f"$ {' '.join(map(str, cmd))}\n[{time.strftime('%Y-%m-%d %H:%M:%S')}]\n\n{p.stdout}\n{p.stderr}\n[exit code {p.returncode}]\n"
        ok = p.returncode == 0 and (check_text is None or check_text in out)
    except Exception as exc:
        out, ok = f"$ {cmd}\nGAGAL MENJALANKAN: {exc}\n", False
    mode = "a" if Path(EV / logfile).exists() and logfile.startswith("_append_") else "w"
    (EV / logfile.replace("_append_", "")).open(mode, encoding="utf-8").write(out)
    print(out[-1500:])
    RESULTS.append((name, "PASS" if ok else "FAIL", round(time.time() - t, 1), logfile))
    return ok


def step_batch():
    run("batch run1 (reset + 3 simulasi error)", [PY, "-m", "src.run_batch", "--reset", "--simulate-failures", "3"], "log_batch_run1.txt")
    run("batch run2 (watermark)", [PY, "-m", "src.run_batch"], "log_batch_run2.txt")
    run("batch run3 (baca ulang penuh, uji idempotensi)", [PY, "-m", "src.run_batch", "--ignore-watermark"], "log_batch_run3_full_reread.txt")
    run("cek parquet", [PY, "-m", "src.check_parquet"], "raw_parquet_check.txt")
    run("evidence incremental", [PY, "-m", "src.make_incremental_evidence"], "incremental_make.txt")
    run("evidence validasi", [PY, "-m", "src.export_evidence_tables", "--cases-only"], "validation_cases_make.txt")
    run("pytest", [PY, "-m", "pytest", "-q", "-p", "no:cacheprovider"], "pytest_result.txt")


def step_kafka():
    dc = ["docker", "compose"]
    ok = run("docker compose up", dc + ["up", "-d"], "kafka_status.txt")
    time.sleep(12)
    run("topic create", dc + ["exec", "broker", "/opt/kafka/bin/kafka-topics.sh", "--create", "--if-not-exists", "--topic", "orders",
        "--bootstrap-server", "localhost:9092", "--partitions", "3", "--replication-factor", "1"], "_append_kafka_status.txt")
    run("topic describe", dc + ["exec", "broker", "/opt/kafka/bin/kafka-topics.sh", "--describe", "--topic", "orders",
        "--bootstrap-server", "localhost:9092"], "_append_kafka_status.txt", check_text="PartitionCount: 3")
    run("docker ps", dc + ["ps"], "_append_kafka_status.txt")
    for f in (ROOT / "processed" / "stream_valid.jsonl", ROOT / "rejected" / "stream_rejected.jsonl"):
        f.unlink(missing_ok=True)
    (ROOT / "processed").mkdir(exist_ok=True); (ROOT / "rejected").mkdir(exist_ok=True)
    # 1) demo duplikasi: event pertama dikirim 2x
    run("producer duplicate-demo (5 event + 1 duplikat)", [PY, "-m", "src.kafka_producer", "--count", "5", "--duplicate-demo"], "kafka_events.txt")
    run("consumer #1", [PY, "-m", "src.kafka_consumer", "--idle-timeout", "10"], "_append_kafka_events.txt")
    # 2) kirim 40 event penuh (5 pertama = replay) -> duplikat dilewati sink
    run("producer 40 event", [PY, "-m", "src.kafka_producer", "--count", "40"], "_append_kafka_events.txt")
    run("consumer #2", [PY, "-m", "src.kafka_consumer", "--idle-timeout", "10"], "_append_kafka_events.txt")
    run("offset per partition", dc + ["exec", "broker", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", "orders"], "_append_kafka_status.txt")
    run("consumer group describe", dc + ["exec", "broker", "/opt/kafka/bin/kafka-consumer-groups.sh", "--bootstrap-server", "localhost:9092", "--describe", "--group", "orders-etl"], "_append_kafka_status.txt")
    run("ringkasan stream", [PY, "-m", "src.stream_summary"], "stream_summary.txt")


def step_warehouse():
    run("transform polars (lazy)", [PY, "-m", "src.transform"], "log_transform.txt")
    run("load duckdb", [PY, "-m", "src.load_warehouse"], "log_load_warehouse.txt")


def step_dbt():
    dbt = shutil.which("dbt")
    if not dbt:
        print("dbt tidak ditemukan; pip install dbt-duckdb"); RESULTS.append(("dbt", "FAIL", 0, "-")); return
    base = ["--profiles-dir", ".", "--project-dir", "."]
    d = ROOT / "dbt_project"
    run("dbt debug", [dbt, "debug"] + base, "dbt_debug.log", cwd=d)
    run("dbt run", [dbt, "run"] + base, "dbt_run.log", cwd=d)
    run("dbt test", [dbt, "test"] + base, "dbt_test.log", cwd=d)
    run("dbt docs generate", [dbt, "docs", "generate"] + base, "dbt_docs_generate.log", cwd=d)
    run("analytics", [PY, "-m", "src.analytics"], "analytics_run.log")
    run("evidence transformasi", [PY, "-m", "src.export_evidence_tables"], "transformation_make.txt")


def step_bench(rows):
    run("benchmark", [PY, "-m", "src.benchmark", "--rows", str(rows)], "benchmark_run.log")
    run("skew", [PY, "-m", "src.skew", "--rows", str(rows)], "skew_run.log")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default="all", choices=["batch", "kafka", "warehouse", "dbt", "bench", "all"])
    ap.add_argument("--bench-rows", type=int, default=1_000_000)
    a = ap.parse_args()
    s = a.steps
    if s in ("batch", "all"): step_batch()
    if s in ("kafka", "all"): step_kafka()
    if s in ("warehouse", "all"): step_warehouse()
    if s in ("dbt", "all"): step_dbt()
    if s in ("bench", "all"): step_bench(a.bench_rows)
    lines = [f"{n:48s} {st:5s} {sec:7.1f}s  {lf}" for n, st, sec, lf in RESULTS]
    (EV / "run_summary.txt").write_text("\n".join(lines), encoding="utf-8")
    print("\n===== RINGKASAN =====\n" + "\n".join(lines))
