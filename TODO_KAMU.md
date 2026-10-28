# ✅ CHECKLIST KAMU — selesaikan sebelum 23.00

Yang sudah saya siapkan: seluruh kode, dbt project, tes, diagram, README, laporan (kerangka), paket Modul 3, dan skrip pengisi self-assessment.
Yang **tidak bisa** saya kerjakan: menjalankan Docker/Kafka/dbt/Polars/DuckDB (sandbox saya tanpa internet), screenshot, video demo, dan mengisi Nama/NRP. Semua angka bukti harus berasal dari **eksekusi di laptopmu**.

## Urutan kerja (estimasi total ±3 jam)
| ± Waktu | Langkah | Perintah |
|---|---|---|
| 10 mnt | Setup | `python -m venv .venv` → aktifkan → `pip install -r requirements.txt` → nyalakan Docker Desktop |
| 5 mnt | Batch (tanpa Docker) | `python run_all.py --steps batch` |
| 10 mnt | Kafka mini project | `python run_all.py --steps kafka` |
| 5 mnt | Transform + DuckDB | `python run_all.py --steps warehouse` |
| 10 mnt | dbt | `python run_all.py --steps dbt` lalu `cd dbt_project && dbt docs serve --profiles-dir .` → screenshot lineage → simpan **evidence/dbt_docs.png** |
| 10 mnt | Benchmark + skew | `python run_all.py --steps bench` (jika lambat: `--bench-rows 300000`) |
| 60–70 mnt | **Modul 3** (screenshot E01–E09 + tantangan) | ikuti `kafka_modul3/RUN_MODUL3.md` |
| 20 mnt | Lengkapi **[ISI]** di `laporan.md` & `kafka_modul3/praktikum_bagian3/experiment-report.md` | salin angka dari `evidence/` |
| 15 mnt | Rekam demo 5–8 mnt (ikuti `evidence/demo_notes.md`), upload, tempel tautan di `demo_notes.md` | |
| 5 mnt | Isi sheet | `python finalize_self_assessment.py` → buka `..._TERISI.xlsx`, isi **B3 (Nama)** & **E3 (NRP)** |

## Jika ada error
Salin pesan error + isi `evidence/run_summary.txt` ke chat; saya bantu perbaiki. Kode saya uji logikanya (pagination, retry, watermark, idempotensi, sink Kafka), tetapi **belum dijalankan dengan Pydantic/Polars/DuckDB/dbt/Kafka asli**, jadi kemungkinan ada penyesuaian kecil.

## Kejujuran self-assessment
- `finalize_self_assessment.py` menghitung nilai dari bukti nyata; bagian yang belum kamu lengkapi (screenshot, video, [ISI]) otomatis menurunkan nilai. Jangan edit nilai menjadi penuh tanpa bukti.
- Pelajari kode sebelum demo (dosen akan bertanya). Cantumkan penggunaan alat bantu AI di laporan bagian 10 sesuai aturan mata kuliah.
- Asumsi bobot per indikator ada di sheet "Ringkasan Mandiri" sel A16 — sesuaikan jika dosen menetapkan lain.

## Soal GitHub
Koneksi GitHub-mu tidak muncul sebagai tool di sesi saya, jadi saya tidak bisa push. Setelah semua jalan: `git init && git add . && git commit -m "mini project" && git remote add origin <url> && git push -u origin main` (tambahkan `.venv/` dan `warehouse/*.wal` ke .gitignore; jangan commit file data besar `processed/bench_*.csv`).
