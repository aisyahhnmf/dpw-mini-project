# Agenda Demo (target 6–7 menit) — `evidence/demo_notes.md`

Rekam layar (OBS / Win+G) sambil menjelaskan. Urutan & fungsi yang diperlihatkan:

| Menit | Tampilkan | Poin yang diucapkan |
|---|---|---|
| 0:00–0:40 | `diagrams/pipeline.png` + struktur folder | arsitektur & 5 topik yang terintegrasi |
| 0:40–1:50 | `log_batch_run1/2/3` + `incremental_run1_run2.png` | pagination 4 halaman (page_size=10), retry/backoff 0.5→1→2 s, watermark, run2 baca 0, run3 tambah 0 (idempoten) |
| 1:50–2:30 | `rejected/rejected_orders_batch.csv` + `validation_cases.csv` | aturan kontrak, alasan penolakan spesifik, pemisahan valid/rejected |
| 2:30–4:00 | `docker compose ps`, describe topic, producer, consumer, duplicate demo, `stream_summary.txt` | 3 partition, group, offset, at-least-once vs exactly-once, sink idempoten, batas jaminan |
| 4:00–5:00 | `pandas_polars_benchmark.csv`, `lazy_query_plan.txt`, `skew_experiment.md` | benchmark setara, lazy vs eager, skew terukur, mitigasi, keterbatasan |
| 5:00–6:30 | `dbt run`, `dbt test`, dbt docs (lineage), `analytics_answers.md` | star schema, tests unique/not_null/relationships, jawaban 5 pertanyaan bisnis |
| 6:30–7:00 | Refleksi | keterbatasan + apa yang akan diperbaiki |

Tautan video: **[ISI setelah upload — mis. Google Drive/YouTube unlisted]**
