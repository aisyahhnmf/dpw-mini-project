| Metrik | Run 1 (full load) | Run 2 (watermark) | Run 3 (baca ulang penuh) |
|---|---|---|---|
| watermark masuk | None | 2026-09-11T15:00:00 | None |
| watermark keluar | 2026-09-11T15:00:00 | 2026-09-11T15:00:00 | 2026-09-11T15:00:00 |
| halaman | 4 | 1 | 4 |
| retry | 3 | 0 | 0 |
| record dibaca | 36 | 0 | 36 |
| raw ditambahkan | 36 | 0 | 0 |
| valid ditambahkan | 30 | 0 | 0 |
| ditolak | 6 | 0 | 0 |
| total raw | 36 | 36 | 36 |
| total valid | 30 | 30 | 30 |
| order_id unik (valid) | 30 | 30 | 30 |

Kesimpulan: eksekusi ulang tidak menambah data (raw/valid_appended = 0) dan jumlah order_id unik tetap.
