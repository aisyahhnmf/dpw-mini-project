# Technical Analysis

## Architecture Decision

Partition: 3 — paralelisme antar-order.

Consumer Group: `payment-processors` (satu group; partition dibagi antar anggota).

Consumer: 2 (C1, C2) — satu cadangan saat rebalance.

Event Key / Ordering: key = `order_id` → semua event satu order masuk partition sama sehingga urut; antar-order paralel.

Offset / Commit: auto-commit mati; commit manual SESUDAH diproses; `auto.offset.reset=earliest`.

Delivery Semantics: at-least-once + pemrosesan idempotent (dedup `event_id`); bukan exactly-once.

Alasan: ordering hanya dijamin per partition, maka key harus order_id. Commit sesudah proses mencegah event hilang; duplikat akibat crash dineutralkan idempotensi.

## Failure Analysis

Action: C1 dihentikan (FAIL_AFTER=3) sebelum commit; C2 tetap jalan, lalu C1 dijalankan lagi.

Observation: log C2 menunjukkan REVOKED/ASSIGNED; event terakhir C1 muncul lagi di C2.

Diagnosis: offset belum di-commit, jadi group membaca ulang; rebalance memindahkan partition.

Fix: biarkan C2 mengambil alih; jalankan ulang C1 dengan group sama.

Verification: processing-log.csv menandai `DUPLICATE_SKIPPED`, tidak ada `ORDER_VIOLATION`, semua 8 event terproses.
