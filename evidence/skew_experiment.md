# Eksperimen Data Skew

**Cara membuat skew:** dataset sintetis 300,000 baris; customer `C00537` dipilih sebagai dominan, barisnya digandakan 4053x hingga porsinya ±50% dari total (599,848 baris).

| dataset   |   rows |   n_customers |   max_group_rows |   top1_share_pct |   median_group_rows |   max_over_median |   pandas_groupby_median_s |   polars_groupby_median_s |   polars_partial_agg_chunked_median_s | dominant_customer   |
|:----------|-------:|--------------:|-----------------:|-----------------:|--------------------:|------------------:|--------------------------:|--------------------------:|--------------------------------------:|:--------------------|
| balanced  | 300000 |          5000 |               92 |             0.03 |                  60 |               1.5 |                    0.0203 |                    0.0072 |                                0.0062 | C00537              |
| skewed    | 599848 |          5000 |           299922 |            50    |                  60 |            4998.7 |                    0.0582 |                    0.0117 |                                0.0146 | C00537              |

**Lingkungan:** 8 CPU; pandas 3.1.0; polars 2.1.0; median dari 5 kali jalan.

## Interpretasi (isi setelah melihat angka di atas)
- Bandingkan `max_group_rows`, `top1_share_pct`, dan `max_over_median`: skew membuat satu grup jauh lebih besar dari median.
- Bandingkan waktu `group_by` sebelum vs sesudah. Pada mesin satu node, perbedaan waktu bisa kecil; skew terutama merugikan pada eksekusi paralel/terdistribusi karena satu worker memegang grup terbesar.
- Kolom `polars_partial_agg_chunked_median_s` menunjukkan mitigasi agregasi parsial per potongan 250 ribu baris.

## Keterbatasan
- Satu mesin, dataset sintetis, tanpa uji OOM: **tidak ada klaim OOM** karena memang tidak terjadi/diuji.
- Waktu sangat bergantung pada beban mesin, cache OS, dan jumlah core; ulangi di mesin berbeda untuk generalisasi.
- group_by tunggal pada satu node tidak mereplikasi masalah straggler pada cluster.