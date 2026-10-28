# Experiment Report — DPW Modul 3

> Bagian bertanda **[ISI]** harus diisi dari hasil eksekusimu sendiri. Penjelasan konsep di bawah ditulis ulang dari pemahaman teknis, bukan hasil eksperimen.

## 1. Partition Analysis
**Action:** Mengirim 15 event ke `dpw3-events` dengan `--key-mode entity`, lalu membaca dengan group `dpw3-partition`.
**Observation:** [ISI] tabel event_id / partition / offset (min. 5 baris) + output `kafka-get-offsets`.
**Conclusion:** Satu topic = beberapa partition (3). Event dengan key sama selalu ke partition yang sama (hash(key) mod jumlah partition); event berbeda dapat tersebar. Offset adalah posisi record **di dalam satu partition**, sehingga offset 5 di partition 0 bukan record yang sama dengan offset 5 di partition 1.

## 2. Consumer Group Analysis
**Action:** Dua consumer (C1, C2) pada group `dpw3-group`, produce 20 event, lalu C2 dihentikan.
**Observation:** [ISI] pembagian partition ke C1/C2 sebelum dan sesudah C2 berhenti (dari log + `--describe`).
**Conclusion:** Dalam satu group, tiap partition dimiliki **paling banyak satu** consumer pada satu waktu, sehingga beban terbagi tanpa pemrosesan ganda. Saat anggota keluar, Kafka melakukan *rebalance*; jumlah partition topic tidak berubah, yang berubah adalah *assignment*.

## 3. Offset Analysis
**Action:** Consumer group `dpw3-offset` membaca 5 event lalu berhenti; dijalankan ulang tanpa `--reset`.
**Observation:** [ISI] CURRENT-OFFSET, LOG-END-OFFSET, LAG sebelum/sesudah.
**Conclusion:** Committed offset disimpan per (group, topic, partition); consumer yang kembali melanjutkan dari posisi itu, bukan dari awal. `auto.offset.reset` hanya berlaku bila group belum punya committed offset. LAG = LOG-END-OFFSET − CURRENT-OFFSET.

## 4. Failure Analysis
**Action:** C1 dijalankan dengan `--fail-after 3`, C2 aktif, producer 10 event.
**Observation:** [ISI] log C1 crash, log C2 setelah rebalance, `--describe` group.
**Diagnosis:** Crash mengubah keanggotaan group → rebalance; partition milik C1 dipindah ke C2. Event yang belum di-commit akan dibaca ulang.
**Fix:** Menjalankan `C1-recovery` dengan group sama (tanpa perubahan kode).
**Verification:** [ISI] `--describe` menunjukkan member aktif & lag turun.

## 5. At-least-once vs Exactly-once
| Parameter | At-least-once | Exactly-once |
|---|---:|---:|
| Input event | 10 | 10 |
| Total output | [ISI] | [ISI] |
| Unique event_id | [ISI] | [ISI] |
| Duplicate output | [ISI] | [ISI] |
| Failure terjadi | Ya | Ya |
| Output transaction committed | Tidak berlaku | [ISI] |

1. **Mengapa ALO menghasilkan duplikat?** Output sudah ditulis, tetapi proses mati sebelum offset di-commit; saat restart pesan yang sama dibaca ulang dan outputnya ditulis lagi.
2. **Mengapa EOS tidak menghasilkan duplikat committed?** Output dan offset konsumsi berada dalam satu transaksi Kafka; transaksi yang di-abort tidak terlihat oleh consumer `read_committed`, dan offset tidak maju.
3. **Perbedaan hasil:** [ISI berdasarkan angka aktual]. **Batas jaminan:** exactly-once di sini hanya untuk alur Kafka→Kafka; sistem eksternal (DB/file) tetap butuh sink idempoten.

## 6. Technical Reflection
Partition menentukan paralelisme dan batas urutan; consumer group menentukan siapa memproses partition mana; offset menentukan dari mana pembacaan dilanjutkan; failure menentukan kapan offset belum tersimpan sehingga pesan diproses ulang. Kombinasi *kapan commit* dan *apakah sink idempoten* menentukan duplikat vs kehilangan data. [ISI: tambahkan 1–2 kalimat dari pengalamanmu, mis. perilaku offset pada 3.6.]

## Checkpoint 3.10 (jawaban singkat)
1. Partition membagi topic agar bisa paralel & menjaga urutan per key.
2. Karena Kafka memberikan partition berbeda ke tiap anggota group agar beban terbagi.
3. Committed offset = posisi terakhir yang disimpan group untuk melanjutkan konsumsi.
4. Terjadi rebalance; partition anggota yang gagal dipindah ke anggota aktif.
5. Lihat tabel bagian 5 (berdasarkan hasilmu).
