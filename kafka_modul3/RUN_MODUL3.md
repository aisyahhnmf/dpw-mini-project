# Panduan Menjalankan Modul 3 (Kafka) — kerjakan di laptopmu, ±60–70 menit

> Semua bukti Modul 3 harus berasal dari **eksekusi di mesinmu** (screenshot E01–E09 + 3 evidence tantangan).
> Saya tidak bisa menjalankan Docker/Kafka di lingkungan saya, jadi tidak ada angka hasil yang saya isikan untuk Anda.
> Buka 3 terminal di folder `kafka_modul3/praktikum_bagian3/`. Hanya satu stack Docker yang boleh aktif (port 9092 sama): `docker compose down` sebelum pindah folder.

## Catatan penting (celah di PDF)
Perintah bagian 3.8 di PDF tidak menyebut `--input/--output` padahal wajib. Gunakan topic **baru** (satu pasang per mode, supaya hitungan 10 event bersih):
```
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --create --topic dpw3-alo-in  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --create --topic dpw3-alo-out --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --create --topic dpw3-eos-in  --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --create --topic dpw3-eos-out --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```
Jelaskan di laporan bahwa topic tambahan ini dibuat karena PDF tidak menetapkan nama input/output.
(Pada Windows PowerShell, tulis perintah dalam SATU baris — tanda `\` diganti spasi.)

## 3.1 → 3.2 Environment & broker (E01, E02)
```
docker --version ; docker compose version ; python --version
python -c "import confluent_kafka; print(confluent_kafka.version())"
docker compose config ; docker compose up -d ; docker compose ps
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --create --topic dpw3-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker compose exec broker /opt/kafka/bin/kafka-topics.sh --describe --topic dpw3-events --bootstrap-server localhost:9092
```
📸 **E01-environment.png** (4 versi) · **E02-broker.png** (`ps` + describe: PartitionCount 3).

## 3.3 Producer–Consumer (E03)
T1: `python app/consumer.py --topic dpw3-events --group dpw3-basic --reset earliest` · T2: `python app/producer.py --topic dpw3-events --count 10`
Catat 3 event (event_id, partition, offset). Ctrl+C consumer → `producer --count 5` (tetap bisa kirim) → jalankan consumer lagi (group sama).
📸 **E03-producer-consumer.png**.

## 3.4 Partition (E04)
`python app/producer.py --topic dpw3-events --count 15 --key-mode entity` → `python app/consumer.py --topic dpw3-events --group dpw3-partition --reset earliest` → `docker compose exec broker /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic dpw3-events`
📸 **E04-partition.png**.

## 3.5 Consumer group (E05)
T1: `python app/consumer.py --topic dpw3-events --group dpw3-group --consumer-name C1` · T2: sama dengan `--consumer-name C2` · T3: `producer --count 20`
Ctrl+C C2 → amati C1 → `docker compose exec broker /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group dpw3-group`
📸 **E05-consumer-group.png** (sebelum dan/atau sesudah C2 berhenti).

## 3.6 Offset (E06)
`python app/consumer.py --topic dpw3-events --group dpw3-offset --reset earliest --stop-after 5` → `kafka-consumer-groups.sh --describe --group dpw3-offset` → jalankan consumer lagi tanpa `--reset` → describe lagi.
⚠️ Catatan: `consumer.py` hanya commit offset bila `--manual-commit`. Tanpa flag itu CURRENT-OFFSET mungkin tidak berubah; gunakan `--manual-commit` pada langkah ini bila tabel offset kosong, dan **catat** itu di laporan sebagai temuan (tanpa mengubah source code).
📸 **E06-offset.png**.

## 3.7 Failure injection (E07)
T1: `consumer --group dpw3-failure --consumer-name C1 --fail-after 3` · T2: `consumer --group dpw3-failure --consumer-name C2` · T3: `producer --count 10`
Setelah C1 crash → `consumer --group dpw3-failure --consumer-name C1-recovery` → `kafka-consumer-groups.sh --describe --group dpw3-failure`.
📸 **E07-failure.png** (susun FAIL→OBSERVE→DIAGNOSE→FIX→VERIFY di report).

## 3.8 At-least-once vs Exactly-once (E08, E09)
Isi tiap input topic dengan 10 event: `python app/producer.py --topic dpw3-alo-in --count 10` dan `--topic dpw3-eos-in --count 10`.
**At-least-once**
```
python app/consume_transform_produce.py --input dpw3-alo-in --output dpw3-alo-out --mode atleast-once --fail-after-produce
python app/consume_transform_produce.py --input dpw3-alo-in --output dpw3-alo-out --mode atleast-once      # biarkan jalan beberapa detik, Ctrl+C
python app/inspect.py --topic dpw3-alo-out --count 11
```
(Perkiraan: total 11, unik 10, duplikat 1 — **pakai angka aktualmu**; jika hanya 10 pesan terbaca, `inspect` menunggu; Ctrl+C.)
**Exactly-once**
```
python app/consume_transform_produce.py --input dpw3-eos-in --output dpw3-eos-out --mode exactly-once --fail-before-commit
python app/consume_transform_produce.py --input dpw3-eos-in --output dpw3-eos-out --mode exactly-once       # Ctrl+C setelah beberapa detik
python app/inspect.py --topic dpw3-eos-out --isolation read_committed --count 10
```
📸 **E08-atleast-once.png**, **E09-exactly-once.png** (isi tabel perbandingan di `experiment-report.md`).

## Tantangan mandiri (Bagian 4) — `tantangan_bagian4/`
1. `docker compose down` lalu masuk `tantangan_bagian4/changed-files/` (docker-compose starter) → `docker compose up -d`.
2. Buat topic: `docker compose exec broker /opt/kafka/bin/kafka-topics.sh --create --topic dpw3-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1`
3. Salin starter `student-starter/app/producer.py` + `config.py` ke `changed-files/app/` (producer starter sudah memakai key=order_id), dan `data/payment-events.json` ke `changed-files/data/`.
4. T1: `set CONSUMER_NAME=C1 & set FAIL_AFTER=3 & python app/consumer.py` (PowerShell: `$env:CONSUMER_NAME="C1"; $env:FAIL_AFTER="3"; python app/consumer.py`) · T2: `CONSUMER_NAME=C2` tanpa FAIL_AFTER · T3: `python app/producer.py`
5. Setelah C1 crash: jalankan C1 lagi (tanpa FAIL_AFTER). Buka `processing-log.csv` (urutan per order, ORDER_OK, DUPLICATE_SKIPPED).
6. 📸 **E01-configuration.png** (describe topic + group + isi config/cmd), **E02-ordering.png** (log/`processing-log.csv` per order), **E03-failure-recovery.png** (crash → REVOKED/ASSIGNED → lanjut).
7. Perbarui bagian *Failure Analysis* di `technical-analysis.md` dengan hasil aktualmu (maks. 5 kalimat) dan ubah teks jika hasilmu berbeda.
