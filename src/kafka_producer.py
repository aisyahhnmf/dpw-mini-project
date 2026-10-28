"""Producer: menerbitkan event pesanan dari CSV ke topic `orders`.
Key = order_id -> event dengan order_id sama selalu ke partition yang sama (urutan terjaga).
--duplicate-demo : event pertama dikirim DUA kali (simulasi redelivery/replay) untuk uji idempotensi sink.
"""
import argparse, csv, json, time
from . import config as C
from .logging_setup import get_logger

log = get_logger("producer")


def main():
    from confluent_kafka import Producer  # import di sini agar modul bisa di-import tanpa Kafka
    p = argparse.ArgumentParser()
    p.add_argument("--topic", default=C.KAFKA_TOPIC)
    p.add_argument("--source", default=str(C.STREAM_CSV))
    p.add_argument("--count", type=int, default=40)
    p.add_argument("--duplicate-demo", action="store_true")
    a = p.parse_args()

    with open(a.source, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))[: a.count]
    if a.duplicate_demo and rows:
        rows.append(dict(rows[0]))          # kirim ulang event pertama -> order_id sama dua kali
        log.info("DUPLICATE DEMO: order_id=%s dikirim 2x", rows[0]["order_id"])

    delivered, failed = [0], [0]

    def on_delivery(err, msg):
        if err:
            failed[0] += 1
            log.error("gagal terkirim: %s", err)
        else:
            delivered[0] += 1
            log.info("terkirim key=%s partition=%d offset=%d", msg.key().decode() if msg.key() else None,
                     msg.partition(), msg.offset())

    producer = Producer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "acks": "all", "enable.idempotence": True})
    for r in rows:
        producer.produce(a.topic, key=r["order_id"] or None, value=json.dumps(r), on_delivery=on_delivery)
        producer.poll(0)
        time.sleep(0.02)
    producer.flush()
    log.info("PRODUCER SELESAI | dikirim=%d terkonfirmasi=%d gagal=%d topic=%s", len(rows), delivered[0], failed[0], a.topic)


if __name__ == "__main__":
    main()
