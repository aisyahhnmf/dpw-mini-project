"""Consumer: baca event -> validasi kontrak -> tulis valid/rejected ke file terpisah.

Semantik (batas jaminan yang BENAR):
  * enable.auto.commit=False; offset di-commit SESUDAH sink ditulis  => AT-LEAST-ONCE.
    Jika proses mati antara 'tulis sink' dan 'commit', pesan dibaca ulang (duplikat teknis).
  * Duplikasi BISNIS dicegah oleh sink yang idempotent: order_id yang sudah ada di stream_valid.jsonl
    dilewati. Ini BUKAN exactly-once end-to-end; hanya efek bisnis yang idempotent.
  * Sink berupa file JSONL (bukan transaksi Kafka) -> tidak ada jaminan atomik antara sink dan offset.
"""
import argparse, hashlib, json
from datetime import datetime, timezone
from . import config as C
from .contract import validate_record
from .logging_setup import get_logger

log = get_logger("consumer")


def load_seen_ids() -> set:
    seen = set()
    if C.STREAM_VALID_JSONL.exists():
        with open(C.STREAM_VALID_JSONL, encoding="utf-8") as f:
            seen = {json.loads(line)["order_id"] for line in f if line.strip()}
    return seen


def load_seen_rejected() -> set:
    seen = set()
    if C.STREAM_REJECTED_JSONL.exists():
        with open(C.STREAM_REJECTED_JSONL, encoding="utf-8") as f:
            seen = {json.loads(line).get("payload_sha1") for line in f if line.strip()}
    return seen


def process_message(raw_value: bytes, seen: set, valid_f, rej_f, counters: dict, meta: dict, seen_rej: set | None = None) -> None:
    """Satu pesan -> valid / rejected / duplicate. Dipisah agar bisa diuji tanpa Kafka."""
    counters["read"] += 1
    seen_rej = seen_rej if seen_rej is not None else set()
    sha = hashlib.sha1(raw_value).hexdigest()
    try:
        rec = json.loads(raw_value)
    except json.JSONDecodeError as exc:
        if sha in seen_rej:
            counters["duplicate_skipped"] += 1
            return
        seen_rej.add(sha)
        rej_f.write(json.dumps({"raw": raw_value.decode("utf-8", "replace"), "reject_reason": f"JSON rusak: {exc}",
                                "reject_fields": "record", "source": "kafka", "payload_sha1": sha, **meta}) + "\n")
        counters["rejected"] += 1
        return
    ok, err = validate_record(rec)
    if ok is None:
        if sha in seen_rej:                  # replay record cacat yang sama -> tidak ditulis ulang
            counters["duplicate_skipped"] += 1
            return
        seen_rej.add(sha)
        rej_f.write(json.dumps({**rec, "reject_reason": err["reason"], "reject_fields": err["fields"],
                                "rejected_at": datetime.now(timezone.utc).isoformat(), "source": "kafka",
                                "payload_sha1": sha, **meta}) + "\n")
        counters["rejected"] += 1
        return
    if ok["order_id"] in seen:               # IDEMPOTENSI SINK
        counters["duplicate_skipped"] += 1
        log.info("DUPLIKAT dilewati order_id=%s (sink idempotent)", ok["order_id"])
        return
    seen.add(ok["order_id"])
    valid_f.write(json.dumps({**ok, "source": "kafka", **meta}) + "\n")
    counters["valid"] += 1


def main():
    from confluent_kafka import Consumer
    p = argparse.ArgumentParser()
    p.add_argument("--topic", default=C.KAFKA_TOPIC)
    p.add_argument("--group", default=C.KAFKA_GROUP)
    p.add_argument("--reset", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--name", default="consumer-1")
    p.add_argument("--idle-timeout", type=int, default=15, help="berhenti jika tidak ada pesan N detik (0=tanpa batas)")
    p.add_argument("--fail-after", type=int, default=0, help="simulasi crash SETELAH sink ditulis, SEBELUM commit")
    a = p.parse_args()

    C.PROCESSED_DIR.mkdir(exist_ok=True); C.REJECTED_DIR.mkdir(exist_ok=True)
    c = Consumer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "group.id": a.group, "client.id": a.name,
                  "auto.offset.reset": a.reset, "enable.auto.commit": False})
    c.subscribe([a.topic])
    seen = load_seen_ids()
    seen_rej = load_seen_rejected()
    counters = {"read": 0, "valid": 0, "rejected": 0, "duplicate_skipped": 0}
    idle = 0
    log.info("[%s] mulai | topic=%s group=%s reset=%s | order_id sudah ada di sink: %d", a.name, a.topic, a.group, a.reset, len(seen))
    try:
        with open(C.STREAM_VALID_JSONL, "a", encoding="utf-8") as vf, open(C.STREAM_REJECTED_JSONL, "a", encoding="utf-8") as rf:
            while True:
                m = c.poll(1.0)
                if m is None:
                    idle += 1
                    if a.idle_timeout and idle >= a.idle_timeout:
                        break
                    continue
                idle = 0
                if m.error():
                    log.error("%s", m.error()); continue
                meta = {"kafka_partition": m.partition(), "kafka_offset": m.offset()}
                process_message(m.value(), seen, vf, rf, counters, meta, seen_rej)
                vf.flush(); rf.flush()
                log.info("[%s] partition=%d offset=%d key=%s", a.name, m.partition(), m.offset(), m.key().decode() if m.key() else None)
                if a.fail_after and counters["read"] >= a.fail_after:
                    log.error("[%s] CRASH disengaja sebelum commit (pesan akan dibaca ulang)", a.name)
                    raise SystemExit(1)
                c.commit(message=m, asynchronous=False)   # commit SESUDAH sink -> at-least-once
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        log.info("[%s] SELESAI | %s", a.name, json.dumps(counters))


if __name__ == "__main__":
    main()
