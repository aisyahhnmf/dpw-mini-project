"""Consumer Payment Event Stream (perubahan dari starter).

Perubahan: (1) CONSUMER_NAME untuk membedakan instance, (2) log pemrosesan ke processing-log.csv,
(3) pemeriksaan URUTAN per order (CREATED -> PAID -> SHIPPED), (4) idempotensi pemrosesan via event_id
(processed_event_ids.txt) agar redelivery setelah failure tidak memproses ulang efek bisnisnya,
(5) commit manual SESUDAH diproses => at-least-once.
"""
import csv, json, os, time
from pathlib import Path
from confluent_kafka import Consumer

BROKER = "localhost:9092"
TOPIC = "dpw3-events"
GROUP = os.getenv("GROUP_ID", "payment-processors")
NAME = os.getenv("CONSUMER_NAME", "C1")
FAIL_AFTER = int(os.getenv("FAIL_AFTER", "0"))
STAGE = {"CREATED": 1, "PAID": 2, "SHIPPED": 3}

here = Path(__file__).parent.parent
LOG = here / "processing-log.csv"
DONE = here / "processed_event_ids.txt"

done_ids = set(DONE.read_text().split()) if DONE.exists() else set()
last_stage = {}
if not LOG.exists():
    LOG.write_text("ts,consumer,event_id,order_id,event_type,partition,offset,status\n")

c = Consumer({"bootstrap.servers": BROKER, "group.id": GROUP, "client.id": NAME,
              "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"), "enable.auto.commit": False})
c.subscribe([TOPIC],
            on_assign=lambda cons, parts: print(f"[{NAME}] ASSIGNED partitions={sorted(p.partition for p in parts)}"),
            on_revoke=lambda cons, parts: print(f"[{NAME}] REVOKED partitions={sorted(p.partition for p in parts)}"))
print(f"[{NAME}] group={GROUP} fail_after={FAIL_AFTER}")
count = 0
try:
    while True:
        m = c.poll(1.0)
        if m is None:
            continue
        if m.error():
            print("[ERROR]", m.error()); continue
        e = json.loads(m.value())
        count += 1
        if e["event_id"] in done_ids:
            status = "DUPLICATE_SKIPPED"
        else:
            stage = STAGE[e["event_type"]]
            prev = last_stage.get(e["order_id"], 0)
            status = "ORDER_OK" if stage > prev else "ORDER_VIOLATION"
            last_stage[e["order_id"]] = max(prev, stage)
            time.sleep(0.8)                              # simulasi pemrosesan
            with open(DONE, "a") as f:
                f.write(e["event_id"] + "\n")
            done_ids.add(e["event_id"])
        print(f"[{NAME}] {e['event_id']} order={e['order_id']} type={e['event_type']} p={m.partition()} o={m.offset()} {status}")
        with open(LOG, "a", newline="") as f:
            csv.writer(f).writerow([time.strftime("%H:%M:%S"), NAME, e["event_id"], e["order_id"], e["event_type"], m.partition(), m.offset(), status])
        if FAIL_AFTER and count == FAIL_AFTER:
            print(f"[{NAME}] FAILURE disengaja SETELAH proses, SEBELUM commit")
            raise SystemExit(1)
        c.commit(message=m, asynchronous=False)
finally:
    c.close()
