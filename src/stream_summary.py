"""Ringkasan hasil streaming: jumlah valid/rejected/duplikat & penyebab penolakan (bukti B06/B07)."""
import json, collections
from . import config as C
def rd(p):
    return [json.loads(x) for x in open(p, encoding="utf-8") if x.strip()] if p.exists() else []
v, r = rd(C.STREAM_VALID_JSONL), rd(C.STREAM_REJECTED_JSONL)
print(f"valid di sink   : {len(v)}  (order_id unik: {len({x['order_id'] for x in v})})")
print(f"rejected di sink: {len(r)}")
print("distribusi partition (valid):", dict(collections.Counter(x['kafka_partition'] for x in v)))
print("alasan penolakan:"); [print("  -", k, ":", n) for k, n in collections.Counter(x['reject_fields'] for x in r).most_common()]
print("\nContoh rejected:"); [print(" ", x.get('order_id'), '|', x['reject_reason'][:110]) for x in r[:5]]
