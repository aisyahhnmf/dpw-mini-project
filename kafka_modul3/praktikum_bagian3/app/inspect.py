import argparse,json
from confluent_kafka import Consumer
def main():
    p=argparse.ArgumentParser(); p.add_argument("--topic",required=True); p.add_argument("--group",default="dpw3-inspect"); p.add_argument("--reset",choices=["earliest","latest"],default="earliest"); p.add_argument("--isolation",choices=["read_uncommitted","read_committed"],default="read_uncommitted"); p.add_argument("--count",type=int,default=10); a=p.parse_args()
    c=Consumer({"bootstrap.servers":"localhost:9092","group.id":a.group,"auto.offset.reset":a.reset,"enable.auto.commit":False,"isolation.level":a.isolation}); c.subscribe([a.topic]); ids=[]
    try:
        while len(ids)<a.count:
            m=c.poll(1)
            if m is None: continue
            if m.error(): continue
            e=json.loads(m.value()); ids.append(e["event_id"]); print(f"event_id={e['event_id']} partition={m.partition()} offset={m.offset()}")
    except KeyboardInterrupt: pass
    finally: c.close()
    print(f"total={len(ids)}"); print(f"unique={len(set(ids))}"); print(f"duplicates={len(ids)-len(set(ids))}")
if __name__=="__main__": main()
