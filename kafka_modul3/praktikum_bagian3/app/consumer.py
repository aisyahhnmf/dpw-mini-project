import argparse,json,sys
from confluent_kafka import Consumer
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--topic",required=True); p.add_argument("--group",required=True); p.add_argument("--reset",choices=["earliest","latest"],default="latest")
    p.add_argument("--consumer-name",default="consumer"); p.add_argument("--stop-after",type=int,default=0); p.add_argument("--manual-commit",action="store_true"); p.add_argument("--fail-after",type=int,default=0)
    a=p.parse_args()
    c=Consumer({"bootstrap.servers":"localhost:9092","group.id":a.group,"auto.offset.reset":a.reset,"enable.auto.commit":False,"client.id":a.consumer_name})
    c.subscribe([a.topic]); n=0
    try:
        print(f"[{a.consumer_name}] waiting...")
        while True:
            m=c.poll(1)
            if m is None: continue
            if m.error(): print(m.error()); continue
            e=json.loads(m.value()); n+=1
            print(f"[{a.consumer_name}] event_id={e['event_id']} partition={m.partition()} offset={m.offset()}")
            if a.fail_after and n>=a.fail_after: raise RuntimeError("intentional failure")
            if a.manual_commit: c.commit(message=m,asynchronous=False)
            if a.stop_after and n>=a.stop_after: break
    except KeyboardInterrupt: pass
    finally: c.close()
if __name__=="__main__":
    try: main()
    except Exception as e: print(f"[consumer] FAILED: {e}"); sys.exit(1)
