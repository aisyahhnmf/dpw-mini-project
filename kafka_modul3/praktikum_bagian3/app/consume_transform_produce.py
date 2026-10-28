import argparse,json,sys
from confluent_kafka import Consumer,Producer
def main():
    p=argparse.ArgumentParser(); p.add_argument("--input",required=True); p.add_argument("--output",required=True); p.add_argument("--group"); p.add_argument("--mode",choices=["atleast-once","exactly-once"],required=True); p.add_argument("--transactional-id",default="dpw3-eos-worker-1"); p.add_argument("--fail-after-produce",action="store_true"); p.add_argument("--fail-before-commit",action="store_true"); a=p.parse_args()
    c=Consumer({"bootstrap.servers":"localhost:9092","group.id":a.group or ("dpw3-atleast" if a.mode=="atleast-once" else "dpw3-exactly"),"auto.offset.reset":"earliest","enable.auto.commit":False,"isolation.level":"read_committed"})
    c.subscribe([a.input])
    if a.mode=="atleast-once":
        pr=Producer({"bootstrap.servers":"localhost:9092"})
        while True:
            m=c.poll(1)
            if m is None: continue
            if m.error(): continue
            pr.produce(a.output,key=m.key(),value=m.value()); pr.flush()
            if a.fail_after_produce: raise RuntimeError("intentional failure after output")
            c.commit(message=m,asynchronous=False)
    else:
        pr=Producer({"bootstrap.servers":"localhost:9092","transactional.id":a.transactional_id}); pr.init_transactions(); failed=False
        while True:
            m=c.poll(1)
            if m is None: continue
            if m.error(): continue
            pr.begin_transaction(); pr.produce(a.output,key=m.key(),value=m.value()); pr.send_offsets_to_transaction([m],c.consumer_group_metadata())
            if a.fail_before_commit and not failed:
                failed=True; pr.abort_transaction(); raise RuntimeError("intentional failure before commit")
            pr.commit_transaction()
if __name__=="__main__":
    try: main()
    except KeyboardInterrupt: pass
    except Exception as e: print(f"[delivery-semantics] FAILED: {e}"); sys.exit(1)
