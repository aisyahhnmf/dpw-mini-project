import argparse,json,time
from confluent_kafka import Producer
def main():
    p=argparse.ArgumentParser(); p.add_argument("--topic",required=True); p.add_argument("--count",type=int,default=10); p.add_argument("--key-mode",choices=["none","entity","fixed"],default="none"); a=p.parse_args()
    pr=Producer({"bootstrap.servers":"localhost:9092"})
    for i in range(1,a.count+1):
        e={"event_id":f"event-{i:03d}","entity_id":f"entity-{i%3}","value":i*10}
        k=None if a.key_mode=="none" else ("fixed-key" if a.key_mode=="fixed" else e["entity_id"])
        pr.produce(a.topic,key=k,value=json.dumps(e))
        pr.poll(0)
        print(f"[producer] sent {e['event_id']}")
        time.sleep(.05)
    pr.flush(); print("[producer] completed")
if __name__=="__main__": main()
