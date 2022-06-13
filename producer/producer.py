from sseclient import SSEClient as EventSource
from argparse import ArgumentParser
from kafka import KafkaProducer
from time import time, sleep
import logging
import json

def msg_stream(t: int) -> dict:
    t_end = time() + t
    #https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams#Python
    for event in EventSource("https://stream.wikimedia.org/v2/stream/page-create", last_id=None):
        if time() > t_end: break
        if event.event != 'message': continue
        try: data = json.loads(event.data)
        except ValueError: continue
        try: yield {
                "dt":           data["meta"]["dt"],
                "domain":       data["meta"]["domain"],
                "page":         data["meta"]["uri"],
                "page_id":      data["page_id"],
                "page_name":    data["page_title"],
                "user_id":      data["performer"]["user_id"],
                "user_is_bot":  data["performer"]["user_is_bot"],
                "user_name":    data["performer"]["user_text"]
            }
        except: continue

def connect_producer(name: str, port: str, lim: int = 1) -> KafkaProducer:
    for _ in range(lim):
        try: return KafkaProducer(bootstrap_servers=f"{name}:{port}", 
            value_serializer=lambda d: json.dumps(d).encode("utf-8"))
        except Exception as err: logging.error(err)
        sleep(.5)

def send_message(producer: KafkaProducer, msg: str, topic: str):
    try: producer.send(topic, msg)
    except Exception as e: 
        logging.error(e)
        logging.error(f"Failed to send msg({msg}) to topic({topic})")

if __name__ == "__main__":
    p = ArgumentParser("xxx")
    p.add_argument("BROKER", type=str)
    p.add_argument("PORT", type=int)
    p.add_argument("TOPIC", type=str)
    p.add_argument("TIME", type=int)
    a = p.parse_args()
    stream = msg_stream(a.TIME)
    producer = connect_producer(a.BROKER, a.PORT)
    if producer is None:
        logging.error("FAILED TO CONNECT")
        exit(1)
    logging.warning(f"Connected to BROKER({a.BROKER}) on PORT({a.PORT})") 

    [send_message(producer, msg, a.TOPIC) for msg in stream]
    producer.flush()

    logging.warning(f"Terminated succesfully") 