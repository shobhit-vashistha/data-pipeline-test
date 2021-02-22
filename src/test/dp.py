import json
import threading
import traceback
import time
import logging

from kafka import KafkaProducer, KafkaConsumer

from env import KAFKA_PORT, KAFKA_IP, KAFKA_TOPIC_INGEST, KAFKA_TOPIC_RAW, KAFKA_TOPIC_UNIQUE, KAFKA_TOPIC_DE_NORM, \
    KAFKA_TOPIC_DRUID_EVENTS
from event import get_impression_event_data


BOOTSTRAP_SERVERS = [
    '%s:%s' % (KAFKA_IP, KAFKA_PORT)
]

CONSUMER_GROUP = 'dp-test'

CONSUMER_WAIT_TIMEOUT_MS = 60 * 1000  # 60 seconds

CONSUMER_POLL_TIMEOUT_MS = 500  # 0.5 second


def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_kafka_consumer(topic, latest=True):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest' if latest else 'earliest',
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )


def look_for_events(consumer, id_func, limit=None):
    ret = []
    start = time.time()
    while time.time() - start < (CONSUMER_WAIT_TIMEOUT_MS / 1000.0):
        messages = consumer.poll(500)
        for message in messages:
            if id_func(message):
                ret.append(message)
                if limit and len(ret) == limit:
                    return ret
    return ret


def get_events(consumer, id_func, limit=None):
    start = time.time()
    messages = look_for_events(consumer, id_func, limit)
    wait = time.time() - start
    logging.info("Wait: %.2f ms" % (wait * 1000))
    if messages:
        logging.info("Success!")
        logging.info(messages)
        return messages
    else:
        logging.info("Failed to get any messages")
        return messages


class Tester(object):

    def __init__(self):
        self.kafka_connections = {}

    def produce(self, event_data):
        logging.info("\n-> Producing event in topic: %s" % KAFKA_TOPIC_INGEST)
        self.kafka_connections['producer'].send(topic=KAFKA_TOPIC_INGEST, value=event_data)

    def consume_ingest(self, msg_id):
        logging.info("\n-> Waiting for message to appear in ingest topic...")
        messages = get_events(self.kafka_connections['consumer_ingest'], lambda m: m['params']['msgid'] == msg_id, limit=1)

    def consume_raw(self, event_mid_set):
        logging.info("\n-> Waiting for message to appear in raw topic...")
        messages = get_events(self.kafka_connections['consumer_raw'], lambda m: m['mid'] in event_mid_set)

    def consume_unique(self, event_mid_set):
        logging.info("\n-> Waiting for message to appear in unique topic...")
        messages = get_events(self.kafka_connections['consumer_unique'], lambda m: m['mid'] in event_mid_set)

    def consume_de_norm(self, event_mid_set):
        logging.info("\n-> Waiting for message to appear in de-norm topic...")
        messages = get_events(self.kafka_connections['consumer_de_norm'], lambda m: m['mid'] in event_mid_set)

    def consume_druid_events(self, event_mid_set):
        logging.info("\n-> Waiting for message to appear in druid-events topic...")
        messages = get_events(self.kafka_connections['consumer_druid_events'], lambda m: m['mid'] in event_mid_set)

    def test_flow(self):
        print("Testing event data")
        event_data = get_impression_event_data()
        msg_id = event_data['params']['msgid']
        event_mid_set = set([e['mid'] for e in event_data['events']])
        print(event_data)

        threads = [
            threading.Thread(target=self.consume_ingest, args=(msg_id,)),
            threading.Thread(target=self.consume_raw, args=(event_mid_set,)),
            threading.Thread(target=self.consume_unique, args=(event_mid_set,)),
            threading.Thread(target=self.consume_de_norm, args=(event_mid_set,)),
            threading.Thread(target=self.consume_druid_events, args=(event_mid_set,)),
        ]

        for thread in threads:
            thread.start()

        self.produce(event_data)

        for thread in threads:
            thread.join()

        return True

    def test(self):
        try:
            self.kafka_connections['producer'] = get_kafka_producer()
            self.kafka_connections['consumer_ingest'] = get_kafka_consumer(KAFKA_TOPIC_INGEST)
            self.kafka_connections['consumer_raw'] = get_kafka_consumer(KAFKA_TOPIC_RAW)
            self.kafka_connections['consumer_unique'] = get_kafka_consumer(KAFKA_TOPIC_UNIQUE)
            self.kafka_connections['consumer_de_norm'] = get_kafka_consumer(KAFKA_TOPIC_DE_NORM)
            self.kafka_connections['consumer_druid_events'] = get_kafka_consumer(KAFKA_TOPIC_DRUID_EVENTS)
            return self.test_flow()
        except Exception as e:
            traceback.print_exc()
            print(e)
            print("FAILED!!")
        finally:
            try:
                for c in self.kafka_connections.values():
                    c.close()
            except Exception as e:
                traceback.print_exc()
                print(e)
                print("FAILED!!")

        return False


if __name__ == '__main__':
    tester = Tester()
    success = tester.test()
    print("_" * 20 + "\n")
    print("TEST SUCCESS" if success else "TEST FAILED")
    print("_" * 20)
