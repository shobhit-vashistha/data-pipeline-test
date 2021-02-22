import json
import traceback

from kafka import KafkaProducer, KafkaConsumer

from test.env import KAFKA_PORT, KAFKA_IP, KAFKA_TOPIC_INGEST, KAFKA_TOPIC_RAW, KAFKA_TOPIC_UNIQUE, KAFKA_TOPIC_DE_NORM, \
    KAFKA_TOPIC_DRUID_EVENTS
from test.event import get_impression_event_data


BOOTSTRAP_SERVERS = [
    '%s:%s' % (KAFKA_IP, KAFKA_PORT)
]

CONSUMER_GROUP = 'dp-test'

CONSUMER_POLL_TIMEOUT_MS = 60 * 1000  # 60 seconds


def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def get_kafka_consumer(topic, latest=True):
    return KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest' if latest else 'earliest',
        enable_auto_commit=False,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS
    )


def look_for_event(consumer, id_func):
    for message in consumer:
        if id_func(message):
            return message


def look_for_events(consumer, id_func):
    ret = []
    for message in consumer:
        if id_func(message):
            ret.append(message)
    return ret


def test_flow(kafka_connections):
    print("Testing event data")
    event_data = get_impression_event_data()
    msg_id = event_data['params']['msgid']
    event_mid_set = set([e['mid'] for e in event_data['events']])
    print(event_data)

    print("\n-> Producing event")
    kafka_connections['producer'].send(topic=KAFKA_TOPIC_INGEST, value=event_data)

    print("\n-> Waiting for message to appear in ingest topic...")
    message = look_for_event(kafka_connections['consumer_ingest'], lambda m: m['params']['msgid'] == msg_id)
    print("Success!")
    print(message)

    print("\n-> Waiting for message to appear in raw topic...")
    messages = look_for_events(kafka_connections['consumer_raw'], lambda m: m['mid'] in event_mid_set)
    print("Success!")
    print(messages)

    print("\n-> Waiting for message to appear in unique topic...")
    messages = look_for_events(kafka_connections['consumer_unique'], lambda m: m['mid'] in event_mid_set)
    print("Success!")
    print(messages)

    print("\n-> Waiting for message to appear in de-norm topic...")
    messages = look_for_event(kafka_connections['consumer_de_norm'], lambda m: m['mid'] in event_mid_set)
    print("Success!")
    print(messages)

    print("\n-> Waiting for message to appear in druid-events topic...")
    messages = look_for_event(kafka_connections['consumer_druid_events'], lambda m: m['mid'] in event_mid_set)
    print("Success!")
    print(messages)

    print("\nSuccess! Events have reached druid ingestion topic")
    return True


def test():
    kafka_connections = {}
    try:
        kafka_connections['producer'] = get_kafka_producer()
        kafka_connections['consumer_ingest'] = get_kafka_consumer(KAFKA_TOPIC_INGEST)
        kafka_connections['consumer_raw'] = get_kafka_consumer(KAFKA_TOPIC_RAW)
        kafka_connections['consumer_unique'] = get_kafka_consumer(KAFKA_TOPIC_UNIQUE)
        kafka_connections['consumer_de_norm'] = get_kafka_consumer(KAFKA_TOPIC_DE_NORM)
        kafka_connections['consumer_druid_events'] = get_kafka_consumer(KAFKA_TOPIC_DRUID_EVENTS)
        return test_flow(kafka_connections)
    except Exception as e:
        traceback.print_exc()
        print(e)
        print("FAILED!!")
    finally:
        try:
            for c in kafka_connections.values():
                c.close()
        except Exception as e:
            traceback.print_exc()
            print(e)
            print("FAILED!!")

    return False


if __name__ == '__main__':
    test()
