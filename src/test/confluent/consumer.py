import time

from confluent_kafka import Consumer, KafkaError

from common import FILTER_FUNCTIONS
from env import BOOTSTRAP_SERVER, KAFKA_TOPIC_RAW, KAFKA_TOPIC_DRUID_EVENTS, KAFKA_TOPIC_DE_NORM, KAFKA_TOPIC_UNIQUE, \
    KAFKA_TOPIC_INGEST
from util import pr, deserialize, pre


CONSUMER_GROUP = 'dp-test'

CONSUMER_SESSION_TIMEOUT_MS = 6 * 1000  # 6 seconds


def get_kafka_consumer(topic, latest=True):
    consumer_settings = {
        'bootstrap.servers': BOOTSTRAP_SERVER,
        'group.id': '%s-%s' % (CONSUMER_GROUP, topic),
        'client.id': '%s-client-%s' % (CONSUMER_GROUP, topic),
        'enable.auto.commit': True,
        'session.timeout.ms': CONSUMER_SESSION_TIMEOUT_MS
    }
    if not latest:
        consumer_settings['default.topic.config'] = {'auto.offset.reset': 'smallest'}
    consumer = Consumer(consumer_settings)
    consumer.subscribe([topic])
    return consumer


def get_messages(consumer, filter_func, filter_func_args, limit=None,  timeout=10):
    messages = []
    start = time.time()
    try:
        while time.time() - start < timeout:
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                message = msg.value()
                pr('- [Consumer] Received message: {0}'.format(message))
                deserialize_message = deserialize(msg)
                if deserialize_message is None:
                    pr('- [Consumer] Error: Could not deserialize message, ignoring')
                else:
                    if filter_func(deserialize_message, *filter_func_args):
                        pr('- [Consumer] YAY: Message found, adding')
                        messages.append(deserialize_message)
                        # exit if limit reached
                        if limit and len(messages) == limit:
                            break
                    else:
                        pr('- [Consumer] Message does not clear the filter, ignoring')

            elif msg.error().code() == KafkaError._PARTITION_EOF:
                pr('- [Consumer] End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
            else:
                pr('- [Consumer] Error: {0}'.format(msg.error().str()))

    except Exception as e:
        pr('- [Consumer] Error:')
        pre(e)

    finally:
        consumer.close()

    wait = time.time() - start
    return messages, wait


def get_kafka_messages(results, topic, event_data, limit=None):
    filter_func, filter_func_args_extractor = FILTER_FUNCTIONS[topic]
    filter_func_args = filter_func_args_extractor(event_data)
    consumer = get_kafka_consumer(topic)
    messages = get_messages(consumer, filter_func, filter_func_args, limit)
    results[topic] = messages
