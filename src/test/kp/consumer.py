import time

from kafka import KafkaConsumer

from common import FILTER_FUNCTIONS
from env import BOOTSTRAP_SERVER
from util import pr, deserialize, pre


CONSUMER_GROUP = 'dp-test'

CONSUMER_SESSION_TIMEOUT_MS = 6 * 1000  # 6 seconds


def get_kafka_consumer(topic, latest=True):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[BOOTSTRAP_SERVER],
        auto_offset_reset='latest' if latest else 'earliest',
        enable_auto_commit=True,
        group_id='%s-%s' % (CONSUMER_GROUP, topic),
        client_id='%s-client-%s' % (CONSUMER_GROUP, topic),
        session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
        value_deserializer=deserialize
    )
    return consumer


def get_messages(consumer, filter_func, filter_func_args, limit=None,  timeout=10):
    messages = []
    start = time.time()
    try:
        while time.time() - start < timeout:
            msg_data = consumer.poll(0.1)
            msgs = msg_data.values()[0]
            if not msgs:
                continue
            pr('- [Consumer] Received message: {0}'.format(msgs))
            for msg in msgs:
                # deserialize_message = deserialize(msg)
                # if deserialize_message is None:
                #     pr('- [Consumer] Error: Could not deserialize message, ignoring')

                if filter_func(msg, *filter_func_args):
                    pr('- [Consumer] YAY: Message found, adding')
                    messages.append(msg)
                    # exit if limit reached
                    if limit and len(messages) == limit:
                        break
                else:
                    pr('- [Consumer] Message does not clear the filter, ignoring')

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
