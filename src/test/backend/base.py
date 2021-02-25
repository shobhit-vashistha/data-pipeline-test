
from backend.clients import MockClient, ConfluentClient, KafkaPythonClient
from filter_extract import get_filter_func


def get_kafka_client():
    # return ConfluentClient()
    return KafkaPythonClient()
    # return MockClient()


def get_kafka_messages(results, topic, event_data, expected_count):
    filter_func = get_filter_func(topic, event_data)

    client = get_kafka_client()
    consumer = client.get_kafka_consumer(topic)
    messages, ignored_messages, wait_seconds = client.get_messages(consumer, filter_func)

    message_count = len(messages)

    results[topic] = {
        'topic': topic,
        'passed': expected_count == message_count,
        'wait_seconds': wait_seconds,
        'expected_count': expected_count,
        'actual_count': message_count,
        'ignored_count': len(ignored_messages),
        'messages': messages,
        # 'ignored_messages': ignored_messages
    }
