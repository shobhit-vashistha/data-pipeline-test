from dp.backend.clients import ConfluentClient, KafkaPythonClient
from dp.filter_extract import get_filter_func


def get_kafka_client():
    return KafkaPythonClient()
    # return ConfluentClient()


def get_filtered_kafka_messages(topic, filter_func, expected_count, offset, timeout):
    client = get_kafka_client()
    consumer = client.get_kafka_consumer(topic, latest=(offset != 'earliest'))
    messages, ignored_messages, wait_seconds = client.get_messages(consumer, filter_func, timeout=timeout)

    message_count = len(messages)
    return {
        'topic': topic,
        'passed': expected_count == message_count,
        'wait_seconds': wait_seconds,
        'expected_count': expected_count,
        'actual_count': message_count,
        'ignored_count': len(ignored_messages),
        'messages_desc': [make_readable(m) for m in messages],
        'messages': messages,
        'ignored_messages': ignored_messages
    }


def get_kafka_messages_multi_topic(results, event_data, expected_counts, consumer_timeout):

    client = get_kafka_client()
    consumer = client.get_kafka_consumer(topics=list(expected_counts.keys()))

    all_topic_messages, wait_seconds = client.get_messages(consumer, consumer_timeout)

    for topic, all_messages in all_topic_messages.items():
        filter_func = get_filter_func(topic, event_data)
        expected_count = expected_counts[topic]

        messages = []
        ignored_messages = []

        for m in all_messages:
            if filter_func(m):
                messages.append(m)
            else:
                ignored_messages.append(m)

        message_count = len(messages)

        results[topic] = {
            'topic': topic,
            'passed': expected_count == message_count,
            'wait_seconds': wait_seconds,
            'expected_count': expected_count,
            'actual_count': message_count,
            'ignored_count': len(ignored_messages),
            'messages_desc': [make_readable(m) for m in messages],
            'messages': messages,
            'ignored_messages': ignored_messages
        }


def get_kafka_messages(results, topic, event_data, expected_count, offsets):
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
        'messages_desc': [make_readable(m) for m in messages]
        # 'messages': messages,
        # 'ignored_messages': ignored_messages
    }


def make_readable(msg):
    if 'events' in msg:
        return 'BATCH {ets} count={count} mid={mid}'.format(count=len(msg['events']), mid=msg['mid'], ets=msg['ets'])
    else:
        return 'EVENT {ets} eid={eid} pdata.id={pdata_id} page={pid} mid={mid}'.format(
            mid=msg['mid'], ets=msg['ets'], eid=msg['eid'], pid=msg.get('edata', {}).get('pageid', None),
            pdata_id=msg.get('context', {}).get('pdata', {}).get('id', None)
        )
