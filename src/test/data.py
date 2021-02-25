from env import KAFKA_TOPIC_INGEST, KAFKA_TOPIC_RAW, KAFKA_TOPIC_UNIQUE, KAFKA_TOPIC_DE_NORM, KAFKA_TOPIC_DRUID_EVENTS, \
    KAFKA_TOPIC_DUPLICATE, KAFKA_TOPIC_BATCH_DUPLICATE
from event import get_batch_event_data


def get_individual_event_test_data():
    messages = [
        get_batch_event_data(['IMPRESSION'])
    ]
    expected_count = {
        KAFKA_TOPIC_INGEST: 1,
        KAFKA_TOPIC_RAW: 1,
        KAFKA_TOPIC_UNIQUE: 1,
        KAFKA_TOPIC_DE_NORM: 1,
        KAFKA_TOPIC_DRUID_EVENTS: 1
    }
    return messages, expected_count


def get_multiple_event_test_data():
    messages = [
        get_batch_event_data(['IMPRESSION', 'IMPRESSION'])
    ]
    expected_count = {
        KAFKA_TOPIC_INGEST: 1,
        KAFKA_TOPIC_RAW: 2,
        KAFKA_TOPIC_UNIQUE: 2,
        KAFKA_TOPIC_DE_NORM: 2,
        KAFKA_TOPIC_DRUID_EVENTS: 2
    }
    return messages, expected_count


def get_multiple_batch_event_test_data():
    messages = [
        get_batch_event_data(['IMPRESSION']),
        get_batch_event_data(['IMPRESSION', 'IMPRESSION']),
        get_batch_event_data(['IMPRESSION', 'IMPRESSION', 'IMPRESSION'])
    ]
    expected_count = {
        KAFKA_TOPIC_INGEST: 3,
        KAFKA_TOPIC_RAW: 6,
        KAFKA_TOPIC_UNIQUE: 6,
        KAFKA_TOPIC_DE_NORM: 6,
        KAFKA_TOPIC_DRUID_EVENTS: 6
    }
    return messages, expected_count


def get_duplicate_batch_event_test_data():
    messages = [
        get_batch_event_data(['IMPRESSION'])
    ]
    messages = messages * 2
    expected_count = {
        KAFKA_TOPIC_INGEST: 2,
        KAFKA_TOPIC_BATCH_DUPLICATE: 1,
        KAFKA_TOPIC_RAW: 1,
        KAFKA_TOPIC_UNIQUE: 1,
        KAFKA_TOPIC_DE_NORM: 1,
        KAFKA_TOPIC_DRUID_EVENTS: 1
    }
    return messages, expected_count


def get_duplicate_event_test_data():
    batch_event_data = get_batch_event_data(['IMPRESSION'])
    # duplicate the event
    batch_event_data['events'].append(batch_event_data['events'][0])
    messages = [
        batch_event_data
    ]
    expected_count = {
        KAFKA_TOPIC_INGEST: 1,
        KAFKA_TOPIC_RAW: 2,
        KAFKA_TOPIC_UNIQUE: 1,
        KAFKA_TOPIC_DUPLICATE: 1,
        KAFKA_TOPIC_DE_NORM: 1,
        KAFKA_TOPIC_DRUID_EVENTS: 1
    }
    return messages, expected_count
