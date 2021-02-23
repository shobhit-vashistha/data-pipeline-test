from env import KAFKA_TOPIC_RAW, KAFKA_TOPIC_DRUID_EVENTS, KAFKA_TOPIC_DE_NORM, KAFKA_TOPIC_UNIQUE, \
    KAFKA_TOPIC_INGEST


def msg_id_func(message, msg_id):
    message_msg_id = message.get('params', {}).get('msgid', None)
    return message_msg_id and message_msg_id == msg_id


def extract_msg_id(event_data):
    return event_data.get('params', {}).get('msgid', None),


def event_mid_func(message, mid_set):
    event_mid = message.get('mid', None)
    return event_mid and event_mid in mid_set


def extract_mid_set(event_data):
    return set([e['mid'] for e in event_data['events']]),


FILTER_FUNCTIONS = {
    KAFKA_TOPIC_INGEST: (msg_id_func, extract_msg_id),
    KAFKA_TOPIC_RAW: (event_mid_func, extract_mid_set),
    KAFKA_TOPIC_UNIQUE: (event_mid_func, extract_mid_set),
    KAFKA_TOPIC_DE_NORM: (event_mid_func, extract_mid_set),
    KAFKA_TOPIC_DRUID_EVENTS: (event_mid_func, extract_mid_set)
}
