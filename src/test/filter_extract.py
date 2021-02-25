from env import KAFKA_TOPIC_RAW, KAFKA_TOPIC_DRUID_EVENTS, KAFKA_TOPIC_DE_NORM, KAFKA_TOPIC_UNIQUE, \
    KAFKA_TOPIC_INGEST, KAFKA_TOPIC_BATCH_DUPLICATE, KAFKA_TOPIC_DUPLICATE


def filter_by_msg_id_func(message, msg_id_set):
    message_msg_id = message.get('params', {}).get('msgid', None)
    return message_msg_id and message_msg_id in msg_id_set


def extract_msg_id_set(events_data):
    msg_id_set = set()
    for event_data in events_data:
        msg_id = event_data.get('params', {}).get('msgid', None)
        if msg_id:
            msg_id_set.add(msg_id)
    return msg_id_set,


def filter_by_event_mid_func(message, mid_set):
    event_mid = message.get('mid', None)
    return event_mid and event_mid in mid_set


def extract_mid_set(events_data):
    mid_set = set()
    for event_data in events_data:
        for e in event_data['events']:
            mid = e.get('mid', None)
            if mid:
                mid_set.add(mid)
    return mid_set,


FILTER_EXTRACT_FUNCTIONS = {
    KAFKA_TOPIC_INGEST: (filter_by_msg_id_func, extract_msg_id_set),
    KAFKA_TOPIC_BATCH_DUPLICATE: (filter_by_msg_id_func, extract_msg_id_set),
    KAFKA_TOPIC_RAW: (filter_by_event_mid_func, extract_mid_set),
    KAFKA_TOPIC_UNIQUE: (filter_by_event_mid_func, extract_mid_set),
    KAFKA_TOPIC_DUPLICATE: (filter_by_event_mid_func, extract_mid_set),
    KAFKA_TOPIC_DE_NORM: (filter_by_event_mid_func, extract_mid_set),
    KAFKA_TOPIC_DRUID_EVENTS: (filter_by_event_mid_func, extract_mid_set),
}


def get_filter_func(topic, event_data):
    filter_criterion_func, filter_args_extractor = FILTER_EXTRACT_FUNCTIONS[topic]
    filter_func_args = filter_args_extractor(event_data)
    return lambda msg: filter_criterion_func(msg, *filter_func_args)