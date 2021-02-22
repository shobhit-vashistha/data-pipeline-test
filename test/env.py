import os

ENV = os.environ.get('env')

KAFKA_IP = os.environ.get('kafka_ip')
KAFKA_PORT = os.environ.get('kafka_port')

KAFKA_TOPIC_INGEST = os.environ.get('kafka_topic_ingest')
KAFKA_TOPIC_RAW = os.environ.get('kafka_topic_raw')
KAFKA_TOPIC_UNIQUE = os.environ.get('kafka_topic_unique')
KAFKA_TOPIC_DE_NORM = os.environ.get('kafka_topic_de_norm')
KAFKA_TOPIC_DRUID_EVENTS = os.environ.get('kafka_topic_druid_events')

KAFKA_TOPIC_LOGS = os.environ.get('kafka_topic_logs')

KAFKA_TOPIC_FAILED = os.environ.get('kafka_topic_failed')
KAFKA_TOPIC_DUPLICATE = os.environ.get('kafka_topic_duplicate')
