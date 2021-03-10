import os

ENV = os.environ.get('env')

KAFKA_IP = os.environ.get('kafka_ip')
KAFKA_PORT = os.environ.get('kafka_port')

KAFKA_TOPIC_INGEST = ENV + '.' + os.environ.get('kafka_topic_ingest')
KAFKA_TOPIC_RAW = ENV + '.' + os.environ.get('kafka_topic_raw')
KAFKA_TOPIC_UNIQUE = ENV + '.' + os.environ.get('kafka_topic_unique')
KAFKA_TOPIC_DE_NORM = ENV + '.' + os.environ.get('kafka_topic_de_norm')
KAFKA_TOPIC_DRUID_EVENTS = ENV + '.' + os.environ.get('kafka_topic_druid_events')

KAFKA_TOPIC_LOGS = ENV + '.' + os.environ.get('kafka_topic_logs')

KAFKA_TOPIC_FAILED = ENV + '.' + os.environ.get('kafka_topic_failed')
KAFKA_TOPIC_BATCH_FAILED = ENV + '.' + os.environ.get('kafka_topic_batch_failed')
KAFKA_TOPIC_DUPLICATE = ENV + '.' + os.environ.get('kafka_topic_duplicate')
KAFKA_TOPIC_BATCH_DUPLICATE = ENV + '.' + os.environ.get('kafka_topic_batch_duplicate')


BOOTSTRAP_SERVER = '%s:%s' % (KAFKA_IP, KAFKA_PORT)

