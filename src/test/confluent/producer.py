from confluent_kafka import Producer

from env import BOOTSTRAP_SERVERS
from util import serialize


def get_kafka_producer():
    return Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})


def send(producer, topic, data):
    producer.produce(topic, value=serialize(data))
    producer.flush(30)
