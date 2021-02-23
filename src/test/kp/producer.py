from kafka import KafkaProducer

from env import BOOTSTRAP_SERVER
from util import serialize


def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER], value_serializer=serialize)


def send(producer, topic, data):
    producer.send(topic, value=data)
