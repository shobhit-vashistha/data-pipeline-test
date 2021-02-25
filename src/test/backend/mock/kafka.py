import json
import os
import time

import env


class MockKafka(object):
    """ mock kafka functionality, mostly thread safe """

    def __init__(self, path='data', filename='mock-kafka-data.json', offsets_filename='mock-kafka-topic-offsets.json',
                 lock_filename='mock-kafka.lock'):

        self.filename = filename
        self.offsets_filename = filename
        self.lock_filename = filename

        transport_data = {
            env.KAFKA_TOPIC_INGEST: [],
            env.KAFKA_TOPIC_RAW: [],
            env.KAFKA_TOPIC_UNIQUE: [],
            env.KAFKA_TOPIC_DE_NORM: [],
            env.KAFKA_TOPIC_DRUID_EVENTS: [],
            env.KAFKA_TOPIC_DUPLICATE: [],
            env.KAFKA_TOPIC_BATCH_DUPLICATE: [],
            env.KAFKA_TOPIC_LOGS: [],
            env.KAFKA_TOPIC_FAILED: [],
        }
        transport_offsets = {topic: 0 for topic in transport_data.keys()}
        self.save_data(transport_data)
        self.save_offsets(transport_offsets)

    def add(self, topic, msg):
        transport_data = self.get_data()
        transport_data[topic].append(msg)
        self.save_data(transport_data)

    def poll(self, topic, timeout):
        time.sleep(timeout)
        transport_data = self.get_data()
        return transport_data.get(topic, [])

    def ingest(self, data):
        # write to ingest
        self.add(env.KAFKA_TOPIC_INGEST, data)

        # write to raw
        for event in data.get('events', []):
            self.add(env.KAFKA_TOPIC_RAW, event)

        # write to unique
        for event in data.get('events', []):
            self.add(env.KAFKA_TOPIC_UNIQUE, event)

        # write to de-norm
        for event in data.get('events', []):
            self.add(env.KAFKA_TOPIC_DE_NORM, event)

        # write to druid-events
        for event in data.get('events', []):
            self.add(env.KAFKA_TOPIC_DRUID_EVENTS, event)

    def get_data(self):
        return json.load(open(self.filename, 'rb'))

    def get_offsets(self):
        return json.load(open(self.offsets_filename, 'rb'))

    def save_data(self, data):
        json.dump(data, open(self.filename, 'wb'))

    def save_offsets(self, data):
        json.dump(data, open(self.offsets_filename, 'wb'))


class MockProducer(object):
    pass


class MockConsumer(object):
    def __init__(self, topic, latest):
        self.topic = topic
        self.latest = latest
