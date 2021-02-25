
import time
import traceback

from backend.mock.kafka import MockKafka, MockProducer, MockConsumer
from util import serialize, deserialize

import env
import log


CONSUMER_GROUP = 'dp-test'
CONSUMER_SESSION_TIMEOUT_MS = 6 * 1000  # 6 seconds

CONSUMER_POLL_WAIT_SECONDS = 0.1
MESSAGE_WAIT_TIMEOUT_SECONDS = 10


class Client(object):
    """ Kafka client adapter interface """

    def get_kafka_producer(self):
        raise NotImplementedError

    def send(self, producer, topic, data):
        raise NotImplementedError

    def get_kafka_consumer(self, topic, latest=True):
        raise NotImplementedError

    def collect_messages(self, consumer, filter_func, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        raise NotImplementedError

    def get_messages(self, consumer, filter_func, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        filtered_messages = []
        ignored_messages = []
        start = time.time()
        try:
            filtered_messages, ignored_messages = self.collect_messages(consumer, filter_func, timeout)
        except Exception as e:
            log.e(e, '- [Consumer] Error:', traceback=traceback.format_exc())
        finally:
            consumer.close()
        wait = time.time() - start
        return filtered_messages, ignored_messages, wait


class ConfluentClient(Client):

    def __init__(self):
        from confluent_kafka import KafkaError
        self.eof_code = getattr(KafkaError, '_PARTITION_EOF', None)

    def is_kafka_eof(self, error):
        return self.eof_code and error.code() == self.eof_code

    def get_kafka_producer(self):
        from confluent_kafka import Producer
        return Producer({'bootstrap.servers': env.BOOTSTRAP_SERVER})

    def send(self, producer, topic, data):
        producer.produce(topic, value=serialize(data))
        producer.flush(30)

    def get_kafka_consumer(self, topic, latest=True):
        from confluent_kafka import Consumer
        consumer_settings = {
            'bootstrap.servers': env.BOOTSTRAP_SERVER,
            'group.id': '%s-%s' % (CONSUMER_GROUP, topic),
            'client.id': '%s-client-%s' % (CONSUMER_GROUP, topic),
            'enable.auto.commit': True,
            'session.timeout.ms': CONSUMER_SESSION_TIMEOUT_MS
        }
        if not latest:
            consumer_settings['default.topic.config'] = {'auto.offset.reset': 'smallest'}
        consumer = Consumer(consumer_settings)
        consumer.subscribe([topic])
        return consumer

    def collect_messages(self, consumer, filter_func, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        filtered_messages = []
        ignored_messages = []
        start = time.time()

        while time.time() - start < timeout:
            msg = consumer.poll(CONSUMER_POLL_WAIT_SECONDS)

            # got nothing, continue
            if not msg:
                continue

            # got errors, log and continue
            if msg.error():
                if self.is_kafka_eof(msg.error()):
                    log.w('- [Consumer] End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                else:
                    log.e(msg='- [Consumer] Error: {0}'.format(msg.error().str()))
                continue

            # got an empty message, should not happen, warning and ignoring
            message = msg.value()
            message = message.strip()
            if not message:
                log.w('- [Consumer] Warning: empty message, ignoring')
                continue

            # log message
            log.i('- [Consumer] Received message: {0}'.format(str(message)[:20]))
            log.d('- [Consumer] message = {0}'.format(message))

            # try deserialize
            deserialize_message = deserialize(msg)

            # could not deserialize, warning and ignoring
            if deserialize_message is None:
                log.e(msg='- [Consumer] Error: Could not deserialize message, ignoring')
                continue

            # check if message is one of the ones we are looking for
            passes_filter = filter_func(deserialize_message)

            # does not pass filter, log and ignore
            if not passes_filter:
                log.i('- [Consumer] Message does not clear the filter, ignoring')
                ignored_messages.append(deserialize_message)
                continue

            # passes filter, add
            log.i('- [Consumer] YAY: Message found, adding')
            filtered_messages.append(deserialize_message)

        return filtered_messages, ignored_messages


class KafkaPythonClient(Client):

    def get_kafka_producer(self):
        from kafka import KafkaProducer
        return KafkaProducer(bootstrap_servers=[env.BOOTSTRAP_SERVER], value_serializer=serialize)

    def send(self, producer, topic, data):
        producer.send(topic, value=data)

    def get_kafka_consumer(self, topic, latest=True):
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[env.BOOTSTRAP_SERVER],
            auto_offset_reset='latest' if latest else 'earliest',
            enable_auto_commit=True,
            group_id='%s-%s' % (CONSUMER_GROUP, topic),
            client_id='%s-client-%s' % (CONSUMER_GROUP, topic),
            session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
            value_deserializer=deserialize
        )
        return consumer

    def collect_messages(self, consumer, filter_func, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        filtered_messages = []
        ignored_messages = []
        start = time.time()

        while time.time() - start < timeout:
            msg = consumer.poll(CONSUMER_POLL_WAIT_SECONDS)

            # got nothing, continue
            if not msg:
                continue

            # get messages
            messages = (msg_record.value for msg_records in msg.values() for msg_record in msg_records)

            # loop over
            for message in messages:
                if message:
                    # log message
                    log.i('- [Consumer] Received message: {0}'.format(str(message)[:20]))
                    log.d('- [Consumer] message = {0}'.format(message))

                    # check if message is one of the ones we are looking for
                    passes_filter = filter_func(message)

                    if passes_filter:
                        # passes filter, add
                        log.i('- [Consumer] YAY: Message found, adding')
                        filtered_messages.append(message)
                    else:
                        # does not pass filter, log and ignore
                        if not passes_filter:
                            log.i('- [Consumer] Message does not clear the filter, ignoring')
                            ignored_messages.append(message)
                            continue
                else:
                    # got an empty message, should not happen, warning and ignoring
                    log.w('- [Consumer] Warning: empty message, ignoring')

        return filtered_messages, ignored_messages


class MockClient(Client):
    """ mock client, mocks expected functionality without broker, uses file system """

    def __init__(self):
        self.kafka = MockKafka()

    def get_kafka_producer(self):
        return MockProducer()

    def send(self, producer, topic, data):
        if topic != env.KAFKA_TOPIC_INGEST:
            raise AssertionError('cant produce to topic %s from mock client' % topic)
        self.kafka.ingest(data)

    def get_kafka_consumer(self, topic, latest=True):
        return MockConsumer(topic, latest)

    def collect_messages(self, consumer, filter_func, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        filtered_messages = []
        ignored_messages = []
        start = time.time()

        while time.time() - start < timeout:
            msg = self.kafka.poll(consumer.topic, CONSUMER_POLL_WAIT_SECONDS)

            # got nothing, continue
            if not msg:
                continue

            # loop over
            for message in msg:
                if message:
                    # log message
                    log.i('- [Consumer] Received message: {0}'.format(str(message)[:20]))
                    log.d('- [Consumer] message = {0}'.format(message))

                    # check if message is one of the ones we are looking for
                    passes_filter = filter_func(message)

                    if passes_filter:
                        # passes filter, add
                        log.i('- [Consumer] YAY: Message found, adding')
                        filtered_messages.append(message)
                    else:
                        # does not pass filter, log and ignore
                        if not passes_filter:
                            log.i('- [Consumer] Message does not clear the filter, ignoring')
                            ignored_messages.append(message)
                            continue
                else:
                    # got an empty message, should not happen, warning and ignoring
                    log.w('- [Consumer] Warning: empty message, ignoring')
