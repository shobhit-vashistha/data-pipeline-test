import time

import env
import log
from backend.clients import CONSUMER_POLL_WAIT_SECONDS, MESSAGE_WAIT_TIMEOUT_SECONDS, CONSUMER_GROUP, Client
from backend.mock.kafka import MockKafka


class MockClient(Client):
    """ mock client, mocks expected functionality without broker, uses file system """

    def __init__(self):
        self.kafka = MockKafka(CONSUMER_GROUP)

    def get_kafka_producer(self):
        return self.kafka.get_producer()

    def send(self, producer, topic, data):
        if topic != env.KAFKA_TOPIC_INGEST:
            raise AssertionError('cant produce to topic %s from mock client' % topic)
        self.kafka.ingest(data)

    def get_kafka_consumer(self, topic, latest=True):
        return self.kafka.get_consumer(topic, latest)

    def collect_messages(self, consumer, filter_func, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        filtered_messages = []
        ignored_messages = []
        start = time.time()

        while time.time() - start < timeout:
            msg = consumer.poll(CONSUMER_POLL_WAIT_SECONDS)

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

        return filtered_messages, ignored_messages