
import time
import traceback

from dp import log, env
from dp.util import deserialize, serialize

CONSUMER_GROUP = 'dp-test'
CONSUMER_SESSION_TIMEOUT_MS = 60 * 1000  # 60 seconds

CONSUMER_POLL_WAIT_SECONDS = 0.05
MESSAGE_WAIT_TIMEOUT_SECONDS = 20


class Client(object):
    """ Kafka client adapter interface """

    def get_kafka_producer(self):
        raise NotImplementedError

    def send(self, producer, topic, data):
        raise NotImplementedError

    def get_kafka_consumer(self, topics, latest=True):
        raise NotImplementedError

    def get_kafka_offsets(self, topics):
        raise NotImplementedError

    def collect_messages(self, consumer, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        raise NotImplementedError

    def get_messages(self, consumer, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        messages_topic_map = {}
        start = time.time()
        try:
            topic_messages = self.collect_messages(consumer, timeout)
            for topic, message in topic_messages:
                if topic in messages_topic_map:
                    messages_topic_map[topic].append(message)
                else:
                    messages_topic_map[topic] = [message]
        except Exception as e:
            log.e(e, '- [Consumer] Error:', traceback=traceback.format_exc())
        finally:
            consumer.close()
        wait = time.time() - start
        return messages_topic_map, wait


class Consumer(object):

    def __init__(self, client, consumer, topic, latest, consumer_group, deserialize_func=deserialize):
        self.client = client
        self.consumer = consumer
        self.topic = topic
        self.latest = latest
        self.consumer_group = consumer_group
        self.deserialize_func = deserialize_func

    def poll(self, timeout):
        raise NotImplementedError

    def get_current_offset(self):
        raise NotImplementedError

    def close(self):
        self.consumer.close()


class Producer(object):

    def __init__(self, client, producer):
        self.client = client
        self.producer = producer

    def close(self):
        self.producer.close()


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

    def get_kafka_consumer(self, topics, latest=True):
        from confluent_kafka import Consumer
        consumer_settings = {
            'bootstrap.servers': env.BOOTSTRAP_SERVER,
            'group.id': '%s-%s' % (CONSUMER_GROUP, 'all'),
            'client.id': '%s-client-%s' % (CONSUMER_GROUP, 'all'),
            'enable.auto.commit': True,
            'session.timeout.ms': CONSUMER_SESSION_TIMEOUT_MS
        }
        if not latest:
            consumer_settings['default.topic.config'] = {'auto.offset.reset': 'smallest'}
        consumer = Consumer(consumer_settings)
        consumer.subscribe(topics)
        return consumer

    def get_kafka_offsets(self, topics):
        from confluent_kafka import Consumer, TopicPartition
        consumer_settings = {
            'bootstrap.servers': env.BOOTSTRAP_SERVER,
            'group.id': CONSUMER_GROUP,
            'client.id': '%s-client' % CONSUMER_GROUP,
            'enable.auto.commit': False,
            'session.timeout.ms': CONSUMER_SESSION_TIMEOUT_MS,
        }
        consumer = Consumer(consumer_settings)
        offsets = {}
        for topic_name in topics:
            topic = consumer.list_topics(topic=topic_name)
            partitions = [
                TopicPartition(topic_name, partition) for partition in list(topic.topics[topic_name].partitions.keys())
            ]
            offset_per_partition = consumer.position(partitions)
            offsets[topic_name] = offset_per_partition

    def collect_messages(self, consumer, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        fil_messages = []
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

            # passes filter, add
            log.i('- [Consumer] YAY: Message found, adding')
            fil_messages.append(deserialize_message)

        return fil_messages


class KafkaPythonClient(Client):

    def get_kafka_producer(self):
        from kafka import KafkaProducer
        print('kafka=', env.BOOTSTRAP_SERVER)
        return KafkaProducer(bootstrap_servers=[env.BOOTSTRAP_SERVER], value_serializer=serialize)

    def send(self, producer, topic, data):
        producer.send(topic, value=data)

    def get_kafka_consumer(self, topics, latest=True):
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=[env.BOOTSTRAP_SERVER],
            auto_offset_reset='latest' if latest else 'earliest',
            enable_auto_commit=True,
            group_id='%s-%s' % (CONSUMER_GROUP, 'all'),
            client_id='%s-client-%s' % (CONSUMER_GROUP, 'all'),
            session_timeout_ms=CONSUMER_SESSION_TIMEOUT_MS,
            value_deserializer=deserialize
        )
        consumer.subscribe(topics)
        return consumer

    def get_kafka_offsets(self, topics):
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=[env.BOOTSTRAP_SERVER],
            auto_offset_reset='latest',
            enable_auto_commit=False,
            group_id='%s-%s' % (CONSUMER_GROUP, 'offset-check'),
            client_id='%s-client-%s' % (CONSUMER_GROUP, 'offset-check'),
        )
        offsets = {}
        for topic in topics:
            partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
            last_offset_per_partition = consumer.end_offsets(partitions)
            offsets[topic] = last_offset_per_partition
        return offsets

    def collect_messages(self, consumer, timeout=MESSAGE_WAIT_TIMEOUT_SECONDS):
        topic_messages = []
        start = time.time()

        while time.time() - start < timeout:
            msg = consumer.poll(CONSUMER_POLL_WAIT_SECONDS)

            # got nothing, continue
            if not msg:
                continue

            # get messages
            messages = ((topic, msg_record.value) for topic, msg_records in msg.items() for msg_record in msg_records)

            # loop over
            for topic, message in messages:
                if message:
                    # log message
                    log.i('- [Consumer] Received message: {0}'.format(str(message)[:20]))
                    log.d('- [Consumer] message = {0}'.format(message))

                    log.i('- [Consumer] YAY: Message found, adding')
                    topic_messages.append((topic, message))
                else:
                    # got an empty message, should not happen, warning and ignoring
                    log.w('- [Consumer] Warning: empty message, ignoring')

        return topic_messages

