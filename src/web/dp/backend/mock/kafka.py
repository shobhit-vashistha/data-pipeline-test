from web.dp import env
from web.dp.backend.mock.driver import Driver
from web.dp.util import serialize

TOPICS = [
    env.KAFKA_TOPIC_INGEST,
    env.KAFKA_TOPIC_RAW,
    env.KAFKA_TOPIC_UNIQUE,
    env.KAFKA_TOPIC_DE_NORM,
    env.KAFKA_TOPIC_DRUID_EVENTS,
    env.KAFKA_TOPIC_DUPLICATE,
    env.KAFKA_TOPIC_BATCH_DUPLICATE,
    env.KAFKA_TOPIC_LOGS,
    env.KAFKA_TOPIC_FAILED,
]


class MockKafka(object):
    """ mock kafka functionality, mostly thread safe """

    def __init__(self, consumer_group):
        self.driver = Driver()
        self.flink = MockFlink(self.driver)
        self.consumer_group = consumer_group

        for topic in TOPICS:
            self.driver.create_topic(topic)

    def ingest(self, data):
        self.flink.process(data)

    def get_producer(self):
        return self.driver.get_producer()

    def get_consumer(self, topic, latest):
        return self.driver.get_consumer(topic, latest, self.consumer_group)


class MockFlink(object):

    batch_id_set = set()
    event_id_set = set()

    consumers = {
        env.KAFKA_TOPIC_INGEST: 'run_telemetry_extractor',
        env.KAFKA_TOPIC_RAW: 'run_pipeline_preprocessor',
        env.KAFKA_TOPIC_UNIQUE: 'run_de_normalizer',
        env.KAFKA_TOPIC_DE_NORM: 'run_druid_validator',
        env.KAFKA_TOPIC_DRUID_EVENTS: 'druid_ingestion'
    }

    def __init__(self, driver):
        self.driver = driver

    def sink(self, topic, data):
        self.driver.add_message(topic, serialize(data))
        self.propagate(topic, data)

    def process(self, data):
        self.sink(env.KAFKA_TOPIC_INGEST, data)

    def propagate(self, topic, data):
        process_func_name = self.consumers.get(topic, None)
        if process_func_name:
            try:
                process_func = getattr(self, process_func_name)
                process_func(data)
            except AttributeError:
                raise

    def run_telemetry_extractor(self, data):
        if self.batch_invalid(data):
            return self.sink(env.KAFKA_TOPIC_BATCH_FAILED, data)
        elif not self.batch_unique(data):
            return self.sink(env.KAFKA_TOPIC_BATCH_DUPLICATE, data)
        events = data.get('events', [])
        for event in events:
            self.sink(env.KAFKA_TOPIC_RAW, event)

    def run_pipeline_preprocessor(self, event):
        if self.event_invalid(event):
            return self.sink(env.KAFKA_TOPIC_FAILED, event)
        elif not self.event_unique(event):
            return self.sink(env.KAFKA_TOPIC_DUPLICATE, event)
        # TODO: route LOG, ERROR etc.
        self.sink(env.KAFKA_TOPIC_UNIQUE, event)

    def run_de_normalizer(self, event):
        event = self.event_de_normalize(event)
        self.sink(env.KAFKA_TOPIC_DE_NORM, event)

    def run_druid_validator(self, event):
        if self.druid_validate(event):
            return self.sink(env.KAFKA_TOPIC_DRUID_EVENTS, event)

    def druid_validate(self, data):
        # TODO
        return True

    def druid_ingestion(self, data):
        pass

    def batch_invalid(self, data):
        # TODO
        return False

    def batch_unique(self, data):
        mid = data.get('mid')
        if mid in self.batch_id_set:
            return False
        else:
            self.batch_id_set.add(mid)
            return True

    def event_invalid(self, event):
        # TODO
        return False

    def event_unique(self, event):
        mid = event.get('mid')
        if mid in self.event_id_set:
            return False
        else:
            self.event_id_set.add(mid)
            return True

    def event_de_normalize(self, event):
        # TODO
        return event
