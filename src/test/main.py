import multiprocessing as mp

from env import KAFKA_TOPIC_INGEST, KAFKA_TOPIC_UNIQUE, KAFKA_TOPIC_RAW, KAFKA_TOPIC_DE_NORM, KAFKA_TOPIC_DRUID_EVENTS
from event import get_impression_event_data

# try:
#     from confluent.consumer import get_kafka_messages
#     from confluent import producer
# except ImportError:

from kp.consumer import get_kafka_messages
from kp import producer

from util import pr, pre

TEST_CONFIG = [
    {
        'name': 'normal-event-test',
        'consumer_topics': [
            KAFKA_TOPIC_INGEST,  # KAFKA_TOPIC_RAW, KAFKA_TOPIC_UNIQUE, KAFKA_TOPIC_DE_NORM, KAFKA_TOPIC_DRUID_EVENTS
        ],
        'limits': {
            KAFKA_TOPIC_INGEST: 1
        }
    }
]


def run_test(test_config):
    pr("Running Test with config")
    pr(test_config)
    limits = test_config['limits']

    pr("Generating event data")
    event_data = get_impression_event_data()
    pr(event_data)

    ingest_producer = None
    consumer_processes = []

    try:
        manager = mp.Manager()
        results = manager.dict()

        ingest_producer = producer.get_kafka_producer()

        for topic in test_config['consumer_topics']:
            pro = mp.Process(
                target=get_kafka_messages,
                args=(results, topic, event_data, limits.get(topic, None))
            )
            consumer_processes.append(pro)

        pr('Starting consumer processes...')
        for pro in consumer_processes:
            pro.start()

        pr('Sending event data...')
        producer.send(ingest_producer, KAFKA_TOPIC_INGEST, event_data)

        pr('Waiting for consumers to finish...')
        for pro in consumer_processes:
            pro.join()

        pr('Results:')
        pr(results)

    except Exception as e:
        pre(e, 'error while running test')
    finally:
        try:
            pr('Closing producer')
            if ingest_producer:
                ingest_producer.close()

            pr('Closing consumer')
            for pro in consumer_processes:
                pro.close()
        except Exception as e:
            pre(e, 'error while closing processes')


def run_all_tests():
    for test_config in TEST_CONFIG:
        run_test(test_config)


if __name__ == '__main__':
    run_all_tests()
