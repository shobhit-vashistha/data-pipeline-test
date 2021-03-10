import json
import multiprocessing as mp
import traceback
import time

from dp.backend.base import get_kafka_client, get_kafka_messages
from dp.config import TEST_CONFIG
from dp.env import KAFKA_TOPIC_INGEST

from dp import log
from dp.backend.base import get_kafka_messages_multi_topic


def test_ingestion_async(batch_data, spec_data):

    ingest_producer = None
    consumer_processes = []

    client = get_kafka_client()

    try:
        manager = mp.Manager()
        results = manager.dict()

        topics = [spec['topic'] for spec in spec_data if spec.get('topic', None)]

        current_offsets = client.get_kafka_offsets(topics)
        ingest_producer = client.get_kafka_producer()

        log.i('Sending event data...')
        for event_data in batch_data:
            client.send(ingest_producer, KAFKA_TOPIC_INGEST, event_data)

        for spec in spec_data:
            topic = spec['topic']
            offsets = current_offsets[topic]
            pro = mp.Process(
                target=get_kafka_messages,
                args=(results, topic, batch_data, spec_data, offsets)
            )
            consumer_processes.append(pro)

        log.i('Starting consumer processes...')
        for pro in consumer_processes:
            pro.start()

        # wait for all processes to become active
        while not all(pro.is_alive() for pro in consumer_processes):
            time.sleep(0.1)
        # wait 2 more seconds
        time.sleep(2.0)

        log.i('Waiting for consumers to finish...')
        for pro in consumer_processes:
            pro.join()

        return process_results(results)

    except Exception as e:
        log.e(e, 'Error while running test', traceback=traceback.format_exc())
    finally:
        try:
            log.i('Closing producer')
            if ingest_producer:
                ingest_producer.close()

            log.i('Closing consumers')
            for pro in consumer_processes:
                pro.close()
        except Exception as e:
            log.e(e, msg='Error while closing processes')


def test_ingestion_multi_topic(batch_data, expected_counts, consumer_timeout):

    ingest_producer = None
    consumer_process = None

    client = get_kafka_client()

    try:
        manager = mp.Manager()
        results = manager.dict()

        ingest_producer = client.get_kafka_producer()

        consumer_process = mp.Process(
            target=get_kafka_messages_multi_topic,
            args=(results, batch_data, expected_counts, consumer_timeout)
        )

        log.i('Starting consumer processes...')
        consumer_process.start()

        # wait for all processes to become active
        while not consumer_process.is_alive():
            time.sleep(0.1)
        # wait 3 more seconds
        time.sleep(3.0)

        log.i('Sending event data...')
        for event_data in batch_data:
            client.send(ingest_producer, KAFKA_TOPIC_INGEST, event_data)

        log.i('Waiting for consumers to finish...')
        consumer_process.join()

        return process_results(results)

    except Exception as e:
        log.e(e, 'Error while running test', traceback=traceback.format_exc())
    finally:
        try:
            log.i('Closing producer')
            if ingest_producer:
                ingest_producer.close()

            log.i('Closing consumers')
            if consumer_process:
                consumer_process.close()
        except Exception as e:
            log.e(e, msg='Error while closing processes')


def test_ingestion(batch_data, expected_counts):

    ingest_producer = None
    consumer_processes = []

    client = get_kafka_client()

    try:
        manager = mp.Manager()
        results = manager.dict()

        ingest_producer = client.get_kafka_producer()

        for topic, expected_count in expected_counts.items():
            pro = mp.Process(
                target=get_kafka_messages,
                args=(results, topic, batch_data, expected_count)
            )
            consumer_processes.append(pro)

        log.i('Starting consumer processes...')
        for pro in consumer_processes:
            pro.start()

        # wait for all processes to become active
        while not all(pro.is_alive() for pro in consumer_processes):
            time.sleep(0.1)
        # wait 2 more seconds
        time.sleep(2.0)

        log.i('Sending event data...')
        for event_data in batch_data:
            client.send(ingest_producer, KAFKA_TOPIC_INGEST, event_data)

        log.i('Waiting for consumers to finish...')
        for pro in consumer_processes:
            pro.join()

        return process_results(results)

    except Exception as e:
        log.e(e, 'Error while running test', traceback=traceback.format_exc())
    finally:
        try:
            log.i('Closing producer')
            if ingest_producer:
                ingest_producer.close()

            log.i('Closing consumers')
            for pro in consumer_processes:
                pro.close()
        except Exception as e:
            log.e(e, msg='Error while closing processes')


def run_test(test_config):
    log.p("_" * 40)
    log.i("Running Test: " + test_config['name'])
    log.d("- config =")
    log.d(test_config)

    events_data_function = test_config['events_data_function']

    log.i("Generating event data")
    events_data, expected_counts = events_data_function()
    log.d(events_data)

    return test_ingestion(events_data)


def run_all_tests():
    all_results = []
    for test_config in TEST_CONFIG:
        results = run_test(test_config)
        all_results.append({
            'name': test_config['name'],
            'results': results
        })
    return all_results


def process_results(results):
    test_passed_dict = {}
    result_data_dict = {}
    log.i('Results:')
    for topic, data in results.items():
        wait_seconds = data.get('wait_seconds', None)
        passed = data.get('passed', False)
        actual_count = data.get('actual_count', None)
        expected_count = data.get('expected_count', None)
        ignored_count = data.get('ignored_count', None)
        messages = data.get('messages', None)

        log.i('Topic: %s' % topic)
        log.i('- Wait: %s' % wait_seconds)
        log.i('- Ignored: %s' % ignored_count)
        log.d('- Messages:')
        log.d(messages)

        result_data_dict[topic] = data
        test_passed_dict[topic] = passed
        if passed:
            log.i('- PASSED: message_count = expected_message_count = %s' % actual_count)
        else:
            log.i('- FAILED: message_count = %s,  expected_message_count = %s' % (actual_count, expected_count))

    return {
        'results': result_data_dict,
        'passed': bool(test_passed_dict) and all(p for p in test_passed_dict.values())
    }


def format_results(test_results, html=False):
    # TODO
    return json.dumps(test_results, indent=4)


def print_test_results(test_results, html=False, file=None):
    # TODO
    log.p(format_results(test_results))
