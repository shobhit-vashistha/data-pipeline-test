import json
import multiprocessing as mp
import traceback

from backend import get_kafka_client, get_kafka_messages
from config import TEST_CONFIG
from env import KAFKA_TOPIC_INGEST

import log


def run_test(test_config):
    log.p("_" * 40)
    log.i("Running Test: " + test_config['name'])
    log.d("- config =")
    log.d(test_config)

    events_data_function = test_config['events_data_function']

    log.i("Generating event data")
    events_data, expected_counts = events_data_function()
    log.d(events_data)

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
                args=(results, topic, events_data, expected_count)
            )
            consumer_processes.append(pro)

        log.i('Starting consumer processes...')
        for pro in consumer_processes:
            pro.start()

        log.i('Sending event data...')
        for event_data in events_data:
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
