import json
import time
import uuid

from django.http import JsonResponse
from django.shortcuts import render
from rest_framework import status
# from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response
from rest_framework.decorators import api_view

# from web.dp.run import test_ingestion, test_ingestion_async
# from web.interface.models import TestCase
from dp.backend.base import get_filtered_kafka_messages
from dp.run import test_ingestion, test_ingestion_multi_topic


def home(request):
    if request.method != 'GET':
        return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)
    return render(request, 'poc.html', {})


def test(request):
    if request.method != 'POST':
        return Response(status=status.HTTP_405_METHOD_NOT_ALLOWED)
    data = json.loads(request.body)
    event_data = data['data']
    consumer_timeout = data['consumer_timeout']

    to_de_duplicate = data.get('de_duplicate', True)
    if to_de_duplicate:
        event_data = [de_duplicate(e) for e in event_data]

    consumer_data = data['consumers']

    expected_counts = {d['kafka_topic']: d['expected_count'] for d in consumer_data if d['capture']}

    results = test_ingestion_multi_topic(event_data, expected_counts, consumer_timeout)
    result_data = {
        'data': event_data,
        'expected_counts': expected_counts,
        'result': results
    }
    return JsonResponse({'status': 'success', 'data': result_data})


def de_duplicate(event_data):
    event_data['mid'] = new_uuid()
    event_data['params']['msgid'] = new_uuid()
    event_data['ets'] = timestamp()
    event_data['syncts'] = timestamp()
    for event in event_data['events']:
        event['ets'] = timestamp()
        event['mid'] = new_uuid()
    return event_data


def timestamp():
    return int(round(time.time() * 1000))


def new_uuid(prefix='dp-test'):
    return '%s:%s' % (prefix, uuid.uuid4())


#
# def run(request):
#     data = request.data
#     test_case_data = data['test']
#
#
#
# @api_view(['POST'])
# def consume(request):
#     data = request.data
#     topic = data.get('topic', None)
#     consumer_timeout = data.get('consumer_timeout', None) or 1
#     offset = data.get('offset', 'latest')
#     offset_valid = offset == 'latest' or offset == 'earliest' or int(offset) > 0
#
#     filter_func_data = data.get('filter', None)
#     expected_count = data.get('expected_count', None)
#     filter_func = get_filter_func(filter_func_data)
#
#     download = data.get('download', None)
#
#     if not topic or not offset_valid:
#         return Response({'status': 'error', 'errors': ['topic or offset not valid']},
#                         status=status.HTTP_400_BAD_REQUEST)
#
#     results = get_filtered_kafka_messages(topic, filter_func, expected_count, offset, consumer_timeout)
#     data = {
#         'status': 'success',
#         'data': {
#             'request_data': data,
#             'results': results
#         },
#     }
#     return Response(data)
#
#
# def get_filter_func(filter_func_data):
#     # return pass all by default
#     return lambda *args, **kwargs: True
#
#
# @api_view(['POST'])
# def consume_test_case(request):
#     data = request.data
#     test_case_data = data['test']
#     test_case, errors, resp_status = get_or_create_test_case(test_case_data)
#     if errors:
#         resp = {''}
#         return Response(data=resp, status=resp_status)
#
#
# def get_or_create_test_case(test_case_data):
#     tc_id = test_case_data.get('id', None)
#     # TestCase

# class JSONSchemaValidator(object):
#
#     def __init__(self, schema):
#         self.schema = schema
#
#     def validate(self, data):
#         return []
#
#
# def validate_request_data(data, validators=None):
#     if not validators:
#         return []
#     all_errors = []
#     for v in validators:
#         if isinstance(v, str):
#             v = JSONSchemaValidator(v)
#         errors = v.validate(data)
#         all_errors.extend(errors)
#     return all_errors
#
#
# @api_view(['POST'])
# def ingest_test_async(request):
#     data = request.data
#     if not data.get('validation_off', False):
#         errors = validate_request_data(data)
#         if errors:
#             return Response({'status': 'error', 'errors': errors}, status=400)
#
#     test_spec = data('spec')
#     batch_event_data = data.get('data')
#
#     results = run_test(test_spec, batch_event_data)
#
#     return Response({'status': 'success', 'data': results})
#
#
# def run_test(test_spec, batch_event_data):
#     test_case = get_or_create_test_case(test_spec, batch_event_data)
#     return {}
#
#
# def get_or_create_test_case(test_spec, batch_event_data):
#     test_case = TestCase.objects.create(name=test_spec['name'], data=batch_event_data)
#     return test_case
#
#
# @api_view(['GET'])
# def ingest_test_blocking(request):
#     data = request.data
#     batch_data = data['batch_data']
#     expected_counts = data['expected_counts']
#     results = test_ingestion(batch_data, expected_counts)
#     result_data = {
#         'batch_data': batch_data,
#         'expected_counts': expected_counts,
#         'result': results
#     }
#     return Response(result_data)


