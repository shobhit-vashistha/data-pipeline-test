from dp import data

"""
Flows to test:

- normal event flow  # done
- normal event flow multiple events  # done
- normal event flow multiple batch events  # done
- duplicate batch event flow  # done
- duplicate event flow  # done

TODO:

- audit event flow
- assess event flow

- log event flow

- error event flow
- share event flow

- de-norm event flow
- summary de-norm event flow

- failed event flow

"""


TEST_CONFIG = [
    {
        'name': 'invalid-event-test',
        'events_data_function': data.get_invalid_event_test_data,
    },
    # {
    #     'name': 'individual-event-test',
    #     'events_data_function': data.get_individual_event_test_data,
    # }, {
    #     'name': 'multiple-event-test',
    #     'events_data_function': data.get_multiple_event_test_data,
    # }, {
    #     'name': 'multiple-batch-event-test',
    #     'events_data_function': data.get_multiple_batch_event_test_data,
    # }, {
    #     'name': 'duplicate-batch-event-test',
    #     'events_data_function': data.get_duplicate_batch_event_test_data,
    # }, {
    #     'name': 'duplicate-event-test',
    #     'events_data_function': data.get_duplicate_event_test_data,
    # }
]