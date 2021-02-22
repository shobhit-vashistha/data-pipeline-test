"""
Start - This method initializes capture of telemetric data associated to the start of user action
Impression - This method is used to capture telemetry for user visits to a specific page.
Interact - This method is used to capture user interactions on a page. For example, search, click, preview, move, resize, configure
Assess - This method is used to capture user assessments that happen while playing content.
Response - This method is used to capture user responses. For example; response to a poll, calendar event or a question.
Interrupt - This method is used to capture  interrupts triggered during user activity. For example;  mobile app sent to background, call on the mobile, etc.
Feedback - This method is used to capture user feedback
Share - This method is used to capture everything associated with sharing. For example; Share content, telemetry data, link, file etc.
Audit - This method is used to log telemetry when an object is changed. This includes life-cycle changes as well
Error - This method is used to capture when users face an error
Heartbeat - This method is used to log telemetry for heartbeat event to denote that the process is running
Log - This method is used to capture generic logging of events. For example; capturing logs for API calls, service calls, app updates etc.
Search - This method is used to capture the search state i.e. when search is triggered for content, item, assets etc.
Metrics - This method is used to log telemetry for service business metrics
Summary - This method is used to log telemetry summary event
Exdata - This method is used as a generic wrapper event to capture encrypted or serialized data
End - This method is used to capture closure after all the activities are completed

"""
import uuid
import time

from test.env import ENV

EVENT_TYPES = {
    'START',
    'IMPRESSION',
    'INTERACT',
    'ASSESS',
    'RESPONSE',
    'INTERRUPT',
    'FEEDBACK',
    'SHARE',
    'AUDIT',
    'ERROR',
    'HEARTBEAT',
    'LOG',
    'SEARCH',
    'METRICS',
    'SUMMARY',
    'EXDATA',
    'END'
}


def get_impression_event_data():
    return get_event_data('IMPRESSION')


def get_event_data(event_type):
    if event_type not in EVENT_TYPES:
        raise AssertionError('Invalid event type "%s"' % event_type)
    return {
        "id": "api.sunbird.telemetry",
        "ver": "3.0",
        "ets": timestamp(),
        "syncts": timestamp(),
        "mid": new_uuid(),
        "params": {
            "msgid": new_uuid()
        },
        "events": [
            {
                "eid": event_type,
                "ets": timestamp(),
                "ver": "3.0",
                "mid": new_uuid(),
                "actor": {
                    "id": "1f84c02a-5098-4358-b014-318fecb69327",
                    "type": "User"
                },
                "context": {
                    "channel": "in.ekstep",
                    "pdata": {
                        "id": "web-ui",
                        "ver": "1.0.0",
                        "pid": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36"
                    },
                    "env": ENV,
                    "sid": "",
                    "did": "2c90e550e243f6285c0955b1e70d1681",
                    "cdata": [],
                    "rollup": {}
                },
                "object": {
                    "ver": "1.0",
                    "id": "page/home"
                },
                "tags": [],
                "edata": {
                    "pageid": "page/home",
                    "pageUrl": "page/home",
                    "pageUrlParts": ["page", "home"],
                    "refferUrl": "page/home",
                    "objectId": None
                }
            }
        ],

    }


def timestamp():
    return int(round(time.time() * 1000))


def new_uuid(prefix='dp-test'):
    return '%s:%s' % (prefix, uuid.uuid4())
