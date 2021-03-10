import json
import traceback

from dp import log


def serialize(stuff):
    try:
        return json.dumps(stuff).encode('utf-8')
    except Exception as e:
        log.e(e, traceback=traceback.format_exc())
        return None


def deserialize(string):
    if isinstance(string, bytes):
        string = string.decode('utf-8')
    try:
        return json.loads(string)
    except Exception as e:
        log.e('Could not decode')
        log.e(string)
        log.e(e, traceback=traceback.format_exc())
        return ''
