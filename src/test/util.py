import json
import traceback

import log


def serialize(stuff):
    try:
        return json.dumps(stuff).encode('utf-8')
    except Exception as e:
        log.e(e, traceback=traceback.format_exc())
        return None


def deserialize(string):
    try:
        return json.loads(string.decode('utf-8'))
    except Exception as e:
        log.e(e, traceback=traceback.format_exc())
        return ''
