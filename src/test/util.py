import json
import sys
import traceback


def pr(stuff):
    print(stuff)
    sys.stdout.flush()


def pre(error, msg=None):
    pr('\nERROR! ' + ('' if msg is None else str(msg)))
    pr(error)
    traceback.print_exc()


def serialize(stuff):
    try:
        return json.dumps(stuff).encode('utf-8')
    except Exception as e:
        traceback.print_exc()
        print(e)
        return None


def deserialize(string):
    try:
        return json.loads(string.decode('utf-8'))
    except Exception as e:
        traceback.print_exc()
        print(e)
        return ''