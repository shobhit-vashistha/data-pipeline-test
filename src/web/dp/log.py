import functools
import logging
import sys


# debug
def d(stuff):
    p(stuff)
    logging.debug(stuff)


# info
def i(stuff):
    p(stuff)
    logging.info(stuff)


# warning
def w(stuff):
    p(stuff)
    logging.warning(stuff)


# error
def e(error=None, msg=None, traceback=None):
    if msg:
        p(msg)
        logging.error(msg)
    if error:
        p(error)
        logging.error(error)
    if traceback:
        p(traceback)
        logging.debug(traceback)


# only print
def p(stuff):
    print(stuff)
    sys.stdout.flush()


current_tag = ''


def set_tag(tag_str):
    global current_tag
    current_tag = tag_str


def tag(tag_str=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            curr_tag = current_tag
            set_tag(tag_str or '[%s]' % func.__name__)
            result = func(*args, **kwargs)
            set_tag(curr_tag)
            return result
        return wrapper
    return decorator



# def pre(error, msg=None):
#     pr('\nERROR! ' + ('' if msg is None else str(msg)))
#     pr(error)
#     traceback.print_exc()
