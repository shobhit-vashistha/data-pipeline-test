import os
import shutil
import time

from util import deserialize

import locket


def lock(driver, topic):
    return locket.lock_file(driver.topic_lock_file_path(topic))


DATA_DIR_PATH = os.path.join('/mock-kafka-data', '.data')


def write(file_path, msg):
    # log.p('%s < %s' % (file_path, msg))
    if isinstance(msg, str):
        msg = msg.encode('utf-8')
    if not isinstance(msg, bytes):
        raise AssertionError('msg must be str or bytes')
    open(file_path, 'wb').write(msg)


def read(file_path):
    """ returns string """
    # log.p('%s > ' % file_path)
    return open(file_path, 'rb').read().decode('utf-8')


class Producer(object):

    def __init__(self, driver):
        self.driver = driver

    def close(self):
        pass


class Consumer(object):

    def __init__(self, driver, topic, latest, consumer_group, deserialize_func=deserialize):
        self.driver = driver
        self.topic = topic
        self.latest = latest
        self.consumer_group = consumer_group
        self.deserialize_func = deserialize_func
        if self.latest:
            self.set_latest_offset()

    def poll(self, timeout):
        messages = []
        idle_since = time.time()
        while True:
            message = self.driver.consume_message(self.topic, self.consumer_group)
            if not message:
                if time.time() - idle_since > timeout:
                    break
                time.sleep(timeout / 50.0)
            else:
                idle_since = time.time()
                messages.append(self.deserialize_func(message))

        return messages

    def set_latest_offset(self):
        self.driver.set_offset_latest(self.topic, self.consumer_group)

    def close(self):
        pass


class Driver(object):
    messages_dir_name = 'messages'
    offsets_dir_name = 'offsets'
    count_file_name = 'topic.count'

    lock_file_name = 'topic.lock'

    def __init__(self, data_path=DATA_DIR_PATH):
        self.data_path = data_path
        if not os.path.isdir(self.data_path):
            os.mkdir(data_path)

    def get_producer(self):
        return Producer(self)

    def get_consumer(self, topic, latest, consumer_group):
        return Consumer(self, topic, latest, consumer_group)

    def create_topic(self, topic):
        topic_dir_path = self.topic_dir_path(topic)

        if os.path.isdir(topic_dir_path):
            return False

        os.mkdir(topic_dir_path)

        with lock(self, topic):
            # create count file
            count_file_path = self.topic_count_file_path(topic)
            write(count_file_path, '0')

            # create dir for messages
            messages_dir_path = self.topic_message_dir_path(topic)
            os.mkdir(messages_dir_path)

            # create dir for offsets
            offsets_dir_path = self.topic_offsets_dir_path(topic)
            os.mkdir(offsets_dir_path)
            return True

    def _delete_topic(self, topic):
        """ acquires lock and then deletes topic directory
            do not use out of testing
        """
        topic_dir_path = self.topic_dir_path(topic)
        if not os.path.isdir(topic_dir_path):
            return False
        shutil.rmtree(topic_dir_path)
        return True

    def topic_exists(self, topic):
        topic_dir_path = self.topic_dir_path(topic)
        return os.path.isdir(topic_dir_path)

    def get_topic_length(self, topic):
        """ atomic """
        with lock(self, topic):
            return self._get_topic_length(topic)

    def get_message(self, topic, index):
        """ does not need to be atomic, only reading, not changing offsets """
        return self._get_message(topic, index)

    def add_message(self, topic, msg):
        """ atomic """
        with lock(self, topic):
            count = self._get_topic_length(topic)
            self._add_message(topic, count, msg)
            self._set_topic_length(topic, count + 1)

    def consume_message(self, topic, consumer_group):
        """ atomic, read next message and update offset """
        with lock(self, topic):
            offset = self._get_consumer_offset(topic, consumer_group)
            count = self._get_topic_length(topic)
            if offset == count:
                return None
            message = self._get_message(topic, offset)
            self._set_consumer_offset(topic, consumer_group, offset + 1)
            return message

    def set_offset_latest(self, topic, consumer_group):
        """ atomic, set offset to latest """
        with lock(self, topic):
            count = self._get_topic_length(topic)
            self._set_consumer_offset(topic, consumer_group, count)

    def get_consumer_offset(self, topic, consumer_group):
        with lock(self, topic):
            return self._get_consumer_offset(topic, consumer_group)

    def _get_consumer_offset(self, topic, consumer_group):
        try:
            return int(read(self.topic_consumer_offset_file_path(topic, consumer_group)))
        except FileNotFoundError:
            self._set_consumer_offset(topic, consumer_group, 0)
            return 0

    def _set_consumer_offset(self, topic, consumer_group, offset):
        return write(self.topic_consumer_offset_file_path(topic, consumer_group), str(offset))

    def _get_topic_length(self, topic):
        return int(read(self.topic_count_file_path(topic)))

    def _set_topic_length(self, topic, count):
        return write(self.topic_count_file_path(topic), str(count))

    def _get_message(self, topic, index):
        return read(self.topic_message_file_path(topic, index))

    def _add_message(self, topic, index, msg):
        write(self.topic_message_file_path(topic, index), msg)

    def topic_message_file_path(self, topic, index):
        return os.path.join(self.topic_message_dir_path(topic), str(index))

    def topic_dir_path(self, topic):
        return os.path.join(self.data_path, topic)

    def topic_message_dir_path(self, topic):
        return os.path.join(self.topic_dir_path(topic), self.messages_dir_name)

    def topic_offsets_dir_path(self, topic):
        return os.path.join(self.topic_dir_path(topic), self.offsets_dir_name)

    def topic_consumer_offset_file_path(self, topic, consumer_group):
        return os.path.join(self.topic_offsets_dir_path(topic), consumer_group)

    def topic_count_file_path(self, topic):
        return os.path.join(self.topic_dir_path(topic), self.count_file_name)

    def topic_lock_file_path(self, topic):
        return os.path.join(self.topic_dir_path(topic), self.lock_file_name)
