import os
import time


SOURCE_FILE_DIR_PATH = os.path.dirname(__file__)

DATA_DIR_PATH = SOURCE_FILE_DIR_PATH / '.data'


def write(file_path, msg):
    open(file_path, 'wb').write(msg.encode('utf-8'))


def read(file_path):
    return open(file_path, 'rb').read().decode('utf-8')


class TopicLock(object):

    def __init__(self, driver, topic):
        self.driver = driver
        self.topic = topic

    def __enter__(self):
        self.driver.get_lock(self.topic)

    def __exit__(self, typ, value, traceback):
        self.driver.release_lock(self.topic)


class Driver(object):
    messages_dir_name = 'messages'
    offsets_dir_name = 'offsets'
    count_file_name = 'topic.count'

    lock_file_name = 'topic.lock'

    def __init__(self, data_path):
        self.data_path = data_path
        if not os.path.isdir(self.data_path):
            os.mkdir(data_path)

    def create_topic(self, topic):
        topic_dir_path = self.topic_dir_path(topic)
        if os.path.isdir(topic_dir_path):
            return False
        os.mkdir(topic_dir_path)

        # get lock before doing anything else
        lock_file_path = self.topic_lock_file_path(topic)
        write(lock_file_path, '1')

        # create count file
        count_file_path = self.topic_count_file_path(topic)
        write(count_file_path, '0')

        # create dir for messages
        messages_dir_path = self.topic_message_dir_path(topic)
        os.mkdir(messages_dir_path)

        # create dir for offsets
        offsets_dir_path = self.topic_offsets_dir_path(topic)
        os.mkdir(offsets_dir_path)

        self.release_lock(topic)

    def get_topic_length(self, topic):
        """ atomic """
        with TopicLock(self, topic):
            return self._get_topic_length(topic)

    def get_message(self, topic, index):
        """ does not need to be atomic only reading, not changing offsets """
        return self._get_message(topic, index)

    def add_message(self, topic, msg):
        """ atomic """
        with TopicLock(self, topic):
            count = self._get_topic_length(topic)
            self._add_message(topic, count, msg)
            self._set_topic_length(topic, count + 1)

    def consume_message(self, topic, consumer_group):
        """ atomic, read next message and update offset """
        with TopicLock(self, topic):
            offset = self._get_consumer_offset(topic, consumer_group)
            message = self._get_message(topic, offset)
            self._set_consumer_offset(topic, consumer_group, offset + 1)
            return message

    def _get_consumer_offset(self, topic, consumer_group):
        return int(read(self.topic_consumer_offset_file_path(topic, consumer_group)))

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

    def is_locked(self, topic):
        return int(read(self.topic_lock_file_path(topic))) == 1

    def release_lock(self, topic):
        write(self.topic_lock_file_path(topic), '0')

    def get_lock(self, topic, timeout=2.0):
        start = time.time()
        while self.is_locked(topic):
            timed_out = time.time() - start < timeout
            if timed_out:
                break
            time.sleep(0.05)
        if self.is_locked(topic):
            raise AssertionError("Could not acquire lock for Topic: %s" % topic)
        else:
            write(self.topic_lock_file_path(topic), '1')

    def topic_message_file_path(self, topic, index):
        return self.topic_message_dir_path(topic) / str(index)

    def topic_dir_path(self, topic):
        return self.data_path / topic

    def topic_message_dir_path(self, topic):
        return self.topic_dir_path(topic) / self.messages_dir_name

    def topic_offsets_dir_path(self, topic):
        return self.topic_dir_path(topic) / self.offsets_dir_name

    def topic_consumer_offset_file_path(self, topic, consumer_group):
        return self.topic_offsets_dir_path(topic) / consumer_group

    def topic_count_file_path(self, topic):
        return self.topic_dir_path(topic) / self.count_file_name

    def topic_lock_file_path(self, topic):
        return self.topic_dir_path(topic) / self.lock_file_name
