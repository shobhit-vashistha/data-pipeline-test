import multiprocessing
import time
import unittest

import log
from backend.mock.driver import Driver


class BaseDriverTest(unittest.TestCase):

    def setUp(self) -> None:
        self.driver = Driver()
        self.test_topic = 'test.topic'
        self.consumer_group = 'test_consumer_group'
        self.consumer_group_other = 'test_consumer_group_other'
        self.consumer_group_latest = 'test_consumer_group_latest'

    def tearDown(self) -> None:
        pass


class DriverTest(BaseDriverTest):

    def test_create_delete_topic(self):
        # create
        topic_created = self.driver.create_topic(self.test_topic)
        self.assertTrue(topic_created)
        self.assertTrue(self.driver.topic_exists(self.test_topic))

        # try to create duplicate topic
        dup_topic_created = self.driver.create_topic(self.test_topic)
        self.assertFalse(dup_topic_created)

        # delete
        topic_deleted = self.driver._delete_topic(self.test_topic)
        self.assertTrue(topic_deleted)
        self.assertFalse(self.driver.topic_exists(self.test_topic))

        # try to delete again
        topic_deleted = self.driver._delete_topic(self.test_topic)
        self.assertFalse(topic_deleted)


class BaseTopicTest(BaseDriverTest):

    def setUp(self) -> None:
        super().setUp()
        topic_created = self.driver.create_topic(self.test_topic)
        self.assertTrue(topic_created)
        self.assertTrue(self.driver.topic_exists(self.test_topic))
        self.assertEqual(self.driver.get_topic_length(self.test_topic), 0)

    def tearDown(self) -> None:
        super().tearDown()
        topic_deleted = self.driver._delete_topic(self.test_topic)
        self.assertTrue(topic_deleted)
        self.assertFalse(self.driver.topic_exists(self.test_topic))


class DriverTopicTest(BaseTopicTest):

    def test_message(self):
        # add first message
        self.driver.add_message(self.test_topic, 'hello')
        self.assertEqual(self.driver.get_topic_length(self.test_topic), 1)
        self.assertEqual(self.driver.get_message(self.test_topic, 0), 'hello')

        # add second message
        self.driver.add_message(self.test_topic, 'world')
        self.assertEqual(self.driver.get_topic_length(self.test_topic), 2)
        self.assertEqual(self.driver.get_message(self.test_topic, 1), 'world')

        # check if we can read the messages from multiple consumers
        for consumer_group in [self.consumer_group, self.consumer_group_other]:
            self.assertEqual(self.driver.get_consumer_offset(self.test_topic, consumer_group), 0)

            self.assertEqual(self.driver.consume_message(self.test_topic, consumer_group), 'hello')
            self.assertEqual(self.driver.get_consumer_offset(self.test_topic, consumer_group), 1)

            self.assertEqual(self.driver.consume_message(self.test_topic, consumer_group), 'world')
            self.assertEqual(self.driver.get_consumer_offset(self.test_topic, consumer_group), 2)

            self.assertEqual(self.driver.consume_message(self.test_topic, consumer_group), None)
            self.assertEqual(self.driver.get_consumer_offset(self.test_topic, consumer_group), 2)

        # check set_offset_latest
        self.driver.set_offset_latest(self.test_topic, self.consumer_group_latest)
        self.assertEqual(self.driver.consume_message(self.test_topic, self.consumer_group_latest), None)
        self.assertEqual(self.driver.get_consumer_offset(self.test_topic, self.consumer_group_latest), 2)

        # check if they get more messages
        self.driver.add_message(self.test_topic, 'wide')
        self.assertEqual(self.driver.get_topic_length(self.test_topic), 3)
        self.assertEqual(self.driver.consume_message(self.test_topic, self.consumer_group), 'wide')
        self.assertEqual(self.driver.consume_message(self.test_topic, self.consumer_group_other), 'wide')
        self.assertEqual(self.driver.consume_message(self.test_topic, self.consumer_group_latest), 'wide')


def produce_random_messages(name, topic, count):
    driver = Driver()
    for i in range(count):
        driver.add_message(topic, '%s:%s' % (name, i))


def consume_messages_and_print(group, topic, idle_timeout=0.5):
    driver = Driver()
    count = 0
    start = time.time()
    while True:
        msg = driver.consume_message(topic, group)
        if msg is None:
            if time.time() - start > idle_timeout:
                return count
            time.sleep(0.01)
            continue
        start = time.time()
        log.p('%s - %s' % (group, msg))
        count += 1
    return count


class DriverTopicParallelTest(BaseTopicTest):

    def test_parallel(self):
        pool = multiprocessing.Pool(4)

        producer_1_name = 'P_1'
        producer_2_name = 'P_2'

        consumer_1_group = 'G_1'
        consumer_2_group = 'G_2'

        producer_1_count = 100
        producer_2_count = 150
        total_messages = producer_1_count + producer_2_count

        p1_async_result = pool.apply_async(produce_random_messages, args=(producer_1_name, self.test_topic, producer_1_count))
        p2_async_result = pool.apply_async(produce_random_messages, args=(producer_2_name, self.test_topic, producer_2_count))

        c1a_count_async_result = pool.apply_async(consume_messages_and_print, args=(consumer_1_group, self.test_topic))
        c1b_count_async_result = pool.apply_async(consume_messages_and_print, args=(consumer_1_group, self.test_topic))
        c2a_count_async_result = pool.apply_async(consume_messages_and_print, args=(consumer_2_group, self.test_topic))
        c2b_count_async_result = pool.apply_async(consume_messages_and_print, args=(consumer_2_group, self.test_topic))

        results = [p1_async_result, p2_async_result, c1a_count_async_result, c1b_count_async_result, c2a_count_async_result, c2b_count_async_result]

        [r.wait() for r in results]

        c1a_count = c1a_count_async_result.get()
        c1b_count = c1b_count_async_result.get()
        c2a_count = c2a_count_async_result.get()
        c2b_count = c2b_count_async_result.get()

        self.assertEqual(c1a_count + c1b_count, total_messages)
        self.assertEqual(c2a_count + c2b_count, total_messages)


def run_test_tests():
    suite = unittest.TestSuite()
    suite.addTest(DriverTest('test_create_delete_topic'))
    suite.addTest(DriverTopicTest('test_message'))
    suite.addTest(DriverTopicParallelTest('test_parallel'))
    runner = unittest.TextTestRunner()
    runner.run(suite)
