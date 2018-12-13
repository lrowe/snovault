"""Test Base Queue File"""
from unittest import (
    TestCase,
    mock,
)

from ..queues.base_queue import (
    BASE_QUEUE_TYPE,
    BaseQueueClient,
    BaseQueueMeta,
    BaseQueue,
)


MOCK_TIME = 123456.654321


class TestBaseClient(TestCase):
    '''Test Base Queue Client Class'''
    def test_get_queue(self):
        '''Test get_queue'''
        client = BaseQueueClient()
        queue = client.get_queue(BASE_QUEUE_TYPE)
        self.assertIsInstance(queue, BaseQueue)

    def test_get_queue_bad_type(self):
        '''Test get_queue with bad queue type'''
        client = BaseQueueClient()
        try:
            _ = client.get_queue('FakeType')
        except ValueError:
            pass
        else:
            self.fail('ValueError expected')


class TestBaseQueueMeta(TestCase):
    '''Test Base Queue Meta Class'''
    # pylint: disable=too-many-public-methods
    members = [
        ('_base_id', int),
        ('_errors', list),
        ('_uuid_count', int),
        ('_worker_conns', dict),
        ('_worker_results', dict),
    ]
    meth_func = [
        # Errors
        'add_errors',
        'has_errors',
        'pop_errors',
        # Worker
        '_get_blank_worker',
        'add_worker_conn',
        'get_worker_conns',
        'get_worker_conn_count',
        'update_worker_conn',
        'save_work_results',
        # Uuids
        'has_uuids',
        'update_uuid_count',
    ]

    @classmethod
    @mock.patch('time.time', mock.MagicMock(return_value=MOCK_TIME))
    def setUpClass(cls):
        cls.mock_time_us = int(MOCK_TIME * 1000000)
        cls.queue_meta = BaseQueueMeta()
        cls.standard_errors = ['e' + str(cnt) for cnt in range(10)]

    @classmethod
    def tearDownClass(cls):
        '''Clean up after running test class'''
        pass

    def setUp(self):
        '''Setup before each method call'''
        pass

    def tearDown(self):
        """Clean up after each method call"""
        # pylint: disable=protected-access
        self.queue_meta._errors = []
        self.queue_meta._worker_results = {}
        self.queue_meta._worker_conns = {}

    # private/protected
    def test_init_dir(self):
        '''Test SimpleUuidServer has expected function and variables'''
        dir_items = [
            item
            for item in dir(self.queue_meta)
            if item[0:2] != '__'
        ]
        member_names = [name for name, _ in self.members]
        for item in dir_items:
            if item in member_names:
                continue
            if item in self.meth_func:
                continue
            raise AssertionError('Member or method not being tested:%s' % item)
        for item, item_type in self.members:
            self.assertTrue(hasattr(self.queue_meta, item))
            self.assertIsInstance(
                getattr(self.queue_meta, item),
                item_type
            )
        for item in self.meth_func:
            self.assertTrue(hasattr(self.queue_meta, item))
            self.assertTrue(
                str(type(getattr(self.queue_meta, item))) in [
                    "<class 'method'>",
                    "<class 'function'>"
                ]
            )

    def test_init_basic(self):
        '''Test basic queue meta init'''
        # pylint: disable=protected-access
        self.assertEqual(self.mock_time_us, self.queue_meta._base_id)
        self.assertListEqual([], self.queue_meta._errors)
        self.assertEqual(0, self.queue_meta._uuid_count)
        self.assertDictEqual({}, self.queue_meta._worker_conns)
        self.assertDictEqual({}, self.queue_meta._worker_results)

    # Errors
    def test_add_errors(self):
        '''Test add_errors'''
        errors_to_add = self.standard_errors.copy()
        expected_result = len(errors_to_add)
        result = self.queue_meta.add_errors(errors_to_add)
        self.assertEqual(expected_result, result)
        errors_to_add.sort()
        # pylint: disable=protected-access
        self.queue_meta._errors.sort()
        self.assertListEqual(errors_to_add, self.queue_meta._errors)

    def test_add_errors_none(self):
        '''Test add_errors when no errors'''
        errors_to_add = []
        expected_result = len(errors_to_add)
        result = self.queue_meta.add_errors(errors_to_add)
        self.assertEqual(expected_result, result)
        errors_to_add.sort()
        # pylint: disable=protected-access
        self.queue_meta._errors.sort()
        self.assertListEqual(errors_to_add, self.queue_meta._errors)

    def test_has_errors(self):
        '''Test has_errors'''
        # pylint: disable=protected-access
        self.queue_meta._errors = self.standard_errors.copy()
        self.assertTrue(self.queue_meta.has_errors())

    def test_has_errors_none(self):
        '''Test has_errors when no errors'''
        # pylint: disable=protected-access
        self.assertFalse(self.queue_meta.has_errors())

    def test_pop_errors(self):
        '''Test pop_errors'''
        # pylint: disable=protected-access
        errors_to_add = self.standard_errors.copy()
        self.queue_meta._errors = errors_to_add
        result = self.queue_meta.pop_errors()
        self.standard_errors.sort()
        result.sort()
        self.assertListEqual(self.standard_errors, result)
        self.assertListEqual([], self.queue_meta._errors)

    # Workers
    def test_get_blank_worker(self):
        '''Test _get_blank_worker'''
        # pylint: disable=protected-access
        self.assertDictEqual(
            {
                'uuid_cnt': 0,
                'get_cnt': 0,
            },
            self.queue_meta._get_blank_worker()
        )

    def test_add_worker_conn(self):
        '''Test add_worker_conn'''
        worker_id = 'test-worker-id'
        self.queue_meta.add_worker_conn(worker_id)
        # pylint: disable=protected-access
        self.assertTrue(worker_id in self.queue_meta._worker_results)
        self.assertListEqual([], self.queue_meta._worker_results[worker_id])
        self.assertTrue(worker_id in self.queue_meta._worker_conns)
        self.assertDictEqual(
            {
                'uuid_cnt': 0,
                'get_cnt': 0,
            },
            self.queue_meta._worker_conns[worker_id]
        )

    def test_get_worker_conns(self):
        '''Test get_worker_conns'''
        # pylint: disable=protected-access
        self.assertEqual(
            id(self.queue_meta._worker_conns),
            id(self.queue_meta.get_worker_conns())
        )

    def test_get_worker_conn_count(self):
        '''Test get_worker_conn_count'''
        # pylint: disable=protected-access
        self.assertEqual(
            len(self.queue_meta._worker_conns),
            self.queue_meta.get_worker_conn_count()
        )
        self.queue_meta._worker_conns['fake-id'] = {}
        self.assertEqual(
            len(self.queue_meta._worker_conns),
            self.queue_meta.get_worker_conn_count()
        )

    def test_update_worker_conn(self):
        '''Test update_worker_conn'''
        # pylint: disable=protected-access
        self.assertEqual(
            id(self.queue_meta._worker_conns),
            id(self.queue_meta.get_worker_conns())
        )


