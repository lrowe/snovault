"""Tests the MPIndexer with simple queue"""
from multiprocessing.pool import Pool
from unittest import TestCase

from snovault.elasticsearch.mpindexer import MPIndexer
from snovault.elasticsearch.simple_queue import (
    SimpleUuidServer,
    SimpleUuidWorker,
)

from .test_indexer_simple import (
    MockRegistry,
)


class TestMPIndexer(TestCase):
    """Test Indexer in indexer.py"""
    @classmethod
    def setUpClass(cls):
        # Request
        cls.embed_wait = 0.01
        # Server Objedcts
        cls.xmin = None
        cls.snapshot_id = 'Some-Snapshot-Id'
        cls.restart = False
        # Indexer
        batch_size = 100
        processes = 4
        cls.registry = MockRegistry(batch_size, processes=processes)
        cls.mpindexer = MPIndexer(cls.registry, processes=processes)

    def test_init(self):
        """Test Indexer Init"""
        self.assertEqual(self.mpindexer.maxtasks, 1)
        self.assertIsInstance(self.mpindexer.queue_server, SimpleUuidServer)
        self.assertIsInstance(self.mpindexer.queue_worker, SimpleUuidWorker)
        self.assertTrue(hasattr(self.mpindexer, 'initargs'))
        self.assertIsInstance(self.mpindexer.initargs, tuple)
        self.assertEqual(
            str(type(self.mpindexer.initargs[0])),
            "<class 'function'>"
        )
        self.assertTrue(self.mpindexer.initargs[1] is self.registry.settings)

    def test_pool_init(self):
        """Test pool initializes"""
        mp_pool = self.mpindexer.pool
        self.assertIsInstance(mp_pool, Pool)

    def test_pool_reify(self):
        """Test pool reify"""
        try:
            self.mpindexer.pool()  # pylint: disable=not-callable
        except TypeError:
            pass
        else:
            print('MPIndexer should not be callable.  Missing @reify?')
            assert False
