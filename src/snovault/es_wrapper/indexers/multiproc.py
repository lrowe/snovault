'''Multiprocessing Indexer'''
import atexit
import logging
import time

from contextlib import contextmanager
from multiprocessing import get_context
from multiprocessing.pool import Pool

import transaction

from pyramid.decorator import reify
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager,
)

from snovault import DBSESSION

from . import INDEXER
from .primary import PrimaryIndexer


log = logging.getLogger(__name__)  # pylint: disable=invalid-name
current_xmin_snapshot_id = None  # pylint: disable=invalid-name
app = None  # pylint: disable=invalid-name


def initializer(app_factory, settings):
    '''Pool Function'''
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    global app  # pylint: disable=global-statement, invalid-name
    atexit.register(clear_snapshot)
    app = app_factory(settings, indexer_worker=True, create_tables=False)
    signal.signal(signal.SIGALRM, clear_snapshot)


def set_snapshot(xmin, snapshot_id):
    '''Pool Function'''
    global current_xmin_snapshot_id  # pylint: disable=global-statement, invalid-name
    if current_xmin_snapshot_id == (xmin, snapshot_id):
        return
    clear_snapshot()
    current_xmin_snapshot_id = (xmin, snapshot_id)

    while True:
        txn = transaction.begin()
        txn.doom()
        if snapshot_id is not None:
            txn.setExtendedInfo('snapshot_id', snapshot_id)
        session = app.registry[DBSESSION]()
        connection = session.connection()
        db_xmin = connection.execute(
            "SELECT txid_snapshot_xmin(txid_current_snapshot());").scalar()
        if db_xmin >= xmin:
            break
        transaction.abort()
        log.info('Waiting for xmin %r to reach %r', db_xmin, xmin)
        time.sleep(0.1)

    registry = app.registry
    request = app.request_factory.blank('/_indexing_pool')
    request.registry = registry
    request.datastore = 'database'
    apply_request_extensions(request)
    request.invoke_subrequest = app.invoke_subrequest
    request.root = app.root_factory(request)
    request._stats = {}  # pylint: disable=protected-access
    manager.push({'request': request, 'registry': registry})


def clear_snapshot(signum=None, frame=None):
    '''Pool Function'''
    # pylint: disable=unused-argument
    global current_xmin_snapshot_id  # pylint: disable=global-statement, invalid-name
    if current_xmin_snapshot_id is None:
        return
    transaction.abort()
    manager.pop()
    current_xmin_snapshot_id = None


@contextmanager
def snapshot(xmin, snapshot_id):
    '''Pool Function'''
    import signal
    signal.alarm(0)
    set_snapshot(xmin, snapshot_id)
    yield
    signal.alarm(5)


def update_object_in_snapshot(args):
    '''Pool Function'''
    uuid, xmin, snapshot_id, restart = args
    with snapshot(xmin, snapshot_id):
        request = get_current_request()
        indexer = request.registry[INDEXER]
        return indexer.update_object(request, uuid, xmin, restart)


class MPIndexer(PrimaryIndexer):
    '''Multi Processing Indexer'''
    #   Pooled processes will exit and be replaced after this
    # many tasks are completed.
    maxtasks = 1
    def __init__(self, es_inst, es_index, **kwargs):
        super(MPIndexer, self).__init__(es_inst, es_index)
        self.processes = kwargs['processes']
        self.chunksize = kwargs['chunksize']
        self.initargs = (
            kwargs['app_factory'],
            kwargs['registry_settings'],
        )

    @reify
    def pool(self):
        '''Multiproceessing Pool'''
        return Pool(
            processes=self.processes,
            initializer=initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasks,
            context=get_context('forkserver'),
        )

    def update_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False
        ):
        '''Indexes uuids'''
        # pylint: disable=too-many-arguments, unused-argument
        chunkiness = ((len(uuids) - 1) // self.processes) + 1
        if chunkiness > self.chunksize:
            chunkiness = self.chunksize
        tasks = [(uuid, xmin, snapshot_id, restart) for uuid in uuids]
        errors = []
        try:
            for i, error in enumerate(
                    self.pool.imap_unordered(
                        update_object_in_snapshot, tasks, chunkiness
                    )
                ):
                if error is not None:
                    errors.append(error)
                if (i + 1) % 50 == 0:
                    log.info('Indexing %d', i + 1)
        except:
            self.shutdown()
            raise
        return errors

    def shutdown(self):
        '''Mulitprocess Shutdown Function'''
        if 'pool' in self.__dict__:
            self.pool.terminate()
            self.pool.join()
            del self.pool
