from os import getpid as os_getpid
from snovault import DBSESSION
from contextlib import contextmanager
from multiprocessing import get_context
from multiprocessing.pool import Pool
from pyramid.decorator import reify
from pyramid.request import apply_request_extensions
from pyramid.threadlocal import (
    get_current_request,
    manager,
)
import atexit
import logging
import time
import transaction
from .indexer import (
    INDEXER,
    Indexer,
)
from .interfaces import APP_FACTORY

log = logging.getLogger(__name__)


def includeme(config):
    if config.registry.settings.get('indexer_worker'):
        return
    processes = config.registry.settings.get('indexer.processes')
    try:
        processes = int(processes)
    except:
        processes = None
    config.registry[INDEXER] = MPIndexer(config.registry, processes=processes)


# Running in subprocess

current_xmin_snapshot_id = None
app = None


def initializer(app_factory, settings):
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    global app
    atexit.register(clear_snapshot)
    app = app_factory(settings, indexer_worker=True, create_tables=False)
    signal.signal(signal.SIGALRM, clear_snapshot)


def set_snapshot(xmin, snapshot_id):
    global current_xmin_snapshot_id
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
    request._stats = {}
    manager.push({'request': request, 'registry': registry})


def clear_snapshot(signum=None, frame=None):
    global current_xmin_snapshot_id
    if current_xmin_snapshot_id is None:
        return
    transaction.abort()
    manager.pop()
    current_xmin_snapshot_id = None


@contextmanager
def snapshot(xmin, snapshot_id):
    import signal
    signal.alarm(0)
    set_snapshot(xmin, snapshot_id)
    yield
    signal.alarm(5)


def update_object_in_snapshot(args):
    uuid, xmin, snapshot_id = args
    with snapshot(xmin, snapshot_id):
        request = get_current_request()
        indexer = request.registry[INDEXER]
        return indexer.update_object(request, uuid, xmin)


class MPIndexer(Indexer):
    '''Multiprocess Indexer'''
    # pooled processes will exit and be replaced after
    # this many tasks are completed.
    maxtasks = 1
    is_mp_indexer = True
    def __init__(self, registry, processes=None):
        super(MPIndexer, self).__init__(registry)
        self.processes = processes
        self.chunksize = int(registry.settings.get('indexer.chunk_size', 1024))
        self.initargs = (registry[APP_FACTORY], registry.settings,)

    @reify
    def pool(self):
        '''Cached pool instance'''
        # pylint: disable=unexpected-keyword-arg
        return Pool(
            processes=self.processes,
            initializer=initializer,
            initargs=self.initargs,
            maxtasksperchild=self.maxtasks,
            context=get_context('forkserver'),
        )

    def update_objects(self, request, uuids, xmin, is_reindex=True):
        # Ensure that we iterate over uuids in this thread not the pool task handler.
        uuid_count = len(uuids)
        workers = 1
        if self.processes is not None and self.processes > 0:
            workers = self.processes
        chunkiness = int((uuid_count - 1) / workers) + 1
        if chunkiness > self.chunksize:
            chunkiness = self.chunksize
        # TODO: REMOVE SHORT HERE
        uuids = self.indexer_data_dump.debug_short_indexer(uuids, 1000)
        # TODO: REMOVE SHORT HERE
        tasks = [(uuid, xmin, self._snapshot_id) for uuid in uuids]
        errors = []
        outputs = []
        updates = {
            '_dump_size': 50000,
            '_is_reindex': is_reindex,
            'chunksize': self.chunksize,
            'chunkiness': chunkiness,
            'processes': self.processes,
            'workers': workers,
        }
        run_info = self.indexer_data_dump.get_run_info(
            os_getpid(),
            uuid_count,
            xmin,
            self._snapshot_id,
            **updates
        )
        try:
            for i, output in enumerate(self.pool.imap_unordered(
                    update_object_in_snapshot, tasks, chunkiness)):
                if output:
                    outputs.append(output)
                    error = output.get('error')
                    if error is not None:
                        errors.append(error)
                if (i + 1) % 50 == 0:
                    log.info('Indexing %d', i + 1)
        except:
            self.shutdown()
            raise
        run_info['end_time'] = time.time()
        self._post_index_process(outputs, run_info)
        return errors

    def shutdown(self):
        if 'pool' in self.__dict__:
            self.pool.terminate()
            self.pool.join()
            del self.pool
