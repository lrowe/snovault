import os
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

log = logging.getLogger('snovault.elasticsearch.es_index_listener')


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
    uuid, xmin, snapshot_id, restart = args
    with snapshot(xmin, snapshot_id):
        request = get_current_request()
        indexer = request.registry[INDEXER]
        map_info = {
            'start_time': time.time(),
            'end_time': None,
            'run_time': None,
            'pid':os.getpid(),
        }
        update_info = indexer.update_object(request, uuid, xmin, restart)
        update_info['snapshot_id'] = snapshot_id
        map_info['end_time'] = time.time()
        map_info['run_time'] = map_info['end_time'] - map_info['start_time']
        update_info['map_info'] = map_info
        return update_info


# Running in main process

class MPIndexer(Indexer):
    maxtasks = 1  # pooled processes will exit and be replaced after this many tasks are completed.

    def __init__(self, registry, processes=None):
        super(MPIndexer, self).__init__(registry)
        self.initargs = (registry[APP_FACTORY], registry.settings,)

    @reify
    def pool(self):
        return Pool(
            processes=self.queue_worker.processes,
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
            restart=False,
        ):
        # pylint: disable=too-many-arguments, unused-argument
        '''Run multiprocess indexing process on uuids'''
        # Ensure that we iterate over uuids in this thread not the pool task handler.
        processes = self.queue_worker.processes
        chunk_size = self.queue_worker.chunk_size
        uuid_count = len(uuids)
        chunkiness = int((uuid_count - 1) / processes) + 1
        if chunkiness > chunk_size:
            chunkiness = chunk_size
        tasks = [
            (uuid, xmin, snapshot_id, restart)
            for uuid in uuids
        ]
        errors = []
        update_infos = []
        start_time = time.time()
        try:
            for i, update_info in enumerate(
                    self.pool.imap_unordered(
                        update_object_in_snapshot,
                        tasks,
                        chunkiness)
                ):
                update_info['return_time'] = time.time()
                update_infos.append(update_info)
                error = update_info['error']
                if error is not None:
                    print('Error', error)
                    errors.append(error)
                if (i + 1) % 1000 == 0:
                    log.info('Indexing %d', i + 1)
        except:
            self.shutdown()
            raise
        total_time = time.time() - start_time
        tasks_len = len(tasks)
        self._print_update_infos(
            update_infos,
            total_time,
            tasks_len,
            chunkiness,
        )
        return errors

    def _print_update_infos(
            self,
            update_infos,
            total_time,
            tasks_len,
            chunkiness,
            sub_print=False,
        ):
        if sub_print:
            self._sub_print(update_infos)
        print(
            self.queue_type,
            self.queue_worker.processes,
            self.chunk_size,
            chunkiness,
            self.batch_size,
            tasks_len,
            total_time,
        )

    def _sub_print(self, update_infos):
        for cnt, update_info in enumerate(update_infos, 1):
            print(cnt, self.queue_type)
            print(
                '  update_info',
                update_info['uuid'],
                update_info['run_time']
            )
            map_info = update_info['map_info']
            print(
                '  map_info',
                map_info['pid'],
                map_info['run_time'],
            )
            req_info = update_info['req_info']
            print(
                '  req_info',
                req_info['url'],
                req_info['run_time'],
                len(req_info['errors'])
            )
            es_info = update_info['es_info']
            print(
                '   es_info',
                es_info['item_type'],
                es_info['run_time'],
                len(es_info['backoffs'])
            )
            for num, backoff in es_info['backoffs'].items():
                print(
                    '      backoff',
                    num,
                    backoff['run_time'],
                    backoff['error'],
                )

    def shutdown(self):
        if 'pool' in self.__dict__:
            self.pool.terminate()
            self.pool.join()
            del self.pool
