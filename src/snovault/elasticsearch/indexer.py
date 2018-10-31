'''Indexer Listener Callback'''
import datetime
import logging
import copy
import itertools
import sys
import time

import pytz

from elasticsearch.exceptions import ConflictError as ESConflictError
from pyramid.view import view_config
from pyramid.settings import asbool

from snovault import DBSESSION
from snovault.storage import TransactionRecord
from snovault.elasticsearch.primary_indexer import PrimaryIndexer
from snovault.elasticsearch.mpindexer import MPIndexer

from .indexer_state import (
    IndexerState,
    all_uuids,
    SEARCH_MAX
)
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER
)
from .uuid_queue import (
    UuidQueue,
    UuidQueueTypes,
    UuidQueueWorker,
)


log = logging.getLogger('snovault.elasticsearch.es_index_listener')  # pylint: disable=invalid-name
MAX_CLAUSES_FOR_ES = 8192
SHORT_INDEXING = None  # Falsey value will turn off short
SHORT_INDEXING = 10000  # Falsey value will turn off short
INDEX_SETTINGS = 'INDEX_SETTINGS'
LOCAL_INDEX_QUEUES = [UuidQueueTypes.BASE_IN_MEMORY]
NON_REDIS_INDEX_QUEUES = [
    UuidQueueTypes.AWS_SQS,
    UuidQueueTypes.BASE_IN_MEMORY,
]
PY2 = sys.version_info.major == 2


def includeme(config):
    '''Initialize ES Indexers'''
    config.add_route('index', '/index')
    config.add_route('index_worker', '/index_worker')
    config.scan(__name__)
    indexer, indexer_settings = _get_indexer(config.registry)
    if indexer_settings:
        config.registry[INDEX_SETTINGS] = indexer_settings
    if indexer:
        config.registry[INDEXER] = indexer
    # TO DO: Remove this?  It is only used on restart
    config.registry['DEBUG_RESET_QUEUE'] = True


def _is_indexer(
        conf_name,
        is_index_server,
        index_queue_type,
        index_worker_procs
    ):
    # pylint: disable=too-many-branches
    msg = None
    # TO DO: Clean this up once cases are set
    if is_index_server:
        msg = 'Index Server'
        if index_queue_type in LOCAL_INDEX_QUEUES:
            msg += ' with Local Queue'
            if index_worker_procs <= 0:
                msg += ' and Zero! Local Workers'
                raise ValueError(msg)
            elif index_worker_procs > 1:
                msg += ' and Local MultiProcessing Worker'
            else:
                msg += ' and One Local worker'
        else:
            msg += ' with Remote Queue'
            if index_worker_procs <= 0:
                msg += ' and Zero Local Workers'
            elif index_worker_procs > 1:
                msg += ' and Local MultiProcessing Worker'
            else:
                msg += ' and One Local Worker'
    elif index_worker_procs:
        msg = 'Index Worker'
        if index_queue_type in LOCAL_INDEX_QUEUES:
            msg += ' with Local Queue'
            raise ValueError(msg)
        else:
            msg += ' with Remote Queue'
            if index_worker_procs > 1:
                msg += ' and Local MultiProcessing Worker'
            else:
                msg += ' and One Local Worker'
    if msg:
        log.info('%s: %s', conf_name, msg)
        return True
    return False


def _get_indexer(cnf_registry):
    '''
    Create indexer based on resigtry vars. includeme helper
    - returns indexer instance and settings
    - should only be called from includeme
    '''
    reg_settings = cnf_registry.settings
    conf_name = reg_settings.get('conf_name')
    is_index_server = asbool(reg_settings.get('is_index_server', False))
    index_queue_type = reg_settings.get(
        'index_queue_type',
        UuidQueueTypes.BASE_IN_MEMORY
    )
    index_worker_procs = int(reg_settings.get('index_worker_procs', 0))
    if not _is_indexer(
            conf_name,
            is_index_server,
            index_queue_type,
            index_worker_procs,
        ):
        return None, None
    indexer_settings = {
        'conf_name': conf_name,
        'is_server': is_index_server,
        'queue_type': index_queue_type,
        'worker_procs': index_worker_procs,
        'do_log': asbool(reg_settings.get('index_do_log', False)),
        'queue_name': reg_settings.get(
            'index_queue_name',
            'defaultindexQ'
        ),
        'chunk_size': int(reg_settings.get('index_worker_chunk_size', 100)),
        'index': reg_settings['snovault.elasticsearch.index'],
    }
    if index_queue_type not in NON_REDIS_INDEX_QUEUES:
        indexer_settings['redis_ip'] = reg_settings['redis_ip']
        indexer_settings['redis_port'] = reg_settings['redis_port']
    if index_worker_procs > 1:
        return MPIndexer(cnf_registry, indexer_settings), indexer_settings
    elif index_worker_procs == 1:
        return PrimaryIndexer(cnf_registry, indexer_settings), indexer_settings
    return None, indexer_settings


def _do_record(index_listener, result):
    '''
    Helper for index view_config function
    Runs after run index if record parameter was in request
    '''
    try:
        index_listener.registry_es.index(
            index=index_listener.index_registry_key,
            doc_type='meta',
            body=result,
            id='indexing'
        )
    except Exception as ecp:  # pylint: disable=broad-except
        log.warning('Index listener: %r', ecp)
        error_messages = copy.deepcopy(result['errors'])
        del result['errors']
        index_listener.registry_es.index(
            index=index_listener.index_registry_key,
            doc_type='meta',
            body=result,
            id='indexing'
        )
        for item in error_messages:
            if 'error_message' in item:
                log.error(
                    'Indexing error for %s, error message: %s',
                    item['uuid'],
                    item['error_message']
                )
                item['error_message'] = "Error occured during indexing, check the logs"
        result['errors'] = error_messages


def get_current_xmin(request):
    '''Determine Postgres minimum transaction'''
    session = request.registry[DBSESSION]()
    connection = session.connection()
    recovery = request.json.get('recovery', False)
    if recovery:
        query = connection.execute(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;"
            "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        )
    else:
        query = connection.execute(
            "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE;"
            "SELECT txid_snapshot_xmin(txid_current_snapshot());"
        )
    xmin = query.scalar()  # lowest xid that is still in progress
    return xmin


def get_related_uuids(request, registry_es, updated, renamed):
    '''
    Returns (set of uuids, False) or
    (list of all uuids, True) if full reindex triggered
    '''
    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)
    elif (updated_count + renamed_count) == 0:
        return (set(), False)
    registry_es.indices.refresh('_all')
    res = registry_es.search(
        index='_all',
        size=SEARCH_MAX,
        request_timeout=60,
        body={
            'query': {
                'bool': {
                    'should': [
                        {
                            'terms': {
                                'embedded_uuids': updated,
                                '_cache': False,
                            },
                        },
                        {
                            'terms': {
                                'linked_uuids': renamed,
                                '_cache': False,
                            },
                        },
                    ],
                },
            },
            '_source': False,
        }
    )
    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)
    related_set = {hit['_id'] for hit in res['hits']['hits']}
    return (related_set, False)


@view_config(route_name='index_worker', request_method='POST', permission="index")
def index_worker(request):
    '''Listener for index worker machines for remote queue types'''
    indexer_settings = request.registry.get(INDEX_SETTINGS)
    if not indexer_settings:
        return {'index_status': 'not applicable'}
    indexer = request.registry.get(INDEXER)
    is_server = indexer_settings['is_server']
    queue_type = indexer_settings['queue_type']
    if (
            not indexer or
            is_server or
            queue_type in LOCAL_INDEX_QUEUES
        ):
        return {'index_status': 'not applicable'}
    skip_consume = 0
    batch_size = indexer_settings['chunk_size']
    uuid_queue_worker = _get_uuid_queue(indexer_settings, worker=True)
    _run_uuid_queue_worker(
        uuid_queue_worker,
        request,
        skip_consume,
        batch_size
    )
    return {'index_status': 'did run'}


def _get_uuid_queue(indexer_settings, worker=False):
    client_options = {}
    queue_name = indexer_settings['queue_name']
    queue_type = indexer_settings['queue_type']
    if queue_type not in NON_REDIS_INDEX_QUEUES:
        client_options['host'] = indexer_settings['redis_ip']
        client_options['port'] = indexer_settings['redis_port']
    if worker:
        return UuidQueueWorker(
            queue_name,
            queue_type,
            client_options,
        )
    return UuidQueue(
        queue_name,
        queue_type,
        client_options,
    )


def _run_uuid_queue_worker(
        uuid_queue_worker,
        request,
        skip_consume,
        get_batch_size,
        uuid_queue_server=None,
        max_age_secs=7200,
        listener_restarted=False,
    ):
    # pylint: disable=too-many-arguments
    '''
    index_worker helper that can be used from index listener too
    - uuid queue worker and server are the same instance
    when queue type is local
    - With remote queues the uuid queue server and worker are different
    but called from the same process
    '''
    indexer = request.registry[INDEXER]
    log_tag = 'wrk'
    if uuid_queue_server:
        log_tag = 'srv'
    if uuid_queue_worker.server_ready():
        if uuid_queue_worker.queue_running():
            print('index worker uuid_queue.queue_running looping')
            processed = 0
            while uuid_queue_worker.queue_running():
                batch_id, uuids, _ = uuid_queue_worker.get_uuids(
                    get_count=get_batch_size
                )
                if batch_id and uuids:
                    if skip_consume > 0:
                        skip_consume -= 1
                    else:
                        indexer.log_store = []
                        errors = indexer.update_objects(
                            request,
                            uuids,
                            uuid_queue_worker.xmin,
                            is_reindex=False,
                            log_tag=log_tag,
                        )
                        successes = len(uuids) - len(errors)
                        processed += successes
                        uuid_queue_worker.add_finished(
                            batch_id,
                            successes,
                            errors,
                            batch_logs=indexer.log_store,
                        )
                        indexer.log_store = []
                time.sleep(0.05)
                if uuid_queue_server:
                    return uuid_queue_server.is_finished(
                        max_age_secs=max_age_secs,
                        listener_restarted=listener_restarted,
                    )
            print('run_worker done', processed)
    # return values for uuid_queue_server
    # not used in real worker
    return [], False


class UuidStore(object):
    '''
    Consumable holder for uuids
    - Previously called 'invalidated'
    '''
    def __init__(self):
        self.uuids = set()

    def is_empty(self):
        '''Returns true if store has uuids'''
        if self.uuids:
            return False
        return True

    def over_threshold(self, threshold):
        '''Returns true if store is greater than threshold'''
        if len(self.uuids) > threshold:
            return True
        return False


class IndexListener(object):
    '''Encapsulated index view config functionality'''
    def __init__(self, request):
        self.session = request.registry[DBSESSION]()
        self.dry_run = request.json.get('dry_run', False)
        self.index_registry_key = request.registry.settings[
            'snovault.elasticsearch.index'
        ]
        self.uuid_store = UuidStore()
        self.registry_es = request.registry[ELASTIC_SEARCH]
        self.request = request
        self.xmin = -1

    def _get_transactions(self, last_xmin, first_txn=None):
        'Check postgres transaction with last xmin'''
        txns = self.session.query(TransactionRecord).filter(
            TransactionRecord.xid >= last_xmin,
        )
        updated = set()
        renamed = set()
        max_xid = 0
        txn_count = 0
        for txn in txns.all():
            txn_count += 1
            max_xid = max(max_xid, txn.xid)
            if not first_txn:
                first_txn = txn.timestamp
            else:
                first_txn = min(first_txn, txn.timestamp)
            renamed.update(txn.data.get('renamed', ()))
            updated.update(txn.data.get('updated', ()))
        return renamed, updated, txn_count, max_xid, first_txn

    def get_current_last_xmin(self, result):
        '''Handle xmin and last_xmin'''
        xmin = get_current_xmin(self.request)
        last_xmin = None
        if 'last_xmin' in self.request.json:
            last_xmin = self.request.json['last_xmin']
        else:
            status = self.registry_es.get(
                index=self.index_registry_key,
                doc_type='meta',
                id='indexing',
                ignore=[400, 404]
            )
            if status['found'] and 'xmin' in status['_source']:
                last_xmin = status['_source']['xmin']
        if last_xmin is None:
            if 'last_xmin' in result:
                last_xmin = result['last_xmin']
            elif 'xmin' in result and result['xmin'] < xmin:
                last_xmin = result['state']
        return xmin, last_xmin

    def get_txns_and_update(self, last_xmin, result):
        '''Get PG Transaction and check against uuid_store'''
        (renamed, updated, txn_count,
         max_xid, first_txn) = self._get_transactions(last_xmin)
        result['txn_count'] = txn_count
        if txn_count == 0 and self.uuid_store.is_empty():
            return None, txn_count
        flush = None
        if not self.uuid_store.is_empty():
            updated |= self.uuid_store.uuids
        related_set, full_reindex = get_related_uuids(
            self.request,
            self.registry_es,
            updated,
            renamed
        )
        if full_reindex:
            self.uuid_store.uuids = related_set
            flush = True
        else:
            self.uuid_store.uuids = related_set | updated
            result.update(
                max_xid=max_xid,
                renamed=renamed,
                updated=updated,
                referencing=len(related_set),
                invalidated=len(self.uuid_store.uuids),
            )
            if first_txn is not None:
                result['first_txn_timestamp'] = first_txn.isoformat()
        return flush, txn_count

    def set_priority_cycle(self, indexer_state):
        '''Call priority cycle and update self'''
        (xmin, uuids_set, restart) = indexer_state.priority_cycle(self.request)
        indexer_state.log_reindex_init_state()
        # Currently not bothering with restart!!!
        if restart:
            xmin = -1
            uuids_set = set()
        self.uuid_store.uuids = uuids_set
        self.xmin = xmin
        return restart

    def short_uuids(self, short_to=100):
        '''
        Limit uuids to index for debugging
        '''
        if short_to <= 0:
            short_to = 100
        self.uuid_store.uuids = set(itertools.islice(
            self.uuid_store.uuids, short_to
        ))

    def try_set_snapshot_id(self, recovery, snapshot_id):
        '''Check for snapshot_id in postgres under certain conditions'''
        if not self.uuid_store.is_empty() and not self.dry_run:
            if snapshot_id is None and not recovery:
                connection = self.session.connection()
                snapshot_id = connection.execute(
                    'SELECT pg_export_snapshot();'
                ).scalar()
        return snapshot_id


@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    '''Index listener for main indexer'''
    # pylint: disable=too-many-branches, too-many-locals, too-many-statements
    indexer_settings = request.registry.get(INDEX_SETTINGS)
    if not indexer_settings or not indexer_settings['is_server']:
        return {}
    request.datastore = 'database'
    followups = list(
        request.registry.settings.get(
            "stage_for_followup",
            ''
        ).replace(' ', '').split(',')
    )
    index_listener = IndexListener(request)
    indexer_state = IndexerState(
        index_listener.registry_es,
        index_listener.index_registry_key,
        followups=followups
    )
    restart = index_listener.set_priority_cycle(indexer_state)
    result = indexer_state.get_initial_state()
    snapshot_id = None
    first_txn = None
    last_xmin = None
    if index_listener.xmin == -1 or index_listener.uuid_store.is_empty():
        tmp_xmin, last_xmin = index_listener.get_current_last_xmin(result)
        result.update(
            xmin=tmp_xmin,
            last_xmin=last_xmin,
        )
        index_listener.xmin = tmp_xmin
    uuid_queue = _get_uuid_queue(indexer_settings)
    if asbool(request.registry['DEBUG_RESET_QUEUE']):
        print('purging')
        uuid_queue.purge()
        request.registry['DEBUG_RESET_QUEUE'] = False
        return result
    flush = False
    if index_listener.uuid_store.over_threshold(SEARCH_MAX):
        flush = True
    elif last_xmin is None:
        result['types'] = types = request.json.get('types', None)
        index_listener.uuid_store.uuids = set(
            all_uuids(request.registry, types)
        )
        flush = True
    else:
        tmp_flush, txn_count = index_listener.get_txns_and_update(last_xmin, result)
        if txn_count == 0 and index_listener.uuid_store.is_empty():
            indexer_state.send_notices()
            uuid_queue.purge()
            return result
        if tmp_flush:
            flush = tmp_flush
        snapshot_id = index_listener.try_set_snapshot_id(
            request.json.get('recovery', False),
            snapshot_id
        )
    if index_listener.uuid_store.is_empty():
        uuid_queue.purge()
    elif not index_listener.dry_run:
        _run_index(
            index_listener,
            indexer_state,
            uuid_queue,
            result,
            restart,
            snapshot_id,
            request,
            indexer_settings,
        )

        if request.json.get('record', False):
            _do_record(index_listener, result)
        index_listener.registry_es.indices.refresh('_all')
        if flush:
            try:
                index_listener.registry_es.indices.flush_synced(index='_all')
            except ESConflictError as ecp:
                log.warning('Index listener ESConflictError: %r', ecp)
    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
    indexer_state.send_notices()
    return result


def _run_index(
        index_listener,
        indexer_state,
        uuid_queue,
        result,
        restart,
        snapshot_id,
        request,
        indexer_settings,
    ):
    '''
    Indexer Server Helper for index view_config function
    Runs the indexing processes for index listener
    '''
    # pylint: disable=too-many-arguments
    if indexer_state.followups:
        indexer_state.prep_for_followup(
            index_listener.xmin,
            index_listener.uuid_store.uuids
        )
    uuid_queue_run_args = {
        'batch_by': indexer_settings['chunk_size'],
        'uuid_len': 36,
        'xmin': index_listener.xmin,
        'snapshot_id': snapshot_id,
        'restart': restart,
    }
    listener_restarted = False
    did_fail = True
    uuids = index_listener.uuid_store.uuids
    uuid_queue_worker = None
    indexer = index_listener.request.registry.get(INDEXER, None)
    if indexer_settings['queue_type'] in LOCAL_INDEX_QUEUES:
        # Base memory queue server must be its own worker
        uuid_queue_worker = uuid_queue
    elif indexer_settings['worker_procs']:
        # Non base memory queue server may have its own workers
        uuid_queue_worker = _get_uuid_queue(
            indexer_settings,
            worker=True,
        )
    if uuid_queue.queue_running():
        print('indexer uuid_queue.queue_running')
        listener_restarted = True
        did_fail = False
    else:
        if SHORT_INDEXING:
            # If value is truthly then uuids will be limited.
            log.warning(
                'Shorting UUIDS from %d to %d',
                len(index_listener.uuid_store.uuids),
                SHORT_INDEXING,
            )
            index_listener.short_uuids(SHORT_INDEXING)
        result, did_fail = init_cycle(
            uuid_queue,
            uuids,
            indexer_state,
            result,
            uuid_queue_run_args,
            indexer=indexer,
        )
    if did_fail:
        log.warning(
            'Index initalization failed for %d uuids.',
            len(uuids)
        )
    else:
        errors = server_loop(
            uuid_queue,
            uuid_queue_run_args,
            request,
            indexer_settings['chunk_size'],
            listener_restarted=listener_restarted,
            uuid_queue_worker=uuid_queue_worker,
        )
        result = indexer_state.finish_cycle(result, errors)
        if indexer:
            indexer.clear_state()
        if errors:
            result['errors'] = errors


def init_cycle(
        uuid_queue,
        uuids,
        indexer_state,
        result,
        run_args,
        indexer=None
    ):
    # pylint: disable=too-many-arguments
    '''Starts an index cycle'''
    did_pass = uuid_queue.initialize(run_args)
    did_fail = True
    if did_pass:
        did_fail = False
        failed, success_cnt, call_cnt = uuid_queue.load_uuids(uuids)
        print('indexer init_cycle load_uuids', failed, success_cnt, call_cnt)
        if not success_cnt:
            did_fail = True
        else:
            if indexer:
                indexer.set_state(
                    indexer_state.is_initial_indexing,
                    indexer_state.is_reindexing,
                )
            result = indexer_state.start_cycle(uuids, result)
    return result, did_fail


def server_loop(
        uuid_queue,
        run_args,
        request,
        chunk_size,
        listener_restarted=False,
        uuid_queue_worker=None,
    ):
    # pylint: disable=too-many-arguments
    '''wait for workers to finish loop'''
    skip_consume = 0
    max_age_secs = 7200
    queue_done = False
    errors = None
    print('server looping')
    while not queue_done:
        if uuid_queue_worker:
            # This does not loop like the regular uuid queue worker
            # Runs once if queue server passed in
            readd_uuids, queue_done = _run_uuid_queue_worker(
                uuid_queue_worker,
                request,
                skip_consume,
                chunk_size,
                uuid_queue_server=uuid_queue,
                max_age_secs=max_age_secs,
                listener_restarted=listener_restarted,
            )
        else:
            readd_uuids, queue_done = uuid_queue.is_finished(
                max_age_secs=max_age_secs, listener_restarted=listener_restarted,
            )
        if readd_uuids:
            if listener_restarted:
                if not uuid_queue.initialize(run_args):
                    print('restart issue, not solved')
            uuid_queue.load_uuids(
                readd_uuids,
                readded=True,
            )
        if queue_done:
            errors, _ = uuid_queue.get_errors()
        time.sleep(1.00)
    print('done, try readding errors?', len(errors))
    uuid_queue.stop()
    return errors
