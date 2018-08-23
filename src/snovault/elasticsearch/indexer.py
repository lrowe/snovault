import os
from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
from pyramid.settings import asbool
from sqlalchemy.exc import StatementError
from snovault import (
    COLLECTIONS,
    DBSESSION,
    STORAGE
)
from snovault.storage import (
    TransactionRecord,
)
from urllib3.exceptions import ReadTimeoutError
from .interfaces import (
    ELASTIC_SEARCH,
    INDEXER
)
from .index_logger import IndexLogger
from .indexer_state import (
    IndexerState,
    all_uuids,
    all_types,
    SEARCH_MAX
)
import datetime
import logging
import pytz
import time
import copy
import json
import requests

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger(__name__)
MAX_CLAUSES_FOR_ES = 8192

def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    do_log = False
    if asbool(registry.settings.get('indexer')):
        do_log = False
        print('Set primary indexer in indexer.py')
    registry[INDEXER] = Indexer(registry, do_log=do_log)

def get_related_uuids(request, es, updated, renamed):
    '''Returns (set of uuids, False) or (list of all uuids, True) if full reindex triggered'''

    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique
    elif (updated_count + renamed_count) == 0:
        return (set(), False)

    es.indices.refresh('_all')

    # TODO: batching may allow us to drive a partial reindexing much greater than 99999
    #BATCH_COUNT = 100  # NOTE: 100 random uuids returned > 99999 results!
    #beg = 0
    #end = BATCH_COUNT
    #related_set = set()
    #updated_list = list(updated)  # Must be lists
    #renamed_list = list(renamed)
    #while updated_count > beg or renamed_count > beg:
    #    if updated_count > end or beg > 0:
    #        log.error('Indexer looking for related uuids by BATCH[%d,%d]' % (beg, end))
    #
    #    updated = []
    #    if updated_count > beg:
    #        updated = updated_list[beg:end]
    #    renamed = []
    #    if renamed_count > beg:
    #        renamed = renamed_list[beg:end]
    #
    #     search ...
    #     accumulate...
    #
    #    beg += BATCH_COUNT
    #    end += BATCH_COUNT


    res = es.search(index='_all', size=SEARCH_MAX, request_timeout=60, body={
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
    })

    if res['hits']['total'] > SEARCH_MAX:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique

    related_set = {hit['_id'] for hit in res['hits']['hits']}

    return (related_set, False)



@view_config(route_name='index', request_method='POST', permission="index")
def index(request):
    INDEX = request.registry.settings['snovault.elasticsearch.index']
    # Setting request.datastore here only works because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)
    dry_run = request.json.get('dry_run', False)
    recovery = request.json.get('recovery', False)
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    session = request.registry[DBSESSION]()
    connection = session.connection()
    first_txn = None
    snapshot_id = None
    restart=False
    invalidated = []
    xmin = -1

    # Currently 2 possible followup indexers (base.ini [set stage_for_followup = vis_indexer, region_indexer])
    stage_for_followup = list(request.registry.settings.get("stage_for_followup", '').replace(' ','').split(','))

    # May have undone uuids from prior cycle
    state = IndexerState(es, INDEX, followups=stage_for_followup)

    (xmin, invalidated, restart) = state.priority_cycle(request)
    # OPTIONAL: restart support
    if restart:  # Currently not bothering with restart!!!
        xmin = -1
        invalidated = []
    # OPTIONAL: restart support

    result = state.get_initial_state()  # get after checking priority!

    if xmin == -1 or len(invalidated) == 0:
        xmin = get_current_xmin(request)

        last_xmin = None
        if 'last_xmin' in request.json:
            last_xmin = request.json['last_xmin']
        else:
            status = es.get(index=INDEX, doc_type='meta', id='indexing', ignore=[400, 404])
            if status['found'] and 'xmin' in status['_source']:
                last_xmin = status['_source']['xmin']
        if last_xmin is None:  # still!
            if 'last_xmin' in result:
                last_xmin = result['last_xmin']
            elif 'xmin' in result and result['xmin'] < xmin:
                last_xmin = result['state']

        result.update(
            xmin=xmin,
            last_xmin=last_xmin,
        )

    if len(invalidated) > SEARCH_MAX:  # Priority cycle already set up
        flush = True
    else:

        flush = False
        if last_xmin is None:
            result['types'] = types = request.json.get('types', None)
            invalidated = list(all_uuids(request.registry, types))
            flush = True
        else:
            txns = session.query(TransactionRecord).filter(
                TransactionRecord.xid >= last_xmin,
            )

            invalidated = set(invalidated)  # not empty if API index request occurred
            updated = set()
            renamed = set()
            max_xid = 0
            txn_count = 0
            for txn in txns.all():
                txn_count += 1
                max_xid = max(max_xid, txn.xid)
                if first_txn is None:
                    first_txn = txn.timestamp
                else:
                    first_txn = min(first_txn, txn.timestamp)
                renamed.update(txn.data.get('renamed', ()))
                updated.update(txn.data.get('updated', ()))

            if invalidated:        # reindex requested, treat like updated
                updated |= invalidated

            result['txn_count'] = txn_count
            if txn_count == 0 and len(invalidated) == 0:
                state.send_notices()
                return result

            (related_set, full_reindex) = get_related_uuids(request, es, updated, renamed)
            if full_reindex:
                invalidated = related_set
                flush = True
            else:
                invalidated = related_set | updated
                result.update(
                    max_xid=max_xid,
                    renamed=renamed,
                    updated=updated,
                    referencing=len(related_set),
                    invalidated=len(invalidated),
                    txn_count=txn_count
                )
                if first_txn is not None:
                    result['first_txn_timestamp'] = first_txn.isoformat()

            if invalidated and not dry_run:
                # Exporting a snapshot mints a new xid, so only do so when required.
                # Not yet possible to export a snapshot on a standby server:
                # http://www.postgresql.org/message-id/CAHGQGwEtJCeHUB6KzaiJ6ndvx6EFsidTGnuLwJ1itwVH0EJTOA@mail.gmail.com
                if snapshot_id is None and not recovery:
                    snapshot_id = connection.execute('SELECT pg_export_snapshot();').scalar()

    if invalidated and not dry_run:
        result = indexer_updater(
            request, invalidated, stage_for_followup, state, indexer,
            result, xmin, snapshot_id, restart, record, INDEX, es, flush,
        )

    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)

    state.send_notices()
    return result


def indexer_updater(
        request, invalidated, stage_for_followup, state, indexer,
        result, xmin, snapshot_id, restart, record, index_str, elastic_search, flush,
    ):  # pylint: disable=too-many-locals, too-many-arguments
    '''Indexing work part'''
    short_size = 100  # None implies all
    batch_size = 10  # None implies all
    invalidated = short_indexer(invalidated, max_invalid=short_size)
    len_invalid = len(invalidated)
    if batch_size is None:
        batch_size = len(invalidated)
    if batch_size > len_invalid:
        batch_size = len_invalid
    batch_cnt = 0
    while invalidated:
        batch_cnt += 1
        batch_invalidated = invalidated[:batch_size]
        print('s', len(invalidated), len(batch_invalidated))
        invalidated = invalidated[batch_size:]
        print('f', len(invalidated))
        if stage_for_followup:
            state.prep_for_followup(xmin, batch_invalidated)
        result = state.start_cycle(batch_invalidated, result)
        outputs, errors = indexer.update_objects(
            request, batch_invalidated, xmin, snapshot_id, restart
        )
        result = state.finish_cycle(result, errors)
        if errors:
            result['errors'] = errors
        if record:
            try:
                elastic_search.index(index=index_str, doc_type='meta', body=result, id='indexing')
            except:  # pylint: disable=bare-except
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                elastic_search.index(index=index_str, doc_type='meta', body=result, id='indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error(
                            'Indexing error for %s, error message: %s',
                            item['uuid'],
                            item['error_message'],
                        )
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages
        elastic_search.indices.refresh('_all')
        if flush:
            try:
                elastic_search.indices.flush_synced(index='_all')  # Faster recovery on ES restart
            except ConflictError:
                pass
        file_path = 'xmin%s-cnt%d-size%d-tlt%d.json' % (
            str(xmin),
            batch_cnt,
            batch_size,
            len_invalid,
        )
        dump_output_to_file(
            file_path,
            outputs,
            out_size=batch_size,
        )
    return result


def dump_output_to_file(file_path, outputs, out_size=100000):
    '''For Debug, dump indexer updates objects to file'''
    print('start', file_path)
    print('dump_output_to_file', len(outputs))
    path_index = 0
    while outputs:
        path_index += 1
        if len(outputs) >= out_size:
            out = outputs[:out_size]
            outputs = outputs[out_size:]
        else:
            out = outputs[:]
            outputs = []
        next_file_path = 'log-test/' + str(path_index) + '-' + file_path
        print(next_file_path, len(out))
        with open(next_file_path, 'w') as file_handler:
            # json.dump(out, file_handler, indent=4, separators=(',', ': '))
            json.dump(out, file_handler)


def short_indexer(invalidated, max_invalid=None):
    '''For Debug, shorten invalidated and make list'''
    invalid = []
    for item in invalidated:
         invalid.append(item)
         if max_invalid and len(invalid) >= max_invalid:
             break
    return invalid


def get_current_xmin(request):
    session = request.registry[DBSESSION]()
    connection = session.connection()
    recovery = request.json.get('recovery', False)

    # http://www.postgresql.org/docs/9.3/static/functions-info.html#FUNCTIONS-TXID-SNAPSHOT
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
    # DEFERRABLE prevents query cancelling due to conflicts but requires SERIALIZABLE mode
    # which is not available in recovery.
    xmin = query.scalar()  # lowest xid that is still in progress
    return xmin

class Indexer(object):
    def __init__(self, registry, do_log=False):
        self.es = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']
        self._indexer_log = IndexLogger(do_log=do_log)

    @staticmethod
    def _get_embed_doc(request, uuid, backoff=None):
        info_dict = {
            'backoff': backoff,
            'doc_embedded': None,
            'doc_linked': None,
            'doc_path': None,
            'doc_type': None,
            'end_time': None,
            'exception': None,
            'exception_type': None,
            'failed': False,
            'start_time': time.time(),
            'url': "/%s/@@index-data/" % uuid,
        }
        try:
            doc = request.embed(info_dict['url'], as_user='INDEXER')
        except Exception as ecp:  # pylint: disable=broad-except
            info_dict['exception_type'] = 'Embed Exception'
            info_dict['exception'] = repr(ecp)
        else:
            doc_paths = doc.get('paths')
            if doc_paths:
                info_dict['doc_path'] = doc_paths[0]
            info_dict['doc_type'] = doc.get('item_type')
            info_dict['doc_embedded'] = len(doc.get('embedded_uuids', []))
            info_dict['doc_linked'] = len(doc.get('linked_uuids', []))
        info_dict['end_time'] = time.time()
        return info_dict, doc

    def _index_doc(self, doc, uuid, xmin, backoff=None):
        info_dict = {
            'backoff': backoff,
            'exception': None,
            'exception_type': None,
            'failed': False,
            'start_time': time.time(),
            'end_time': None,
        }
        try:
            self.es.index(
                index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                id=str(uuid), version=xmin, version_type='external_gte',
                request_timeout=30,
            )
        except Exception as ecp:  # pylint: disable=broad-except
            info_dict['exception_type'] = 'Embed Exception'
            info_dict['exception'] = repr(ecp)
        info_dict['end_time'] = time.time()
        return info_dict

    def _backoff_get_embed_doc(self, request, uuid):
        doc = None
        doc_infos = []
        doc_err_msg = None
        for backoff in [0, 10, 20, 40, 80]:
            time.sleep(backoff)
            doc_info, doc = self._get_embed_doc(request, uuid, backoff=backoff)
            doc_infos.append(doc_info)
            if doc_info['failed']:
                doc_err_msg = (
                    'Get embed for doc failed on backoff %d' % backoff
                )
            else:
                break
        return doc, doc_infos, doc_err_msg

    def _backoff_index_doc(self, doc, uuid, xmin):
        es_infos = []
        es_err_msg = None
        for backoff in [0, 10, 20, 40, 80]:
            time.sleep(backoff)
            es_info = self._index_doc(doc, uuid, xmin, backoff=backoff)
            es_infos.append(es_info)
            if es_info['failed']:
                es_err_msg = (
                    'Index doc failed on backoff %d' % backoff
                )
            else:
                break
        return es_infos, es_err_msg

    def update_object(self, request, uuid, xmin, restart=False):
        '''Get embed doc to index in elastic search, return run info'''
        request.datastore = 'database'  # required by 2-step indexer
        output = {
            'doc_infos': [],
            'end_time': None,
            'end_timestamp': None,
            'error_message': None,
            'es_infos': [],
            'pid': os.getpid(),
            'restart': restart,
            'start_time': time.time(),
            'start_timestamp': datetime.datetime.now().isoformat(),
            'uuid': str(uuid),
            'xmin': xmin,
        }
        doc, doc_infos, doc_err_msg = self._backoff_get_embed_doc(
            request, uuid
        )
        output['doc_infos'] = doc_infos
        if doc_err_msg:
            output['error_message'] = doc_err_msg
        elif not doc:
            output['error_message'] = 'Get embed for doc none with no error'
        else:
            es_infos, es_err_msg = self._backoff_index_doc(doc, uuid, xmin)
            output['es_infos'] = es_infos
            if es_err_msg:
                output['error_message'] = es_err_msg
        output['end_time'] = time.time()
        output['end_timestamp'] = datetime.datetime.now().isoformat()
        return output

    def update_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False
        ): # pylint: disable=too-many-arguments
        '''Run update object on all uuids'''
        self._indexer_log.new_log(len(uuids), xmin, snapshot_id)
        errors = []
        outputs = [
            {
                'chunkiness': None,
                'uuid': 'info',
                'processes': None,
                'snapshot_id': snapshot_id,
                'uuid_count': len(uuids),
                'xmin': xmin,
                'pid': os.getpid(),
            }
        ]
        for i, uuid in enumerate(uuids):
            output = self.update_object(request, uuid, xmin, restart=restart)
            outputs.append(output)
            if output['error_message']:
                errors.append({
                    'error_message': output['error_message'],
                    'timestamp': output['end_timestamp'],
                    'uuid': output['uuid'],
                })
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)
        return outputs, errors

    def shutdown(self):
        '''Shutdown used in other indexers'''
        pass
