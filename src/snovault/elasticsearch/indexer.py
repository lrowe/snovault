from os import getpid as os_getpid
from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
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
from .indexer_data_dump import IndexDataDump

es_logger = logging.getLogger("elasticsearch")
es_logger.setLevel(logging.ERROR)
log = logging.getLogger(__name__)
MAX_CLAUSES_FOR_ES = 8192

def includeme(config):
    config.add_route('index', '/index')
    config.scan(__name__)
    registry = config.registry
    registry[INDEXER] = Indexer(registry)

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
    is_reindex = False
    invalidated = []
    xmin = -1

    # Currently 2 possible followup indexers (base.ini [set stage_for_followup = vis_indexer, region_indexer])
    stage_for_followup = list(request.registry.settings.get("stage_for_followup", '').replace(' ','').split(','))

    # May have undone uuids from prior cycle
    state = IndexerState(es, INDEX, followups=stage_for_followup)

    (xmin, invalidated, restart, is_reindex) = state.priority_cycle(request)

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
        if len(stage_for_followup) > 0:
            # Note: undones should be added before, because those uuids will (hopefully) be indexed in this cycle
            state.prep_for_followup(xmin, invalidated)

        result = state.start_cycle(invalidated, result)

        # Do the work...
        indexer.set_snapshot_id(snapshot_id)
        errors = indexer.update_objects(
            request,
            invalidated,
            xmin,
            is_reindex=is_reindex,
        )

        result = state.finish_cycle(result,errors)

        if errors:
            result['errors'] = errors

        if record:
            try:
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
            except:
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                es.index(index=INDEX, doc_type='meta', body=result, id='indexing')
                for item in error_messages:
                    if 'error_message' in item:
                        log.error('Indexing error for {}, error message: {}'.format(item['uuid'], item['error_message']))
                        item['error_message'] = "Error occured during indexing, check the logs"
                result['errors'] = error_messages


        es.indices.refresh('_all')
        if flush:
            try:
                es.indices.flush_synced(index='_all')  # Faster recovery on ES restart
            except ConflictError:
                pass

    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)

    state.send_notices()
    return result


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
    '''Standard Indexer and Base class for all indexers'''
    is_mp_indexer = False
    def __init__(self, registry):
        self.es_wrapper = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']
        self._snapshot_id = None
        self._force = None
        self.indexer_name = self._get_name(registry)
        do_log = False
        if self.indexer_name in ['primaryindexer', 'mp-primaryindexer']:
            do_log = True
        self.indexer_data_dump = IndexDataDump(
            self.indexer_name,
            registry,
            do_log=do_log
        )

    def _get_name(self, registry):
        '''
        Sets the name of the indexer type
        * Ideally this would be a property of the subclasses
        because the parent class should not depend on childern.
        '''
        indexer_name = ''
        if self.is_mp_indexer:
            indexer_name = 'mp-'
        if registry.settings.get('indexer'):
            indexer_name += 'primaryindexer'
        elif registry.settings.get('visindexer'):
            indexer_name += 'visindexer'
        elif registry.settings.get('regionindexer'):
            indexer_name += 'regionindexer'
        return indexer_name

    def set_force(self, value):
        '''Set force, not used for all indexers'''
        self._force = value

    def set_snapshot_id(self, value):
        '''Set snapshot_id, not used for all indexers'''
        self._snapshot_id = value

    def _request_embed(self, request, uuid):
        sub_output_dict = self.indexer_data_dump.get_embed_dict(uuid)
        doc = None
        try:
            doc = request.embed(sub_output_dict['url'], as_user='INDEXER')
            doc_paths = doc.get('paths')
            if doc_paths:
                sub_output_dict['doc_path'] = doc_paths[0]
            sub_output_dict['doc_type'] = doc.get('item_type')
            sub_output_dict['doc_embedded'] = len(doc.get('embedded_uuids', []))
            sub_output_dict['doc_linked'] = len(doc.get('linked_uuids', []))
            sub_output_dict['end_time'] = time.time()
        except StatementError as ecp:
            # Can't reconnect until invalid transaction is rolled back
            sub_output_dict['exception_type'] = 'StatementError Exception'
            sub_output_dict['exception'] = repr(ecp)
            raise
        except Exception as ecp:  # pylint: disable=broad-except
            log.error(
                'Error rendering %s',
                sub_output_dict['url'],
                exc_info=True
            )
            sub_output_dict['exception_type'] = 'Exception'
            sub_output_dict['exception'] = repr(ecp)
        return doc, sub_output_dict

    def _index(self, doc, uuid, xmin):
        last_exc = None
        sub_output_dicts = []
        do_break = False
        for backoff in [0, 10, 20, 40, 80]:
            sub_output_dict = self.indexer_data_dump.get_es_dict(backoff)
            time.sleep(backoff)
            try:
                self.es_wrapper.index(
                    index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                    id=str(uuid), version=xmin, version_type='external_gte',
                    request_timeout=30,
                )
            except StatementError as ecp:
                # Can't reconnect until invalid transaction is rolled back
                sub_output_dict['exception_type'] = 'StatementError Exception'
                sub_output_dict['exception'] = repr(ecp)
                raise
            except ConflictError as ecp:
                log.warning('Conflict indexing %s at version %d', uuid, xmin)
                sub_output_dict['exception_type'] = 'ConflictError Exception'
                sub_output_dict['exception'] = repr(ecp)
                do_break = True
            except ConnectionError as ecp:
                log.warning('Retryable error indexing %s: %r', uuid, ecp)
                last_exc = repr(ecp)
                sub_output_dict['exception_type'] = 'ConnectionError Exception'
                sub_output_dict['exception'] = last_exc
            except ReadTimeoutError as ecp:
                log.warning('Retryable error indexing %s: %r', uuid, ecp)
                last_exc = repr(ecp)
                sub_output_dict['exception_type'] = 'ReadTimeoutError Exception'
                sub_output_dict['exception'] = last_exc
            except TransportError as ecp:
                log.warning('Retryable error indexing %s: %r', uuid, ecp)
                last_exc = repr(ecp)
                sub_output_dict['exception_type'] = 'TransportError Exception'
                sub_output_dict['exception'] = last_exc
            except Exception as ecp:  # pylint: disable=broad-except
                log.error('Error indexing %s', uuid, exc_info=True)
                last_exc = repr(ecp)
                sub_output_dict['exception_type'] = 'Exception'
                sub_output_dict['exception'] = last_exc
                do_break = True
            else:
                # Get here on success and outside of try
                do_break = True
            sub_output_dict['end_time'] = time.time()
            if do_break:
                break
        return sub_output_dicts, last_exc

    def _post_index_process(self, outputs, run_info):
        '''
        Handles any post processing needed for finished indexing processes
        '''
        dump_path = self.indexer_data_dump.handle_outputs(outputs, run_info)
        # Change to info or warn after deugging
        log.error('Indexing data dump directory %s.', dump_path)

    def update_objects(self, request, uuids, xmin, is_reindex=False):
        '''
        Standard Indexer loop to run update object on iterable of uuids
        '''
        uuid_count = len(uuids)
        outputs = []
        errors = []
        overrides = {
            '_dump_size': 50000,
            '_is_reindex': is_reindex,
        }
        run_info = self.indexer_data_dump.get_run_info(
            os_getpid(),
            uuid_count,
            xmin,
            self._snapshot_id,
            **overrides
        )
        for i, uuid in enumerate(uuids):
            output = self.update_object(request, uuid, xmin)
            if output:
                outputs.append(output)
                error = output.get('error')
                if error is not None:
                    errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)
        run_info['end_time'] = time.time()
        self._post_index_process(outputs, run_info)
        return errors

    def update_object(self, request, uuid, xmin):
        '''
        Handles indexing for one uuid
        * doc embedding and doc indexing
        '''
        pid = os_getpid()
        output = self.indexer_data_dump.get_output_dict(pid, uuid, xmin)
        error = None
        request.datastore = 'database'  # required by 2-step indexer
        doc, embed_dict = self._request_embed(request, uuid)
        output['embed_dict'] = embed_dict
        last_exc = None
        if doc and not embed_dict['exception']:
            es_dicts, last_exc = self._index(doc, uuid, xmin)
            output['es_dicts'] = es_dicts
        else:
            last_exc = embed_dict['exception']
        if last_exc:
            timestamp = datetime.datetime.now().isoformat()
            error = {
                'error_message': last_exc,
                'timestamp': timestamp,
                'uuid': str(uuid),
            }
        output['error'] = error
        output['end_time'] = time.time()
        return output

    def shutdown(self):
        '''? Not Implemented'''
        pass
