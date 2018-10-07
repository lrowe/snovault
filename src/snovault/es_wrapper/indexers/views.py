'''Indexer Views'''
import datetime
import logging
import copy
import json
import pytz
import requests

from elasticsearch.exceptions import ConflictError as ESConflictError
from pyramid.view import view_config

from snovault import (
    COLLECTIONS,
    DBSESSION,
)

from snovault.storage import TransactionRecord

from . import (
    ELASTIC_SEARCH,
    INDEXER,
    MAX_CLAUSES_FOR_ES,
    SEARCH_MAX,
)
from .state import (
    IndexerState,
    all_uuids,
)


log = logging.getLogger(__name__)  #pylint: disable=invalid-name


def includeme(config):
    '''Indexer Views'''
    config.add_route('index', '/index')
    config.add_route('_indexer_state', '/_indexer_state')
    config.scan(__name__)


def all_types(registry):
    '''Return sorted types collection'''
    collections = registry[COLLECTIONS]
    return sorted(collections.by_item_type)


def get_current_xmin(request):
    '''index view function'''
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
    # lowest xid that is still in progress
    xmin = query.scalar()
    return xmin


def get_related_uuids(request, es_inst, updated, renamed):
    '''
    Returns (set of uuids, False) or
    (list of all uuids, True) if full reindex triggered
    '''
    updated_count = len(updated)
    renamed_count = len(renamed)
    if (updated_count + renamed_count) > MAX_CLAUSES_FOR_ES:
        return (list(all_uuids(request.registry)), True)  # guaranteed unique
    elif (updated_count + renamed_count) == 0:
        return (set(), False)
    es_inst.indices.refresh('_all')
    res = es_inst.search(index='_all', size=SEARCH_MAX, request_timeout=60, body={
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
def index(request):  # pylint: disable=too-many-branches, too-many-statements, too-many-locals
    '''
    Es Index Listener Callback Loop
    - holds on indexing process, could be 4 or 5 hours
    '''
    index_str = request.registry.settings['snovault.elasticsearch.index']
    #   Setting request.datastore here only works
    # because routed views are not traversed.
    request.datastore = 'database'
    record = request.json.get('record', False)
    dry_run = request.json.get('dry_run', False)
    recovery = request.json.get('recovery', False)
    es_inst = request.registry[ELASTIC_SEARCH]
    indexer = request.registry[INDEXER]
    session = request.registry[DBSESSION]()
    connection = session.connection()
    first_txn = None
    snapshot_id = None
    restart = False
    invalidated = []
    xmin = -1
    stage_for_followup = list(
        request.registry.settings.get(
            "stage_for_followup",
            ''
        ).replace(' ', '').split(',')
    )
    # May have undone uuids from prior cycle
    indexer_state = IndexerState(es_inst, index_str, followups=stage_for_followup)
    (xmin, invalidated, restart) = indexer_state.priority_cycle(request)
    if restart:
        xmin = -1
        invalidated = []
    result = indexer_state.get_initial_state()  # get after checking priority!
    if xmin == -1 or not invalidated:
        xmin = get_current_xmin(request)
        last_xmin = None
        if 'last_xmin' in request.json:
            last_xmin = request.json['last_xmin']
        else:
            status = es_inst.get(
                index=index_str,
                doc_type='meta',
                id='indexing',
                ignore=[400, 404]
            )
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
    if len(invalidated) > SEARCH_MAX:
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
            invalidated = set(invalidated)
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
            if invalidated:
                updated |= invalidated
            result['txn_count'] = txn_count
            if txn_count == 0 and not invalidated:
                indexer_state.send_notices()
                return result
            (related_set, full_reindex) = get_related_uuids(
                request,
                es_inst,
                updated,
                renamed
            )
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
                if snapshot_id is None and not recovery:
                    snapshot_id = connection.execute(
                        'SELECT pg_export_snapshot();'
                    ).scalar()
    if invalidated and not dry_run:
        if stage_for_followup:
            indexer_state.prep_for_followup(xmin, invalidated)
        result = indexer_state.start_cycle(invalidated, result)
        errors = indexer.update_objects(
            request,
            invalidated,
            xmin,
            snapshot_id=snapshot_id,
            restart=restart,
        )
        result = indexer_state.finish_cycle(result, errors)
        if errors:
            result['errors'] = errors
        if record:
            try:
                es_inst.index(
                    index=index_str,
                    doc_type='meta',
                    body=result,
                    id='indexing'
                )
            except:  # pylint: disable=bare-except
                error_messages = copy.deepcopy(result['errors'])
                del result['errors']
                es_inst.index(
                    index=index_str,
                    doc_type='meta',
                    body=result,
                    id='indexing',
                )
                for item in error_messages:
                    if 'error_message' in item:
                        log.error(
                            'Indexing error for %s, error message: %s',
                            item['uuid'],
                            item['error_message'],
                        )
                        item['error_message'] = (
                            "Error occured during indexing, check the logs"
                        )
                result['errors'] = error_messages
        es_inst.indices.refresh('_all')
        if flush:
            try:
                es_inst.indices.flush_synced(index='_all')
            except ESConflictError:
                pass
    if first_txn is not None:
        result['txn_lag'] = str(datetime.datetime.now(pytz.utc) - first_txn)
    indexer_state.send_notices()
    return result


@view_config(route_name='_indexer_state', request_method='GET', permission="index")
def indexer_state_show(request):
    '''Indexer State Endpoint'''
    es_inst = request.registry[ELASTIC_SEARCH]
    index_str = request.registry.settings['snovault.elasticsearch.index']
    indexer_state = IndexerState(es_inst, index_str)
    reindex = request.params.get("reindex")
    if reindex is not None:
        msg = indexer_state.request_reindex(reindex)
        if msg is not None:
            return msg
    who = request.params.get("notify")
    bot_token = request.params.get("bot_token")
    if who is not None or bot_token is not None:
        notices = indexer_state.set_notices(
            request.host_url,
            who,
            bot_token,
            request.params.get("which")
        )
        if notices is not None:
            return notices
    display = indexer_state.display(uuids=request.params.get("uuids"))
    item_types = all_types(request.registry)
    count = 0
    for item_type in item_types:
        try:
            type_count = es_inst.count(index=item_type).get('count', 0)
            count += type_count
        except:  #pylint: disable=bare-except
            pass
    if count:
        display['docs_in_index'] = count
    else:
        display['docs_in_index'] = 'Not Found'
    if not request.registry.settings.get('testing', False):
        try:
            res = requests.get(request.host_url + '/_indexer')
            display['listener'] = json.loads(res.text)
            display['status'] = display['listener']['status']
        except:  #pylint: disable=bare-except
            log.error('Error getting /_indexer', exc_info=True)
    display['registered_indexers'] = indexer_state.get_list('registered_indexers')
    request.query_string = "format=json"
    return display
