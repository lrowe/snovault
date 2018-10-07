'''ES Storeage Wrapper'''

from elasticsearch.helpers import scan
from pyramid.threadlocal import get_current_request
from zope.interface import alsoProvides

from snovault.util import get_root_request

from .indexers import ELASTIC_SEARCH
from .interfaces import (
    ICachedItem,
    RESOURCES_INDEX,
)


def includeme(config):
    '''Initialize es storeage'''
    from snovault import STORAGE
    es_storeage = ElasticSearchStorage(
        config.registry[ELASTIC_SEARCH],
        RESOURCES_INDEX
    )
    config.registry[STORAGE] = PickStorage(
        es_storeage,
        config.registry[STORAGE],
    )


def force_database_for_request():
    '''PickStorage helpers'''
    request = get_current_request()
    if request:
        request.datastore = 'database'


class CachedModel(object):
    '''Cached Model'''
    def __init__(self, hit):
        self.hit = hit
        self.source = hit['_source']

    @property
    def item_type(self):
        '''Return item_type from source'''
        return self.source['item_type']

    @property
    def properties(self):
        '''Return properties from source'''
        return self.source['properties']

    @property
    def propsheets(self):
        '''Return propsheets from source'''
        return self.source['propsheets']

    @property
    def uuid(self):
        '''Return uuid from source'''
        return self.source['uuid']

    @property
    def tid(self):
        '''Return tid from source'''
        return self.source['tid']

    def invalidated(self):
        '''Return invalidated uuids'''
        request = get_root_request()
        if request is None:
            return False
        edits = dict.get(request.session, 'edits', None)
        if edits is None:
            return False
        version = self.hit['_version']
        source = self.source
        linked_uuids = set(source['linked_uuids'])
        embedded_uuids = set(source['embedded_uuids'])
        for xid, updated, linked in edits:
            if xid < version:
                continue
            if not embedded_uuids.isdisjoint(updated):
                return True
            if not linked_uuids.isdisjoint(linked):
                return True
        return False

    def used_for(self, item):
        '''Call alsoProvides with item on ICachedItem'''
        # pylint: disable=no-self-use
        alsoProvides(item, ICachedItem)


class PickStorage(object):
    '''Pick Storeage'''
    def __init__(self, read, write):
        self.read = read
        self.write = write

    def storage(self):
        '''Read and write storage from current request'''
        request = get_current_request()
        if request and request.datastore == 'elasticsearch':
            return self.read
        return self.write

    def get_by_uuid(self, uuid):
        '''Get by uuid form storeage'''
        storage = self.storage()
        model = storage.get_by_uuid(uuid)
        if storage is self.read:
            if model is None or model.invalidated():
                force_database_for_request()
                return self.write.get_by_uuid(uuid)
        return model

    def get_by_unique_key(self, unique_key, name, index=None):
        '''Get by unique_key form storeage'''
        storage = self.storage()
        model = storage.get_by_unique_key(unique_key, name, index=index)
        if storage is self.read:
            if model is None or model.invalidated():
                force_database_for_request()
                return self.write.get_by_unique_key(unique_key, name, index=index)
        return model

    def get_rev_links(self, model, rel, *item_types):
        '''get_rev_links'''
        storage = self.storage()
        if isinstance(model, CachedModel) and storage is self.write:
            model = storage.get_by_uuid(str(model.uuid))
        return storage.get_rev_links(model, rel, *item_types)

    def __iter__(self, *item_types):
        return self.storage().__iter__(*item_types)

    def __len__(self, *item_types):
        return self.storage().__len__(*item_types)

    def create(self, item_type, uuid):
        '''Write item_type and uuid'''
        return self.write.create(item_type, uuid)

    def update(
            self,
            model,
            properties=None,
            sheets=None,
            unique_keys=None,
            links=None
        ):
        '''Write item_type and uuid'''
        # pylint: disable=too-many-arguments
        return self.write.update(model, properties, sheets, unique_keys, links)


class ElasticSearchStorage(object):
    '''ES Storeage'''
    writeable = False

    def __init__(self, es_inst, index):
        self.es_inst = es_inst
        self.index = index

    def _one(self, query, index=None):
        if index is None:
            index = self.index
        data = self.es_inst.search(index=index, body=query)
        hits = data['hits']['hits']
        if len(hits) != 1:
            return None
        model = CachedModel(hits[0])
        return model

    def get_by_uuid(self, uuid):
        '''Get from es by uuid'''
        query = {
            'query': {
                'term': {
                    'uuid': str(uuid)
                }
            },
            'version': True
        }
        result = self.es_inst.search(
            index=self.index,
            body=query,
            _source=True,
            size=1
        )
        if result['hits']['total'] == 0:
            return None
        hit = result['hits']['hits'][0]
        return CachedModel(hit)

    def get_by_unique_key(self, unique_key, name, index=None):
        '''Get from es by unique_key'''
        term = 'unique_keys.' + unique_key
        query = {
            'query': {
                'term': {term: name}
            },
            'version': True,
        }
        return self._one(query, index)

    def get_rev_links(self, model, rel, *item_types):
        '''Get get_rev_links es'''
        filter_ = {'term': {'links.' + rel: str(model.uuid)}}
        if item_types:
            filter_ = [
                filter_,
                {'terms': {'item_type': item_types}},
            ]
        query = {
            'stored_fields': [],
            'query': {
                'bool': {
                    'filter': filter_,
                }
            }
        }
        return [
            hit['_id'] for hit in scan(self.es_inst, query=query)
        ]

    def __iter__(self, *item_types):
        query = {
            'stored_fields': [],
            'query': {
                'bool': {
                    'filter': {
                        'terms': {
                            'item_type': item_types
                        }
                    } if item_types else {'match_all': {}}
                }
            }
        }
        for hit in scan(self.es_inst, query=query):
            yield hit['_id']

    def __len__(self, *item_types):
        query = {
            'filter': {
                'terms': {
                    'item_type': item_types
                }
            } if item_types else {'match_all': {}}
        }
        result = self.es_inst.count(index=self.index, body=query)
        return result['count']
