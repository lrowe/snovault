'''Initialize Elasticsearch Indexer'''
from multiproc import MPIndexer
from primary import PrimaryIndexer


APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
INDEXER = 'indexer'

MAX_CLAUSES_FOR_ES = 8192
SEARCH_MAX = 99999


def includeme(config):
    '''Add Indexer To Registry'''
    if config.registry.settings.get('indexer_worker'):
        return
    es_inst = config.registry[ELASTIC_SEARCH]
    es_index = config.registry.settings['snovault.elasticsearch.index']
    processes = int(config.registry.settings.get('indexer.processes', 1))
    if processes > 1:
        chunk_size = int(
            config.registry.settings.get('indexer.chunk_size', 1024)
        )
        kwargs = {
            'app_factory': config.registry[APP_FACTORY],
            'chunk_size': chunk_size,
            'processes': processes,
            'registry_settings': config.registry.settings,
        }
        config.registry[INDEXER] = MPIndexer(
            es_inst,
            es_index,
            **kwargs,
        )
    else:
        config.registry[INDEXER] = PrimaryIndexer(es_inst, es_index)
