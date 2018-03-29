from zope.interface import Interface

# Registry tool id
APP_FACTORY = 'app_factory'
ELASTIC_SEARCH = 'elasticsearch'
SNP_SEARCH_ES = 'snp_search'
INDEXER = 'indexer'
RESOURCES_INDEX = 'snovault-resources'


class ICachedItem(Interface):
    """ Marker for cached Item
    """
