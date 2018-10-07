from zope.interface import Interface

# Registry tool id
SNP_SEARCH_ES = 'snp_search'
RESOURCES_INDEX = 'snovault-resources'


class ICachedItem(Interface):
    """ Marker for cached Item
    """
