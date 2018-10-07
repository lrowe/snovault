'''Define ES Wrapper Interfaces'''
from zope.interface import Interface


SNP_SEARCH_ES = 'snp_search'
RESOURCES_INDEX = 'snovault-resources'


# pylint: disable=inherit-non-class
class ICachedItem(Interface):
    """Marker for cached Item"""
