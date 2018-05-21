from pyramid.view import view_config

from .interfaces import INDEXER

from .indexer import _get_queue
from .indexer import _poll_queue


def includeme(config):
    config.add_route('mpindex_worker', '/mpindex_worker')
    config.scan(__name__)


@view_config(route_name='mpindex_worker', request_method='POST', permission="index")
def mpindex_worker(request):
    print('mpindex_worker', 'start')
# queue = _get_queue(
#        batch_store_uuids_by,
#        uuid_len,
#        xmin,
#        snapshot_id,
#    )
    print('mpindex_worker', 'end')
    return {'testing': 'foo'}
