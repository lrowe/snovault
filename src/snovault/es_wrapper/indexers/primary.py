'''Primary Indexer Class and Base Class for All Indexers'''
import datetime
import logging
import time

from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError as ESConnectionError,
    TransportError,
)
from sqlalchemy.exc import StatementError
from urllib3.exceptions import ReadTimeoutError


es_logger = logging.getLogger("elasticsearch")  # pylint: disable=invalid-name
es_logger.setLevel(logging.ERROR)
log = logging.getLogger(__name__)  # pylint: disable=invalid-name
MAX_CLAUSES_FOR_ES = 8192


class PrimaryIndexer(object):
    '''Primary Indexer Class'''
    def __init__(self, es_inst, index):
        self.es_inst = es_inst
        self.index = index

    def update_objects(
            self,
            request,
            uuids,
            xmin,
            snapshot_id=None,
            restart=False
        ):
        '''Indexes uuids'''
        # pylint: disable=too-many-arguments, unused-argument
        errors = []
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 50 == 0:
                log.info('Indexing %d', i + 1)
        return errors

    def update_object(self, request, uuid, xmin, restart=False):
        '''Indexes uuids'''
        # pylint: disable=unused-argument
        request.datastore = 'database'
        last_exc = None
        try:
            doc = request.embed('/%s/@@index-data/' % uuid, as_user='INDEXER')
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as ecp:  # pylint: disable=broad-except
            log.error('Error rendering /%s/@@index-data', uuid, exc_info=True)
            last_exc = repr(ecp)
        if last_exc is None:
            for backoff in [0, 10, 20, 40, 80]:
                time.sleep(backoff)
                try:
                    self.es_inst.index(
                        index=doc['item_type'], doc_type=doc['item_type'], body=doc,
                        id=str(uuid), version=xmin, version_type='external_gte',
                        request_timeout=30,
                    )
                except StatementError:
                    # Can't reconnect until invalid transaction is rolled back
                    raise
                except ConflictError:
                    log.warning('Conflict indexing %s at version %d', uuid, xmin)
                    return None
                except (ESConnectionError, ReadTimeoutError, TransportError) as ecp:
                    log.warning('Retryable error indexing %s: %r', uuid, ecp)
                    last_exc = repr(ecp)
                except Exception as ecp:  # pylint: disable=broad-except
                    log.error('Error indexing %s', uuid, exc_info=True)
                    last_exc = repr(ecp)
                    break
                else:
                    return None
        timestamp = datetime.datetime.now().isoformat()
        return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}

    def shutdown(self):
        '''TDB To shutdown indexer'''
        pass
