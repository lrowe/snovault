"""
Adapater to connect public uuid queue functionality to a specific queue type

* The adapter is split into two classes, UuidQueueWorker and UuidQueue.  The
worker is the base class and has limited functionality.


## Queues (External): Specific kind of queue accesible through Adapter
    * The client and queue enums, are imported here and included in
    UuidQueueTypes.
    * Each queue has a unique type and the queue client could have access to
    different types of queue.  For example the Redis client has 5 types of
    queues while the AWS and Base clients only have one.
    * Queues must to expose two functions, add_values and get_values.
    * Purge, to empty a queue, is also implemented but currently not vital.

## UuidQueueWorker: Used to get values loaded by an instance of UuidQueue
    * Base class for UuidQueue
    * Initializes queue client and creates the queue
    * Limited functionality.  Used as a client in the client/server pattern.

## UuidQueue: Used to load values into a queue.  Can also get values.
    * Child class of UuidQueueWorker
    * Expanded functionality.  Used as a server in the client/server pattern.

## Other
    * uuid encode/decode: Uuids can be combined together when stored in a
    queue.  This was added because mainly for AWS SQS but is not queue
    dependent.  The specific queues do no care what they store.  encode/decode
    functionality  should stay in the adapter.  It is much faster to
    encoded/decode the uuids to limit the number of transactions.  _batch_by
    instance variable greater than 1 will utilize this functionality.
    * _batch_by is how many uuids are store in each record.
        Ex) If uuids were 2 chars long in the form 1x, 2x, etc..
            and batch_by = 2, then
            the queue would have values of ['1x2x', '3x4x', '5x']
            Where, depending on the number of items added, there
            could be a single length uuid item.
        All uuids need to be the same length.
        If batch_by = 1 then the batching functionality is ignored.
        Max uuids allow to batch is determined by the type of queue
    * If _batch_by is greater than 1 then all uuids must be the same length
    and be the lenth specified in uuid_len.

"""
import time

from .aws_queues import AwsClient
from .aws_queues import AWS_SQS

from .base_queue import BaseClient
from .base_queue import BASE_IN_MEMORY

from .redis_queues import RedisClient
from .redis_queues import REDIS_LIST
from .redis_queues import REDIS_LIST_PIPE
from .redis_queues import REDIS_SET
from .redis_queues import REDIS_SET_PIPE
from .redis_queues import REDIS_SET_PIPE_EXEC


def _get_combined_uuids_gen(batch_by, uuid_len, max_value_size, uuids):
    combo_uuid = ''
    combine_count = 0
    combo_uuid_size = 0
    for uuid in uuids:
        new_size = len(uuid)
        if not new_size == uuid_len:
            raise ValueError(
                '_get_combined_uuids_gen: uuids are not correct length.'
                ' %d != %d' % (new_size, uuid_len)
            )
        to_be_size = combo_uuid_size + new_size
        if combine_count < batch_by and to_be_size < max_value_size:
            combine_count += 1
            combo_uuid_size = to_be_size
            combo_uuid += uuid
        else:
            yield combo_uuid
            combine_count = 1
            combo_uuid_size = new_size
            combo_uuid = uuid
    if combo_uuid:
        yield combo_uuid


def _get_uncombined_uuids(uuid_len, combo_uuid):
    uuids = []
    uuid_count = len(combo_uuid) // uuid_len
    for ind in range(0, uuid_count):
        start = uuid_len * ind
        uuid = combo_uuid[start:start  + uuid_len]
        uuids.append(uuid)
    return uuids


class UuidQueueTypes(object):
    '''
    Queue Type Manager

    * python enum package was not used due to versioning.  I assume it
    would be a better choice.
    '''
    AWS_SQS = AWS_SQS
    BASE_IN_MEMORY = BASE_IN_MEMORY
    REDIS_LIST = REDIS_LIST
    REDIS_LIST_PIPE = REDIS_LIST_PIPE
    REDIS_SET = REDIS_SET
    REDIS_SET_PIPE = REDIS_SET_PIPE
    REDIS_SET_PIPE_EXEC = REDIS_SET_PIPE_EXEC

    @classmethod
    def check_queue_type(cls, queue_type):
        for key in dir(cls):
            if not key[0] == '_' and key.isupper():
                if getattr(cls, key) == queue_type:
                    return True
        return False

    @classmethod
    def get_all(cls):
        values = []
        for key in dir(cls):
            if not key[0] == '_' and key.isupper():
                value = getattr(cls, key)
                if cls.check_queue_type(value):
                    values.append(value)
        return values


class UuidQueueWorker(object):
    '''
    UuidQueueWorker is the base class for the UuidQueue

    * Here you will find the majority of initialization for both Classes,
    including creating of the queue client.
    * Also the get_uuids functionality is here since both types of queue
    need to get uuids.
    * Loading uuids and other managment functionality is found in the UuidQueue.
    * The 'queue.get_values' method has a 'get_count' parameter which allows
    you to ask for a certain number of uuids in one call.  This is not
    guaranteed if 'batch_by' is more than one and the total number of loaded
    uuids is not evenly divisible by batch_by.  The number returned could
    be a little more or less than asked for.
    '''
    def __init__(self, queue_name, queue_type, args):
        self._batch_by = args.pop('batch_by', 1)
        self.uuid_len = args.pop('uuid_len', 36)
        if not UuidQueueTypes.check_queue_type(queue_type):
            raise ValueError('UuidQueueTypes type %s not handled' % queue_type)
        if UuidQueueTypes.BASE_IN_MEMORY == queue_type:
            client_cls = BaseClient
        elif UuidQueueTypes.AWS_SQS == queue_type:
            client_cls = AwsClient
        else:
            client_cls = RedisClient
        self._client = client_cls(args)
        self._queue = self._client.get_queue(queue_name, queue_type, args)

    @property
    def queue_name(self):
        return self._queue.queue_name

    @property
    def queue_type(self):
        return self._queue.queue_type

    def get_uuids(self, get_count=1):
        uuids = []
        call_cnt = 0
        if self._batch_by == 1:
            uuids, call_cnt = self._queue.get_values(get_count)
        else:
            if get_count < self._batch_by:
                batch_get_count = 1
            else:
                batch_get_count = ((get_count + 1)//self._batch_by) + 1
            combined_uuids_list, call_cnt = self._queue.get_values(batch_get_count)
            for combined_uuids in combined_uuids_list:
                uncombined_uuids = _get_uncombined_uuids(self.uuid_len, combined_uuids)
                uuids.extend(uncombined_uuids)
        return uuids, call_cnt


class UuidQueue(UuidQueueWorker):
    '''
    Indexer to Queue Adapter / Manager

    * UuidQueueWorker should hold functionality needed for client and server.
    * This Class should have expanded capabilities need for a uuid server.
    '''
    def __init__(self, queue_name, queue_type, args):
        self._xmin = args.pop('xmin', None)
        self._snapshot_id = args.pop('snapshot_id', None)
        super().__init__(queue_name, queue_type, args)

    def load_uuids(self, uuids):
        failed = []
        bytes_added = 0
        success_cnt = 0
        call_cnt = 0
        if self._batch_by == 1:
            failed, bytes_added, call_cnt = self._queue.add_values(uuids)
        else:
            combined_uuids_gen = _get_combined_uuids_gen(
                self._batch_by,
                self.uuid_len,
                self._queue.max_value_size,
                uuids,
            )
            failed, bytes_added, call_cnt  = self._queue.add_values(
                combined_uuids_gen
            )
        success_cnt = bytes_added//self.uuid_len
        return failed, success_cnt, call_cnt

    def purge(self):
        self._queue.purge()
