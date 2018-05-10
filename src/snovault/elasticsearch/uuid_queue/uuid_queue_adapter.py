import time

from .base_queue import UuidBaseQueue

from .aws_queues import AwsClient
from .aws_queues import AWS_SQS_TYPE

from .redis_queues import RedisClient
from .redis_queues import REDIS_L_TYPE
from .redis_queues import REDIS_PL_TYPE
from .redis_queues import REDIS_PS_TYPE
from .redis_queues import REDIS_PSE_TYPE
from .redis_queues import REDIS_S_TYPE


def _get_combined_uuids_gen(batch_store_uuids_by, uuids, max_combined_size):
    combo_uuid = ''
    combine_count = 0
    combo_uuid_size = 0
    for uuid in uuids:
        new_size = len(uuid)
        to_be_size = combo_uuid_size + new_size
        if combine_count < batch_store_uuids_by and to_be_size < max_combined_size:
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
    AWS_SQS = AWS_SQS_TYPE
    BASE_MEM = 'in-mem'
    REDIS_L = REDIS_L_TYPE
    REDIS_PL = REDIS_PL_TYPE
    REDIS_PS = REDIS_PS_TYPE
    REDIS_PSE = REDIS_PSE_TYPE
    REDIS_S = REDIS_S_TYPE

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


class UuidQueue(object):
    """
    Indexer to Queue Adapter / Manager

    * AwsSqsQueue
    * A few redis queues
    * InMemoryQueue (In here)

    # Notes:

    * Batch Store is how many uuids are store in each record.
        Ex) If uuids were 2 chars long in the form 1x, 2x, etc..
            and batch_store_uuids_by_max = 2, then
            the queue would have values of ['1x2x', '3x4x', '5x']
            Where, depending on the number of items added there
            could be a single length uuid item.
        So all uuids need to be the same length.
        If batch_store_uuids_by = 1 then the batching functionality is ignored.
        The max number of uuids allow to batch is 'batch_store_uuids_by_max'

    * The 'get_values' method has a 'get_count' parameter which allows you to ask
        for a certain number of uuids in one call.  This is not guaranteed if
        'batch_store_uuids_by' is more than one.  For example, in Batch Store
        example above if 'get_count = 2' then 'get_values' would make 2 pops
        from the queue in order to guarantee you would get at least two uuids;
        To cover the case if the first pop was '5x'.

    """
    def __init__(self, queue_name, queue_type, client_options, queue_options, batch_store_uuids_by=1, uuid_len=36):
        if not UuidQueueTypes.check_queue_type(queue_type):
            raise ValueError('UuidQueueTypes type %s not handled' % queue_type)
        if UuidQueueTypes.BASE_MEM == queue_type:
            client = None
            queue = UuidBaseQueue(queue_name, queue_type, queue_options)
        elif UuidQueueTypes.AWS_SQS == queue_type:
            client = AwsClient(**client_options)
            queue = client.get_queue(queue_name, queue_type, queue_options)
        else:
            client = RedisClient(**client_options)
            queue = client.get_queue(queue_name, queue_type, uuid_len=uuid_len)
        self._client = client
        self._queue = queue
        self._batch_store_uuids_by = batch_store_uuids_by
        self.uuid_len = uuid_len

    @property
    def queue_name():
        return self._queue.queue_name

    def load_uuids(self, uuids):
        failed = []
        bytes_added = 0
        success_cnt = 0
        call_cnt = 0
        if self._batch_store_uuids_by == 1:
            failed, bytes_added, call_cnt = self._queue.add_values(uuids)
        else:
            combined_uuids_gen = _get_combined_uuids_gen(self._batch_store_uuids_by, uuids, self._queue.max_msg_bytes)
            failed, bytes_added, call_cnt  = self._queue.add_values(combined_uuids_gen)
        success_cnt = bytes_added//self.uuid_len
        return failed, success_cnt, call_cnt

    def get_uuids(self, get_count=1):
        uuids = []
        bytes_got = 0
        call_cnt = 0
        if self._batch_store_uuids_by == 1:
            uuids, bytes_got, call_cnt = self._queue.get_values(get_count)
        else:
            batch_get_count = (get_count + 2)//self._batch_store_uuids_by
            combined_uuids_list, bytes_got, call_cnt = self._queue.get_values(batch_get_count)
            for combined_uuids in combined_uuids_list:
                bytes_got += len(combined_uuids)
                uncombined_uuids = _get_uncombined_uuids(self.uuid_len, combined_uuids)
                uuids.extend(uncombined_uuids)
        return uuids, call_cnt

    def purge(self):
        self._queue.purge()

    def test(self):
        self._queue.test()

    def test_client(self):
        if self._client:
            self._client.test()


def _run(queue_kwargs, run_kwargs):
    queue = UuidQueue(**queue_kwargs)
    # Load Uuids
    uuids_to_load = []
    for uuid_int in range(1, run_kwargs['uuids_to_create'] + 1):
        uuid = str(uuid_int)
        if len(uuid) > queue.uuid_len:
            print('uuids_to_create made a uuid greater then uuid_len')
            break
        while len(uuid) < queue.uuid_len:
            uuid += 'x'
        uuids_to_load.append(uuid)
    failed, success_cnt = queue.load_uuids(uuids_to_load)
    print('load_uuids', 'failed:' + str(len(failed)), 'all success:' + str(len(uuids_to_load) == success_cnt))

    # Get Uuids
    uuids_got = []
    cnt = 0
    while len(uuids_got) < success_cnt:
        got_uuids = queue.get_uuids(get_count=run_kwargs['uuids_get_in_batches'])
        if got_uuids:
            uuids_got.extend(got_uuids)
        cnt += 1
        if cnt == 30:
            break
    print('get_uuids', 'match success:' + str(len(uuids_got) == success_cnt))
