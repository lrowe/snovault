import time

from redis import StrictRedis
from redis.exceptions import ConnectionError
from redis.exceptions import TimeoutError

from .base_queue import UuidBaseQueue


REDIS_L_TYPE = 'redis-list'
REDIS_PL_TYPE = 'redis-pipe-list'
REDIS_PS_TYPE = 'redis-pipe-set'
REDIS_PSE_TYPE = 'redis-pipe_set_exec'
REDIS_S_TYPE = 'redis-set'


class RedisClient(StrictRedis):

    def __init__(self, host='localhost', port=6379):
        super().__init__(
            charset="utf-8",
            decode_responses=True,
            db=0,
            host=host,
            port=port,
            socket_timeout=5,
        )

    def get_queue(self, queue_name, queue_type, uuid_len=36):
        if queue_type == REDIS_L_TYPE:
            queue_class = RedisListQueue
        elif queue_type == REDIS_PL_TYPE:
            queue_class = RedisPipeListQueue
        elif queue_type == REDIS_PS_TYPE:
            queue_class = RedisPipeSetQueue
        elif queue_type == REDIS_PSE_TYPE:
            queue_class = RedisPipeSetQueueExec
        elif queue_type == REDIS_S_TYPE:
            queue_class = RedisSetQueue
        else:
            raise ValueError('RedisClient.get_queue %s is not available' % queue_type)
        return queue_class(queue_name, self, uuid_len=uuid_len)

    def test(self):
        test_key = 'foo'
        test_value = str(time.time())
        try:
            self.set(test_key, test_value)
            if not self.get(test_key) == test_value:
                raise ValueError('RedisClient.test() Failed.')
        except ConnectionError as ecp:
            raise ecp
        except TimeoutError as ecp:
            raise ecp


class RedisQueue(UuidBaseQueue):
    key_tag = ''

    def __init__(self, queue_name, client, uuid_len=36):
        self._client = client
        self.queue_name = self.key_tag + '_' + queue_name
        self.queue_meta_name = 'meta:' + self.key_tag + '_' + queue_name
        self.uuid_len = uuid_len
        self.set_meta_data()

    def _call_func(self, func_str, value=None):
        """
        Connection Error wrapper for all redis client calls
        """
        if not hasattr(self._client, func_str):
            raise ValueError('Queue %s does not have %s' % (self.queue_name, func_str))
        func = getattr(self._client, func_str)
        try:
            if value:
                return func(self.queue_name, value)
            else:
                return func(self.queue_name)
        except ConnectionError:
            return False

    # Add Values
    def _add_value(self, value):
        ret_val = self._call_func(self.add_str, value)
        return ret_val

    # Get Values
    def _get_value(self):
        value = self._call_func(self.get_str)
        return value

    # Other
    def purge(self):
        self._client.delete(self.queue_name)

    def queue_length(self):
        return self._call_func(self.len_str)

    def return_value(self, value):
        return self._call_func(self.ret_str, value)

    def test(self):
        self._client.test()

    # Meta Data
    def get_meta_data(self):
        return self._client.hgetall(self.queue_meta_name)

    def _set_meta_data(self, meta_data):
        self._client.hmset(self.queue_meta_name, meta_data)


class RedisPipeQueue(RedisQueue):

    def _call_pipe(self, pipe):
        """
        Connection Error wrapper for Pipe Queues redis client calls
        """
        try:
            # At the time of writting pipe return a list of length
            # Each item is the length of the queue when it was added.
            ret_list = pipe.execute()
            return ret_list
        except ConnectionError:
            return False

    def _get_pipe(self, func_str):
        pipe = self._client.pipeline()
        pipe_func = getattr(pipe, func_str)
        return pipe, pipe_func

    # Add Values
    def add_values(self, values):
        pipe, pipe_func = self._get_pipe(self.add_str)
        failed = []
        bytes_added = 0
        call_cnt = 0
        for value in values:
            pipe_func(self.queue_name, value)
            bytes_added += len(value)
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        # ret_val is a list of the redis queue count after insertion of pipe item
        if ret_val is False:
            ret_val = []
            failed = values
        else:
            diff = call_cnt - len(ret_val)
            bytes_added -= self.uuid_len * diff
            failed = [str(num) for num in range(diff)]
        success_cnt = call_cnt - len(failed)
        return failed, bytes_added, call_cnt

    # Get Values
    def get_values(self, get_count):
        pipe, pipe_func = self._get_pipe(self.get_str)
        values = []
        bytes_got = 0
        call_cnt = 0
        while call_cnt < get_count:
            pipe_func(self.queue_name)
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        if ret_val:
            for val in ret_val:
                if val:
                    bytes_got += len(val)
                    values.append(val)
        return values, bytes_got, call_cnt


class RedisListQueue(RedisQueue):
    key_tag = 'L'
    add_str = 'lpush'
    get_str = 'lpop'
    ret_str = 'rpush'
    len_str = 'llen'


class RedisPipeListQueue(RedisPipeQueue):
    key_tag = 'PL'
    add_str = 'lpush'
    get_str = 'lpop'
    ret_str = 'rpush'
    len_str = 'llen'


class RedisSetQueue(RedisQueue):
    key_tag = 'S'
    add_str = 'sadd'
    get_str = 'spop'
    ret_str = 'sadd'
    len_str = 'scard'


class RedisPipeSetQueue(RedisPipeQueue):
    key_tag = 'PS'
    add_str = 'sadd'
    get_str = 'spop'
    ret_str = 'sadd'
    len_str = 'scard'


class RedisPipeSetQueueExec(RedisPipeSetQueue):
    key_tag = 'PSE'
    get_str = 'SPOP'

    def get_values(self, get_count):
        """
        Remove and return a random member of set ``name``

        Modified from https://github.com/andymccurdy/redis-py/blob/master/redis/client.py
        directly sinces it is not implemented in this version
        """
        values = []
        bytes_got = 0
        call_cnt = 0
        args = (get_count is not None) and [get_count] or []
        ret_val = self._client.execute_command(self.get_str, self.queue_name, *args)
        call_cnt += 1
        if ret_val:
            for val in ret_val:
                if val:
                    bytes_got += len(val)
                    values.append(val)
        return values, bytes_got, call_cnt
