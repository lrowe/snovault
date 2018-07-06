import time

from redis import StrictRedis

from .base_queue import UuidBaseQueue


REDIS_LIST = 'REDIS_LIST'
REDIS_LIST_PIPE = 'REDIS_LIST_PIPE'
REDIS_SET = 'REDIS_SET'
REDIS_SET_PIPE = 'REDIS_SET_PIPE'
REDIS_SET_PIPE_EXEC = 'REDIS_SET_PIPE_EXEC'


class RedisClient(StrictRedis):

    def __init__(self, args):
        host = args.pop('host', 'localhost')
        port = args.pop('port', '6379')
        super().__init__(
            charset="utf-8",
            decode_responses=True,
            db=0,
            host=host,
            port=port,
            socket_timeout=5,
        )

    def get_queue(self, queue_name, queue_type, queue_options):
        if queue_type == REDIS_LIST:
            queue_class = RedisListQueue
        elif queue_type == REDIS_LIST_PIPE:
            queue_class = RedisListPipeQueue
        elif queue_type == REDIS_SET:
            queue_class = RedisSetQueue
        elif queue_type == REDIS_SET_PIPE:
            queue_class = RedisSetPipeQueue
        elif queue_type == REDIS_SET_PIPE_EXEC:
            queue_class = RedisSetPipeExecQueue
        else:
            raise ValueError('Queue %s is not available' % queue_type)
        return queue_class(queue_name, self)


class RedisQueue(UuidBaseQueue):
    max_value_size = 262144  # Arbitraitly set to AWS SQS Limit

    def __init__(self, queue_name, client):
        self._client = client
        self.queue_name = queue_name

    def _call_func(self, func_str, value=None):
        """
        Connection Error wrapper for all redis client calls
        """
        if not hasattr(self._client, func_str):
            raise ValueError(
                'Queue %s does not have %s' % (self.queue_name, func_str)
            )
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
            value_len = len(value) # currently all values should be same length
            pipe_func(self.queue_name, value)
            bytes_added += value_len
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        # ret_val is a list of the redis queue count after insertion of pipe item
        if ret_val is False:
            ret_val = []
            failed = values
        else:
            diff = call_cnt - len(ret_val)
            bytes_added -= value_len * diff
            failed = [str(num) for num in range(diff)]
        success_cnt = call_cnt - len(failed)
        return failed, bytes_added, call_cnt

    # Get Values
    def get_values(self, get_count):
        pipe, pipe_func = self._get_pipe(self.get_str)
        values = []
        call_cnt = 0
        while call_cnt < get_count:
            pipe_func(self.queue_name)
            call_cnt += 1
        ret_val = self._call_pipe(pipe)
        if ret_val:
            for val in ret_val:
                if val:
                    values.append(val)
        return values, call_cnt


class RedisListQueue(RedisQueue):
    queue_type = REDIS_LIST
    add_str = 'lpush'
    get_str = 'lpop'
    ret_str = 'rpush'
    len_str = 'llen'


class RedisListPipeQueue(RedisPipeQueue):
    queue_type = REDIS_LIST_PIPE
    add_str = 'lpush'
    get_str = 'lpop'
    ret_str = 'rpush'
    len_str = 'llen'


class RedisSetQueue(RedisQueue):
    queue_type = REDIS_SET
    add_str = 'sadd'
    get_str = 'spop'
    ret_str = 'sadd'
    len_str = 'scard'


class RedisSetPipeQueue(RedisPipeQueue):
    queue_type = REDIS_SET_PIPE
    add_str = 'sadd'
    get_str = 'spop'
    ret_str = 'sadd'
    len_str = 'scard'


class RedisSetPipeExecQueue(RedisSetPipeQueue):
    queue_type = REDIS_SET_PIPE_EXEC
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
