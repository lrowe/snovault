BASE_IN_MEMORY = 'BASE_IN_MEMORY'


class BaseClient(object):
    '''
    Place holder queue client

    Redis and AWS queues have a client.  This exists to keep the queues
    consistent.

    * Queue client must expose the 'get_queue' function to the adapter.
    '''
    def __init__(self, args):
        pass

    def get_queue(self, queue_name, queue_type, args):
        if queue_type == BASE_IN_MEMORY:
            queue_class = UuidBaseQueue
        else:
            raise ValueError('Queue %s is not available' % queue_type)
        return queue_class(queue_name)


class UuidBaseQueue(object):
    '''
    Basic in memory queue built from Python List

    This Classes defines the publicly allowed methods for any child queue.
    Funciton implementation is made to look like AWS and Redis queues.
    '''
    max_value_size = 262144  # Arbitraitly set to AWS SQS Limit
    queue_type = BASE_IN_MEMORY

    def __init__(self, queue_name):
        self.queue_name = queue_name
        self._values = []

    def _add_value(self, value):
        if value:
            self._values.append(value)
            return True
        return False

    def _get_value(self):
        if self._values:
            return self._values.pop()

    def add_values(self, values):
        failed = []
        bytes_added = 0
        call_cnt = 0
        for value in values:
            ret_value = self._add_value(value)
            if ret_value is False:
                failed.append(value)
            else:
                call_cnt += 1
                bytes_added += len(value)
        return failed, bytes_added, call_cnt

    def get_values(self, get_count):
        values = []
        call_cnt = 0
        value = self._get_value()
        while value:
            call_cnt += 1
            values.append(value)
            if len(values) >= get_count:
                break
            value = self._get_value()
        return values, call_cnt

    def purge(self):
        self._values = []
