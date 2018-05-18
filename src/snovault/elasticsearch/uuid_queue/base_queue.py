class UuidBaseQueue(object):
    """
    Basic in memory queue built from Python List

    This Classes defines the publicly allowed methods for any child queues
    """
    max_msg_bytes = 262144  # Set to hard AWS SQS Limit.

    def __init__(self, queue_name, queue_type, queue_options):
        self.queue_name = queue_name
        self.queue_type = queue_type
        self.queue_options = queue_options
        self._values = []

    # Add Values
    def _add_value(self, value):
        if value:
            self._values.append(value)
            return True
        return False

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

    # Get Values
    def _get_value(self):
        if self._values:
            value = self._values.pop()
            return value
        return False

    def get_values(self, get_count):
        values = []
        bytes_got = 0
        call_cnt = 0
        got_value = self._get_value()
        while got_value:
            bytes_got += len(got_value)
            call_cnt += 1
            values.append(got_value)
            if len(values) >= get_count:
                break
            got_value = self._get_value()
        return values, bytes_got, call_cnt

    # Other
    def purge(self):
        self._values = []

    def test(self):
        pass
