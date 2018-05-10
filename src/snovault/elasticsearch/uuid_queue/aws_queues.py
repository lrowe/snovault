import boto3
import time

from .base_queue import UuidBaseQueue


AWS_SQS_TYPE = 'aws-sqs-msg'


def _check_status_code(res):
    if res.get('ResponseMetadata', {}).get('HTTPStatusCode') == 200:
        return True
    return False


class AwsClient(object):

    def __init__(self):
        self._client = boto3.client('sqs')

    def _create_queue(self, queue_name, queue_options, do_wait=False):
        try:
            res = self._client.create_queue(QueueName=queue_name, Attributes=queue_options)
            return True
        except self._client.exceptions.QueueDeletedRecently:
            if do_wait:
                print('Waiting 65 seconds due to QueueDeletedRecently')
                time.sleep(65)
                return self._create_queue(queue_name, queue_options)
        return False

    def _get_queue_url(self, queue_name):
        try:
            res = self._client.get_queue_url(
                QueueName=queue_name,
            )
        except self._client.exceptions.QueueDoesNotExist:
            res = {}
        if res.get('QueueUrl'):
            return res['QueueUrl']
        return None

    def get_queue(self, queue_name, queue_type, queue_options):
        if queue_type == AWS_SQS_TYPE:
            queue_url = self._get_queue_url(queue_name)
            if not queue_url:
                attributes = queue_options.get('attributes', {})
                if self._create_queue(queue_name, queue_options, do_wait=True):
                    queue_url = self._get_queue_url(queue_name)
            if not queue_url:
                raise ValueError('AwsSqsClient.get_queue %s is not found or be created.' % queue_name)
            return AwsSqsQueue(queue_name, queue_url, self._client)
        else:
            raise ValueError('AwsSqsClient.get_queue %s is not available' % queue_type)

    def test(self):
        res = self._client.list_queues()
        if not _check_status_code(res):
            raise ValueError('AwsSqsClient.test() Failed:' + str(res))


class AwsSqsQueue(UuidBaseQueue):
    max_msgs = 10  # AWS limitation at the time.  May 2018.

    def __init__(self, queue_name, queue_url, client):
        self._client = client
        self.resource = boto3.resource('sqs')
        self.queue = self.resource.Queue(queue_url)
        self.queue_url = queue_url
        self.queue_name = queue_name

    # Add Values
    def _send_message(self, msg):
        res = self.queue.send_message(MessageBody=msg)
        if not _check_status_code(res):
            return 0
        return 1

    @staticmethod
    def _id_values_gen(values):
        id_base = str(int((time.time() * 1000000)))
        for index, value in enumerate(values, 1):
            id_str = '%d-%s' % (index, id_base)
            yield id_str, value

    def _get_entires(self, values):
        entries = []
        entries_msg_size = 0
        for id_str, value in self._id_values_gen(values):
            new_size = len(value)
            to_be_size = entries_msg_size + new_size
            entry = {'Id': id_str, 'MessageBody': value}
            if to_be_size >= self.max_msg_bytes:
                yield entries, entries_msg_size
                entries = []
                entries_msg_size = 0
            entries_msg_size += new_size
            entries.append(entry)
            if len(entries) == self.max_msgs:
                yield entries, entries_msg_size
                entries = []
                entries_msg_size = 0
        yield entries, entries_msg_size

    def _add_value(self, entries):
        res = self.queue.send_messages(Entries=entries)
        if _check_status_code(res):
            return True
        return False

    def add_values(self, values):
        failed = []
        bytes_added = 0
        call_cnt = 0
        for entries, entries_bytes in self._get_entires(values):
            if entries:
                ret_value = self._add_value(entries)
                if ret_value is False:
                    failed.append(entries)
                else:
                    call_cnt += 1
                    bytes_added += entries_bytes
        return failed, bytes_added, call_cnt

    # Get Values
    @staticmethod
    def _get_msg_values(got_msgs):
        values = []
        msg_size = 0
        for sqs_msg in got_msgs:
            value = sqs_msg.body
            res_del = sqs_msg.delete()
            if _check_status_code(res_del):
                msg_size += len(value)
                values.append(value)
        return values, msg_size

    def get_values(self, get_count):
        # Aws has a max number for receive messages and does not guarantee we
        # receive the max.  We always ask for the max, so the number of return
        # values could be greater than asked for.
        values = []
        bytes_got = 0
        call_cnt = 0
        got_msg = self.queue.receive_messages(MaxNumberOfMessages=self.max_msgs)
        while got_msg:
            msg_values, msg_bytes = self._get_msg_values(got_msg)
            values.extend(msg_values)
            bytes_got += msg_bytes
            call_cnt += 1
            if len(values) >= get_count:
                break
            got_msg = self.queue.receive_messages(MaxNumberOfMessages=self.max_msgs)
        return values, bytes_got, call_cnt

    # Other
    def purge(self, counter=0):
        try:
            res = self.queue.purge()
            if not _check_status_code(res):
                raise ValueError('AwsSqsQueue.purge() Failed:' + str(res))
        except self._client.exceptions.PurgeQueueInProgress:
            if not counter == 0:
                raise ValueError('AwsSqsQueue.purge() Wait Failed:' + str(res))
            print('Waiting 60 seconds due to PurgeQueueInProgress')
            time.sleep(60)
            self.purge(counter=1)

    def test(self):
        res = self.queue.send_message(MessageBody='test msg')
        res_message_id = res['MessageId']
        if not _check_status_code(res):
            raise ValueError('AwsSqsQueue.test() Failed to send message:' + str(res))
        sqs_msg_list = self.queue.receive_messages(MaxNumberOfMessages=self.max_msgs)
        for sqs_msg in sqs_msg_list:
            if res_message_id == sqs_msg.message_id:
                sqs_msg.delete()
                return
        raise ValueError(
            'AwsSqsQueue.test() Failed to find test message: '
            'This test will only pull 10 messages from aws sqs.  The queue is not '
            'supposed to have values prior to running this test.  The test '
            'checks initial connection and not on going connection.'
        )
