"""
Logs indexing data for a batch of uuids
* Logs uuid timing, exceptions, and associated data
* Can send info to log, file as json
* Extending to database should be possible
"""
import logging
import time
import json


def _dump_output_to_file(base_file_path, outputs, out_size=100000, pretty=False):
    '''Dump indexer outputs to json in batches'''
    path_index = 0
    while outputs:
        path_index += 1
        if len(outputs) >= out_size:
            out = outputs[:out_size]
            outputs = outputs[out_size:]
        else:
            out = outputs[:]
            outputs = []
        file_path = '%s_batch-%s.json' % (
            base_file_path,
            str(path_index),
        )
        with open(base_file_path, 'w') as file_handler:
            if pretty:
                json.dump(out, file_handler, indent=4, separators=(',', ': '))
            else:
                json.dump(out, file_handler)


class LogIndexData(object):
    '''Wraps the logging module for out indexing process'''
    log_name = 'indexing_time'
    log_path = './'

    def __init__(self, index_info, do_log=False):
        self._index_info = index_info
        self._the_log = None
        self._do_log = do_log

    @staticmethod
    def _get_time_str():
        return str(int(time.time() * 10000000))

    def _close_handlers(self):
        '''Close all log handlers'''
        for handler in self._the_log.handlers:
            handler.close()
            self._the_log.removeHandler(handler)

    def _get_log(self):
        if self._do_log:
            # Timestamp converted to micro seconds to separate index logs
            file_name = "{}-{}.log".format(
                self.log_name,
                self._get_time_str(),
            )
            file_path = "{}/{}".format(self.log_path, file_name)
            level = logging.INFO
            formatter_str = '%(asctime)s %(message)s'
            log = logging.getLogger(self.log_name)
            hanlder = logging.FileHandler(file_path)
            formatter = logging.Formatter(formatter_str)
            hanlder.setFormatter(formatter)
            log.addHandler(hanlder)
            log.setLevel(level)
            return log
        return None

    def _reset_log(self):
        '''
        Close handlers and Get new log
        * Logger gets logs name so we call twice to clear and get a new one
        '''
        if self._do_log:
            if self._the_log:
                self._close_handlers()
            self._the_log = self._get_log()

    def append_output(self, output):
        '''Log the output dict from the Indexer.update_object class'''
        if 'embed_time' in output and isinstance(output['embed_time'], float):
            output['embed_time'] = '%0.6f' % output['embed_time']
        if 'es_time' in output and isinstance(output['es_time'], float):
            output['es_time'] = '%0.6f' % output['es_time']
        self.write_log(
            '{start_time} {end_timestamp} {doc_path} {doc_type} '
            '{embed_time} {embed_ecp} '
            '{es_time} {es_ecp} '
            '{embeds} {linked} '
            ''.format(
                embeds=output.get('doc_embedded'),
                embed_ecp=output.get('embed_ecp'),
                embed_time=output.get('embed_time'),
                end_timestamp=output.get('end_timestamp'),
                es_time=output.get('es_time'),
                es_ecp=output.get('es_ecp'),
                doc_path=output.get('doc_path'),
                doc_type=output.get('doc_type'),
                linked=output.get('doc_linked'),
                start_time=output.get('start_time'),
            )
        )

    def new_log(self, len_uuids, xmin, snapshot_id):
        '''Reset log and add start message'''
        self._reset_log()
        self.write_log(
            'Starting Indexing {} with xmin={} and snapshot_id={}'.format(
                len_uuids, xmin, snapshot_id,
            )
        )
        self.write_log(
            'start_time end_timestamp doc_path doc_type '
            'embed_time embed_ecp es_time es_ecp embeds linked'
        )

    def write_log(self, msg, uuid=None, start_time=None):
        '''Handles all logging message'''
        if self._the_log:
            uuid = str(uuid) + ' ' if uuid else ''
            diff = ''
            if start_time:
                diff = ' %0.6f' % (time.time() - start_time)
            self._the_log.info("%s%s%s", uuid, msg, diff)

    def handle_outputs(self, outputs, run_info):
        '''Do what settings say to do with outputs'''
        if self._do_log:
            outputs.append(run_info)
            base_file_path = '%s/%s_uuids-%d' % (
                '/srv/encoded/indexer-logs',
                self._get_time_str(),
                run_info['uuid_count'],
            )
            _dump_output_to_file(
                base_file_path,
                outputs,
                out_size=100000,
                pretty=True,
            )
