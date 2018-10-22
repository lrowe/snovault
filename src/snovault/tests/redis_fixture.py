'''Redis DB Fixture'''
import os
import sys

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

REDIS_PORT = 9250
REDIS_DEFAULTS = [
    'bind 127.0.0.1 ::1',
    'protected-mode yes',
    # 'port 6379',
    'tcp-backlog 511',
    'timeout 0',
    'tcp-keepalive 300',
    'daemonize no',
    'supervised no',
    'pidfile /var/run/redis_6379.pid',
    'loglevel notice',
    'logfile ""',
    'databases 16',
    'always-show-logo yes',
    'save 900 1',
    'save 300 10',
    'save 60 10000',
    'stop-writes-on-bgsave-error yes',
    'rdbcompression yes',
    'rdbchecksum yes',
    'dbfilename dump.rdb',
    # 'dir /usr/local/var/db/redis/',
    'slave-serve-stale-data yes',
    'slave-read-only yes',
    'repl-diskless-sync no',
    'repl-diskless-sync-delay 5',
    'repl-disable-tcp-nodelay no',
    'slave-priority 100',
    'lazyfree-lazy-eviction no',
    'lazyfree-lazy-expire no',
    'lazyfree-lazy-server-del no',
    'slave-lazy-flush no',
    'appendonly no',
    'appendfilename "appendonly.aof"',
    'appendfsync everysec',
    'no-appendfsync-on-rewrite no',
    'auto-aof-rewrite-percentage 100',
    'auto-aof-rewrite-min-size 64mb',
    'aof-load-truncated yes',
    'aof-use-rdb-preamble no',
    'lua-time-limit 5000',
    'slowlog-max-len 128',
    'latency-monitor-threshold 0',
    'notify-keyspace-events ""',
    'hash-max-ziplist-entries 512',
    'hash-max-ziplist-value 64',
    'list-max-ziplist-size -2',
    'list-compress-depth 0',
    'set-max-intset-entries 512',
    'zset-max-ziplist-entries 128',
    'zset-max-ziplist-value 64',
    'hll-sparse-max-bytes 3000',
    'activerehashing yes',
    'client-output-buffer-limit normal 0 0 0',
    'client-output-buffer-limit slave 256mb 64mb 60',
    'client-output-buffer-limit pubsub 32mb 8mb 60',
    'hz 10',
    'aof-rewrite-incremental-fsync yes',
]


def initdb(datadir, echo=False):
    '''Create redis db config in data dir'''
    redis_config_lines = REDIS_DEFAULTS.copy()
    redis_config_lines.append('port %s' % REDIS_PORT)
    redis_config_lines.append('dir %s' % datadir)
    redis_config_path = '%s/redis.conf' % datadir
    if not os.path.exists(datadir):
        os.makedirs(datadir)
    with open(redis_config_path, '+w') as file_handler:
        file_handler.writelines('\n'.join(redis_config_lines))
    if echo:
        print('Redis Config created: %s' % redis_config_path)
    return redis_config_path

def server_process(redis_config_path, echo=False):
    '''Start redis server'''
    args = [
        os.path.join('redis-server'),
        redis_config_path,
    ]
    if echo:
        print('Starting redis server: %s' % ' '.join(args))
    redis_process = subprocess.Popen(
        args,
        close_fds=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    return redis_process


def _main():
    import atexit
    import shutil
    import tempfile
    datadir = tempfile.mkdtemp()

    def _cleanup():
        shutil.rmtree(datadir)
        print('Redis Cleaned dir: %s' % datadir)

    try:
        print('Starting in dir: %s' % datadir)
        redis_config_dir = initdb(datadir, echo=True)
        redis_process = server_process(redis_config_dir, echo=True)
    except Exception as ecp:  # pylint: disable=broad-except
        _cleanup()
        shutil.rmtree(datadir)
        print('Cleaned dir: %s' % datadir)
        raise ecp

    @atexit.register
    def cleanup_process():  # pylint: disable=unused-variable
        '''System Exit hook to remove db and kill redis server process'''
        try:
            if redis_process.poll() is None:
                redis_process.terminate()
                for line in redis_process.stdout:
                    sys.stdout.write(line.decode('utf-8'))
                redis_process.wait()
        finally:
            _cleanup()

    try:
        break_line = b'Ready to accept connections'
        for line in iter(redis_process.stdout.readline, b''):
            sys.stdout.write(line.decode('utf-8'))
            strip_line = line.strip()
            if strip_line.endswith(break_line) and break_line:
                print('Redis Started. ^C to exit.')
                break_line = None
    except KeyboardInterrupt:
        raise SystemExit(0)


if __name__ == '__main__':
    _main()
