import argparse
import logging
import signal
import sys
import time
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

zk_host='itrac1511:2181'
test_znode = '/kafka_perf_test'

def main(args):
    zk = KazooClient(hosts=zk_host)
    zk.retry(lambda: zk.start())

    register_emergency_signal_handler(zk)

    try:
        zk.retry(lambda: zk.create(path=test_znode, makepath=True))
    except NodeExistsError:
        logging.info('{} already exists, no need to create'.format(test_znode))

    start_command = 'start({},{},{})'.format(args.message_size, args.topics, args.partitions)
    zk.retry(lambda: zk.set(test_znode, start_command))

    t_start = time.time() # in seconds
    t_end = t_start + args.duration

    while(time.time() < t_end):
        time.sleep(t_end - time.time()) # shouldn't introduce error larger than 10-15 ms

    zk.retry(lambda: zk.set(test_znode, 'stop'))
    zk.stop()

def get_cli_arguments():
    parser = argparse.ArgumentParser(description='Synchronize performance tests across nodes.')

    parser.add_argument('-d', dest='duration', action='store', default=60.0, required=False, type=float,
                        help='duration of the test (in seconds)')
    parser.add_argument('-s', dest='message_size', action='store', default=500, required=False, type=int,
                        help='size of messages sent to the broker (in bytes)')
    parser.add_argument('-t', dest='topics', action='store', default=1, required=False, type=int,
                        help='number of topics to which messages will be sent')
    parser.add_argument('-p', dest='partitions', action='store', default=2, required=False, type=int,
                        help='number of partitions in each topic')

    return parser.parse_args()

def create_znodes(zk, path_list):
    for path in path_list:
        try:
            zk.retry(lambda: zk.create(path=path, makepath=True))
        except NodeExistsError:
            logging.info('{} already exists, no need to create'.format(test_znode))

def register_emergency_signal_handler(zk):
    def signal_handler(signal, frame):
        zk.retry(lambda: zk.set(test_znode, 'stop'))
        zk.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    args = get_cli_arguments()
    main(args)