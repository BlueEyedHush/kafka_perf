import logging
import time

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

zk_host='188.184.165.208:2181'
zk_test_sync_znode = '/kafka_perf/test_status'

def main():
    test_time = 120.0 # in seconds

    zk = KazooClient(hosts=zk_host)
    zk.start()

    try:
        zk.create(path=zk_test_sync_znode, makepath=True)
    except NodeExistsError:
        logging.info('{} already exists, no need to create'.format(zk_test_sync_znode))

    zk.set(zk_test_sync_znode, 'start')

    t_start = time.time() # in seconds
    t_end = t_start + test_time

    while(time.time() < t_end):
        time.sleep(t_end - time.time()) # shouldn't introduce error larger than 10-15 ms

    zk.set(zk_test_sync_znode, 'stop')

if __name__ == "__main__":
    main()