from kazoo.client import KazooClient

host = 'itrac1511.cern.ch:2181'

root = '/'

zk = KazooClient(hosts=host)
zk.start()

root_children = zk.retry(lambda: zk.get_children(root))

for child in root_children:
    if(child != "zookeeper" and child != "kafka_perf_test"):
        child_path = root + child
        print('Deleting ' + child_path)
        zk.retry(lambda: zk.delete(path=child_path, recursive=True))

zk.stop()
