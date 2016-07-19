from kazoo.client import KazooClient

host = 'itrac1512.cern.ch:2181'

root = '/'

zk = KazooClient(hosts=host)
zk.start()

for child in zk.get_children(root):
    if(child != "zookeeper"):
        child_path = root + child
        print('Deleting ' + child_path)
        zk.delete(path=child_path, recursive=True)

zk.stop()
