from kazoo.client import KazooClient

host = 'localhost:2181'

root = '/'

zk = KazooClient(hosts=host)
zk.start()

for child in zk.get_children(root):
    child_path = root + child
    print('Deleting ' + child_path)
    zk.delete(child_path)

zk.stop()