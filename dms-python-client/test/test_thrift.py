from threading import Thread
from test.test_server.master import MasterThriftServer
from test.test_server.region import RegionThriftServer


master = MasterThriftServer('127.0.0.1', 9090)
regions = [RegionThriftServer('127.0.0.1', port) for port in range(9200, 9206)]

t = Thread(target=master.serve)
t.start()

for region in regions:
    t = Thread(target=region.serve)
    t.start()
