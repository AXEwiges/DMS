from random import sample

from thrift_gen.clientInfo.ttypes import ClientInfo
from thrift_gen.master import Master
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


class MasterHandler:
    def __init__(self):
        self.regions = [ClientInfo('127.0.0.1', port, 0, 0) for port in range(9200, 9206)]
        self.tables = {}

    def getRegionsOfTable(self, tableName, isCreate, isDrop):
        print(f'getRegionsOfTable({tableName}, {isCreate}, {isDrop})')
        if isCreate:
            if tableName in self.tables.keys():
                return []
            self.tables[tableName] = sample(self.regions, k=3)
        regions = self.tables.get(tableName, [])
        if regions and isDrop:
            del self.tables[tableName]
        return regions

    def finishCopyTable(self, tableName):
        print(f'finishCopyTable({tableName})')


class MasterThriftServer:
    def __init__(self, host: str, port: int):
        handler = MasterHandler()
        processor = Master.Processor(handler)
        transport = TSocket.TServerSocket(host=host, port=port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        self._server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    def serve(self):
        self._server.serve()
