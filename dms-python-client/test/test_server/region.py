from random import sample
from client.interpreter import Interpreter

from thrift_gen.clientInfo.ttypes import ClientInfo
from thrift_gen.region import Region
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from thrift_gen.region.ttypes import execResult


class RegionHandler:
    def __init__(self, host, port):
        self.tables = set()
        self.host = host
        self.port = port

    def statementExec(self, cmd, tableName):
        interpreter = Interpreter(do_exec=False)
        result = execResult()
        cmd = ' '.join(cmd.strip().split())

        if cmd.startswith('drop index '):
            # Imitate the behaviour of our SQL backend that does not recognize 'on table_name'
            # in drop index stmt
            result.status = 1
            result.result = f'executed {cmd}'
            return result

        status = interpreter.parse(cmd)
        if not status:
            result.status = 0
            result.result = 'parse error'
        else:
            stmt, table_name, state = status

            if state != 8 and (table_name not in self.tables):
                # Check if table exists if other than 'create table'
                result.status = 0
                result.result = f'table {table_name} not found'
            elif state == 8:
                if table_name in self.tables:
                    result.status = 0
                    result.result = f'table {table_name} already found'
                else:
                    self.tables.add(table_name)
                    result.status = 1
                    result.result = f'executed {stmt}'
            elif state == 20:
                self.tables.remove(table_name)
                result.status = 1
                result.result = f'executed {stmt}'
            else:
                result.status = 1
                result.result = f'executed {stmt}'
        return result


class RegionThriftServer:
    def __init__(self, host: str, port: int):
        handler = RegionHandler(host, port)
        processor = Region.Processor(handler)
        transport = TSocket.TServerSocket(host=host, port=port)
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()

        self._server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    def serve(self):
        self._server.serve()
