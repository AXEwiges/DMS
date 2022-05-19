from concurrent.futures import ThreadPoolExecutor, as_completed
from io import TextIOWrapper
import logging
import threading
from typing import Any
from thrift.Thrift import TException
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol

from client.config import Config
from client.errors import ExecutionError, SQLConnectionError, TableNotFoundError
from client.tools import output
from thrift_gen.clientInfo.ttypes import ClientInfo
from thrift_gen.master import Master
from random import choice

from thrift_gen.region import Region
from thrift_gen.region.ttypes import execResult

config = Config()

class Executor:
    def __init__(self, host: str = None, port: int = None, file: TextIOWrapper | None = None):
        self.logger = logging.getLogger('executor')
        self.logger.setLevel(config.executor_log_level)
        self.file = file

        if host and port:
            # (host, port): (client, transport, client_class)
            self.clients: dict[tuple[str, int], tuple] = dict()
            self.clients_lock = threading.Lock()
            self.master, self.m_transport = self._get_client(Master.Client, host, port)
            self.table_cache: dict[str, list[tuple[str, int]]] = {}  # table_name: [(host, port)]
        else:
            # This is a toy executor, which simply prints out the statements.
            pass

    def _get_client(self, client_class, host: str, port: int):
        """
        Get the client and transport objects according to host and port, creating
        a new one if needed.

        Args:
            client_class: Thrift Client class used to create a connection.
            host: hostname of the client
            port: port of the client

        Return:
            client, transport
        """
        self.clients_lock.acquire()
        client, transport, cc = self.clients.get((host, port), (None, None, None))
        if (not client) or cc != client_class:
            if client:
                self.logger.warning('Overwriting previous client with the same address but for a different class.')
            # Create a client
            transport = TSocket.TSocket(host, port)
            transport = TTransport.TBufferedTransport(transport)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = client_class(protocol)
            self.clients[(host, port)] = (client, transport, client_class)
        self.clients_lock.release()
        return client, transport

    def _exec(self, stmt: str, table_name: str, host: str, port: int):
        """
        Execute a single statement in a single region server.
        """
        rs, rs_transport = self._get_client(Region.Client, host, port)
        try:
            rs_transport.open()
            result: execResult = rs.statementExec(stmt, table_name)
            if result.status == 1:
                # Success
                return result.result
            elif result.status == 0:
                # Error
                raise ExecutionError(result.result)
            else:
                raise ExecutionError(f'unknown status {result.status}. Result: {result.result}')
        except TTransport.TTransportException:
            raise SQLConnectionError('cannot connect to the server.')
        except TException as e:
            raise ExecutionError(f'from region server {host}:{port}: {e}')
        finally:
            rs_transport.close()

    def _update_table_cache_for(self, table_name: str, is_create_table=False, is_drop_table=False):
        """
        Query for region server list.

        Assume that the master returns [] if no region server for table_name can be found.
        """
        try:
            self.m_transport.open()
            result: list[ClientInfo] = self.master.getRegionsOfTable(table_name, is_create_table, is_drop_table)
            self.m_transport.close()
        except TException:
            raise SQLConnectionError('cannot connect to the server.')
        # Update table cache
        self.table_cache[table_name] = [(info.ip, info.rpcPort) for info in result]
        self.logger.debug(f'Table cache for {table_name} updated: {self.table_cache[table_name]}')
        if not self.table_cache[table_name]:
            del self.table_cache[table_name]
            raise TableNotFoundError(f'Cannot find any region server that holds the table {table_name}')

    def exec(self, stmt: str, table_name: str, op_name: str, is_create_table=False, is_drop_table=False):
        if not hasattr(self, 'master'):
            # This is a toy executor
            output(self.file, f"(master address unset) {op_name} on {table_name}: {stmt}")
            return

        # A very nasty workaround: because the SQL backend doesn't allow the 'on table_name' part of
        # 'drop index index_name on table_name', we manually strip this part off.
        if op_name == 'drop index':
            stmt = " ".join(stmt.split()[:3]) + ";"

        self.logger.debug(f'{op_name} on {table_name}: {stmt}')

        if is_create_table or is_drop_table or table_name not in self.table_cache.keys():
            try:
                self._update_table_cache_for(table_name, is_create_table, is_drop_table)
            except TableNotFoundError as e:
                if op_name == 'create table':
                    # Assume that the master returns [] if the table to be created already exists.
                    raise TableNotFoundError(f'{table_name} is already created.') from None
                else:
                    raise e

        if op_name == 'select':
            # Simple look-up, execute on one region server
            tried = False
            while True:
                try:
                    host, port = choice(self.table_cache[table_name])
                    output(self.file, self._exec(stmt, table_name, host, port))
                    break
                except ExecutionError as e:
                    if tried:
                        break
                    tried = True
                    output(self.file, e)
                    self._update_table_cache_for(table_name)
        else:
            # Mutable operation, perform changes on all region servers
            hp_to_result: dict[tuple[str, int], Any] = {}
            while True:
                has_error = False
                regions = set(self.table_cache[table_name]) - hp_to_result.keys()
                if not regions:
                    break
                self.logger.debug(f'Connecting all region servers...')
                with ThreadPoolExecutor() as t_executor:
                    future_to_hp = {t_executor.submit(self._exec, stmt, table_name, hp[0], hp[1]): hp for hp in regions}
                    for future in as_completed(future_to_hp):
                        hp = future_to_hp[future]
                        try:
                            hp_to_result[hp] = future.result()
                            self.logger.debug(f'{hp[0]}:{hp[1]} replied: {hp_to_result[hp]}')
                        except ExecutionError as e:
                            self.logger.debug(f'error from {hp[0]}:{hp[1]}: {e}')
                            hp_to_result[hp] = e
                            has_error = True
                if has_error:
                    self._update_table_cache_for(table_name)
                    # Delete results from the region servers that no longer hold the table, since
                    # these results probably came from a deleted table with the same table name
                    for hp in hp_to_result.copy():
                        if hp not in self.table_cache[table_name]:
                            del hp_to_result[hp]
            output(self.file, next(x for x in hp_to_result.values()))

        if is_drop_table:
            del self.table_cache[table_name]
