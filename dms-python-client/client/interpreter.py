"""
解析用户单个语句，并调用相应 Thrift 接口。
支持语句：
create table table_name (col1 type1, col2 type2, ...);
create index index_name on table_name (col1, col2, ...);
drop table table_name;
drop index index_name on table_name;
insert into table_name (col1, col2, ...) values (val1, val2, ...);
select * from table_name [where col1 = val1 and col2 = val2 ...];
delete from table_name [where col1 = val1 and col2 = val2 ...];
.quit
"""
import logging

from client.errors import ParseError, SQLError
from client.config import Config
from client.executor import Executor
logging.basicConfig()

config = Config()
TOKENIZER_LOGGER_LEVEL = config.tokenizer_log_level
PARSER_LOGGER_LEVEL = config.parser_log_level


symbols = ['(', ')', ',', ';', '<>', '<=', '>=', '=', '<', '>', '*']
comparison_ops = ['<>', '<=', '>=', '=', '<', '>']

logger = logging.getLogger('tokenizer')
logger.setLevel(TOKENIZER_LOGGER_LEVEL)


def get_token(line: str):
    """
    获取下一个 token。
    :param line: 语句
    :return: (token, rest_of_line)
    """
    line = line.strip()
    if line == "":
        logger.debug('Found empty token')
        return "", ""
    # For symbol tokens
    for symbol in symbols:
        if line.startswith(symbol):
            logger.debug('Found token %s', symbol)
            return symbol, line[len(symbol):]
    # For string tokens
    if line.startswith("'"):
        index = line.find("'", 1)
        while index >= 1 and line[index - 1] == '\\':
            index = line.find("'", index + 1)
        if index >= 1:
            index += 1
            logger.debug('Found token %s', line[:index])
            return line[:index], line[index:]
        else:
            raise ParseError('string literal is unterminated')
    # For other tokens
    token = line.split()[0]
    # Find lowest index of any occurrence of symbols
    min_index = len(token)
    for symbol in symbols:
        index = token.find(symbol)
        if index >= 0:
            min_index = min(index, min_index)
    logger.debug('Found token %s', line[:min_index])
    return line[:min_index], line[min_index:]


class Interpreter:
    def __init__(self, do_exec=True):
        self._clear()
        self.logger = logging.getLogger('parser')
        self.logger.setLevel(PARSER_LOGGER_LEVEL)
        self.executor = None
        self.do_exec = do_exec

    def set_executor(self, executor: Executor):
        self.executor = executor

    def _clear(self):
        self.state = 0
        self.done = False
        self.table_name = ''
        self.stmt = ''

    def _get_next_state(self, allowed, token, do_raise=True):
        for t, s in allowed:
            if isinstance(t, list) or isinstance(t, tuple) or isinstance(t, set):
                for tt in t:
                    if token == tt:
                        return s
            else:
                if token == t:
                    return s
        if do_raise:
            raise ParseError(allowed, token) from None
        else:
            return -1

    def _ensure_for(self, token: str, allowed_type: str):
        if token in symbols:
            raise ParseError(allowed_type, token)
        return token

    def _ensure_empty_token(self, token: str):
        if token:
            raise ParseError('end of line', token)

    def _exec(self, stmt, table_name, op_type, is_create_table=False, is_drop_table=False):
        if not self.do_exec:
            return
        if self.executor:
            self.executor.exec(stmt, table_name, op_type, is_create_table, is_drop_table)
        else:
            print(f'(no executor) {op_type} on {table_name}: {stmt}')

    def parse(self, line):
        """
        解析用户输入的一行语句。如果上一次调用时语句未结束，会和上一句合在一起解析。
        :param line: 语句
        :return: 如果本语句未结束返回False，否则返回(语句, 表名, 状态)
        """
        self.logger.debug('entering parse function')
        if not line:
            # Don't do anything on an empty line
            self.logger.debug('empty line')
            return

        self.stmt += (' ' if self.stmt else '') + line
        try:
            while not self.done:
                token, line = get_token(line)
                self.logger.debug('state %2d, next token %s',
                                  self.state, token)
                # Terminal states
                self.done = True  # Unless wildcard is matched in the next match statement
                match self.state:
                    case 8:
                        # create table
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'create table', True, False)
                    case 16:
                        # create index
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'create index')
                    case 20:
                        # drop table
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'drop table', False, True)
                    case 25:
                        # drop index
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'drop index')
                    case 36:
                        # insert
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'insert')
                    case 46 | 47:
                        # select
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'select')
                    case 55 | 56:
                        # delete
                        self._ensure_empty_token(token)
                        self._exec(self.stmt, self.table_name, 'delete')
                    case _:
                        self.done = False

                if self.done:
                    status = (self.stmt, self.table_name, self.state)
                    self._clear()
                    return status

                if not token:
                    self.logger.debug('no tokens but expecting more')
                    break

                # Non-terminal states
                match self.state:
                    case 0:
                        allowed = [
                            ('create', 1),
                            ('drop', 17),
                            ('insert', 26),
                            ('select', 37),
                            ('delete', 48)
                        ]
                        self.state = self._get_next_state(allowed, token)
                    case 1:
                        allowed = [('table', 2), ('index', 9)]
                        self.state = self._get_next_state(allowed, token)

                    # Create table
                    case 2:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 3
                    case 3:
                        allowed = [('(', 4)]
                        self.state = self._get_next_state(allowed, token)
                    case 4:
                        if token == 'primary':
                            # primary key definition
                            self.state = 61
                        else:
                            # column definition
                            self._ensure_for(token, "column name or 'primary key'")
                            self.state = 5
                    case 5:
                        allowed = [(['int', 'float'], 6), ('char', 57)]
                        self.state = self._get_next_state(allowed, token)
                    case 57:
                        allowed = [('(', 58)]
                        self.state = self._get_next_state(allowed, token)
                    case 58:
                        self.state = 59
                    case 59:
                        allowed = [(')', 6)]
                        self.state = self._get_next_state(allowed, token)
                    case 6:
                        allowed = [(')', 7), (',', 4), ('unique', 60)]
                        self.state = self._get_next_state(allowed, token)
                    case 60:
                        allowed = [(')', 7), (',', 4)]
                        self.state = self._get_next_state(allowed, token)
                    case 61:
                        allowed = [('key', 62)]
                        self.state = self._get_next_state(allowed, token)
                    case 62:
                        allowed = [('(', 63)]
                        self.state = self._get_next_state(allowed, token)
                    case 63:
                        self._ensure_for(token, 'column name')
                        self.state = 64
                    case 64:
                        allowed = [(')', 60)]
                        self.state = self._get_next_state(allowed, token)
                    case 7:
                        allowed = [(';', 8)]
                        self.state = self._get_next_state(allowed, token)

                    # Create index
                    case 9:
                        self._ensure_for(token, 'index name')
                        self.state = 10
                    case 10:
                        allowed = [('on', 11)]
                        self.state = self._get_next_state(allowed, token)
                    case 11:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 12
                    case 12:
                        allowed = [('(', 13)]
                        self.state = self._get_next_state(allowed, token)
                    case 13:
                        self._ensure_for(token, 'column name')
                        self.state = 14
                    case 14:
                        allowed = [(')', 15), (',', 13)]
                        self.state = self._get_next_state(allowed, token)
                    case 15:
                        allowed = [(';', 16)]
                        self.state = self._get_next_state(allowed, token)

                    case 17:
                        allowed = [('table', 18), ('index', 21)]
                        self.state = self._get_next_state(allowed, token)
                    # Drop table
                    case 18:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 19
                    case 19:
                        allowed = [(';', 20)]
                        self.state = self._get_next_state(allowed, token)
                    # Drop index
                    case 21:
                        self._ensure_for(token, 'index name')
                        self.state = 22
                    case 22:
                        allowed = [('on', 23)]
                        self.state = self._get_next_state(allowed, token)
                    case 23:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 24
                    case 24:
                        allowed = [(';', 25)]
                        self.state = self._get_next_state(allowed, token)

                    # insert
                    case 26:
                        allowed = [('into', 27)]
                        self.state = self._get_next_state(allowed, token)
                    case 27:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 28
                    case 28:
                        allowed = [('values', 32)]
                        self.state = self._get_next_state(allowed, token)
                    case 32:
                        allowed = [('(', 33)]
                        self.state = self._get_next_state(allowed, token)
                    case 33:
                        self._ensure_for(token, 'value')
                        self.state = 34
                    case 34:
                        allowed = [(',', 33), (')', 35)]
                        self.state = self._get_next_state(allowed, token)
                    case 35:
                        allowed = [(';', 36)]
                        self.state = self._get_next_state(allowed, token)

                    # Select
                    case 37:
                        allowed_partial = [('*', 39)]
                        state = self._get_next_state(
                            allowed_partial, token, False)
                        if state >= 0:
                            self.state = state
                        else:
                            self._ensure_for(token, "'*' or column name")
                            self.state = 38
                    case 38:
                        allowed = [(',', 29), ('from', 40)]
                        self.state = self._get_next_state(allowed, token)
                    case 29:
                        self._ensure_for(token, 'column name')
                        self.state = 38
                    case 39:
                        allowed = [('from', 40)]
                        self.state = self._get_next_state(allowed, token)
                    case 40:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 41
                    case 41:
                        allowed = [('where', 42), (';', 47)]
                        self.state = self._get_next_state(allowed, token)
                    case 42:
                        self._ensure_for(token, 'column name')
                        self.state = 43
                    case 43:
                        allowed = [(comparison_ops, 44)]
                        self.state = self._get_next_state(allowed, token)
                    case 44:
                        self._ensure_for(token, 'value')
                        self.state = 45
                    case 45:
                        allowed = [('and', 42), (';', 46)]
                        self.state = self._get_next_state(allowed, token)

                    # Delete
                    case 48:
                        allowed = [('from', 49)]
                        self.state = self._get_next_state(allowed, token)
                    case 49:
                        self.table_name = self._ensure_for(token, 'table name')
                        self.state = 50
                    case 50:
                        allowed = [('where', 51), (';', 56)]
                        self.state = self._get_next_state(allowed, token)
                    case 51:
                        self._ensure_for(token, 'column name')
                        self.state = 52
                    case 52:
                        allowed = [(comparison_ops, 53)]
                        self.state = self._get_next_state(allowed, token)
                    case 53:
                        self._ensure_for(token, 'value')
                        self.state = 54
                    case 54:
                        allowed = [(';', 55), ('and', 51)]
                        self.state = self._get_next_state(allowed, token)

        except SQLError as e:
            self._clear()
            raise e
        return False
