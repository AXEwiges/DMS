class SQLError(ValueError):
    """General exception class for handling SQL errors."""
    pass


class ParseError(SQLError):
    """Indicate that an error occurs during SQL statement parsing.
    """

    def __init__(self, expected, found: str = None):
        """Indicate that an error occurs during SQL statement parsing.
        """
        if found is None:
            super(ParseError, self).__init__(expected)
        else:
            if isinstance(expected, list) and isinstance(expected[0], tuple):
                expected = [t for (t, _) in expected]
            if isinstance(expected, list):
                if len(expected) > 1:
                    expected = 'one of ' + str(expected)
                else:
                    expected = repr(expected[0])
            super(ParseError, self).__init__(
                f'expected {expected}, found {repr(found)}')

    def __str__(self):
        return "Parse error: " + super(ParseError, self).__str__()


class ExecutionError(SQLError):
    """Indicate that an error occurs during SQL statement execution.
    """

    def __str__(self):
        return "Execution error: " + super(ExecutionError, self).__str__()


class SQLConnectionError(ExecutionError):
    """Indicate that the query failed because the server cannot be connected.
    """
    pass


class TableNotFoundError(ExecutionError):
    """Indicate that no region server holding the table can be found.
    """
    pass
