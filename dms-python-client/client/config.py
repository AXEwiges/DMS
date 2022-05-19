import logging


class Config:
    def __init__(self):
        self.master_host = '127.0.0.1'
        self.master_port = 9090
        self.executor_log_level = logging.WARNING
        self.tokenizer_log_level = logging.WARNING
        self.parser_log_level = logging.WARNING
