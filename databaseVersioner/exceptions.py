

class FileException(Exception):
    def __init__(self,message):
        self.message = message

class DatabaseException(Exception):
    def __init__(self,message):
        self.message = message

class TerminalException(Exception):
    def __init__(self,message):
        self.message = message

class MysqlExcepton(Exception):
    def __init__(self,message):
        self.message = message

class LogFileException(Exception):
    def __init__(self,message):
        self.message = message

class BinnaryLogException(Exception):
    def __init__(self,message):
        self.message = message