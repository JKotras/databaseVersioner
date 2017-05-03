import config
from databaseVersioner import terminal
from databaseVersioner import utils
from databaseVersioner.database import Database
from databaseVersioner.terminal import TerminalCommand


class DatabaseDump:
    def __init__(self, databaseName, dbConnection: Database):
        self.__command = config.config['mysqldumpLocation']
        self.__databaseName = databaseName
        self.__db = dbConnection

    def dumpTables(self, tables: dict):
        """
        Dump tables and views from database
        :param tables: list[VersionedTable] list of VersionedTables
        :return: dict of key- table name, valua - table/view dump
        """
        tablesDump = {}
        for table in tables.values():
            tableName = table.getValue('name');
            params = [self.__databaseName, "--user=" + self.__db.user, "--password=" + self.__db.password, "--triggers", "--skip-add-locks", "--skip-comments", "--force",
                      "--add-drop-table", "--add-drop-trigger", tableName]
            dump = TerminalCommand.runCommandRetStr(self.__command, params)
            #dump = utils.removeEmptyLinesFromSql(dump)
            tablesDump[tableName] = dump

        return tablesDump

    def dumpDatabase(self):
        """
        Return dump of all database in one string
        :return: str
        """
        params = [self.__databaseName, "--user=" + self.__db.user, "--password=" + self.__db.password, "--triggers", "--skip-add-locks", "--skip-comments", "--force"]
        dump = TerminalCommand.runCommandRetStr(self.__command, params)
        #dump = utils.removeEmptyLinesFromSql(dump)

        return dump

