import os
import subprocess

import sys

from config import *
from databaseVersioner.database import Database
from databaseVersioner.exceptions import FileException, BinnaryLogException
from databaseVersioner.terminal import TerminalCommand


class BinnaryLog:
    """
    One binnary log file

    TODO: zatim to tak necham ale bylo by zve vhodne zaslenit binnarylogparser
    """
    def __init__(self, fileName :str):
        """
        :param fileName: Name of binnary log
        """
        self.__fileDir = config.get('binnaryLogsDir')
        self.__fileName = fileName
        self.__filePath = self.__fileDir + self.__fileName
        self.__command = config.get('mysqlbinlogLocation')
        self.__startPosition = None
        self.__databaseName = None

        if(os.path.isfile(self.__filePath) is False):
            raise FileException("File '" + self.__filePath + "' do not exist")

        if not os.access(self.__filePath, os.R_OK):
            raise BinnaryLogException("Permissinon denied to read file \'"+self.__filePath+"\'")

    def setStartPosition(self, position :int):
        """
        Set start position of parsing in binnary log
        :param position: integer of position
        """
        self.__startPosition = position

    def setDatabaseName(self, dbName :str):
        """
        Set database name for which you want to logs
        :param dbName: name of database
        """
        self.__databaseName = dbName

    def getFileName(self):
        """
        Return filename of binnarylog
        :return: str
        """
        return self.__fileName

    def getLogContent(self):
        """
        Get content of binnary log
        :return: string of conten
        """
        params = []
        if self.__startPosition is not None and self.__startPosition != 0:
            params.append("--start-position=" + str(self.__startPosition))
        if self.__databaseName is not None:
            params.append("--database=" + self.__databaseName)
        else:
            raise BinnaryLogException('Database name have to be set')

        params.append(self.__filePath)
        data = TerminalCommand.runCommandRetStr(self.__command, params)

        return data




class BinnaryLogs:
    """
    Class work with binnary logs
    :type __logs: list[BinnaryLog] list of binnary logs
    """
    def __init__(self, logFileNames = [], databaseName = None):
        """
        Constructor
        :param logFileNames: list of file Name of binnary logs
        :param databaseName: str Name of database for data log
        """
        self.__logs = [];
        if databaseName is None:
            raise Exception("Database name is none")
        for name in logFileNames:
            binnaryLog = BinnaryLog(name)
            binnaryLog.setDatabaseName(databaseName)
            self.__logs.append(binnaryLog)

    def setStartPositionFirstLog(self, position: int):
        """
        Set start position of parsing in first log
        :param position:
        :return:
        """
        self.__logs[0].setStartPosition(position)

    def getAllLogData(self):
        """
        Return string of all log data
        :return: str
        """
        string = ""
        for log in self.__logs:
            string += log.getLogContent()
        return string





class BinnaryLogNames:
    """
    Class with simple method to work with binnary log names
    """
    def __init__(self,dbConnection: Database):
        """
        :param dbConnection:
        """
        self._db = dbConnection


    def getAllBinnaryLogsNames(self):
        """
        Return all binnary logs names
        :return: tuple
        """
        data = self._db.executeSimpleSQL("SHOW BINARY LOGS")
        myData = tuple(v['Log_name'] for v in data)
        return myData

    def getBinnaryLogsNamesOlderThan(self, logName: str):
        """
        Get all binnary logs older than binnary log with name 'logName'

        on error return empty list
        :param logName: binnry log name
        :return: list
        """
        allLogs = self.getAllBinnaryLogsNames()
        if not allLogs:
            return []
        index = 0
        if logName in allLogs:
            index = allLogs.index(logName)
        data = allLogs[index:]
        return data

    def getLastBinnaryLogName(self):
        """
        Get Last binnary log name

        If not binnery log set then return epty string
        :return: str: last binnarylog name
        """
        names = self.getAllBinnaryLogsNames()
        if not names:
            return ''
        return names[-1]


