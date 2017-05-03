import os

import sys
import traceback

import time

import config
from databaseVersioner import database
from databaseVersioner.binnaryLog import BinnaryLogNames, BinnaryLogs, BinnaryLog
from databaseVersioner.binnaryLogParser import BinnaryLogParser
from databaseVersioner.database import Database, VersionerDatabase, VersionedDatabase
from databaseVersioner.databaseDump import DatabaseDump
from databaseVersioner.exceptions import DatabaseException, FileException, BinnaryLogException, LogFileException
from databaseVersioner.logFiles import LogFile, DumpFile, LogFileBasic
from databaseVersioner.terminal import TerminalCommand
from databaseVersioner.utils import isDirectoryExist, isFileExist, getDirectoryPathFromFilename
from terminaltables import AsciiTable

class Application:

    def __init__(self):
        self.dbConnection = Database(Config.getConfig('databaseHost'),
                                     Config.getConfig('databasePort'),
                                     Config.getConfig("databaseUser"),
                                     Config.getConfig("databasePassword"),
                                     Config.getConfig('databaseName'),
                                     )

        self.verDatabaseConnection = None

    def processInit(self):
        """
        Init the databaseVersioner
        :return:
        """

        #check if project has been init
        check = True
        for table in VersionerDatabase.DATABASE_TABLES:
            if not self.dbConnection.isTableExist(table):
                check = False
        if check:
            response = TerminalCommand.runDialogYorN('Project is initialized. Are you want to init project again? You will lost data !!')
            if response == False:
                return

        #delete od elements
        toDeletes = self.dbConnection.executeSimpleSQL('SHOW TABLES')
        for toDelete in toDeletes:
            try:
                toDelete = list(toDelete.values())
                for item in toDelete:
                    self.dbConnection.deleteElementbyName(item)
            except:
                #dont matter
                pass

        #make new db structure
        self.dbConnection.executeSimpleSQL(VersionerDatabase.DATABASE_STRUCTURE)


    def processAddNonExist(self, dbName, destFolder):
        """
        Add database to database versioner and made dump this database
        :param dbName: str database name
        :param destFolder:
        """
        destFolder = os.path.abspath(destFolder)
        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")
        #check if dababase has been versioned
        isExist = database.VersionedDatabase.isDatabaseVersioned(self.dbConnection, dbName)
        if isExist:
            result = TerminalCommand.runDialogYorN("Database '"+dbName+"' has been versioned. Are you want redeclare it?")
            if not result:
                return

        # get last binary log and last position
        binnaryLogNames = BinnaryLogNames(self.dbConnection)
        lastBinnaryLogName = binnaryLogNames.getLastBinnaryLogName()
        binnarylogs = BinnaryLogs([lastBinnaryLogName], dbName)
        logDataString = binnarylogs.getAllLogData()
        binnlogParser = BinnaryLogParser(logDataString)
        lastBinnaryLogPosition = binnlogParser.getLastPosition()


        #make and set new database
        newVersion = 1
        versionedDatabaseRecord = database.VersionedDatabase.addVersionerDatabase(self.dbConnection,dbName, destFolder, lastBinnaryLogName, lastBinnaryLogPosition, newVersion)

        # clear and dump new database into logfiles
        #todo: dump everything not only tables
        LogFileBasic.removeAllFilesInDirectory(destFolder)
        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName)
        dump = DatabaseDump(dbName,self.verDatabaseConnection)
        tablesDump =dump.dumpTables(versionedDatabaseRecord.getAllTables())
        for tableName in tablesDump.keys():
            print("Process : " + tableName)
            logFileName = LogFile.makeLogFilePath(destFolder, tableName)
            logFile = LogFile(logFileName)
            logFile.insertVersionIntoFile([tablesDump.get(tableName)], newVersion)

    def processAddExists(self, dbName, sourceFolder):
        """
        Import exist versioned logs into local database
        :param dbName:
        :param sourceFolder: str path to folder where is logs
        :return:
        """
        destFolder = os.path.abspath(sourceFolder)
        if self.dbConnection.isDatabaseExist(dbName) is False:
            raise DatabaseException('Database \'' + dbName + '\' is not exist. You have to make it')
        if not isDirectoryExist(destFolder):
            raise FileException("Directory '" + destFolder + "' is not exist")

        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName)

        # check if dababase has been versioned
        isExist = database.VersionedDatabase.isDatabaseVersioned(self.dbConnection, dbName)
        if isExist:
            result = TerminalCommand.runDialogYorN(
                "Database '" + dbName + "' has been versioned. Are you want redeclare it?")
            if not result:
                return

        # get last binary log and last position
        binnaryLogNames = BinnaryLogNames(self.dbConnection)
        lastBinnaryLogName = binnaryLogNames.getLastBinnaryLogName()
        binnarylogs = BinnaryLogs([lastBinnaryLogName], dbName)
        logDataString = binnarylogs.getAllLogData()
        binnlogParser = BinnaryLogParser(logDataString)
        lastBinnaryLogPosition = binnlogParser.getLastPosition()

        #make dump of database
        dbDump = DatabaseDump(dbName, self.verDatabaseConnection)
        dump = dbDump.dumpDatabase()

        #cleart db
        self.verDatabaseConnection.clearDatabase()

        #make and import database
        logFilesNames = LogFile.getAllVerFileNameInDirectory(destFolder)
        lastVersion = 0
        otherSql = ""
        for fileName in logFilesNames.keys():
            print ("Process : " + logFilesNames[fileName])
            logFile = LogFile(LogFile.makeLogFilePath(destFolder, logFilesNames[fileName]))
            if logFile.getLastVersion() > lastVersion:
                  lastVersion = logFile.getLastVersion()

            if LogFile.OTHER_LOG_NAME_FULL in fileName:
                otherSql = logFile.getAllSql()
            else:
                #import data into database
                try:
                    allSql = logFile.getAllSql()
                    if allSql:
                        self.verDatabaseConnection.executeSimpleSQL(allSql)
                except Exception as e:
                    #special transtaction for ddl
                    self.verDatabaseConnection.clearDatabase()
                    self.verDatabaseConnection.executeSimpleSQL(dump)
                    raise


        #do OtherSql
        try:
            self.verDatabaseConnection.executeSimpleSQL(otherSql)
        except Exception as e:
            self.verDatabaseConnection.clearDatabase()
            self.verDatabaseConnection.executeSimpleSQL(dump)
            raise e

        #set database to versioning
        versionedDatabaseRecord = database.VersionedDatabase.addVersionerDatabase(self.dbConnection, dbName, destFolder,lastBinnaryLogName, lastBinnaryLogPosition, lastVersion)

    def processSnapshot(self, dbName, destination):
        """
        Dump all database into one file
        :param dbName: name of dumped file
        :param destination: path to save file
        """
        destination = os.path.abspath(destination)
        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")

        isExist = isFileExist(destination)
        if isExist:
            result = TerminalCommand.runDialogYorN('File ' + destination + ' is exist. Do you want rewrite it')
            if not result:
                return

        #get dump of all db
        dump = DatabaseDump(dbName, self.dbConnection)
        allSql = dump.dumpDatabase()

        #get dir path and check path
        dirPath = getDirectoryPathFromFilename(destination)
        #write into file
        dumpFile = DumpFile(destination)
        dumpFile.writeIntoFile(allSql)

    def processUp(self, dbName):
        """
        Make new database revision
        :return:
        """

        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")
        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName
                                              )

        versionedDatabaseRecord = database.VersionedDatabase(self.dbConnection,dbName=dbName)
        binnaryLogNames = BinnaryLogNames(self.dbConnection)
        listOfBinneryLogNames = binnaryLogNames.getBinnaryLogsNamesOlderThan(versionedDatabaseRecord.getLogFile())
        lastBinnaryLogName = binnaryLogNames.getLastBinnaryLogName()
        lastBinnaryLogPosition = versionedDatabaseRecord.getLogFilePosition()


        #check if first log name have name like versionedDatabaseRecord bin name
        if versionedDatabaseRecord.getLogFile() not in listOfBinneryLogNames:
            lastBinnaryLogPosition = 0
            print('Warning: Binnary Logs is out of date. Prease increase expire_logs_days')
            res = TerminalCommand.runDialogYorN('All data for version is unavailable. Some data in new version can be missing. Do you want continue')
            if not res:
                return


        #check if version in logFiles is same in db
        destination = versionedDatabaseRecord.getDestinationFolder()
        actualVersion = versionedDatabaseRecord.getActualVersion()
        fileNames = LogFile.getAllVerFileNameInDirectory(destination)
        for name in fileNames:
            logFile = LogFile(LogFile.makeLogFilePath(destination, fileNames[name]))
            fileVersion = logFile.getLastVersion()
            if fileVersion > actualVersion:
                raise LogFileException('Versined files in directory \''+destination+"' have newer version then data in database. Please import data into datababe before make new version")


        #get data from logs
        binnarylogs = BinnaryLogs(listOfBinneryLogNames, dbName)
        binnarylogs.setStartPositionFirstLog(lastBinnaryLogPosition)
        logDataString = binnarylogs.getAllLogData()

        #parse data
        binnlogParser = BinnaryLogParser(logDataString)
        lastBinnaryLogPosition = binnlogParser.getLastPosition()

        #increment actual version - dont save
        actualVersion = versionedDatabaseRecord.getActualVersion()
        actualVersion = actualVersion+1

        #flush sql into log file
        result = binnlogParser.getLogParserResult()

        #save to make transaction
        logFileData = {}
        toDelete = []
        try:
            #process
            for elementName in result.getAllUpdatedElementNames():
                elementData = versionedDatabaseRecord.getTableByName(elementName)
                logFileName = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), elementName)

                if elementName in result.getCreated():
                    logFile = LogFile(logFileName)
                elif result.getRenameOldNameByNewName(elementName) is not None:
                    oldName = result.getRenameOldNameByNewName(elementName)
                    oldLogFileName = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), oldName)
                    oldLogFile = LogFile(oldLogFileName)
                    logFileData[oldLogFileName] = oldLogFile.getAllContent()
                    logFile = LogFile(logFileName)
                    toDelete.append(logFileName)
                    logFile.writeIntoFile(oldLogFile.getAllContent())
                    oldLogFile.deleteFile()
                elif versionedDatabaseRecord.getTableByName(elementName):
                    logFile = LogFile(logFileName)
                    logFileData[logFileName] = logFile.getAllContent()
                else:
                    #someting wrong
                    continue

                logFile.appendVersionIntoFile(result.getAllQueriesByName(elementName), actualVersion)

           #do others
            if result.getOtherQueries():
                logFileName = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), LogFile.OTHER_LOG_NAME)
                logFile = LogFile(logFileName)
                logFileData[logFileName] = logFile.getAllContent()
                logFile.appendVersionIntoFile(result.getOtherQueries(), actualVersion)


            #save metadata
            if(result.haveSomeData()):
                versionedDatabaseRecord.setSomeData(lastBinnaryLogName, lastBinnaryLogPosition, actualVersion)
                versionedDatabaseRecord.updateMetaDataAboutTables()
            else:
                versionedDatabaseRecord.setSomeData(lastBinnaryLogName, lastBinnaryLogPosition)
                versionedDatabaseRecord.updateMetaDataAboutTables()
                print("No data to flush into log files. New version was not make")

        except Exception as e:
            #make transaction
            for fileName in toDelete:
                logFile = LogFile(fileName)
                logFile.deleteFile()
            for fileName in logFileData.keys():
                logFile = LogFile(fileName)
                logFile.writeIntoFile(logFileData[fileName])
            raise

    def processImport(self, dbName):
        """
        Import logs into database
        Local adjustments stay - dont delete it
        :param dbName: name of imported database
        """
        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")

        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName
                                              )

        versionedDatabaseRecord = database.VersionedDatabase(self.dbConnection, dbName=dbName)

        #import slq data
        files = LogFile.getAllVerFileNameInDirectory(versionedDatabaseRecord.getDestinationFolder())
        newLastVersion = 0;
        for fileName in files.keys():
            elementName = files[fileName]
            filePath = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), elementName)
            logFile = LogFile(filePath)

            if fileName == LogFile.OTHER_LOG_NAME_FULL:
                #this is other file
                newSql = logFile.getSqlFromVersion(versionedDatabaseRecord.getActualVersion()+1)
                self.verDatabaseConnection.executeSimpleSQL(newSql)
                continue

            versionedTable = versionedDatabaseRecord.getTableByName(elementName)
            if versionedTable is None:
                #this is new tables
                newSql = logFile.getAllSql()
                if not newSql:
                    continue
                try:
                    self.verDatabaseConnection.executeSimpleSQL(newSql)
                    versionedDatabaseRecord.addTable(elementName, logFile.getLastVersion())
                except Exception as e:
                    self.verDatabaseConnection.deleteElementbyName(elementName)
                    versionedDatabaseRecord.removeTable(elementName)
                    raise
            else:
                #this element has been already in database
                newSql = logFile.getSqlFromVersion(versionedDatabaseRecord.getActualVersion()+1)
                try:
                    self.verDatabaseConnection.executeSimpleSQL(newSql)
                except Exception as e:
                    for fileName in files.keys():
                        elementName = files[fileName]
                        self.__revertByElementName(elementName, versionedDatabaseRecord)
                    raise

            if logFile.getLastVersion() > newLastVersion:
                newLastVersion = logFile.getLastVersion()

        #find last version
        newVersion = versionedDatabaseRecord.getActualVersion()
        if newLastVersion > versionedDatabaseRecord.getActualVersion():
            newVersion = newLastVersion

        # save metadata
        #save only new version
        versionedDatabaseRecord.setSomeData(version = newVersion)

    def processMerge(self, dbName, fromVersion):
        """
        Merge in log files
        Merge fromVersion to lastest version into lastest version in log files
        :param dbName: str name of db
        :param fromVersion: str from version you want to merger
        :return:
        """
        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")
        try:
            fromVersion = int(fromVersion)
        except ValueError:
            raise Exception('Version is not a integer')
        if fromVersion <= 0:
            raise LogFileException("Version number can not be less than zero")
        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName
                                              )

        versionedDatabaseRecord = database.VersionedDatabase(self.dbConnection, dbName=dbName)

        if versionedDatabaseRecord.getActualVersion() < fromVersion:
            pass
            #raise LogFileException("Version number can not be bigger than actual version of database")

        files = LogFile.getAllVerFileNameInDirectory(versionedDatabaseRecord.getDestinationFolder())

        #proces for every versed files
        for fileName in files.keys():
            elementName = files[fileName]
            filePath = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), elementName)
            logFile = LogFile(filePath)

            logFile.mergeFromVersion(fromVersion)

    def processForceSet(self, dbName):
        """
        Cleat database and Import everything in log files into database
        Edited data will be overwrite
        :param dbName: name of force set database
        :return:
        """
        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")
        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName
                                              )
        versionedDatabaseRecord = database.VersionedDatabase(self.dbConnection, dbName=dbName)

        #get ALL sql to process and get last version
        files = LogFile.getAllVerFileNameInDirectory(versionedDatabaseRecord.getDestinationFolder())
        newLastVersion = 0;
        allSql = []
        for fileName in files.keys():
            elementName = files[fileName]
            filePath = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), elementName)
            logFile = LogFile(filePath)

            allSql.append(logFile.getAllSql());

            if logFile.getLastVersion() > newLastVersion:
                newLastVersion = logFile.getLastVersion()

        #get last position of binnary logs and last name
        binnaryLogNames = BinnaryLogNames(self.dbConnection)
        lastBinnaryLogName = binnaryLogNames.getLastBinnaryLogName()
        binnarylogs = BinnaryLogs([lastBinnaryLogName], dbName)
        logDataString = binnarylogs.getAllLogData()
        binnlogParser = BinnaryLogParser(logDataString)
        lastBinnaryLogPosition = binnlogParser.getLastPosition()

        # make databaseDump to transaction
        dbDump = DatabaseDump(dbName, self.verDatabaseConnection)
        dump = dbDump.dumpDatabase()

        #proces in into transaction
        try:
            print("Importing Database...");
            self.verDatabaseConnection.clearDatabase()
            for sql in allSql:
                self.verDatabaseConnection.executeSimpleSQL(sql)
        except Exception as e:
            self.verDatabaseConnection.clearDatabase()
            self.verDatabaseConnection.executeSimpleSQL(dump)
            raise

        # update data in versioned databases
        try:
            versionedDatabaseRecord = VersionedDatabase.addVersionerDatabase(self.dbConnection,dbName,
                                                                             versionedDatabaseRecord.getDestinationFolder(), lastBinnaryLogName,
                                                                             lastBinnaryLogPosition, newLastVersion)
        except Exception as e:
            self.verDatabaseConnection.clearDatabase()
            self.verDatabaseConnection.executeSimpleSQL(dump)
            raise

    def processForceMake(self, dbName):
        """
        Clear all log files and save into it dump of every element in lastest version
        :param dbName:
        :return:
        """
        if not self.dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")
        self.verDatabaseConnection = Database(Config.getConfig('databaseHost'),
                                              Config.getConfig('databasePort'),
                                              Config.getConfig("databaseUser"),
                                              Config.getConfig("databasePassword"),
                                              dbName
                                              )
        versionedDatabaseRecord = database.VersionedDatabase(self.dbConnection, dbName=dbName)

        # get last binary log and last position
        binnaryLogNames = BinnaryLogNames(self.dbConnection)
        lastBinnaryLogName = binnaryLogNames.getLastBinnaryLogName()
        binnarylogs = BinnaryLogs([lastBinnaryLogName], dbName)
        logDataString = binnarylogs.getAllLogData()
        binnlogParser = BinnaryLogParser(logDataString)
        lastBinnaryLogPosition = binnlogParser.getLastPosition()

        #version and des folder
        destFolder = versionedDatabaseRecord.getDestinationFolder()
        newVersion = versionedDatabaseRecord.getActualVersion()
        versionedDatabaseRecord.updateMetaDataAboutTables()

        # data to transaction
        transData = {}
        fileNames = LogFile.getAllVerFileNameInDirectory(destFolder)

        for fileName in fileNames.keys():
            path = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), fileNames[fileName])
            logFile = LogFile(path)
            transData[fileNames.get(fileName)] = logFile.getAllContent()

        try:
            #process
            LogFile.removeAllVerFilesInDirecotry(destFolder)
            dump = DatabaseDump(dbName, self.verDatabaseConnection)
            tablesDump = dump.dumpTables(versionedDatabaseRecord.getAllTables())
            for tableName in tablesDump.keys():
                logFileName = LogFile.makeLogFilePath(destFolder, tableName)
                logFile = LogFile(logFileName)
                logFile.insertVersionIntoFile([tablesDump.get(tableName)], newVersion)
        except Exception as e:
            #On exception - transaction
            LogFile.removeAllVerFilesInDirecotry(destFolder)
            for name in transData.keys():
                logFileName = LogFile.makeLogFilePath(destFolder, name)
                logFile = LogFile(logFileName)
                logFile.writeIntoFile(transData[name])
            raise


        versionedDatabaseRecord.setSomeData(lastBinnaryLogName, lastBinnaryLogPosition, newVersion)

    def processVersionedDatabasesInfo(self):
        """
        Print data about versioned databases
        """
        databases = VersionedDatabase.getAllVersionedDatabase(self.dbConnection)
        if not databases:
            print('No databes is not versioned')

        #get data
        tableData = [['Database name', 'Actual version', 'Destination folder']]
        for db in databases:
            line = [
                db.getDbName(),
                db.getActualVersion(),
                db.getDestinationFolder()
            ]
            tableData.append(line)

        #print data
        table = AsciiTable(tableData)
        print(table.table)


    def __revertByElementName(self, elementName, versionedDatabaseRecord):
        """
        method to secure transaction on some element
        This method call in faild run sql code
        Method get element into last runing state
        :param elementName: name of faild element
        :param versionedDatabaseRecord: databse record
        :return:
        """
        #init variables
        filePath = LogFile.makeLogFilePath(versionedDatabaseRecord.getDestinationFolder(), elementName)
        logFile = LogFile(filePath)
        binnaryLogNames = BinnaryLogNames(self.dbConnection)
        #get sql saved in logFile
        sql = logFile.getSqlToVersion(versionedDatabaseRecord.getActualVersion())
        #get sql From bin log
        names = binnaryLogNames.getBinnaryLogsNamesOlderThan(versionedDatabaseRecord.getLogFile())
        binnarylogs = BinnaryLogs(names, versionedDatabaseRecord.getDbName())
        binnarylogs.setStartPositionFirstLog(versionedDatabaseRecord.getLogFilePosition())
        allsql = binnarylogs.getAllLogData()
        binnlogParser = BinnaryLogParser(allsql)
        result = binnlogParser.getLogParserResult()
        DDL = (result.getDataDefitionByName(elementName))
        DML = (result.getDataManipulationByName(elementName))
        if DDL:
            sql += ';'.join(DDL)
            sql += ";"
        if DML:
            sql += ';'.join(DML)
            sql += ";"
        self.verDatabaseConnection.deleteElementbyName(elementName)
        self.verDatabaseConnection.executeSimpleSQL(sql)

class Config:
    """Simple config class"""
    def getConfig(elemName):
        if elemName in config.config.keys():
            return config.config[elemName]
        else:
            raise Exception('\"' +elemName + '\" is not set in config file')



