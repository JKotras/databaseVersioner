#!/usr/bin/python
import traceback

import pymysql
import sys

import time

from pymysql import MySQLError

from databaseVersioner.exceptions import DatabaseException, FileException, BinnaryLogException
from databaseVersioner.logFiles import LogFile
from databaseVersioner.terminal import TerminalCommand
from databaseVersioner.utils import getAllFileNamesInDirectory, isDirectoryExist


class Database:
    """
    Class Database

    Represent database connection + simple work in this connection

    :type password :str of password to database user
    :type user :str of username
    :type databaseName :str of databasename which is connected
    """
    def __init__(self, host, port, user, password, db):
        """
        Constructor of class Database
        Connection is not autocommit
        :param host: database host
        :param port: number of connect port
        :param user: databsae user
        :param password: password
        :param db: selected database name
        """
        self.password = password
        self.user = user
        self.databaseName = db
        self.port = port
        self.host = host
        self.__connection = pymysql.connect(host=host, port=port, user=user, passwd=password, db=db, autocommit=False, charset='utf8')
        # don use bin log and foreign key check for this session
        try:
            self.executeSQL("SET sql_log_bin=0")
            self.executeSQL("SET FOREIGN_KEY_CHECKS=0")
        except:
            DatabaseException("SET database variables faild")
            pass

    def __del__(self):
        """
        On destruct - close db connection
        """
        try:
            self.executeSQL("SET sql_log_bin=1")
            self.executeSQL("SET FOREIGN_KEY_CHECKS=1")
            self.__connection.close()
        except:
            return

    def __restrartConnertion(self):
        """
        restart databse connetion - on errors
        :return:
        """
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.password, db=self.databaseName, autocommit=False, charset='utf8')
    def getDatabaseConnection(self):
        """
        Get me database conection
        :return: self.__connection
        """
        return self.__connection

    def __makeCursor(self):
        """
        Make and return cursor with dictionary results
        :return: cursor
        """
        cursor = self.__connection.cursor(pymysql.cursors.DictCursor)
        return cursor

    def executeSimpleSQL(self, sql, params=(), oneResult=False):
        """
        Execute simple sql code with commits
        If sql is emptry the return emtry dict
        :param sql: sql code
        :param params: tuple of sql params
        :param oneResult: True if you want only one result, default = False
        :return: list/dict oneResult=True -> dict of values {columnName: value}
                           oneResult=False -> list of (dict of values {columnName: value})
        """
        if not params:
            params = None
        if not sql:
            return {}

        try:
            self.__connection.begin()
            cursor = self.__makeCursor()
            cursor.execute(sql, params)
            if oneResult:
                data = cursor.fetchone()
                if data is None:
                    data = {}
            else:
                data = cursor.fetchall()
                if data is None:
                    data = []
            self.__connection.commit()
            cursor.close()
        except Exception as err:
            self.__restrartConnertion()
            raise

        return data

    def begin(self):
        """
        Begin transaction
        """
        self.__connection.begin()

    def commit(self):
        """
        Commint transaction
        """
        self.__connection.commit()

    def rollback(self):
        """
        rollback transaction
        """
        self.__connection.rollback()

    def executeSQL(self,sql, params=(), oneResult=False):
        """
        Execute sql code without commits and without foreign check
        If you want use transaction, use executeSimpleSql method or use method to make transsaction
        :param sql: sql code to execute
        :param params: tuple of sql params
        :param oneResult: True if you want only one result, default = False
        :return: list/dict oneResult=True -> dict of values {columnName: value}
                           oneResult=False -> list of (dict of values {columnName: value})
        """
        if not params:
            params = None
        if not sql:
            return {}

        try:
            cursor = self.__makeCursor()
            cursor.execute(sql, params)
            if oneResult:
                data = cursor.fetchone()
            else:
                data = cursor.fetchall()

        except Exception as e:
            self.__restrartConnertion()
            raise e


        return data

    def updateInTableById(self, tableName, id, data, withTransaction= True):
        """
        Update data in table by record id

        Updated table have to contain primary key column with name id

        :param tableName:str name of table where i want to update
        :param id: str or int - id of updated data (id of row)
        :param data: dictionary new data {columnName: newData}
        :param withTransaction: determine if method use database transaction
        :return: bool or success return true else return false
        """
        try:
            setString = ', '.join(' = '.join((k, "'" + str(v) + "'")) for k, v in data.items()).replace('\\', '\\\\')
            sql = "UPDATE " + tableName + " SET " + setString + " WHERE id = '" + str(id) + "'"
            if withTransaction:
                self.executeSimpleSQL(sql)
            else:
                self.executeSQL(sql)
            return True
        except:
            return False

    def saveIntoTable(self, tableName, data, withTransaction= True):
        """
        Save new data into table
        Saved table have to contain primary key column with name id
        :param tableName: name of table save into
        :param data: dictionary new data {columnName: newData}
        :param withTransaction: determine if method use database transaction
        :return: str in success return Inserted Id or in error return None
        """

        columnsArray = [];
        dataArray = [];

        for k, v in data.items():
            columnsArray.append(k)
            dataArray.append("'"+str(v)+"'")

        columns = ','.join(columnsArray).replace('\\', '\\\\')
        data = ','.join(dataArray).replace('\\', '\\\\')

        try:
            if withTransaction:
                self.begin()
                sql = "INSERT INTO " + tableName + "(" + columns + ") VALUES (" + data +")"
                self.executeSQL(sql)
                lastId = self.executeSQL('SELECT LAST_INSERT_ID()',(), True)
                self.commit()
                return lastId['LAST_INSERT_ID()']
            else:
                sql = "INSERT INTO " + tableName + "(" + columns + ") VALUES (" + data + ")"
                self.executeSQL(sql)
                lastId = self.executeSQL('SELECT LAST_INSERT_ID()', (), True)
                return lastId['LAST_INSERT_ID()']
        except Exception as e:
            self.rollback()
            return None

    def deleteRecord(self, tableName:str, id, withTransaction= True):
        """
        Delete record by id in specified table
        Deleted table record have to contain primary key column with name id
        :param tableName: name of table
        :param id: id of record to remove
        :param withTransaction: determine if method use database transaction
        :return check: bool True in success or False in Error
        """
        try:
            sql = "DELETE FROM " + tableName + "where id=" +id;
            if withTransaction:
                self.executeSimpleSQL(sql)
            else:
                self.executeSQL(sql)
            return True
        except:
            return False

    def isDatabaseExist(self, databaseName: str):
        """
        Check if database exist
        :param databaseName: string of database name
        :return: bool
        """
        databases = self.executeSimpleSQL('SHOW DATABASES')
        for db in databases:
            name = db['Database']
            if databaseName == name:
                return True
        return False

    def isTableExist(self, tableName):
        """
        Check if table exist in connected database
        :param tableName: string of table name
        :return: bool
        """
        tables = self.executeSimpleSQL('SHOW Tables')
        for table in tables:
            if tableName in table.values():
                return True
        return False

    def deleteElementbyName(self, name: str):
        """
        Delete element in database by nameby name
        :param name: name Of element
        """
        self.executeSimpleSQL("DROP TABLE IF EXISTS `"+name+"`")
        self.executeSimpleSQL("DROP VIEW IF EXISTS `" + name + "`")
        self.executeSimpleSQL("DROP TRIGGER IF EXISTS `" + name + "`")

    def clearDatabase(self):
        """
        Clear all in database
        """
        self.executeSimpleSQL("DROP DATABASE `" + self.databaseName +"`" );
        self.executeSimpleSQL("CREATE DATABASE `" + self.databaseName + "`");
        self.executeSimpleSQL("USE `" + self.databaseName + "`");


class DatabaseTableRecord:
    """
    Class base database table record

    Represend one record in some table
    Table have to contain primary key with name 'id'
    :type _db: Database connection
    :type _data: dict data of record
    :type _tableName: name of table where record is it
    :type _id: id of record
    """
    def __init__(self, dbConnection: Database,tableName, id=None ):
        """
        Constructor
        :param dbConnection: Database connection
        :param id: integer of record id
        """
        self._db = dbConnection
        self._data = {}
        self._tableName = tableName
        self._id = id
        if id != None:
            self._data = self._db.executeSimpleSQL(sql="SELECT * FROM " + self._tableName + " WHERE id = %s", params=(id), oneResult=True)

    def setValue(self, atrName: str, value):
        """
        Set value of record- dont save to database
        If jsou want save into database -> call self.save
        :param atrName: atribute name (column name)
        :param value: value of atribute
        """
        self._data[atrName] = value

    def setValues(self, values: dict):
        """
        Set values of record - dont save to database
        If jsou want save into database -> call self.save
        :param values: dictionary of values {columnName: value}
        """
        self._data = values

    def setValueAndUpdate(self, atrName: str, value):
        """
        Set and save value of record
        :param atrName: column name
        :param value: value to save
        :return:
        """
        self._data[atrName] = value
        self.update()

    def setValuesAndUpdate(self, values: dict):
        """
        Set and save values of record
        :param values: dictionary of values {columnName: value}
         """
        self._data = values
        self.update()

    def getValue(self, atrName: str):
        """
        Return value of this element
        :param atrName: name of atribute - columnName
        :return: value Return value of atrName(columnName) if atrName is not exist then return None
        """
        return self._data.get(atrName)

    def getValues(self):
        """
        Return all values
        :return: dictionary
        """
        return self._data

    def save(self):
        """
        Save values into database
        In error raise DatabaseException
        """
        self._data['id'] = self._id
        id = self._db.saveIntoTable(self._tableName, self._data)
        self._id = id

    def update(self):
        """
        Update value by id
        In error raise DatabaseException
        """
        if self._id is None:
            raise DatabaseException('id of record is not set')
        self._data['id'] = self._id
        ret = self._db.updateInTableById(self._tableName, self._id, self._data)

    def delete(self):
        """
        Delete this record in table
        In error raise DatabaseException
        :return:
        """
        if self._id is None:
            raise DatabaseException('id of record is not set')

        ret = self._db.deleteRecord(self._tableName, self._id)
        if ret == False:
            raise DatabaseException('Delete record with id \''+self._id+'\' into table \''+self._tableName+'\' faild')

class VersionedDatabase(DatabaseTableRecord):
    """
    Versioned database

    Extend DatabaseTable record in db table ver_databases

    Class to work with versioned databases
    :type __tables :dict of tables in this versioned tables {tableName: VersionedTable
    """
    TABLE_NAME = 'ver_databases'
    def __init__(self,dbConnection: Database, id = None, dbName = None):
        """
        Constructor
        if is set dbName then param id is ignored
        :param dbConnection: Database connection
        :param id: integer of record id
        :param dbName: name of database
        """
        if dbName != None:
            myId = dbConnection.executeSimpleSQL("SELECT id FROM "+self.TABLE_NAME+" WHERE `db_name` = %s", (dbName), True)
            id = myId.get('id')
            if id is None:
                raise DatabaseException("Database '"+dbName+"' is not versioned")
        DatabaseTableRecord.__init__(self, dbConnection, self.TABLE_NAME, id)
        self.__tables = {}
        if id is not None:
            ids = self._db.executeSimpleSQL("SELECT id, name FROM "+VersionedTable.TABLE_NAME+" WHERE `fk_ver_databases_id` = %s", (id))
            for id in ids:
                new = VersionedTable(dbConnection, id.get('id'))
                self.__tables[id['name']] = new

    def getAllTables(self):
        """
        Return all tables of this versioned database
        :return: dict{tableName: VersionedTable}
        """
        if self._id is not None:
            return self.__tables
        else:
            raise DatabaseException('Versioned database is not exists')

    def deleteAllTables(self):
        """
        Delete all tables
        :return:
        """
        for table in self.__tables.keys():
            table.delete()
            del table


    def getTableByName(self, name):
        """
        Return table class by table name
        :param name: strng
        :return: VersionedTable
        """
        if self._id is not None:
            return self.__tables.get(name)
        else:
            raise DatabaseException('Versioned element is not exists')

    def addTable(self, tableName, version=None):
        """
        Add table to versioned database
        Method suppose that table exist
        :param tableName: name of added table
        :param version: version of added table, if not set then it is use actual verison of database
        :return:
        """
        if self.__tables.get(tableName) is not None:
            raise DatabaseException('table \''+tableName+'\' are existed')
        if version is None:
            version = self.getActualVersion()
        if self._id is None:
            raise DatabaseException('database (fk_ver_databases_id) is not set')
        data = {'name': tableName, 'actual_version': version, 'fk_ver_databases_id': self._id}
        table = VersionedTable(self._db)
        table.setValues(data)
        table.save()
        self.__tables[tableName] = table

    def removeTable(self, tableName):
        """
        Remove table from versioning
        :param tableName: str
        :return:
        """
        table = self.__tables.get(tableName)
        if table is not None:
            return
        table.delete()


    def getDbName(self):
        return self.getValue('db_name')

    def getActualVersion(self):
        """
        Get actual verion of database
        in error raise DatabaseException
        :return: int
        """
        actVersion = (self.getValue('actual_version'))
        try:
            return int(actVersion);
        except ValueError:
            raise DatabaseException('Database actual version is not integer');

    def getLogFile(self):
        return self.getValue('log_file')

    def getLogFilePosition(self):
        return self.getValue('log_file_position')

    def getDestinationFolder(self):
        return self.getValue('destination_folder')

    def setActualVersion(self,version):
        return self.setValueAndUpdate('actual_version', version)

    def setLogFile(self,logFile):
        return self.setValueAndUpdate('log_file', logFile)

    def setLogFilePosition(self, logFilePosition):
        return self.setValueAndUpdate('log_file_position', logFilePosition)

    def setDestionationFolder(self, destinationFolder):
        return self.setValueAndUpdate('destination_folder', destinationFolder)

    def incrementActualVersion(self):
        """
        Increment and save version of database:
        :return: int: incremented version
        """
        version = self.getActualVersion() + 1;
        self.setActualVersion(version)
        return version

    def setSomeData(self, logFile = None, logFilePosition = None, version = None):
        """
        Set data logFile, logFilePosition, version
        :param logFile: str
        :param logFilePosition: str
        :param version: str
        """
        if logFile:
            self.setValue('log_file', logFile)
        if logFilePosition:
            self.setValue('log_file_position', logFilePosition)
        if version:
            self.setValue('actual_version', version)
        self.update()

    def updateMetaDataAboutTables(self):
        """
        Update meta data in versioned tables
        :return:
        """
        tablesForDatabase = self._db.executeSimpleSQL("select table_name from information_schema.tables where TABLE_SCHEMA='" + self.getDbName() + "'");
        self._db.executeSimpleSQL("DELETE FROM " + VersionedTable.TABLE_NAME + " WHERE `fk_ver_databases_id` = " + str(self._id))
        for table in tablesForDatabase:
            tableName = table['table_name'];
            data = {'name': tableName, 'actual_version': self.getActualVersion(), 'fk_ver_databases_id': str(self._id)}
            self._db.saveIntoTable(VersionedTable.TABLE_NAME, data, withTransaction=True)

        self.__tables = {}
        ids = self._db.executeSimpleSQL("SELECT id, name FROM " + VersionedTable.TABLE_NAME + " WHERE `fk_ver_databases_id` = %s", str(self._id))
        for id in ids:
            new = VersionedTable(self._db, id.get('id'))
            self.__tables[id['name']] = new



    @staticmethod
    def addVersionerDatabase(dbConnection: Database, dbName, destinationFolder, logFile = None, logFilePosition = 0, actualVersion = 0):
        """
        Add database to the versioned databases
        Make record in the databaseVersioner database with actual version 0

        If database has already versioned then delete old records

        :param dbConnection: Database connetion
        :param dbName: name of database to versioned
        :param destinationFolder: folder to save log files, only abs path.
        :param logFile: str of logFile for this database
        :param logFilePosition: int number which declare last position in logFile
        :return: VersionedDatabase
        """
        if logFile is None:
            logs = dbConnection.executeSimpleSQL("SHOW BINARY LOGS")
            lastLog = logs[-1]
            logFile = lastLog.get('Log_name')

        if logFile is None:
            raise BinnaryLogException('No binnary log file find')
        if not dbConnection.isDatabaseExist(dbName):
            raise DatabaseException("Database: '" + dbName + "' is not exist")
        if not isDirectoryExist(destinationFolder):
            raise FileException("Directory '" + destinationFolder + "' is not exist")

        dataNewDatabase = {'db_name': dbName, 'actual_version': actualVersion, 'log_file': logFile,'log_file_position':logFilePosition, 'destination_folder': destinationFolder}
        tablesForDatabase = dbConnection.executeSimpleSQL("select table_name from information_schema.tables where TABLE_SCHEMA='" + dbName + "'");


        byNameInDb = dbConnection.executeSimpleSQL("SELECT id FROM " + VersionedDatabase.TABLE_NAME + " WHERE `db_name` = %s", (dbName), True)


        try:
            dbConnection.begin()
            # delete od records about vesioning
            if byNameInDb.get("id") is not None:
                dbConnection.executeSQL("DELETE FROM " + VersionedTable.TABLE_NAME + " WHERE `fk_ver_databases_id` = " + str(byNameInDb.get("id")))
                dbConnection.executeSQL("DELETE FROM " + VersionedDatabase.TABLE_NAME + " WHERE `id` = " + str(byNameInDb.get("id")))
            # save data about versioning into database
            retId = dbConnection.saveIntoTable(VersionedDatabase.TABLE_NAME, dataNewDatabase, withTransaction=False)
            if retId is None:
                raise DatabaseException('Save record into table \''+VersionedDatabase.TABLE_NAME+'\' faild')
            for table in tablesForDatabase:
                tableName = table['table_name'];
                data = {'name': tableName, 'actual_version': 0, 'fk_ver_databases_id': retId}
                dbConnection.saveIntoTable(VersionedTable.TABLE_NAME,data, withTransaction=False)
        except Exception as e:
            dbConnection.rollback()
            raise e
        dbConnection.commit()
        versionedDatabase = VersionedDatabase(dbConnection, retId)

        return versionedDatabase

    @staticmethod
    def isDatabaseVersioned(dbConnection: Database, dbName):
        """
        Check if dababase with name dbname has already vesioned
        :param dbConnection:
        :param dbName: name of checked database
        :return: bool
        """
        byNameInDb = dbConnection.executeSimpleSQL("SELECT id FROM " + VersionedDatabase.TABLE_NAME + " WHERE `db_name` = %s", (dbName), True)
        if byNameInDb.get("id") is not None:
            return True
        return False

    @staticmethod
    def getAllVersionedDatabase(dbConnection: Database):
        """
        Return list of VersionedDatabase objects
        :param dbConnection:
        :return: list
        """
        versionedDatabases = [];
        ids = dbConnection.executeSimpleSQL("SELECT id FROM " + VersionedDatabase.TABLE_NAME)
        for record in ids:
            id = record.get('id')
            if id:
                versionedDatabase = VersionedDatabase(dbConnection, id)
                versionedDatabases.append(versionedDatabase)

        return versionedDatabases

class VersionedTable(DatabaseTableRecord):
    """
    Versioned tables
    """
    TABLE_NAME = 'ver_databases_tables'
    def __init__(self,dbConnection: Database, id = None):
        """
        Constructor
        :param dbConnection: Database connection
        :param id: integer of record id
        """
        DatabaseTableRecord.__init__(self, dbConnection, self.TABLE_NAME, id)
        self.versionedTableName = self.getValue('name')

    def getName(self):
        return self.getValue('name')

    def getActualVersion(self):
        return self.getValue('actual_version')

    def setName(self, name):
        return self.setValueAndUpdate('name', name)

    def setActualVersion(self, actualVersion):
        return self.setValueAndUpdate('actual_version', actualVersion)

    def incrementActualVersion(self):
        """
        Increment and save version of database:
        :return: int: incremented version
        """
        version = self.getActualVersion() + 1;
        self.setActualVersion(version)
        return version



class VersionerDatabase:
    """
    Class represent default data about versioner database
    """
    DATABASE_STRUCTURE = """

DROP TABLE IF EXISTS `ver_databases`;
CREATE TABLE `ver_databases` (
  `id` int(11) NOT NULL,
  `db_name` varchar(256) NOT NULL,
  `actual_version` int(11) NOT NULL,
  `log_file` varchar(256) CHARACTER SET utf8 COLLATE utf8_czech_ci NOT NULL,
  `log_file_position` int(11) NOT NULL,
  `destination_folder` varchar(1024) CHARACTER SET utf8 COLLATE utf8_czech_ci NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ver_databases_tables`;
CREATE TABLE `ver_databases_tables` (
  `id` int(11) NOT NULL,
  `name` varchar(256) CHARACTER SET utf8 COLLATE utf8_czech_ci NOT NULL,
  `actual_version` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `fk_ver_databases_id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `ver_databases`
  ADD PRIMARY KEY (`id`);
ALTER TABLE `ver_databases_tables`
  ADD PRIMARY KEY (`id`);
ALTER TABLE `ver_databases`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
ALTER TABLE `ver_databases_tables`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;


"""
    DATABASE_TABLES = ["ver_databases","ver_databases_tables"]

    def __init__(self, host, port, user, password, db):
        Database.__init__(host,port,user,password,db)



