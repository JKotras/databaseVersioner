import re

import time

from databaseVersioner.utils import removeQuoteFromString


class BinnaryLogParser:
    """
    Class to parse mysql binnarylogs

    :type _logDatta: string of database log
    """
    def __init__(self, logData: str):
        """
        Constructor
        :param logData: str of binnary log content
        """
        self.__searchedQueries = (
            'insert',
            'update',
            'create',
            'alter',
            'drop',
            'delete',
            'truncate',
            'replace',
            'rename',
        )
        self.__renameQueries = {
            'rename': 'rename table (?P<dbnameFrom>[\w`\'\-"]*\.)(?P<fromName>[\w`\'"]*)[ ]*to[ ]*(?P<dbnameTo>[\w`\'\-"]*\.)(?P<toName>[\w`\'\-"]*).+',
            'alter': 'alter table (?P<fromName>[\w`\'"]*) rename to (?P<toName>[\w`\'\-"]*)'
        }
        self.__dataManipulationStatements = {
            'insert':'insert (low_priority |delayed | high_priority )?(ignore )?(into )?(?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*).+',
            'update':'update (low_priority )?(ignore )?(?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*).+ set.+',
            'delete':'delete (low_priority )?(quick )?(ignore )?from (?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*) .+',
            'replace':'replace (low_priority |delayed )?(into )?(?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*).+',
            'truncate':'truncate (table )?(?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*).+'
        }
        self.__dataDefinitionStatement = {
            'alter': 'alter (ignore )?table (?P<name>[\w`\'\-"]*).+',
            'drop': 'drop (temporary )?table (if exists)?(?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*).+',
            'create': 'create table (if not exists )?(?P<dbname>[\w`\'\-"]*\.)?(?P<name>[\w`\'\-"]*) .+'
        }

        self._logData = logData;



    def getSqlCommands(self):
        """
        Return sql queries from logData which change database structure
        :return: list of commands
        """
        lines = self._logData.split("/*!*/;")
        lines = [w.replace('\r\n', '') for w in lines]
        lines = [w.replace('\n', '') for w in lines]
        commands = []
        for line in lines:
            for start in self.__searchedQueries:
                if line.lower().startswith(start):
                    commands.append(line)
                    break

        return commands

    def __getSqlCommandsWithType(self):
        """
        Return sql queries from logData which change database structure
        :param logData: string of binnaryLog data
        :return: list of lists [type, query]
        """
        lines = self._logData.split("/*!*/;")
        lines = [w.replace('\r\n', '') for w in lines]
        lines = [w.replace('\n', '') for w in lines]
        commands = []
        for line in lines:
            for start in self.__searchedQueries:
                if line.lower().startswith(start):
                    commands.append([start, line])
                    break

        return commands


    def getLogParserResult(self):
        """
        Return logParserResult fill by data from binnarylog data
        :return: logParserResult
        """

        commands = self.__getSqlCommandsWithType()
        parserResult = LogParserResult()
        for command in commands:
            sqlType = command[0]
            sqlQuery = command[1].replace('\n', ' ')

            # special behaviour pro alter because alter can rename or change structure
            if sqlType == 'alter':
                resultRename = re.search(self.__renameQueries[sqlType], sqlQuery, re.IGNORECASE)
                resultStruct = re.search(self.__dataDefinitionStatement[sqlType], sqlQuery, re.IGNORECASE)
                if resultRename is not None:
                    fromName = removeQuoteFromString(resultRename.group('fromName'))
                    toName = removeQuoteFromString(resultRename.group('toName'))
                    parserResult.addRenamed(fromName, toName, sqlQuery)
                elif resultStruct is not None:
                    name = removeQuoteFromString(resultStruct.group('name'))
                    parserResult.addDataDefinition(name, sqlQuery)
                else:
                    parserResult.addOtherQuery(sqlQuery)
                continue


            #data Manipulation
            if sqlType in self.__dataManipulationStatements:
                result = re.search(self.__dataManipulationStatements[sqlType], sqlQuery, re.IGNORECASE)
                if result is not None:
                    name = removeQuoteFromString(result.group('name'))
                    parserResult.addDataManipulation(name, sqlQuery)
                else:
                    parserResult.addOtherQuery(sqlQuery)
            #rename
            elif sqlType in self.__renameQueries:
                result = re.search(self.__renameQueries[sqlType], sqlQuery, re.IGNORECASE)
                if result is not None:
                    fromName = removeQuoteFromString(result.group('fromName'))
                    toName = removeQuoteFromString(result.group('toName'))
                    parserResult.addRenamed(fromName, toName, sqlQuery)
                else:
                    parserResult.addOtherQuery(sqlQuery)
            #data Definition
            elif sqlType in self.__dataDefinitionStatement:
                result = re.search(self.__dataDefinitionStatement[sqlType], sqlQuery, re.IGNORECASE)
                if result is not None:
                    name = removeQuoteFromString(result.group('name'))
                    if sqlType == 'create':
                        parserResult.addCreated(name, sqlQuery)
                    else:
                        parserResult.addDataDefinition(name, sqlQuery)
                else:
                    parserResult.addOtherQuery(sqlQuery)
            #other
            else:
                parserResult.addOtherQuery(sqlQuery)

        return parserResult

    def getLastPosition(self):
        """
        Get last position in this binnary log
        :return: int
        """
        regex = '# at (?P<position>[0-9]*)'
        result = re.findall(regex, self._logData, re.IGNORECASE|re.MULTILINE);
        if not result:
            print("Warning: no last binnary log position found");
            return 0
        return result[-1]




class LogParserResult:
    """
    Class represent binnary log parser result
    :type __dataManipulation: dict  {elementName: [queries]} contain sql queries example. insert,update
    :type __dataDefinition: dict    {elementName: [queries]} contain sql queries example: create, alter, rename
    :type __renamed: dict           {newName: oldName}
    :type __created: list           list of created elements
    :type __other: list             list of other non assigned queries
    :type __allSqlQueries           dict of list which contain sorted sql by time
    """
    def __init__(self):
        self.__dataManipulation = {} #{name:[commands]}
        self.__dataDefinition = {} # {name:[commands]}
        self.__renamed = {} #{new:old}
        self.__created = [] #[list of created]
        self.__other = [] #other queries list
        self.__allSqQueries = {}

    def __str__(self):
        #return str(self.__dataManipulation.keys())
        string = "dataManipulation: " + str(self.__dataManipulation) + "\n"
        string += "dataDefinition: " + str(self.__dataDefinition) + "\n"
        string += "renamed: "+ str(self.__renamed) + "\n"
        string += "created: " + str(self.__created) + "\n"
        string += "other: " + str(self.__other) + "\n"
        string += "all: " + str(self.__allSqQueries)+ "\n"

        return string

    def __addQuerie(self,name, query):
        """
        Add query to self.__allSqlQueries
        :param name: name of element
        :param query: query
        """
        if name not in self.__allSqQueries:
            self.__allSqQueries[name] = []
        self.__allSqQueries[name].append(query+";")

    def getAllQueriesByName(self,name):
        """
        Return all queries of element ordered by time of execution
        :param name: name of element
        :return: list
        """
        return self.__allSqQueries.get(name)

    def getAllUpdatedElementNames(self):
        """
        Return list of updated elements
        :return: list
        """
        return self.__allSqQueries.keys()

    def haveSomeData(self):
        """
        Determine if this binnary flush contain any data
        :return:
        """
        if not self.__allSqQueries:
            return False
        return True

    def addDataManipulation(self, name, query):
        """
        Add queries like alter
        For create use method add create
        :param name: name edited element
        :param query: query which process data manipulation on this element
        """
        if name not in self.__dataManipulation:
            self.__dataManipulation[name] = []
        self.__dataManipulation[name].append(query+";")
        self.__addQuerie(name, query)

    def getDataManipulations(self):
        """
        Return all manipulation queries in binn log result
        :return: dict
        """
        return self.__dataManipulation

    def getDataManipulationByName(self, name):
        """
        Return list of manipulation queries by element name
        :param name: name of element
        :return: list
        """
        return self.__dataManipulation.get(name)

    def addDataDefinition(self, name, query):
        """
        add queries like insert, update
        :param name: name of procesed element
        :param query: query to process it in db
        """
        if name not in self.__dataDefinition:
            self.__dataDefinition[name] = []
        self.__dataDefinition[name].append(query+";")
        self.__addQuerie(name, query)

    def getDataDefinitions(self):
        """
        Return all definitions queries in binn log result
        :return: dict
        """
        return self.__dataDefinition

    def getDataDefitionByName(self, name):
        """
        Return list of definitions queries by element name
        :param name: name of element
        :return: list
        """
        return self.__dataDefinition.get(name)

    def addRenamed(self, fromName, toName, query):
        """
        Add renemed element
        :param fromName: old name
        :param toName: new name
        :param query: query which process renamed in database
        """
        self.__renamed[toName] = fromName
        self.addDataDefinition(fromName, query)
        self.__dataDefinition[toName] = self.__dataDefinition[fromName]
        del self.__dataDefinition[fromName]
        if fromName in self.__dataManipulation:
            self.__dataManipulation[toName] = self.__dataManipulation[fromName]
            del self.__dataManipulation[fromName]
        if fromName in self.__allSqQueries:
            self.__allSqQueries[toName] = self.__allSqQueries[fromName]
            del self.__allSqQueries[fromName]

    def getRenamed(self):
        """
        Get all renamed element
        :return: dict
        """
        return self.__renamed

    def getRenameOldNameByNewName(self, newName):
        """
        Return old name of renamed element to newName
        :param newName: str of newName of element
        :return: str
        """
        return self.__renamed.get(newName)


    def addCreated(self, name, query):
        """
        add create element
        :param name: name of element
        :param query: quuery
        """
        self.__created.append(name)
        self.addDataDefinition(name, query)

    def getCreated(self):
        """
        Created element
        :return: list
        """
        return self.__created

    def addOtherQuery(self,query):
        """
        Add other queries
        :param query:
        """
        self.__other.append(query+";")

    def getOtherQueries(self):
        """
        Return all other queries
        :return:
        """
        return self.__other
