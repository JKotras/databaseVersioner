import os
import re
import string
import time

import sys

from config import config
from databaseVersioner.exceptions import FileException, LogFileException
from databaseVersioner.utils import getAllFileNamesInDirectory, isDirectoryExist


class LogFileBasic:
    """
    Contains simple method to work with text file
    """
    def __init__(self, filePath: string):
        """
        Constructor
        :param filePath: string of file path can be relative or absolute
        """
        if not os.path.isabs(filePath):
            self._filePath = os.path.realpath(filePath)
        else:
            self._filePath = filePath

        #if file exist check if is writeables
        if os.path.isfile(self._filePath):
            if not os.access(self._filePath, os.W_OK):
                raise LogFileException("Permissinon denied to write in file \'" + self.__filePath + "\'")

    def getAllContent(self):
        """
        return all content of log file
        On error throw exception
        :return: str
        """
        with open(self._filePath, "r", encoding="utf-8") as text_file:
            content = text_file.read()
        return content

    def writeIntoFile(self, string):
        """
        Simple writing string into file
        :param string:
        """
        with open(self._filePath, "w", encoding="utf-8") as text_file:
            text_file.write(string)

    def appendIntoFile(self, string):
        """
        Simple writing string into end file
        :param string:
        """
        with open(self._filePath, "a", encoding="utf-8") as text_file:
            text_file.write(string)

    def clearFile(self):
        """
        Get file clean
        """
        with open(self._filePath, "w", encoding="utf-8") as text_file:
            text_file.write('')

    def deleteFile(self):
        """
        Delete file
        On fail - do nothing
        """
        try:
            os.remove(self._filePath)
        except:
            pass

    def removeAllFilesInDirectory(directory: str):
        """
        Return all files names in directory
        :param directory path to directory in abs format
        :return: dict {filename: elementName}
        """
        if not isDirectoryExist(directory):
            raise FileException("Directory '" + directory + "' is not exist")
        fileNames = getAllFileNamesInDirectory(directory)

        for name in fileNames:
            try:
                os.remove(directory+"/"+name)
            except Exception as e:
                pass
        return

class LogFile(LogFileBasic):
    """
    Class represent sql log file
    By method in this class you can work with log file

    :type _file: str of absolute path
    """

    EXTENSION = 'versql'
    OTHER_LOG_NAME = 'otherLog'
    OTHER_LOG_NAME_FULL = 'otherLog.versql'

    def __init__(self, filePath: string):
        """
        Constructor
        :param filePath: string of file path can be relative or absolute
        """
        LogFileBasic.__init__(self,filePath)

    def getFilePath(self):
        """
        Return file path
        :return: string of absolute path
        """
        return self._filePath

    def __isFileLogExist(self):
        """
        Check if logged file exists
        :return: bool
        """
        return os.path.isfile(self._filePath)

    def __getAllFileContentByLine(self):
        """
        Return list lines from log file
        :return: list on lines without '\n'
        """
        with open(self._filePath, "r",encoding="utf-8") as file:
            content = file.readlines()
        content = [x.strip() for x in content]
        return content

    def __makeVersionHead(self, version: int):
        """
        Make and return head of version
        :param version: integer
        :return: string
        """
        string = '\n/*\n'
        string += '* Start version\n'
        string += '* version: ' + str(version) + '\n'
        string += '* date: ' + time.strftime("%d.%m.%Y") + '\n'
        string += '* author: ' + config['authorName'] + '\n'
        string += '*/\n'
        return string

    def __makeVersionFoot(self):
        """
        Make and return foot of version
        :return: string
        """
        string = '/*\n'
        string += '* End version\n'
        string += '*/\n'
        return string

    def __delMultiLineComments(self, sql: string):
        """
        Delete multiline sql comments /*comment*/ from list of lines
        And remove empty lines
        :param sql: string
        :return: string
        """

        sql = re.sub(re.compile("/\*\n\* End version.*?\*/", re.DOTALL), ' ', sql)
        sql = re.sub(re.compile("/\*\n\* Start version.*?\*/", re.DOTALL), ' ', sql)
        sql = "".join([s for s in sql.strip().splitlines(True) if s.strip()])
        return sql

    def insertVersionIntoFile(self, sqlList: list, version: int):
        """
        Insert version into file

        Version is list of sql statements with head and footer
        If file is not empty -> file going to be emptied
        :param sqlList: list of sql statement
        :param version: integer of version
        """
        with open(self._filePath, "w", encoding="utf-8") as file:
            file.write(self.__makeVersionHead(version))
            for sql in sqlList:
                line = sql + "\n"
                file.write(line)
            file.write(self.__makeVersionFoot())

    def appendVersionIntoFile(self, sqlList: list, version: int):
        """
        App version to end of log file

        Version is list of sql statements with head and footer
        :param sqlList: list of sql statement
        :param version: integer of version
        :return:
        """
        with open(self._filePath, "a", encoding="utf-8") as file:
            file.write(self.__makeVersionHead(version))
            for sql in sqlList:
                line = sql + "\n"
                file.write(line)
            file.write(self.__makeVersionFoot())

    def getAllSql(self):
        """
        Return string which contain all log file
        If file dont exist the return empty string
        :return: string
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")
        content = self.getAllContent()
        return content

    def getSqlFromVersion(self, version: int):
        """
        Return sql code from defined version (with version)
        If file dont exist the return empty string
        :param version: integer of version
        :return: string
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")

        list = self.__getAllFileContentByLine()
        numLine = -1
        startLine = len(list)
        check = False

        # find start version
        for key, line in enumerate(list):
            index = line.find("version:")
            if index != -1:
                check = True
                try:
                    versionStr = line[index + 8:]
                    versionInt = int(versionStr)
                    if versionInt >= version:
                        numLine = key
                        break
                except:
                    raise LogFileException('File \'' + self._filePath +'\' have syntax error on line ' + str(startLine-key))

        if not check:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error - no version exist')
        check = False

        if numLine != -1:
            for key, line in enumerate(list[numLine:]):
                if "*/" in line:
                    startLine = key + numLine + 1
                    check = True
                    break

        if numLine != -1 and check == False:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error')

        string = '\n'.join(list[startLine:])
        ret = self.__delMultiLineComments(string)
        return ret

    def getSqlToVersion(self, version: int):
        """
        Return sql code to defined version (with version)
        :param version: integer of version
        :return: string
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")

        list = self.__getAllFileContentByLine()
        numLine = -1
        endLine = len(list)
        check = False

        # find start version
        for key, line in enumerate(list):
            index = line.find("version:")
            if index != -1:
                check = True
                try:
                    versionStr = line[index + 8:]
                    versionInt = int(versionStr)
                    if versionInt > version:
                        numLine = key
                        break
                except:
                    raise LogFileException(
                        'File \'' + self._filePath + '\' have syntax error on line ' + str(endLine- key))

        if not check:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error - no version exist')
        check = False

        if numLine != -1:
            for key, line in (enumerate(reversed(list[numLine:]))):
                if "/*" in line:
                    check = True
                    endLine = numLine - key
                    break

        if numLine != -1 and check == False:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error')

        string = '\n'.join(list[:endLine])
        ret = self.__delMultiLineComments(string)
        return ret


    def getSpecifiedVersionSql(self, version: int):
        """
        Return sql of version
        :param version: integer - number of version
        :return: string - sql code of version
        """
        raise Exception('getSpecifiedVersion is not implemented')

    def getLastVersionSql(self):
        """
        Return last version sql code
        On error raise exceptins
        :return: string
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")

        list = self.__getAllFileContentByLine()
        numLine = -1
        startLine = -1;

        for key, line in enumerate(reversed(list)):
            index = line.find("version:")
            if index != -1:
                numLine = len(list) - key
                break

        if numLine >= 0:
            for key, line in enumerate(list[numLine:]):
                if "*/" in line:
                    startLine = key + numLine + 1
                    break

        if numLine == -1:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error: string \'version:\' was not found in any line')
        if startLine == -1:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error: string \'*/\' was not found in any line')

        string = '\n'.join(list[startLine:])
        ret = self.__delMultiLineComments(string)
        return ret

    def getLastVersion(self):
        """
        Return last version number
        If last version is no find then return 0
        :return: integer
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")
        list = self.__getAllFileContentByLine()
        lastVersion = 0;
        lenOfFile = len(list)
        for key, line in enumerate(reversed(list)):
            index = line.find("version:")
            if index != -1:
                try:
                    versionStr = line[index + 8:]
                    versionInt = int(versionStr)
                    lastVersion = versionInt
                    break;
                except:
                    raise LogFileException('File \'' + self._filePath +'\' have syntax error on line ' + str(lenOfFile-key))
        return lastVersion

    def deleteLastVersion(self):
        """
        Delete last version in log file
        """
        raise Exception('deleteLastVersion is not implemented')

    def deleteVersion(self, version: int):
        """
        Delete version with defined version
        :param version: integer number of version
        """
        raise Exception('deleteVersion is not implemented')

    def deleteFromVersion(self, version: int):
        """
        Delete logs from defined version number with this verison
        :param version: integer number of version
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")

        list = self.__getAllFileContentByLine()
        numLine = -1
        endLine = len(list)
        check = False

        # find start of  version
        for key, line in enumerate(list):
            index = line.find("version:")
            if index != -1:
                check = True
                try:
                    versionStr = line[index + 8:]
                    versionInt = int(versionStr)
                    if versionInt >= version:
                        numLine = key
                        break
                except:
                    raise LogFileException(
                        'File \'' + self._filePath + '\' have syntax error on line ' + str(endLine- key))

        if not check:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error - no version exist')
        check = False

        if numLine != -1:
            for key, line in (enumerate(reversed(list[numLine:]))):
                if "/*" in line:
                    check = True
                    endLine = numLine - key
                    break

        if numLine != -1 and check == False:
            raise LogFileException('File \'' + self._filePath + '\' have syntax error')

        stringToSave = '\n'.join(list[:endLine])

        self.writeIntoFile(stringToSave)

    def mergeFromVersion(self,version):
        """
        merge sql form secifific version to lastet version and save like last version
        This method is transactional
        :param version: integer number of version
        """
        if self.__isFileLogExist() != True:
            raise FileException("File \'" + self._filePath + "\' is not exists")

        if version <= 0:
            raise LogFileException("Version number can not be less than zero")

        all = self.getAllContent()
        try:
            lastestVerions = self.getLastVersion()
            merged = self.getSqlFromVersion(version)
            self.deleteFromVersion(version)
            sqlList = merged.split("\n")
            self.appendVersionIntoFile(sqlList, lastestVerions)
        except Exception as e:
            self.writeIntoFile(all)
            raise

    @staticmethod
    def makeLogFilePath(folder, elementName):
        """
        Make file path string
        :param folder: folder to save
        :param elementName: element name
        :return: str
        """
        name = folder + "/" + elementName + "." + LogFile.EXTENSION
        return name

    @staticmethod
    def getAllVerFileNameInDirectory(directory: str):
        """
        Return all log files names in directory
        :param directory path to directory in abs format
        :return: dict {filename: elementName}
        """
        if not isDirectoryExist(directory):
            raise FileException("Directory '" + directory + "' is not exist")
        fileNames = getAllFileNamesInDirectory(directory)
        retNames = {}
        extensionLen = len(LogFile.EXTENSION)+1 # +1 dot in file name

        for name in fileNames:
            if name.endswith(LogFile.EXTENSION):
                retNames[name] = name[0:-extensionLen]
        return retNames

    @staticmethod
    def removeAllVerFilesInDirecotry(directory: str):
        """
        Remove all versioned files in directory
        :param directory: abs path do directory
        """
        if not isDirectoryExist(directory):
            raise FileException("Directory '" + directory + "' is not exist")
        fileNames = getAllFileNamesInDirectory(directory)
        extensionLen = len(LogFile.EXTENSION) + 1  # +1 dot in file name

        for name in fileNames:
            if name.endswith(LogFile.EXTENSION):
                try:
                    os.remove(LogFile.makeLogFilePath(directory, name[0:-extensionLen]))
                except Exception as e:
                    pass



class DumpFile(LogFileBasic):
    """
    Class represent sql dump
    By method in this class you can work with log file

    :type _file: str of absolute path
    """

    EXTENSION = 'sql'

    def __init__(self, filePath: string):
        """
        Constructor
        :param filePath: string of file path can be relative or absolute
        """
        LogFileBasic.__init__(self,filePath)

