
"""
module contains helpful functions
"""
import re
import os


def removeQuoteFromString(string:str):
    """
    Remove from string a Quotes
    :param string: string to remove quotes
    :return: string withou quotes
    """
    string = string.replace('`', '')
    string = string.replace('\'', '')
    string = string.replace('"', '')

    return string

def removeCommentsFromSql(query: str):
    query = re.sub("\/\*.*?\*\/;\n", "", query, flags=re.S)
    query = re.sub("\/\*.*?\*\/", "", query, flags=re.S)
    #this is insecure
    #query = re.sub("-- .*", "", query)
    return query

def removeEmptyLinesFromSql(query: str):
    query = query.replace('\n;', '')
    query = query.replace('\n\n', '')
    return query

def getAllFileNamesInDirectory(dir: str):
    """
    Return dict of names in directory
    :param dir: directory abs path
    :return: list
    """
    return os.listdir(dir)

def isDirectoryExist(dir: str):
    """
    Check if directory exist
    :param dir: directory abs path
    :return: bool
    """
    return os.path.isdir(dir)

def isFileExist(path: str):
    """
    Check if file exist
    :param path: path to file
    :return: bool
    """
    return os.path.isfile(path)

def getDirectoryPathFromFilename(filePath: str):
    """
    Return directory path from filePath
    :param filePath: str of file path
    :return: str - direcotry path
    """
    if os.path.isdir(filePath):
        raise Exception("File path '"+filePath +"' is directory not file")
    return os.path.dirname(filePath)




