from databaseVersioner.app import Application, Config
from databaseVersioner.exceptions import *
from databaseVersioner.terminal import *
import traceback





terminalArguments = TerminalArguments()
arg = terminalArguments.procesArguments()

if(arg['action'] == terminalArguments.ACTIONS['help']):
    sys.exit()


application = Application()
try:
    if(arg['action'] == terminalArguments.ACTIONS['init']):
        print("Process INIT is beeing processed ... ")
        application.processInit()
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['addExist']):
        print("Process ADD is beeing processed ... ")
        application.processAddExists(arg['dbName'], arg['folder'])
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['addNonExist']):
        print("Process ADD is beeing processed ... ")
        application.processAddNonExist(arg['dbName'], arg['folder'])
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['snapshot']):
        print("Process SNAPSHOT is beeing processed ... ")
        application.processSnapshot(arg['dbName'], arg['destination'])
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['up']):
        print("Process MAKE is beeing processed ... ")
        application.processUp(arg['dbName'])
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['importing']):
        print("Process SET is beeing processed ... ")
        application.processImport(arg['dbName'])
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['merge']):
        print("Process MAKE is beeing processed ... ")
        application.processMerge(arg['dbName'], arg['fromVersion']);
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['forceSet']):
        print("Process FORCE SET is beeing processed ... ")
        application.processForceSet(arg['dbName']);
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['forceMake']):
        print("Process FORCE MAKE is beeing processed ... ")
        application.processForceMake(arg['dbName']);
        print("Successful completion")

    elif(arg['action'] == terminalArguments.ACTIONS['databasesInfo']):
        application.processVersionedDatabasesInfo();
except Exception as e:

    string = "Application error:"
    if isinstance(e,FileException):
        string = "File error:"
    elif isinstance(e, DatabaseException):
        string = "Database error:"
    elif isinstance(e,TerminalException):
        string = "Terminal error:"
    elif isinstance(e, MysqlExcepton):
        string = "MySQL error:"
    elif isinstance(e, LogFileException):
        string = "Application log files error:"
    elif isinstance(e, BinnaryLogException):
        string = "Binnary log files error:"

    print(string)
    print(str(e))

    info = TerminalCommand.runDialogYorN('Do you want to see more about error?')
    if info:
        traceback.print_exc()


