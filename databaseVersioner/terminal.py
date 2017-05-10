
import argparse
import subprocess

import sys

import time
from pip._vendor.distlib.compat import raw_input
import threading

#if progress bar loading
progressBarLoading = True

class TerminalArguments:
    """
    Parse terminal argument
    """
    ACTIONS = {'init':'init','addExist':'addExist','addNonExist': 'addNonExist','snapshot':'snapshot','importing':'importing','up':'up', 'merge':'merge',
               'databasesInfo': 'databasesInfo', 'forceSet':'forceSet', 'forceMake': 'forceMake', 'help': 'help'}
    def __init__(self):
        """
        Define structure of terminal arguments
        """
        self.parser = argparse.ArgumentParser(add_help=True,
                                              description='databaseVersioner support versioning of mysql database'
                                              )
        self.parser.add_argument('--init', action='store_true', help="init the project databaseVersioner")
        self.parser.add_argument('--version', action='version', version='%(prog)s 0.1', help="print version of program")
        self.parser.add_argument('--addNonExist',  nargs=2, type=str, help="add non versioned database to versioning", metavar=('dbName', 'folder'))
        self.parser.add_argument('--addExist', nargs=2, type=str, help="add versioned database to local versioning",metavar=('dbName', 'folder'))
        self.parser.add_argument('--snapshot', nargs=2, type=str, help="make database snapshot", metavar=('dbName', 'destinationFile'))
        self.parser.add_argument('-s','--set', type=str, help="set new versions from repository, adjustments local remain", metavar="dbName")
        self.parser.add_argument('-fs', '--forceSet', type=str,help="clear all db and import from log files", metavar="dbName")
        self.parser.add_argument('-m','--make', type=str,  help="make new revision of database and save into repository", metavar="dbName")
        self.parser.add_argument('-fm', '--forceMake', type=str,help="make export of all database for clear log files", metavar="dbName")
        self.parser.add_argument('--merge', nargs=2, type=str, help="merge all versions from version into last version", metavar=('dbName', 'fromVersion'))
        self.parser.add_argument('--databasesInfo', action='store_true', help="print info about all versioned databases")



    def procesArguments(self):
        """
        Parse terminal argument
        If argument is not recognized then print help
        :return: dictionary with element action which define action to do.
                 if argument have paramenters that it is save into dictionary by name
        """
        action = {'action':''}
        args = self.parser.parse_args()

        if args.init:
            action['action'] = self.ACTIONS['init']
        elif args.addExist:
            action['action'] = self.ACTIONS['addExist']
            action['dbName'] = args.addExist[0]
            action['folder'] = args.addExist[1]
        elif args.addNonExist:
            action['action'] = self.ACTIONS['addNonExist']
            action['dbName'] = args.addNonExist[0]
            action['folder'] = args.addNonExist[1]
        elif args.snapshot:
            action['action'] = self.ACTIONS['snapshot']
            action['dbName'] = args.snapshot[0]
            action['destination'] = args.snapshot[1]
        elif args.set:
            action['action'] = self.ACTIONS['importing']
            action['dbName'] = args.set
        elif args.make:
            action['action'] = self.ACTIONS['up']
            action['dbName'] = args.make
        elif args.merge:
            action['action'] = self.ACTIONS['merge']
            action['dbName'] = args.merge[0]
            action['fromVersion'] = args.merge[1]
        elif args.databasesInfo:
            action['action'] = self.ACTIONS['databasesInfo']
        elif args.forceSet:
            action['action'] = self.ACTIONS['forceSet']
            action['dbName'] = args.forceSet
        elif args.forceMake:
            action['action'] = self.ACTIONS['forceMake']
            action['dbName'] = args.forceMake
        else:
            self.parser.print_help()
            action['action'] = self.ACTIONS['help']
        return action




class TerminalCommand:
    """
    Class contain only static method to work with terminal commands
    """
    @staticmethod
    def runCommandRetList(cmd :str, params = []):
        """
        Run terminal command and return response like list
        :param cmd: string of command
        :param params: list of params
        :return: list of lines
        """
        params = [cmd] + params
        params = ['"' + x + '"' for x in params]
        params = ' '.join(params)
        sp = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = sp.communicate()
        out = out.decode("utf-8", "ignore").split("\n")
        return out

    @staticmethod
    def runCommandRetStr(cmd :str, params = []):
        """
        Run terminal command and return reponse like string
        :param cmd: string of command
        :param params: list of params
        :return: string
        """
        params = [cmd]+params
        params = ['"'+x+'"' for x in params]
        params = ' '.join(params)
        sp = subprocess.Popen(params, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = sp.communicate()
        out = out.decode("utf-8", "ignore")
        return out

    @staticmethod
    def runDialogYorN(question :str):
        """
        Raise yes or no dialog into terminal.
        :param question: string of question
        :return: boolen Yes - true No - false
        """
        promt = "[Y/N]"
        valid = {"yes": True, "y": True,"no": False, "n": False}

        while True:
            sys.stdout.write(question + " " + promt)
            choice = raw_input().lower()
            if choice in valid:
                print("")   #make empty line
                return valid[choice]
            else:
                sys.stdout.write("Please respond with 'Y' or 'N'\n")


    @staticmethod
    def runInfo(infoStr :str):
        """
        Print infoStr to terminal
        :param infoStr:
        """
        sys.stdout.write(infoStr + "\n")

class progressBar(threading.Thread):
    def run(self):
        global progressBarLoading
        progressBarLoading = True
        i = 0
        print('Loading....  ', end="")
        while progressBarLoading != False:
            if (i % 4) == 0:
                sys.stdout.write('\b/')
            elif (i % 4) == 1:
                sys.stdout.write('\b-')
            elif (i % 4) == 2:
                sys.stdout.write('\b\\')
            elif (i % 4) == 3:
                sys.stdout.write('\b|')
            sys.stdout.flush()
            time.sleep(0.2)
            i += 1
        sys.stdout.flush()


