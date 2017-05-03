            DATABASE VERSIONER

Description
-----------
The Database versioner is a application to get better versioning
of MySql Database. Application can run under any versioning systems
like SVN or GIT. Application is multiplatform - run on Linux and Windows
Application run on incremental sql scripts. Enable simple branching.

Requirements
----------
Database MySQL
Versioning system - Git, SVN, Mercurial...
Python 3.5
Python modules pymysql and terminaltables

Install
-------
- Enable in MySQL database binnary logging in STATEMENT format
    [mysqld]
    log-bin=mysql-bin
    binlog_format=statement
- Set data into config.py file
    binnaryLogsDir - location where is binnary log is saved
    mysqlbinlogLocation - path to mysqlbinlog utility of MySQL
    mysqldumpLocation - path to mysqldump utility of MySQL
    authorName - Your name or nickname
    databaseHost - location od database server (mostly "localhost")
    databasePort - port of connection to database
    databaseName - name of database for this application (default databaseVersioner)
    databaseUser - login of root user
    databasePassword - password of root user
- Make database
    Make database with name like in config.py in databaseName
- Run application with argument --init
    This is init the aplication to usual


Using
-----
Before usual using -> run application with argument --init
Apllication have this method:
init, addNonExist, addExist, snapshot, set, make, forceSet, forceMake, merge

init - Use this method only for init the application
    Method make internal structrure od database with name by 'databaseName' in config
    If you run method when you have ran it before, then every versioned data will be removed
addNonExist - Use if you want to add database to versioning which is not been versioned before
    Method add data about versioned database to databaseVersioner database. Make logFile to versioning
    Params:
    dbName - name of added database
    folder - path of directory to save to log files. This directory have to be versioned by some version system
addExist - Use if you want to add database to versioning which is been versioning but not in local database
    Method add data about versioned database to databaseVersioner database. Import data from versioned log files
    Params:
    dbName - name of added database, this database have to be maked in local database system, database have to be empty
    folder - path of directory where is saved log file. This directory is under version control
snapshot - Make file contain sql snapshot of the database.
    Params
    dbName - name of database to be exported
    destinationFile - path to new file (will be create), which will contain snapshot.
Make - This method make new version of database.
    Take data from binnery log parse it and save it into log files. Data will be add as incremental data
    If no data to save into log files, then new version will not be created.
    Use to make new version, after that make commit.
    Params
    dbName - name fo database to be make new version
ForceMake - Export all database into logFiles
    Delete all versions in log file and make newest version with export of db element
    Use to clear log files. This reduce amount of sql commands in logs so execution will be shotrer.
    Params
    dbName - name fo database to be force make
Set - Set (import) all new revision into database
    Method look into log files, find all new sql and process it.
    All binnary log (all database changes) will forget.
    Use after pull or update log files.
    Params
    dbName - name of database to be set
ForceSet - Clear local database and import all versions
    Clear local database and process all sql in log files, so local database will be in same version ass log files
    All binnary log (all database changes) will forget.
    Use this method if you want to forget what you do (testing) in database
    Use this after change branch in version control. To change database by branch
    use this after resolve conflicts in version control.
    Params
    dbName - name of database to be force set

Contact
-------
<xkotra01(@)stud.fit.vutbr.cz>

Tips
----
On some memory error - change memory limit in MySQL Database server
On connect to database logs - change memory limit, change time of execution or size of one sql command in MySql Database server.
On permision denied to binnary log - change authoryty to directory where binnary log is saved.
