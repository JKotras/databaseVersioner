""""*******************  Config module **********************

Define config dict

Global config for alll application
"""

config = {
    "binnaryLogsDir" : "/var/lib/mysql/",
    "mysqlbinlogLocation" : "/usr/bin/mysqlbinlog",
    "mysqldumpLocation": "/usr/bin/mysqldump",

    "authorName": "databaseVersioner",
    "databaseHost": "localhost",
    "databasePort": 3306,
    "databaseName": "databaseVersioner",
    "databaseUser": "root",
    "databasePassword": "heslo"
}
