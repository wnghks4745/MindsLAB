#!/usr/bin/python
# -*- coding: euc-kr -*-

MYSQL_DB_CONFIG = {
    'host': '10.224.51.94',
    'user': 'msl',
    'password': 'Minds12#$',
    'db': 'malife_stta',
    'port': 3399,
    'charset': 'utf8',
    'connect_timeout': 5
}

CHANGE_CONFIG = {
    'log_name': 'change_prgst_cd',
    'log_dir_path': '/app/prd/MindsVOC/CS/CS_monitoring/logs',
    'log_level': 'debug'
}

DELETE_CONFIG = {
    'log_name': 'delete_CS_file',
    'log_dir_path': '/app/prd/MindsVOC/CS/CS_monitoring/logs',
    'log_level': 'debug',
    'rec_path': '/app/rec_server/dev',
    'r_comp_type': 'C101'
}

MSSQL_DB_CONFIG = {
    'user': 'sa',
    'password': 'malife!1',
    'database': 'Vc_RecordDB',
    'host': '10.224.80.52',
    'port': 1433,
    'charset': 'utf8',
    'login_timeout': 5
}