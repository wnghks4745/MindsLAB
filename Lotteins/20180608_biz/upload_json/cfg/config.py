#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 0000-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os


#############
# constants #
#############
CONFIG = {
    'log_dir_path': os.path.join(os.getenv('MAUM_ROOT'), 'logs/upload_json'),
    'log_name': 'upload_json.log',
    'log_level': 'debug',
    'rec_dir_path': [
        '/app/record/CS/CN',
    ],
    'json_output_path': os.path.join(os.getenv('MAUM_ROOT'), 'biz/processed_json'),
    'rec_move_dir_path': '/data/input'
}

DAEMON_CONFIG = {
    'process_interval': 1,
    'log_dir_path': os.path.join(os.getenv('MAUM_ROOT'), 'logs/upload_json'),
    'log_file_name': 'upload_json_daemon.log',
    'pid_dir_path': os.path.join(os.getenv('MAUM_ROOT'), 'bin/upload_json/bin'),
    'pid_file_name': 'upload_json_daemon.pid',
    'script_path': os.path.join(os.getenv('MAUM_ROOT'), 'bin/upload_json')
}

ORACLE_DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTAPP',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}

MSSQL_DB_CONFIG = {
    'host': '10.151.3.23',
    'user': 'stt_user',
    'password': 'stt_user',
    'port': 1433,
    'database': 'VoistoreX',
    'charset': 'utf8',
    'login_timeout': 10
}

OPENSSL_CONFIG = {
    'codec_file_path': os.path.join(os.getenv('MAUM_ROOT'), 'bin/upload_json/cfg/codec.cfg')
}
