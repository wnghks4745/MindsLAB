#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-24, modification: 0000-00-00"

###########
# imports #
###########
import os

#############
# constants #
#############
CONFIG = {
    'log_dir_path': os.path.join(os.getenv('MAUM_ROOT'), 'logs/negative_keyword'),
    'log_name': 'negative_keyword.log',
    'log_level': 'debug'
}

ORACLE_DB_CONFIG = {
    'host': '10.150.5.115',
    'user': 'STTAPP',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'PSTTODBS'
}
