#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

###########
# imports #
###########
import os

#############
# constants #
#############
CONFIG = {
    'select_top_cnt': 20,
    'relation_top_cnt': 10,
    'log_dir_path': os.path.join(os.getenv('MAUM_ROOT'), 'logs/upload_statistics_of_call'),
    'log_name': 'upload_statistics_of_call.log',
    'log_level': 'debug'
}
