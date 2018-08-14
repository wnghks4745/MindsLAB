#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-09, modification: 2018-01-19"

###########
# imports #
###########
import sys
import time
import traceback
from datetime import datetime


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ''
DT = ''

#########
# class #
#########


#######
# def #
#######
def setup_data(job):
    """
    Setup data and target directory
    :param      job:        Job
    :return:                Logger
    """
    # Make Target directory name
    OUTPUT_DIR_NAME = "{0}_{1}"


def processing(job):
    """
    TA processing
    :param      job:        Job
    :return:
    """
    # 0. Setup data
    logger = setup_data(job)


########
# main #
########
def main(job):
    """
    This is a program that execute SELECT TA
    :param      job:        JOB
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        if len(job) > 0:
            processing(job)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)
