#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-08, modification: 2018-02-08"

###########
# imports #
###########
import os
import sys
import time
import shutil
import argparse
import traceback
from datetime import datetime, timedelta
from cfg.config import CONFIG
from lib.iLogger import set_logger


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DELETE_CNT = 0


#######
# def #
#######
def elapsed_time(sdate):
    """
    elapsed time
    :param          sdate:          date object
    :return                         Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param          logger:                     Logger
    :param          delete_file_path:           Input path
    """
    if os.path.exists(delete_file_path):
        try:
            if os.path.isfile(delete_file_path):
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def delete_file(logger, ts, target_info_dict):
    """
    Delete file
    :param      logger:                 Logger
    :param      ts:                     System time
    :param      target_info_dict:       Target information dictionary
    """
    global DELETE_CNT
    # Delete record file
    target_dir_path = target_info_dict.get('directory_path')
    if target_dir_path[-1] == '/':
        target_dir_path = target_dir_path[:-1]
    delete_file_date = int(target_info_dict.get('delete_file_date'))
    delete_target_date = datetime.fromtimestamp(ts) - timedelta(days=delete_file_date)
    logger.info("\tDelete time point is {0}".format(delete_target_date))
    delete_target_dir = datetime.strftime(delete_target_date, '%Y/%m/%d')
    delete_target_dir_path = os.path.join(target_dir_path, delete_target_dir)
    if not os.path.exists(delete_target_dir_path):
        logger.error('\t\t{0} is not exists'.format(delete_target_dir_path))
    else:
        w_ob = os.walk(delete_target_dir_path)
        for dir_path, sub_dirs, files in w_ob:
            for _ in files:
                DELETE_CNT += 1
        del_garbage(logger, delete_target_dir_path)
        logger.info("\t\tDelete directory path : {0}".format(delete_target_dir_path))


def processing(target_dir_list):
    """
    processing
    :param      target_dir_list:        Target directory path
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_file_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info("Start delete log and output file")
    ts = time.time()
    try:
        logger.info('Target directory list')
        for target_info_dict in target_dir_list:
            logger.info('\tdate : {0}\tpath : {1}'.format(
                target_info_dict['delete_file_date'], target_info_dict['directory_path']))
        logger.info("1. Delete file")
        for target_info_dict in target_dir_list:
            # Delete file
            delete_file(logger, ts, target_info_dict)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
    logger.info("END.. Start time = {0}, The time required = {1}, delete count = {2}".format(
        st, elapsed_time(dt), DELETE_CNT))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(args):
    """
    This is a program that delete log and output file
    :param      args:       Arguments
    """
    try:
        if args.target_dir_path and args.delete_file_date:
            target_dir_list = [
                {
                    'directory_path': args.target_dir_path,
                    'delete_file_date': args.delete_file_date
                }
            ]
        elif not args.target_dir_path and not args.delete_file_date:
            target_dir_list = CONFIG['target_directory_list']
        else:
            print 'Check Argument --help or -h'
            sys.exit(0)
        processing(target_dir_list)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store', dest='target_dir_path', default=False, type=str, help='Directory path')
    parser.add_argument('-del', action='store', dest='delete_file_date', default=False, type=int, help='Date of delete target')
    arguments = parser.parse_args()
    main(arguments)
