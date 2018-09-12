#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-29, modification: 2018-09-03"

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
from cfg import config
from lib import logger


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def elapsed_time(start_time):
    """
    elapsed time
    :param          start_time:          date object
    :return                              Required time (type : datetime)
    """
    end_time = datetime.fromtimestamp(time.time())
    required_time = end_time - start_time
    return required_time


def del_garbage(log, delete_file_path):
    """
    Delete directory or file
    :param          log:                        Logger
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
            log.error("Can't delete {0}".format(delete_file_path))
            log.error(exc_info)


def delete_file(log, ts, target_info_dict):
    """
    Delete file
    :param      log:                    Logger
    :param      ts:                     System time
    :param      target_info_dict:       Target information dictionary
    """
    # Delete record file
    target_dir_path = target_info_dict.get('dir_path')
    if target_dir_path[-1] == '/':
        target_dir_path = target_dir_path[:-1]
    mtn_period = int(target_info_dict.get('mtn_period'))
    mtn_period_datetime = datetime.fromtimestamp(ts) - timedelta(days=mtn_period)
    mtn_period_date = mtn_period_datetime.date()
    w_ob = os.walk(target_dir_path)
    target_dir_list = list()
    for dir_path, sub_dirs, files in w_ob:
        for sub_dir in sub_dirs:
            try:
                datetime.strptime(sub_dir, '%Y%m%d')
            except Exception:
                continue
            target_dir_list.append(sub_dir)
    log.info("Delete directory the {0} before".format(mtn_period_date))
    for target_dir in target_dir_list:
        target_dir_date = datetime.strptime(target_dir, '%Y%m%d').date()
        if target_dir_date < mtn_period_date:
            delete_target_dir_path = os.path.join(target_dir_path, target_dir)
            if os.path.exists(delete_target_dir_path):
                log.info('--> Delete {0}'.format(delete_target_dir_path))
                del_garbage(log, delete_target_dir_path)
            else:
                log.error('--> {0} is not exists'.format(delete_target_dir_path))


def processing(conf, target_dir_list):
    """
    processing
    :param      conf:                   Config
    :param      target_dir_list:        Target directory list
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts)
    # Add logging
    log = logger.get_timed_rotating_logger(
        logger_name=conf.logger_name,
        log_dir_path=conf.log_dir_path,
        log_file_name=conf.log_file_name,
        backup_count=conf.backup_count,
        log_level=conf.log_level
    )
    log.info("-" * 100)
    log.info("Start delete log and output file")
    ts = time.time()
    try:
        for target_info_dict in target_dir_list:
            log.info('Directory path : {0}, Maintenance period : {1}'.format(
                target_info_dict['dir_path'], target_info_dict['mtn_period']))
            # Delete file
            try:
                delete_file(log, ts, target_info_dict)
            except Exception:
                exc_info = traceback.format_exc()
                log.error(exc_info)
                continue
    except Exception:
        exc_info = traceback.format_exc()
        log.error(exc_info)
    finally:
        log.info("END.. Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
        for handler in log.handlers:
            handler.close()
            log.removeHandler(handler)


########
# main #
########
def main(args):
    """
    This is a program that delete log and output file
    :param      args:       Arguments
    """
    try:
        conf = config.DELConfig
        if args.dir_path and args.mtn_period:
            target_dir_list = [
                {
                    'dir_path': args.dir_path,
                    'mtn_period': args.mtn_period
                }
            ]
        elif not args.dir_path and not args.mtn_period:
            target_dir_list = conf.target_directory_list
        else:
            print 'Check Argument --help or -h'
            sys.exit(1)
        processing(conf, target_dir_list)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store', dest='dir_path', default=False, help='Directory path')
    parser.add_argument('-m', action='store', dest='mtn_period', default=False, help='Maintenance period')
    arguments = parser.parse_args()
    main(arguments)
