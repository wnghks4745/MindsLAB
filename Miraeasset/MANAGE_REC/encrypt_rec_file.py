#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-09-29, modification: 2018-01-12"

###########
# imports #
###########
import os
import sys
import time
import shutil
import traceback
from datetime import datetime, timedelta
from lib import openssl
from cfg.config import CONFIG
from lib.iLogger import set_logger_period_of_time

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

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


def processing(dir_list):
    """
    Processing
    :param          dir_list:       List of directory
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['encrypt_log_file_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger_period_of_time(logger_args)
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    encrypt_cnt = 0
    for dir_path in dir_list:
        if not os.path.exists(dir_path):
            logger.debug('Directory is not exists -> {0}'.format(dir_path))
            continue
        w_ob = os.walk(dir_path)
        for dir_path2, sub_dirs, files in w_ob:
            for file_name in files:
                try:
                    dir_name = os.path.basename(dir_path)
                    encrypt_dir_path = os.path.join(CONFIG['encrypt_rec_dir_path'], dir_name)
                    encrypt_file_path = "{0}/{1}".format(encrypt_dir_path, file_name)
                    file_path = os.path.join(dir_path2, file_name)
                    if file_name.endswith(".enc"):
                        if not os.path.exists(encrypt_dir_path):
                            os.makedirs(encrypt_dir_path)
                        if os.path.exists(encrypt_file_path):
                            os.remove(encrypt_file_path)
                        logger.debug("Move [ {0} -> {1}.tmp ]".format(file_path, encrypt_file_path))
                        shutil.move(file_path, '{0}.tmp'.format(encrypt_file_path))
                        logger.debug("Rename [ {0}.tmp -> {0} ]".format(encrypt_file_path))
                        os.rename('{0}.tmp'.format(encrypt_file_path), encrypt_file_path)
                        continue
                    if file_name.endswith(".tmp") or file_name.endswith("filepart"):
                        continue
                    rename_file_path = os.path.join(dir_path2, "encrypting_{0}".format(file_name))
                    encrypt_cnt += 1
                    logger.debug("Rename [ {0} -> {1} ]".format(file_path, rename_file_path))
                    os.rename(file_path, rename_file_path)
                    logger.debug("Encrypt [ {0} ]".format(file_name))
                    openssl.encrypt_file([rename_file_path])
                except Exception:
                    exc_info = traceback.format_exc()
                    logger.error("Can't Rename or Encrypt record file. [ {0} ]".format(file_name))
                    logger.error(exc_info)
                    continue
                try:
                    encrypt_file_path += '.enc'
                    enc_file_path = os.path.join(dir_path2, "{0}.enc".format(file_name))
                    if not os.path.exists(encrypt_dir_path):
                        os.makedirs(encrypt_dir_path)
                    if os.path.exists(encrypt_file_path):
                        os.remove(encrypt_file_path)
                    logger.debug("Move [ {0} -> {1}.tmp ]".format(enc_file_path, encrypt_file_path))
                    shutil.move(rename_file_path,'{0}.tmp'.format(encrypt_file_path))
                    logger.debug("Rename [ {0}.tmp -> {0} ]".format(encrypt_file_path))
                    os.rename('{0}.tmp'.format(encrypt_file_path), encrypt_file_path)
                except Exception:
                    exc_info = traceback.format_exc()
                    logger.error("Can't move encrypt file. [ {0} ]".format(file_name))
                    logger.error(exc_info)
                    continue
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    logger.info("END.. Start time = {0}, The time required = {1}, Encrypt target file list count = {2}".format(
        st, elapsed_time(dt), encrypt_cnt))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

########
# main #
########


def main(date):
    """
    This is a program that encrypt record file
    """
    try:
        dir_list = list()
        if not date:
            w_ob = os.walk(CONFIG['rec_dir_path'])
            sub_dir_list = list()
            abs_dir_path = ''
            for dir_path, sub_dir, files in w_ob:
                abs_dir_path = dir_path
                sub_dir_list = sub_dir
                break
            sub_dir_path_list = list()
            for dir_name in sub_dir_list:
                sub_dir_path_list.append(os.path.join(abs_dir_path, dir_name))
            dir_list = sorted(sub_dir_path_list, key=os.path.getmtime, reverse=True)
            if not CONFIG['incident_dir_path'] in dir_list:
                sub_dir_path_list.append(CONFIG['incident_dir_path'])
        else:
            dir_list.append("{0}/{1}".format(CONFIG['rec_dir_path'], date))
        processing(dir_list)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) == 1:
        main(False)
    elif len(sys.argv) == 2 and len(sys.argv[1]) == 8:
        main(sys.argv[1])
    else:
        print "Unknown command"
        print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
