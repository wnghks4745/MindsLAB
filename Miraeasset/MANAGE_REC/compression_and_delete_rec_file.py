#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-10-10, modification: 2018-01-11"

###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import argparse
import traceback
import subprocess
from datetime import datetime, timedelta
from cfg.config import CONFIG
from lib.iLogger import set_logger
from lib.openssl import encrypt_file, decrypt_file

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DELETE_CNT = 0
COMPRESSION_CNT = 0

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


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param          logger:         Logger
    :param          cmd:            Command
    :return                         Standard out
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


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


def compression_rec_file(logger, ts, target_info_dict):
    """
    Compression record file
    :param          logger:                     Logger
    :param          ts:                         System time
    :param          target_info_dict:           Target information dictionary
    """
    global COMPRESSION_CNT
    # Compression record file
    logger.info("1. Compression record file")
    target_dir_path = target_info_dict.get('directory_path')
    if target_dir_path[-1] == "/":
        target_dir_path = target_dir_path[:-1]
    compression_file_date = int(target_info_dict.get('compression_file_date'))
    tmp_date = (datetime.fromtimestamp(ts) - timedelta(days=compression_file_date)).strftime('%Y%m%d')
    target_dir_path += "/{0}".format(tmp_date)
    logger.info("Target directory path : {0}".format(target_dir_path))
    if not os.path.exists(target_dir_path):
        logger.info("Target directory not existed")
        return
    enc = target_info_dict.get('enc')
    w_ob = os.walk(target_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            try:
                file_path = os.path.join(dir_path, file_name)
                if not os.path.exists(file_path):
                    logger.debug("Not existed -> {0}".format(file_path))
                    continue
                ts = time.time()
                dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
                if not file_name.startswith('comp_'):
                    logger.info('Compression file [{0}]'.format(file_path))
                    if file_name.endswith('.enc'):
                        decrypt_file([file_path])
                        tmp_file_name = file_name[:-4].replace('.', '_')
                    else:
                        tmp_file_name = file_name.replace('.', '_')
                    tmp_file_path = file_path if not file_path.endswith('enc') else file_path[:-4]
                    s16_file_path = "{0}.s16".format(os.path.join(dir_path, tmp_file_name))
                    wav_file_path = "{0}/comp_{1}.wav".format(dir_path, tmp_file_name)
                    os.rename(tmp_file_path, s16_file_path)
                    sox_cmd = 'sox -r 8000 -c 1 {0} -r 8000 -c 1 -e gsm {1}'.format(s16_file_path, wav_file_path)
                    sub_process(logger, sox_cmd)
                    COMPRESSION_CNT += 1
                    del_garbage(logger, s16_file_path)
                    if enc:
                        rename_file_path = "{0}/encrypting_comp_{1}.wav".format(dir_path, tmp_file_name)
                        logger.debug("Rename [ {0} -> {1} ]".format(wav_file_path, rename_file_path))
                        os.rename(wav_file_path, rename_file_path)
                        logger.debug("Encrypt [ {0} ]".format(rename_file_path))
                        encrypt_file([rename_file_path])
                    logger.info('Compression file [{0}], The time required = {1}'.format(
                        file_path, elapsed_time(dt)))
            except Exception:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                continue


def delete_rec_file(logger, ts, target_info_dict):
    """
    Delete file
    :param          logger:                     Logger
    :param          ts:                         System time
    :param          target_info_dict:           Target information dictionary
    """
    global DELETE_CNT
    # Delete record file
    logger.info("2. Delete record file")
    target_dir_path = target_info_dict.get('directory_path')
    if target_dir_path[-1] == "/":
        target_dir_path = target_dir_path[:-1]
    w_ob = os.walk(target_dir_path)
    sub_dir_list = list()
    delete_file_date = int(target_info_dict.get('delete_file_date'))
    delete_target_date = datetime.fromtimestamp(ts) - timedelta(days=delete_file_date)
    logger.info("Delete time point is {0}".format(delete_target_date))
    for dir_path, sub_dirs, files in w_ob:
        sub_dir_list = sub_dirs
        break
    logger.info("Target subdirectory list = {0}".format(sub_dir_list))
    for dir_name in sub_dir_list:
        try:
            target_date = datetime.strptime(dir_name, '%Y%m%d')
            if delete_target_date > target_date:
                delete_dir_path = "{0}/{1}".format(target_dir_path, dir_name)
                DELETE_CNT += len(glob.glob("{0}/*".format(delete_dir_path)))
                del_garbage(logger, delete_dir_path)
                logger.info("Delete directory path : {0}".format(delete_dir_path))
        except Exception:
            continue


def processing(target_dir_list):
    """
    Processing
    :param          target_dir_list:            Target directory path
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['compression_log_file_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start compression and delete record file')
    ts = time.time()
    try:
        logger.info('Target directory list = {0}'.format(target_dir_list))
        for target_info_dict in target_dir_list:
            # Compression record file
            compression_rec_file(logger, ts, target_info_dict)
            # Delete record file
            delete_rec_file(logger, ts, target_info_dict)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
    logger.info("END.. Start time = {0}, The time required = {1}, compression count = {2}, delete count = {3}".format(
        st, elapsed_time(dt), COMPRESSION_CNT, DELETE_CNT))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

########
# main #
########


def main(args):
    """
    This is a program that compression(Make wav file) and delete record file
    :param          args:       Arguments
    """
    try:
        if args.target_dir_path:
            target_dir_list = [
                {
                    'directory_path': args.target_dir_path,
                    'compression_file_date': args.compression_file_date,
                    'delete_file_date': args.delete_file_date,
                    'enc': args.enc
                }
            ]
        else:
            target_dir_list = CONFIG['comp_target_directory_list']
        processing(target_dir_list)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store', dest='target_dir_path', default=False,
                        type=str, help='Directory path')
    parser.add_argument('-c', action='store', dest='compression_file_date', default=35, type=int,
                        help='Date of compression target')
    parser.add_argument('-del', action='store', dest='delete_file_date', default=70, type=int,
                        help='Date of delete target')
    parser.add_argument('-e', action='store', dest='enc', default=True, type=bool, help='Encryption [True/False]')
    arguments = parser.parse_args()
    main(arguments)
