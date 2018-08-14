#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-18, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import shutil
import traceback
import subprocess
from datetime import datetime
from distutils.dir_util import copy_tree
from cfg.config import CONFIG
from lib.iLogger import set_logger
from lib.openssl import decrypt_file, encrypt_file

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ERR_CNT = 0
RECORD_CNT = 0

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


def rollback_rec_file(logger, input_dir_path):
    """
    Rollback compression record file
    :param          logger:                     Logger
    :param          input_dir_path:             Input directory path
    """
    global ERR_CNT
    global RECORD_CNT
    # Rollback compression record file
    logger.info("Rollback compression record file")
    if input_dir_path[-1] == "/":
        input_dir_path = input_dir_path[:-1]
    logger.info("Target directory path : {0}".format(input_dir_path))
    temp_input_dir_path = "{0}/temp_{1}".format(CONFIG['temp_rollback_dir_path'], os.path.basename(input_dir_path))
    logger.info("Copy record file directory -> {0}".format(temp_input_dir_path))
    if os.path.exists(temp_input_dir_path):
        del_garbage(logger, temp_input_dir_path)
    copy_tree(input_dir_path, temp_input_dir_path)
    w_ob = os.walk(temp_input_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            target_file = os.path.join(dir_path, file_name)
            logger.info("Target file -> {0}".format(target_file))
            try:
                # Decrypt file
                if target_file.endswith('.enc'):
                    logger.debug("Decrypt {0}".format(target_file))
                    decrypt_file([target_file])
                decrypted_rec_file = target_file[:-4] if target_file.endswith('.enc') else target_file
                temp_rec_file_name = os.path.basename(decrypted_rec_file)
                temp_rec_file_name = temp_rec_file_name[5:] if temp_rec_file_name.startswith('comp_') else temp_rec_file_name
                temp_rec_file_name = temp_rec_file_name[:-4] if temp_rec_file_name.endswith('.wav') else temp_rec_file_name
                modified_rec_file_name = temp_rec_file_name.replace("_", ".").replace("__", ".")
                output_rec_file = "{0}/{1}".format(os.path.dirname(target_file), modified_rec_file_name)
                sox_cmd = "sox -t wav {0} -r 8000 -b 16 -t raw {1}".format(decrypted_rec_file, output_rec_file)
                sub_process(logger, sox_cmd)
                logger.debug("Encrypt {0}".format(output_rec_file))
                encrypt_file([output_rec_file])
                RECORD_CNT += 1
                del_garbage(logger, decrypted_rec_file)
                if not os.path.exists(CONFIG['decompression_dir_path']):
                    os.makedirs(CONFIG['decompression_dir_path'])
                if os.path.exists("{0}/{1}.enc".format(CONFIG['decompression_dir_path'], modified_rec_file_name)):
                    del_garbage(logger, "{0}/{1}.enc".format(CONFIG['decompression_dir_path'], modified_rec_file_name))
                if os.path.exists("{0}.enc".format(output_rec_file)):
                    shutil.move("{0}.enc".format(output_rec_file), CONFIG['decompression_dir_path'])
            except Exception:
                ERR_CNT += 1
                exc_info = traceback.format_exc()
                logger.error("Can't rollback record file -> {0}".format(target_file))
                logger.error(exc_info)
                continue
    del_garbage(logger, temp_input_dir_path)


def processing(input_dir_path):
    """
    Processing
    :param          input_dir_path:            Input directory path
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['rollback_log_file_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start rollback record file')
    try:
        rollback_rec_file(logger, input_dir_path)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
    logger.info("END.. Start time = {0}, The time required = {1}, Total record file count = {2}, sucess count = {3},"
                " fail count = {4}".format(st, elapsed_time(dt), RECORD_CNT + ERR_CNT, RECORD_CNT, ERR_CNT))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

########
# main #
########


def main(input_dir_path):
    """
    This is a program that rollback compression(Make wav file) record file
    :param          input_dir_path:       Input directory path
    """
    try:
        if not os.path.exists(input_dir_path):
            print "Not existed -> {0}".format(input_dir_path)
        if not os.path.isdir(input_dir_path):
            print "Not directory -> {0}".format(input_dir_path)
        else:
            processing(input_dir_path)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print "Unknown command"
        print "usage : python {0} [Target directory path]".format(sys.argv[0])
