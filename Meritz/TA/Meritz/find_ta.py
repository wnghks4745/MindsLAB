#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-11-01, modification: 2017-11-29"


###########
# imports #
###########
import os
import sys
import time
import shutil
import traceback
from datetime import datetime
from cfg.config import FIND_TA_CONFIG
from lib.meritz_enc import decrypt
from bin.lib.iLogger import set_logger

sys.path.append('/data1/MindsVOC/TA/LA/bin')
import TA_process

#############
# constants #
#############
DT = ''
TARGET_DIR_PATH = ''
TARGET_DIR_NAME = ''
DELETE_FILE_LIST = list()


###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#######
# def #
#######
def check_file(name_form, file_name):
    """
    Extract need TA file
    :param      name_form:      Check file name form
    :param      file_name:      Input file name
    :return:                    True or False
    """
    return file_name.endswith(name_form)


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param      logger:                 Logger
    :param      delete_file_path:       Input path
    """
    if os.path.exists(delete_file_path):
        try:
            if os.path.isfile(delete_file_path):
                logger.debug('delete file -> {0}'.format(delete_file_path))
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                logger.debug('delete directory -> {0}'.format(delete_file_path))
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info('3. Delete garbage file')
    for delete_file in DELETE_FILE_LIST:
        try:
            logger.debug('del_garbage : {0}'.format(delete_file))
            del_garbage(logger, delete_file)
        except Exception as e:
            logger.error(e)
            continue


def analysis_summary_file(logger, dir_path):
    """
    Summary file analysis
    :param      logger:         Logger
    :param      dir_path:       Directory path
    """
    logger.info('2. Anaysis summary file')
    search_file_path = '{0}/{1}_hmd_search.tsv'.format(TARGET_DIR_PATH, TARGET_DIR_NAME)
    search_file = open(search_file_path, 'r').readlines()
    file_name_line_number_dic = dict()
    data_list = list()
    dir_name = os.path.basename(dir_path)
    for line in search_file:
        logger.debug(line)
        line = line.split('\t')
        if line[4] == 'none':
            continue
        file_name_line_number = '{0}_{1}'.format(line[1], line[2])
        if file_name_line_number in file_name_line_number_dic:
            continue
        file_name_line_number_dic[file_name_line_number] = '1'
        data = '{0}\t{1}\t{2}'.format(dir_name, line[1], line[6])
        data_list.append(data)
    analysis_result_dir_path = '{0}'.format(FIND_TA_CONFIG['analysis_result_path'])
    analysis_result_file_path = '{0}/{1}.txt'.format(analysis_result_dir_path, DT[:8])
    # file 작성 및 출력
    for data in data_list:
        if not os.path.exists(analysis_result_dir_path):
            os.makedirs(analysis_result_dir_path)
        logger.info('data = {0}'.format(data))
        result_file = open(analysis_result_file_path, 'a')
        print >> result_file, data
        result_file.close()


def execute_ta_process(logger, dir_path):
    """
    Do TA process
    :param      logger:         Logger
    :param      dir_path:       Directory path
    """
    logger.info('1. Do TA_process')
    # Bring the matrix file
    matrix_file_path = FIND_TA_CONFIG['matrix_file_path']
    # Make output directory
    hmd_dir_path = '{0}/HMD'.format(TARGET_DIR_PATH)
    if not os.path.exists(hmd_dir_path):
        os.makedirs(hmd_dir_path)
    # Copy target date
    w_ob = os.walk(dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file('_trx_updated.hmd.txt.enc', file_name) or check_file('_trx_updated.hmd.txt', file_name):
                shutil.copy(os.path.join(dir_path, file_name), hmd_dir_path)
    decrypt(hmd_dir_path)
    TA_process.func_FindHMD(TARGET_DIR_NAME, matrix_file_path, int(FIND_TA_CONFIG['thread']))


def processing(dir_path):
    """
    This is function that TA process
    :param      dir_path:       Path of directory
    """
    global TARGET_DIR_PATH
    global TARGET_DIR_NAME
    global DELETE_FILE_LIST
    cnt = 0
    # Determine temp directory name to be used in script
    while True:
        TARGET_DIR_PATH = '{0}/data/find_ta_temp_directory_{1}'.format(FIND_TA_CONFIG['ta_path'], cnt)
        if not os.path.exists(TARGET_DIR_PATH):
            os.makedirs(TARGET_DIR_PATH)
            TARGET_DIR_NAME = os.path.basename(TARGET_DIR_PATH)
            DELETE_FILE_LIST.append(TARGET_DIR_PATH)
            break
        cnt += 1
    # Determining log_name
    log_name = '{0}_{1}'.format(FIND_TA_CONFIG['log_name'], cnt)
    # Add logging
    logger_args = {
        'base_path': FIND_TA_CONFIG['log_dir_path'],
        'log_file_name': log_name,
        'log_level': FIND_TA_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info('-' * 100)
    logger.info('Start TA')
    try:
        # 1. Execute TA_process
        execute_ta_process(logger, dir_path)
        # 2. Analysis summary file
        analysis_summary_file(logger, dir_path)
        # 3. Delete garbage file
        delete_garbage_file(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error('----------    ERROR   ----------')
        delete_garbage_file(logger)
        sys.exit(1)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job_list):
    """
    This is a program that execute TA for re-check
    :param      job_list:       dir_path_list
    """
    global DT
    ts = time.time()
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(job_list[0])
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "----------   ERROR   ----------"
        sys.exit(1)