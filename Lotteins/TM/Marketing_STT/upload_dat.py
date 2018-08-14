#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-24, modification: 2018-05-24"

###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import cx_Oracle
import traceback
import collections
from datetime import datetime, timedelta
from cfg.config import UPLOAD_CONFIG, ORACLE_DB_CONFIG
from lib.iLogger import set_logger
from lib.openssl import encrypt_file

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ""
DT = ""
UPLOAD_CNT = 0
ERROR_CNT = 0
DELETE_CNT = 0


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = cx_Oracle.makedsn(
            ORACLE_DB_CONFIG['host'],
            ORACLE_DB_CONFIG['port'],
            sid=ORACLE_DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            ORACLE_DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def check_marketing_rec_meta_tb(self, task_date, file_name):
        try:
            query = """
                SELECT
                    *
                FROM
                    MARKETING_REC_META_TB
                WHERE 1=1
                    AND TASK_DATE = TO_DATE(:1, 'YYYYMMDD')
                    AND FILE_NAME = :2
            """
            bind = (
                task_date,
                file_name,
            )
            self.cursor.execute(query, bind)
            result = self.cursor.fetchone()
            if result is bool:
                return False
            if result:
                return True
            return False
        except Exception:
            exc_info = traceback.format_exc()
            raise Exception(exc_info)

    def update_data_to_rec_meta(self, **kwargs):
        try:
            query = """
                UPDATE
                    MARKETING_REC_META_TB
                SET
                    CU_ID = :1,
                    CU_NAME = M2U_MASKING_NAME(:2),
                    CU_NAME_HASH = M2U_CRYPTO_SHA256_ENCRYPT(:3),
                    ASSOCIATOR_CD = :4,
                    ASSOCIATOR_NAME = :5,
                    STT_PRGST_CD = '00',
                    UPDATED_DTM = SYSDATE,
                    UPDATOR_ID = :6
                WHERE 1=1
                    AND TASK_DATE = TO_DATE(:7, 'YYYYMMDD')
                    AND FILE_NAME = :8
            """
            bind = (
                kwargs.get('cu_id'),
                kwargs.get('cu_name'),
                kwargs.get('cu_name'),
                kwargs.get('associator_cd'),
                kwargs.get('associator_name'),
                kwargs.get('creator_id'),
                kwargs.get('task_date'),
                kwargs.get('file_name'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_data_to_rec_meta(self, **kwargs):
        try:
            query = """
                INSERT INTO MARKETING_REC_META_TB
                (
                    TASK_DATE,
                    FILE_NAME,
                    CU_ID,
                    CU_NAME,
                    CU_NAME_HASH,
                    ASSOCIATOR_CD,
                    ASSOCIATOR_NAME,
                    CREATED_DTM,
                    UPDATED_DTM,
                    CREATOR_ID,
                    UPDATOR_ID,
                    STT_PRGST_CD
                )
                VALUES (
                    TO_DATE(:1, 'YYYYMMDD'), :2, :3, M2U_MASKING_NAME(:4), M2U_CRYPTO_SHA256_ENCRYPT(:5), :6, :7,
                    SYSDATE, SYSDATE, :8, :9, '00'
                )
            """
            bind = (
                kwargs.get('task_date'),
                kwargs.get('file_name'),
                kwargs.get('cu_id'),
                kwargs.get('cu_name'),
                kwargs.get('cu_name'),
                kwargs.get('associator_cd'),
                kwargs.get('associator_name'),
                kwargs.get('creator_id'),
                kwargs.get('creator_id'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            exc_info = traceback.format_exc()
            self.conn.rollback()
            raise Exception(exc_info)

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
    :param      logger:                 Logger
    :param      delete_file_path:       Input path
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


def upload_data_to_db(logger, oracle, checked_dat_data_dict):
    """
    Upload data to database
    :param      logger:         Logger
    :param      oracle:         Oracle
    :param      checked_dat_data_dict:      Checked dat data dictionary
    """
    global UPLOAD_CNT
    global ERROR_CNT
    logger.debug("dat data upload to DB")
    for file_path, dat_data_dict_list in checked_dat_data_dict.items():
        task_date = os.path.basename(file_path)[:8]
        for dat_data_dict in dat_data_dict_list:
            try:
                logger.debug("Target dat data -> TASK_DATE : {0}, FILE_NAME : {1}".format(
                    task_date, dat_data_dict['FILE_NAME']))
                if oracle.check_marketing_rec_meta_tb(task_date, dat_data_dict['FILE_NAME']):
                    oracle.update_data_to_rec_meta(
                        task_date=task_date,
                        file_name=dat_data_dict['FILE_NAME'],
                        cu_id=dat_data_dict['CU_ID'],
                        cu_name=dat_data_dict['CU_NAME'],
                        associator_cd=dat_data_dict['ASSOCIATOR_CD'],
                        associator_name=dat_data_dict['ASSOCIATOR_NAME'],
                        creator_id='MA_REC'
                    )
                else:
                    oracle.insert_data_to_rec_meta(
                        task_date=task_date,
                        file_name=dat_data_dict['FILE_NAME'],
                        cu_id=dat_data_dict['CU_ID'],
                        cu_name=dat_data_dict['CU_NAME'],
                        associator_cd=dat_data_dict['ASSOCIATOR_CD'],
                        associator_name=dat_data_dict['ASSOCIATOR_NAME'],
                        creator_id='MA_REC'
                    )
                oracle.conn.commit()
                UPLOAD_CNT += 1
            except Exception:
                exc_info = traceback.format_exc()
                logger.error('upsert error -> {0}'.format('|'.join(dat_data_dict.values())))
                logger.error(exc_info)
                oracle.conn.rollback()
                ERROR_CNT += 1
                continue
        call_date = os.path.basename(file_path)[:8]
        dat_output_dir_path = '{0}/{1}/{2}/{3}'.format(
            UPLOAD_CONFIG['dat_output_path'], call_date[:4], call_date[4:6], call_date[6:8])
        dat_output_file_path = os.path.join(dat_output_dir_path, os.path.basename(file_path))
        if not os.path.exists(dat_output_dir_path):
            os.makedirs(dat_output_dir_path)
        if os.path.exists(dat_output_file_path):
            del_garbage(logger, dat_output_file_path)
        logger.debug("Move dat file to output directory.")
        shutil.move(file_path, dat_output_dir_path)
        encrypt_file([dat_output_file_path])



def check_rec_file(logger, dir_path, checked_line_dict):
    """
    Check record file
    :param      logger:                 Logger
    :param      dir_path:               directory path
    :param      checked_line_dict:      Line dictionary
    :return:                            True or False
    """
    file_name = checked_line_dict['FILE_NAME']
    rec_file_path = "{0}/{1}.wav".format(dir_path, file_name)
    if not os.path.exists(rec_file_path):
        logger.error('Record file is not exist -> {0}'.format(rec_file_path))
        return False
    if os.stat(rec_file_path)[6] == 0:
        logger.error('Record file size is 0 -> {0}'.format(rec_file_path))
    return True


def check_dat_data(logger, line_list):
    """
    Check dat data
    :param      logger:         Logger
    :param      line_list:      Line list
    :return:                    True or False
    """
    dat_data_dict = {
        'CU_ID': line_list[0][:7].strip(),
        'CU_NAME': line_list[1].strip(),
        'FILE_NAME': os.path.splitext(line_list[2].strip())[0],
        'ASSOCIATOR_NAME': line_list[3].strip(),
        'ASSOCIATOR_CD': line_list[4].strip()
    }
    return dat_data_dict


def check_raw_data(logger, sorted_dat_list):
    """
    Check record file and dat data
    :param      logger:                 Logger
    :param      sorted_dat_list:        Dat file list
    :return:                            Checked dat data
    """
    global ERROR_CNT
    logger.debug("-" * 100)
    logger.debug("Check raw data (DAT, RECORD)")
    dat_data_dict = collections.OrderedDict()
    dict_list = list()
    for file_path in sorted_dat_list:
        try:
            logger.debug("Open dat file. [{0}]".format(file_path))
            dir_path = os.path.dirname(file_path)
            dat_file = open(file_path, 'r')
            dat_data = dat_file.readlines()
            dat_file.close()
            # Check dat data
            logger.debug("Check dat data.")
            for line in dat_data:
                line_list = line.strip().split("|")
                checked_line_dict = check_dat_data(logger, line_list)
                if not checked_line_dict:
                    logger.error('Line is wrong. [{0}]'.format(line))
                    ERROR_CNT += 1
                    continue
                # Check record file
#                logger.debug("Check record file.")
#                if not check_rec_file(logger, dir_path, checked_line_dict):
#                    logger.error('file is not exist line. [{0}]'.format(line))
#                    ERROR_CNT += 1
#                    continue
                dict_list.append(checked_line_dict)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            continue
        dat_data_dict[file_path] = dict_list
    return dat_data_dict


def make_dat_list(logger):
    """
    Make sorted dat file list
    :param          logger:         Logger
    :return:                        Sorted dat list
    """
    global DELETE_CNT
    logger.debug('Make dat list')
    target_dir_path = UPLOAD_CONFIG['dat_dir_path']
    logger.debug("Dat file directory = {0}".format(target_dir_path))
    # dat file list 생성
    file_list = list()
    if not os.path.exists(target_dir_path):
        err_str = "Dat file directory is not exist. -> {0}".format(target_dir_path)
        logger.error(err_str)
    logger.debug("Extract json file list from {0}".format(target_dir_path))
    file_list += glob.glob("{0}/*.dat".format(target_dir_path))
    ts = time.time()
    delete_date = (datetime.fromtimestamp(ts) - timedelta(days=7)).strftime('%Y%m%d')
    delete_file_list = glob.glob("{0}/{1}*.wav".format(target_dir_path, delete_date))
    for delete_file_path in delete_file_list:
        del_garbage(logger, delete_file_path)
        DELETE_CNT += 1
    # 최종 변경 시간 기준 녹취 파일 정렬
    logger.debug("dat file list, sorted by last modified time.")
    sorted_dat_list = sorted(file_list, key=os.path.getmtime, reverse=False)
    return sorted_dat_list


def connect_db(logger, db):
    """
    Connect database
    :param          logger:         Logger
    :param          db:             Database
    :return                         SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
                sql = Oracle(logger)
            elif db == 'MsSQL':
                sql = MSSQL(logger)
            else:
                logger.error("Unknown DB [{0}]".format(db))
                return False
            logger.debug("Success connect {0} DB ...".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0} DB, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def processing():
    """
    Processing
    """
    # Add logging
    logger_args = {
        'base_path': UPLOAD_CONFIG['log_dir_path'],
        'log_file_name': UPLOAD_CONFIG['log_name'],
        'log_level': UPLOAD_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger, 'Oracle')
        if not oracle:
            print "---------- Can't connect db ----------"
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- Can't connect db ----------"
        logger.error(exc_info)
        logger.error("---------- Can't connect db ----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        # Make dat file list
        sorted_dat_list = make_dat_list(logger)
        if len(sorted_dat_list) > 0:
            # Record file exist check
            checked_dat_data_dict = check_raw_data(logger, sorted_dat_list)
            # Upload dat data to db
            upload_data_to_db(logger, oracle, checked_dat_data_dict)
            logger.info(
                "Total dat target count = {0}, upload count = {1}, error count = {2}, delete count = {3} "
                "The time required = {4}".format(
                    len(sorted_dat_list), UPLOAD_CNT, ERROR_CNT, DELETE_CNT, elapsed_time(DT)))
        else:
            logger.debug("No dat file")
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        logger.error(exc_info)
        logger.error("---------- ERROR ----------")
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    oracle.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main():
    """
    This is a program that upload .dat data to Oracle DB
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        print(exc_info)


if __name__ == '__main__':
    main()
