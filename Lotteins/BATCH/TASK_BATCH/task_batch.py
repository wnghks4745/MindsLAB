#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-04, modification: 2018-05-04"

###########
# imports #
###########
import os
import sys
import glob
import time
import shutil
import traceback
import cx_Oracle
import collections
from datetime import datetime, timedelta
from cfg.config import CONFIG, ORACLE_DB_CONFIG
from lib.iLogger import set_logger

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

    def delete_data_to_outbound_monitoring_task_tb(self, today_date, delete_date):
        try:
            query = """
                DELETE
                    Outbound_Monitoring_Task_TB
                WHERE 1=0
                    OR TASK_DATE = TO_DATE(:1, 'YYYY/MM/DD')
                    OR TASK_DATE < TO_DATE(:2, 'YYYY/MM/DD')
            """
            bind = (
                today_date,
                delete_date,
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

    def insert_data_to_outbound_monitoring_task_tb(self, **kwargs):
        try:
            query = """
                INSERT INTO OUTBOUND_MONITORING_TASK_TB (
                        TASK_DATE,
                        CODE,
                        TASKNAME,
                        RUSER_ID,
                        RUSER_NAME,
                        CU_ID,
                        CU_NAME,
                        CU_NAME_HASH,
                        CU_NUMBER,
                        POLI_NO,
                        CREATED_DTM,
                        UPDATED_DTM,
                        CREATOR_ID,
                        UPDATOR_ID
                    )
                    VALUES (
                        TO_DATE(:1, 'YYYY/MM/DD'), :2, :3, :4, :5, :6, M2U_MASKING_NAME(:7), 
                        M2U_CRYPTO_SHA256_ENCRYPT(:8), :9, :10, SYSDATE, SYSDATE, :11, :11
                    )
            """
            bind = (
                kwargs.get('task_date'),
                kwargs.get('code'),
                kwargs.get('taskname'),
                kwargs.get('ruser_id'),
                kwargs.get('ruser_name'),
                kwargs.get('cu_id'),
                kwargs.get('cu_name'),
                kwargs.get('cu_name'),
                kwargs.get('cu_number'),
                kwargs.get('poli_no'),
                kwargs.get('regp_cd'),
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


def error_process(logger, file_path):
    """
    Error file process
    :param      logger:         Logger
    :param      file_path:      Org file path
    """
    global ERROR_CNT
    ERROR_CNT += 1
    try:
        logger.error("Error process. [file = {0}]".format(file_path))
        error_dir_path = '{0}/error_data/{1}/{2}/{3}'.format(os.path.dirname(file_path), DT[:4], DT[4:6], DT[6:8])
        if not os.path.exists(error_dir_path):
            os.makedirs(error_dir_path)
        error_org_file_path = '{0}/{1}'.format(error_dir_path, os.path.basename(file_path))
        if os.path.exists(error_org_file_path):
            del_garbage(logger, error_org_file_path)
        logger.error('Error org file move {0} -> {1}'.format(file_path, error_dir_path))
        shutil.move(file_path, error_dir_path)
    except Exception:
        exc_info = traceback.format_exc()
        logger.critical("Critical error {0}".format(exc_info))


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


def upload_data_to_db(logger, oracle, org_file_dict):
    """
    Upload data to database
    :param      logger:             Logger
    :param      oracle:             Oracle
    :param      org_file_dict:      Organization file dictionary
    """
    global UPLOAD_CNT
    logger.debug("Task data upload to DB")
    for table_name, file_path_list in org_file_dict.items():
        for file_path in file_path_list:
            try:
                logger.debug('Open file. [{0}]'.format(file_path))
                file_date = os.path.basename(file_path)[:8]
                org_file = open(file_path, 'r')
                try:
                    datetime.strptime(file_date, '%Y%m%d')
                except Exception:
                    logger.error("file date in file name is wrong : {0}".format(file_date))
                    raise Exception("file date in file name is wrong : {0}".format(file_date))
                if table_name == 'Outbound_Monitoring_Task_TB':
                    today_date = '{0}/{1}/{2}'.format(file_date[:4], file_date[4:6], file_date[6:8])
                    delete_datetime = (datetime.strptime(file_date, '%Y%m%d') - timedelta(
                        days=int(CONFIG['delete_date']))).strftime('%Y%m%d')
                    delete_date = '{0}/{1}/{2}'.format(delete_datetime[:4], delete_datetime[4:6], delete_datetime[6:8])
                    logger.debug('DELETE today date = {0}'.format(today_date))
                    logger.debug('DELETE {0} before date = {1}'.format(CONFIG['delete_date'], delete_date))
                    oracle.delete_data_to_outbound_monitoring_task_tb(today_date, delete_date)
                    for line in org_file:
                        line_list = line.split('|')
                        logger.debug('INSERT {0} -> {1}'.format(table_name, line_list))
                        oracle.insert_data_to_outbound_monitoring_task_tb(
                            task_date=today_date,
                            code=line_list[0].strip(),
                            taskname=line_list[1].strip(),
                            ruser_id=line_list[2].strip(),
                            ruser_name=line_list[3].strip(),
                            cu_id=line_list[4].strip()[:7],
                            cu_name=line_list[5].strip(),
                            cu_number=line_list[6].strip(),
                            poli_no=line_list[7].strip(),
                            regp_cd='CS_BAT'
                        )
                        UPLOAD_CNT += 1
                else:
                    logger.error('Table name is not exists -> {0}'.format(table_name))
                org_file.close()
                org_output_dir_path = '{0}/{1}/{2}/{3}'.format(CONFIG['org_output_path'], DT[:4], DT[4:6], DT[6:8])
                org_output_file_path = os.path.join(org_output_dir_path, os.path.basename(file_path))
                if not os.path.exists(org_output_dir_path):
                    os.makedirs(org_output_dir_path)
                if os.path.exists(org_output_file_path):
                    del_garbage(logger, org_output_file_path)
                logger.debug("Move task file to output directory")
                shutil.move(file_path, org_output_dir_path)
                oracle.conn.commit()
            except Exception:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                oracle.conn.rollback()
                error_process(logger, file_path)
                continue


def make_file_dict(logger):
    """
    Make file path dict
    :param      logger:     Logger
    :return:                file path dict
    """
    logger.debug('Make file list')
    target_file_dict = CONFIG['org_file_path_list']
    logger.debug("Target file list = {0}".format(", ".join(target_file_dict.keys())))
    # 파일 존재 유무 확인
    output_file_dict = dict()
    for target_table, target_file_path in target_file_dict.items():
        target_dir_path = os.path.dirname(target_file_path)
        target_file_name = os.path.basename(target_file_path)
        target_file_path_list = glob.glob('{0}/*{1}'.format(target_dir_path, target_file_name))
        for glob_target_file_path in target_file_path_list:
            if os.path.exists(glob_target_file_path):
                if not target_table in output_file_dict:
                    output_file_dict[target_table] = [glob_target_file_path]
                else:
                    output_file_dict[target_table].append(glob_target_file_path)
            else:
                logger.error('Path is not exist -> {0}'.format(glob_target_file_path))
    return output_file_dict


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
                sql = Oracle(logger)
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
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    # connect db
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
        # Make org file dict
        task_file_dict = make_file_dict(logger)
        # Upload org data to db
        upload_data_to_db(logger, oracle, task_file_dict)
        logger.info(
            "Total TASK target count = {0}, upload count = {1}, error count = {2}, The time required = {3}".format(
                len(task_file_dict), UPLOAD_CNT, ERROR_CNT, elapsed_time(DT)))
        logger.debug("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
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
    This is a program that update organization data to Oracle DB
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
        print exc_info


if __name__ == '__main__':
    main()
