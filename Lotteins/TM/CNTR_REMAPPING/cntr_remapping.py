#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-03-12, modification: 2018-03-14"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
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
UPDATE_CNT = 0

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

    def update_qa_stta_prgst_cd(self, start_target_date, end_target_date):
        global UPDATE_CNT
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    QA_STTA_PRGST_CD = '00',
                    TA_REQ_DTM = SYSDATE
                WHERE 1=1
                    AND RGST_DTM BETWEEN TO_DATE(:1, 'YYYY-MM-DD HH24:MI')
                                    AND TO_DATE(:2, 'YYYY-MM-DD HH24:MI')
                    AND ( 1=0
                        OR QA_STTA_PRGST_CD = '01'
                        OR QA_STTA_PRGST_CD = '02'
                        OR QA_STTA_PRGST_CD = '03'
                        OR QA_STTA_PRGST_CD = '12'
                        OR QA_STTA_PRGST_CD = '13'
                        OR QA_STTA_PRGST_CD = '90'
                    )
            """
            bind = (
                start_target_date,
                end_target_date,
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                UPDATE_CNT = self.cursor.rowcount
                self.conn.commit()
                return True
            else:
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
        ts = time.time()
        dt = datetime.fromtimestamp(ts) - timedelta(minutes=CONFIG['start_time'])
        ldt = dt - timedelta(minutes=CONFIG['time_range'])
        start_target_date = ldt.strftime('%Y-%m-%d %H:%M')
        end_target_date = dt.strftime('%Y-%m-%d %H:%M')
        logger.info("Target time {0} ~ {1}".format(start_target_date, end_target_date))
        oracle.update_qa_stta_prgst_cd(start_target_date, end_target_date)
        logger.info("END.. Start time = {0}, The time required = {1}, update count = {2}".format(
            ST, elapsed_time(DT), UPDATE_CNT))
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
    This is a program that cntr remapping
    :return:
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
