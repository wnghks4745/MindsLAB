#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-01, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
from operator import itemgetter
from datetime import datetime, timedelta
from lib.iLogger import set_logger_period_of_time
from cfg.config import CONFIG
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from common.config import Config
from common.openssl import decrypt_string

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
CREATOR_ID = 'CS_BAT'


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.conf = Config()
        self.conf.init('biz.conf')
        self.dsn_tns = self.conf.get('oracle.dsn').strip()
        passwd = decrypt_string(self.conf.get('oracle.passwd'))
        self.conn = cx_Oracle.connect(
            self.conf.get('oracle.user'),
            passwd,
            self.dsn_tns
        )
        self.logger = logger
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def insert_cs_statistics_dashbrd_tb(self, call_date):
        self.logger.info('1. Insert CS STATISTICS DASHBRD TB')
        dev = ''
        # dev = '@D_KBLUAT_ZREAD'
        query = """
        INSERT INTO CS_STATISTICS_DASHBRD_TB
        (
            SUM_DATE,
            SUM_CNT,
            CALL_TYPE_CODE,
            CNTC_USER_DEPART_C,
            CNTC_USER_PART_C,
            CNTC_USER_DEPART_NM,
            CNTC_USER_PART_NM
        )
        SELECT
            TO_DATE(:1, 'YYYY/MM/DD') AS SUM_DATE,
            NVL(COUNT(CALL_ID), 0) AS SUM_CNT,
            CALL_TYPE_CODE,
            CNTC_USER_DEPART_C,
            CNTC_USER_PART_C,
            (
                SELECT
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'SS08'
                    AND CODE = CNTC_USER_DEPART_C
                    AND USE_YN = 'Y'
            ) AS CNTC_USER_DEPART_NM,
            (
                SELECT 
                    CODE_NM
                FROM
                    ZCS.TCL_CO_CODE{0}
                WHERE 1=1
                    AND DOMAIN_CD = 'SS09'
                    AND CODE_VAL_1 = 10
                    AND CODE = CNTC_USER_PART_C
                    AND USE_YN = 'Y'
            ) AS CNTC_USER_PART_NM
        FROM
            CM_CALL_META_TB
        WHERE 1=1
            AND CALL_DATE = TO_DATE(:1, 'YYYY/MM/DD')
            AND NVL(CNTC_USER_DEPART_C, 10) <> '40'
        GROUP BY
            CALL_TYPE_CODE,
            CNTC_USER_DEPART_C,
            CNTC_USER_PART_C
        UNION ALL
            SELECT
                TO_DATE(:1, 'YYYY/MM/DD'), 
                0,
                '-',
                '-',
                '-',
                '-',
                '-'
            FROM
                DUAL
        """.format(dev)
        bind = (
            call_date,
        )
        self.cursor.execute(query, bind)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_cs_statistics_dashbrd_tb(self, call_date):
        self.logger.info("0. Delete CS STATISTICS DASHBRD TB")
        try:
            query = """
                DELETE FROM
                    CS_STATISTICS_DASHBRD_TB
                WHERE 1=1
                    AND SUM_DATE = TO_DATE(:1, 'YYYY-MM-DD')
            """
            bind = (
                call_date,
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


def connect_db(logger):
    """
    Connect database
    :param          logger:         Logger
    :return                         SQL Object
    """
    # Connect DB
    logger.debug('Connect Oracle DB ...')
    sql = False
    for cnt in range(1, 4):
        try:
            os.environ["NLS_LANG"] = ".AL32UTF8"
            sql = Oracle(logger)
            logger.debug("Success connect Oracle DB ...")
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect Oracle DB, retrying count = {0}".format(cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def processing(target_call_date):
    """
    Processing
    :param          target_call_date:       Target call date
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger_period_of_time(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger)
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
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        logger.info("START.. Call date = {0}".format(call_date))
        oracle.delete_cs_statistics_dashbrd_tb(call_date)
        oracle.insert_cs_statistics_dashbrd_tb(call_date)
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
    logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    logger.info("-" * 100)
    oracle.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(target_call_date):
    """
    This is a program that extract frequent keyword
    :param          type_code:               CALL_TYPE_CODE
    :param          target_call_date:        Target call date
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(target_call_date)
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if len(sys.argv[1].strip()) != 8:
            print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} 20180416".format(sys.argv[0])
            sys.exit(1)
        try:
            int(sys.argv[1])
        except Exception:
            print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip())
    elif len(sys.argv) == 1:
        main((datetime.fromtimestamp(time.time()) - timedelta(days=1)).strftime('%Y%m%d'))
    else:
        print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
        print "ex) python {0} 20180416".format(sys.argv[0])
        sys.exit(1)
