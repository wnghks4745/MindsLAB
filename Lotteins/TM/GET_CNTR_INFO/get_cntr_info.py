#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-19, modification: 2018-02-25"

###########
# imports #
###########
import os
import sys
import time
import requests
import cx_Oracle
import traceback
from datetime import datetime
from cfg.config import CONFIG, DB_CONFIG
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


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = cx_Oracle.makedsn(
            DB_CONFIG['host'],
            DB_CONFIG['port'],
            sid=DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
            DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_target(self):
        query = """
            SELECT
                POLI_NO,
                CTRDT,
                CNTR_COUNT,
                IP_DCD,
                QA_STTA_PRGST_CD
            FROM
                TB_TM_CNTR_INFO
            WHERE 1=1
                AND HTTP_TRANS_CD = '02'
                AND ( 0=1
                    OR QA_STTA_PRGST_CD = '02'
                    OR QA_STTA_PRGST_CD = '12'
                    OR QA_STTA_PRGST_CD = '13'
                )
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def update_http_status(self, **kwargs):
        try:
            sql = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    HTTP_TRANS_CD = :1,
                    LST_CHGP_CD = 'TM_HTTP',
                    LST_CHG_PGM_ID = 'TM_HTTP',
                    LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND POLI_NO = :2
                    AND CTRDT = :3
                    AND CNTR_COUNT = :4
            """
            bind = (
                kwargs.get('http_trans_cd'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
                kwargs.get('cntr_count')
            )
            self.cursor.execute(sql, bind)
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


def get_cntr_info(logger, oracle, poli_no, qa_stta_prgst_cd, ip_dcd, ctrdt, cntr_count):
    """
    http requests get cntr info
    :param      logger:                 Logger
    :param      oracle:                 Oracle
    :param      poli_no:                POLI_NO(증서번호)
    :param      qa_stta_prgst_cd:       QA_STTA_PRGST_CD(QA_STTA 상태코드)
    :param      ip_dcd:                 IP_DCD(보험상품구분코드)
    :param      ctrdt:                  CTRDT(청약일자)
    :param      cntr_count:             CNTR_COUNT(심하회차)
    :return:
    """
    bjgb = ''
    if ip_dcd == '01':
        bjgb = 'L'
    elif ip_dcd == '02':
        bjgb = 'O'
    elif ip_dcd == '03':
        bjgb = 'A'
    url = CONFIG['http_url']
    params = {
        'bjgb': bjgb,
        'polno': poli_no,
        'sttstatus': qa_stta_prgst_cd,
        'cntr_count': cntr_count
    }
    try:
        res = requests.get(url, params=params, timeout=CONFIG['requests_timeout'])
        oracle.update_http_status(
            http_trans_cd='01',
            poli_no=poli_no,
            ctrdt=ctrdt,
            cntr_count=cntr_count
        )
        logger.info('http status send -> {0}'.format(res.url))
    except Exception:
        logger.error('Fail http status send -> {0}'.format(poli_no))


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return                 SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
                sql = Oracle(logger)
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
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
    # Connect db
    try:
        oracle = connect_db(logger, 'Oracle')
        if not oracle:
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("---------- Can't connect db ----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        results = oracle.select_target()
        for target in results:
            poli_no = target[0].strip()
            ctrdt = target[1].strip()
            cntr_count = target[2].strip()
            ip_dcd = target[3].strip()
            qa_stta_prgst_cd = target[4].strip()
            get_cntr_info(logger, oracle, poli_no, qa_stta_prgst_cd, ip_dcd, ctrdt, cntr_count)
        if len(results) > 0:
            logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    except Exception:
        exc_info = traceback.format_exc()
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
    This is a program that http url get cntr info
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
