#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-02, modification: 2018-03-15"

###########
# imports #
###########
import os
import sys
import time
import requests
import traceback
import cx_Oracle
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

    def select_qa_target_from_cntr_info(self):
        try:
            query = """
                SELECT
                    POLI_NO,
                    CTRDT,
                    CNTR_COUNT,
                    IP_DCD,
                    QA_STTA_PRGST_CD
                FROM
                    TB_TM_CNTR_INFO
                WHERE 1=0
                    OR QA_STTA_PRGST_CD = '01'
                    OR QA_STTA_PRGST_CD = '02'
                    OR QA_STTA_PRGST_CD = '82'
            """
            self.cursor.execute(query, )
            result = self.cursor.fetchall()
            if result is bool:
                return False
            if result:
                return result
            return False
        except Exception:
            exc_info = traceback.format_exc()
            raise Exception(exc_info)

    def select_rec_info_from_cntr_rcdg_info(self, poli_no, ctrdt, cntr_count):
        try:
            query = """
                SELECT
                    REC_ID,
                    RFILE_NAME
                FROM
                    TB_TM_CNTR_RCDG_INFO
                WHERE 1=1
                    AND POLI_NO = :1
                    AND CTRDT = :2
                    AND CNTR_COUNT = :3
            """
            bind = (
                poli_no,
                ctrdt,
                cntr_count
            )
            self.cursor.execute(query, bind)
            result = self.cursor.fetchall()
            if result is bool:
                return False
            if result:
                return result
            return False
        except Exception:
            exc_info = traceback.format_exc()
            raise Exception(exc_info)

    def select_nqa_stta_prgst_cd_from_rcdg_info(self, rec_id, rfile_name):
        try:
            query = """
                SELECT
                    NQA_STTA_PRGST_CD
                FROM
                    TB_TM_STT_RCDG_INFO
                WHERE 1=1
                    AND REC_ID = :1
                    AND RFILE_NAME = :2
            """
            bind = (
                rec_id,
                rfile_name
            )
            self.cursor.execute(query, bind)
            result = self.cursor.fetchall()
            if result is bool:
                return False
            if result:
                return result
            return False
        except Exception:
            exc_info = traceback.format_exc()
            raise Exception(exc_info)

    def update_qa_stta_prgst_cd(self, status, poli_no, ctrdt, cntr_count):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    QA_STTA_PRGST_CD = :1
                WHERE 1=1
                    AND POLI_NO = :2
                    AND CTRDT = :3
                    AND CNTR_COUNT = :4
            """
            bind = (
                status,
                poli_no,
                ctrdt,
                cntr_count
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
            self.disconnect()
            raise Exception(traceback.format_exc())

    def update_ta_cmdtm(self, poli_no, ctrdt, cntr_count):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    TA_CMDTM = SYSDATE
                WHERE 1=1
                    AND POLI_NO = :1
                    AND CTRDT = :2
                    AND CNTR_COUNT = :3
            """
            bind = (
                poli_no,
                ctrdt,
                cntr_count
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
            self.disconnect()
            raise Exception(traceback.format_exc())

    def update_http_status(self, **kwargs):
        try:
            sql = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    HTTP_TRANS_CD = :1,
                    LST_CHGP_CD = 'TM_CT_UP',
                    LST_CHG_PGM_ID = 'TM_CT_UP',
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


def get_cntr_info(logger, oracle, poli_no, status, ip_dcd, ctrdt, cntr_count):
    """
    http requests get cntr info
    :param      logger:         Logger
    :param      oracle:         Oracle
    :param      poli_no:        POLI_NO(증서번호)
    :param      status:
    :param      ip_dcd:         IP_DCD(보험상품구분코드)
    :param      ctrdt:          CTRDT(청약일자)
    :param      cntr_count:     CNTR_COUNT(심하회차)
    :return:
    """
    bjgb = ''
    if ip_dcd == '01' or ip_dcd == '04':
        bjgb = 'L'
    elif ip_dcd == '02':
        bjgb = 'O'
    elif ip_dcd == '03':
        bjgb = 'A'
    url = CONFIG['http_url']
    params = {
        'bjgb': bjgb,
        'polno': poli_no,
        'sttstatus': status,
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
        logger.info('\thttp status send -> {0}'.format(res.url))
    except Exception:
        logger.error('\tFail http status send -> {0}'.format(poli_no))
        oracle.update_http_status(
            http_trans_cd='02',
            poli_no=poli_no,
            ctrdt=ctrdt,
            cntr_count=cntr_count
        )


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
        sql = connect_db(logger, 'Oracle')
        result = sql.select_qa_target_from_cntr_info()
        if result:
            logger.debug("Update target QA_STT_PRGST_CD('01', '02', '82') count = {0}".format(len(result)))
            flag = False
            for item in result:
                nqa_stta_prgst_cd_list = list()
                poli_no = item[0]
                ctrdt = item[1]
                cntr_count = item[2]
                ip_dcd = item[3]
                qa_stta_prgst_cd = item[4]
                logger.debug('-' * 100)
                logger.debug("Select REC_ID, RFILE_NAME from TB_TM_CNTR_RCDG_INFO")
                rec_info_result = sql.select_rec_info_from_cntr_rcdg_info(poli_no, ctrdt, cntr_count)
                if rec_info_result:
                    for rec_info_item in rec_info_result:
                        rec_id = rec_info_item[0]
                        rfile_name = rec_info_item[1]
                        logger.debug("REC_ID = {0}, RFILE_NAME = {1}".format(rec_id, rfile_name))
                        logger.debug("Select NQA_STTA_PRGST_CD from TB_TM_CNTR_RCDG_INFO")
                        nqa_stta_prgst_cd_result = sql.select_nqa_stta_prgst_cd_from_rcdg_info(rec_id, rfile_name)
                        if nqa_stta_prgst_cd_result:
                            for nqa_stta_prgst_cd_item in nqa_stta_prgst_cd_result:
                                nqa_stta_prgst_cd = nqa_stta_prgst_cd_item[0]
                                if nqa_stta_prgst_cd not in nqa_stta_prgst_cd_list:
                                    nqa_stta_prgst_cd_list.append(nqa_stta_prgst_cd)
                        else:
                            logger.error("No data in TB_TM_CNTR_RCDG_INFO")
                    logger.debug("NQA_STTA_PRGST_CD_LIST = {0}".format(nqa_stta_prgst_cd_list))
                    if len(nqa_stta_prgst_cd_list) == 1 and '13' in nqa_stta_prgst_cd_list:
                        new_qa_stta_prgst_cd = '03'
                    else:
                        cd_list = ['02', '12', '90']
                        set_cnt = len(set(cd_list) & set(nqa_stta_prgst_cd_list))
                        new_qa_stta_prgst_cd = '01' if set_cnt == 0 else '02'
                    if qa_stta_prgst_cd != new_qa_stta_prgst_cd:
                        logger.info("POLI_NO = {0}, CTRDT = {1}, CNTR_COUNT = {2}, QA_STTA_PRGST_CD = {3}".format(
                            poli_no, ctrdt, cntr_count, qa_stta_prgst_cd
                        ))
                        logger.info("Update QA_STTA_PRGST_CD {0} -> {1}".format(qa_stta_prgst_cd, new_qa_stta_prgst_cd))
                        sql.update_qa_stta_prgst_cd(new_qa_stta_prgst_cd, poli_no, ctrdt, cntr_count)
                        get_cntr_info(logger, oracle, poli_no, new_qa_stta_prgst_cd, ip_dcd, ctrdt, cntr_count)
                        flag = True
                else:
                    logger.error("No data in TB_TM_CNTR_RCDG_INFO")
                    logger.info("Update QA_STTA_PRGST_CD {0} -> '90'".format(qa_stta_prgst_cd))
                    sql.update_qa_stta_prgst_cd('90', poli_no, ctrdt, cntr_count)
                    sql.update_ta_cmdtm(poli_no, ctrdt, cntr_count)
                    get_cntr_info(logger, oracle, poli_no, '90', ip_dcd, ctrdt, cntr_count)
            if flag:
                logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
                logger.info("-" * 100)
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
    This is a program that update QA_STTA_PRGST_CD
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
