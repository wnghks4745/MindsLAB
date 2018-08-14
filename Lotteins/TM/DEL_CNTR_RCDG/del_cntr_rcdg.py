#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-27, modification: 2018-02-27"

###########
# imports #
###########
import os
import sys
import time
import argparse
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
ST = ''
DT = ''
DELETE_CNT = 0


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
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_target_list(self, ctrdt):
        query = """
            SELECT DISTINCT
                REC_ID,
                RFILE_NAME
            FROM
                TB_TM_CNTR_RCDG_INFO
            WHERE 1=1
                AND CTRDT = :1
        """
        bind = (ctrdt,)
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_nqa_stta_prgst_cd(self, **kwargs):
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
            kwargs.get('rec_id'),
            kwargs.get('rfile_name')
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def delete_tb_tm_cntr_rcdg_info(self, **kwargs):
        global DELETE_CNT
        try:
            query = """
                DELETE
                    TB_TM_CNTR_RCDG_INFO
                WHERE 1=1
                    AND CTRDT = :1
                    AND REC_ID = :2
                    AND RFILE_NAME = :3
            """
            bind = (
                kwargs.get('ctrdt'),
                kwargs.get('rec_id'),
                kwargs.get('rfile_name')
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                DELETE_CNT += self.cursor.rowcount
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
    :return:                SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ['NLS_LANG'] = '.KO16MSWIN949'
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


def processing(ctrdt):
    """
    Processing
    :param      ctrdt:      CTRDT(청약 일자)
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_file_name'],
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
        target_list = oracle.select_target_list(ctrdt)
        for target in target_list:
            rec_id = target[0]
            rfile_name = target[1]
            nqa_stta_prgst_cd = oracle.select_nqa_stta_prgst_cd(
                rec_id=rec_id,
                rfile_name=rfile_name
            )
            if nqa_stta_prgst_cd:
                nqa_stta_prgst_cd = nqa_stta_prgst_cd[0]
            if nqa_stta_prgst_cd == '90' or nqa_stta_prgst_cd is False:
                logger.info('\tDELETE ROW -> CTRDT : {0} REC_ID : {1} RFILE_NAME : {2}'.format(ctrdt, rec_id, rfile_name))
                oracle.delete_tb_tm_cntr_rcdg_info(
                    ctrdt=ctrdt,
                    rec_id=rec_id,
                    rfile_name=rfile_name
                )
        if DELETE_CNT > 0:
            logger.info("END.. Start time = {0}, The time required = {1}, Delete count = {2}".format(
                ST, elapsed_time(DT), DELETE_CNT))
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
def main(args):
    """
    This is a program that delete not exists rcdg file in TB_TM_CNTR_RCDG_INFO
    :param      args:       Arguments
    """
    global ST
    global DT
    try:
        ts = time.time()
        ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
        DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        processing(args.ctrdt)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store', dest='ctrdt', default=False, required=True, type=str, help='CTRDT')
    arguments = parser.parse_args()
    main(arguments)
