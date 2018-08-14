#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-13, modification: 0000-00-00"

###########
# imports #
###########
import sys
import time
import MySQLdb
import argparse
import traceback
from datetime import datetime
from lib.iLogger import set_logger
from cfg.config import MYSQL_DB_CONFIG, LOG_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#########
# class #
#########


class MySQL(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['passwd'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_stt_prgst_cd(self, poli_no, ctrdt):
        query = """
            SELECT
                STT_PRGST_CD
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                 AND POLI_NO = %s
                 AND CTRDT = %s
        """
        bind = (
            poli_no,
            ctrdt,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def update_stt_prgst_cd(self, **kwargs):
        try:
            query = """
                UPDATE
                    TB_QA_STT_TM_CNTR_INFO
                SET
                    STT_PRGST_CD = %s
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('stt_prgst_cd'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute("set names utf8")
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
    :return:                        SQL Object
    """
    # Connect DB
    logger.info('Connect MySQL ...')
    sql = False
    for cnt in range(1, 4):
        try:
            sql = MySQL()
            logger.info("Success connect.")
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            if cnt < 3:
                print "Fail connect MySQL, retrying count = {0}".format(cnt)
                logger.error("Fail connect MySQL, retrying count = {0}".format(cnt))
            time.sleep(10)
            continue
    if not sql:
        err_str = "Fail connect MySQL"
        raise Exception(err_str)
    return sql


def processing(logger, args):
    """
    Processing
    :param          logger:         Logger
    :param          args:           Arguments
    """
    cnt = 0
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Connect MySQL
    mysql = connect_db(logger)
    input_file = open(args.input_file, 'r')
    input_file_list = input_file.readlines()
    input_file.close()
    for line_num in range(0, len(input_file_list)):
        line = input_file_list[line_num].strip()
        if len(line) < 1:
            continue
        line_list = line.split("\t")
        if len(line_list) != 3:
            print 'Error! Input file format. line number is {0}'.format(line_num + 1)
            print input_file_list[line_num]
            logger.error('Error! Input file format. line number is {0}'.format(line_num + 1))
            logger.error(input_file_list[line_num])
            continue
        poli_no = line_list[0].strip()
        ctrdt = line_list[1].strip()
        stt_prgst_cd = line_list[2].strip()
        try:
            result = mysql.select_stt_prgst_cd(poli_no, ctrdt)
            status = str(result[0]).strip()
            if status == '02':
                print "Already processing.. -> POLI_NO = {0}, CTRDT = {1}, STT_PRGST_CD = {2}".format(
                    poli_no, ctrdt, stt_prgst_cd)
                logger.error("Already processing.. -> POLI_NO = {0}, CTRDT = {1}, STT_PRGST_CD = {2}".format(
                    poli_no, ctrdt, stt_prgst_cd))
                continue
            mysql.update_stt_prgst_cd(
                poli_no=poli_no,
                ctrdt=ctrdt,
                stt_prgst_cd=stt_prgst_cd
            )
            cnt += 1
        except Exception:
            exc_info = traceback.format_exc()
            print "Can't update status -> POLI_NO = {0}, CTRDT = {1}, STT_PRGST_CD = {2}".format(
                poli_no, ctrdt, stt_prgst_cd)
            print exc_info
            logger.error("Can't update status -> POLI_NO = {0}, CTRDT = {1}, STT_PRGST_CD = {2}".format(
                poli_no, ctrdt, stt_prgst_cd))
            logger.error(exc_info)
            continue
    logger.info("TOTAL END.. Start time = {0}, The time required = {1}, Update count = {2}".format(
        st, elapsed_time(dt), cnt))
    mysql.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

########
# main #
########


def main(args):
    """
    This is a program that update STT_PRGST_CD from TB_QA_STT_TM_CNTR_INFO
    :param          args:            Arguments
    """
    # Add logging
    log_file_name = args.log_file if args.log_file != 'default' else "update_tm_status_{0}".format(
        datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M%S'))
    logger_args = {
        'base_path': LOG_CONFIG['log_dir_path'],
        'log_file_name': "{0}.log".format(log_file_name),
        'log_level': LOG_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    try:
        processing(logger, args)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', action='store', dest='input_file', required=True, type=str, help='Input file name')
    parser.add_argument('-l', action='store', dest='log_file', default='default', type=str, help='Log file name')
    arguments = parser.parse_args()
    main(arguments)
