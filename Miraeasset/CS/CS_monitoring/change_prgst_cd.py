#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-05, modification: 2017-12-11"

###########
# imports #
###########
import os
import sys
import time
import MySQLdb
import traceback
from datetime import datetime
from cfg.config import MYSQL_DB_CONFIG, CHANGE_CONFIG
from lib.iLogger import set_logger

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DT = ''
ST = ''

#########
# class #
#########
class MySQL(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['password'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def select_prgst_cd(self, rcdg_id, rcdg_file_nm):
        """
        Select status code
        :param      rcdg_id:            RCDG_ID
        :param      rcdg_file_nm:       RCDG_FILE_NM
        :return:                        PRGST_CD
        """
        sql = """
            SELECT
                PRGST_CD
            FROM
                TB_QA_STT_RECINFO
            WHERE 1=1
                AND RCDG_ID = %s
                AND RCDG_FILE_NM = %s
        """
        bind = (rcdg_id, rcdg_file_nm, )
        self.cursor.execute(sql, bind)
        row = self.cursor.fetchone()
        if row is bool or not row:
            return False
        return row['PRGST_CD']

    def update_prgst_cd(self, rcdg_id, rcdg_file_nm, logger):
        """
        Update prgst_cd 90
        :param      rcdg_id:            Recording ID
        :param      rcdg_file_nm:       Recording File name
        :param      logger:             LOGGER
        :return:                        True/False
        """
        sql = """
            UPDATE
                TB_QA_STT_RECINFO
            SET
                PRGST_CD = '90'
            WHERE 1=1
                AND RCDG_ID = %s
                AND RCDG_FILE_NM = %s
        """
        bind = (rcdg_id, rcdg_file_nm,)
        self.cursor.execute(sql, bind)
        if self.cursor.rowcount > 0:
            logger.info('Update is success -> RCDG_ID : {0}\tRCDG_FILE_NM : {1}'.format(rcdg_id, rcdg_file_nm))
            self.conn.commit()
            return self.cursor.rowcount
        else:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error('Update is Fail -> RCDG_ID : {0}\tRCDG_FILE_NM : {1}'.format(rcdg_id, rcdg_file_nm))
            logger.error(exc_info)
            return False

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

#######
# def #
#######
def mysql_connect(logger):
    """
    Trying Connect to MySQL
    :param      logger:     LOGGER
    :return:                MySQL
    """
    mysql = False
    for cnt in range(1, 4):
        try:
            mysql = MySQL()
            break
        except Exception as e:
            print e
            if cnt < 3:
                logger.error('Fail connect MySQL, retrying count = {0}'.format(cnt))
            continue
    if not mysql:
        raise Exception("Fail connect MySQL")
    return mysql


def processing(txt_path):
    """
    Change Processing
    :param      txt_path:       Path of text
    """
    # Add logging
    logger_args = {
        'base_path': CHANGE_CONFIG['log_dir_path'],
        'log_file_name': CHANGE_CONFIG['log_name'],
        'log_level': CHANGE_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info('START..')
    update_count = 0
    mysql = mysql_connect(logger)
    txt_file = open(txt_path, 'r')
    for line in txt_file:
        line_list = line.split('\t')
        if not len(line_list) == 2:
            continue
        rcdg_id = line_list[0].strip()
        rcdg_file_nm = line_list[1].strip()
        prgst_cd = mysql.select_prgst_cd(rcdg_id, rcdg_file_nm)
        if prgst_cd == '01' or prgst_cd == '02':
            logger.info("rcdg_id = {0}\trcdg_file_nm = {1} can't change prgst cd\t-> current prgst_cd = {2}".format(
                rcdg_id, rcdg_file_nm, prgst_cd))
            continue
        if mysql.update_prgst_cd(rcdg_id, rcdg_file_nm, logger):
            logger.info("rcdg_id = {0}\trcdg_file_nm = {1} update prgst_cd success\t>> {2} -> 90".format(
                rcdg_id, rcdg_file_nm, prgst_cd))
            update_count += 1
        else:
            logger.info('rcdg_id = {0}\trcdg_file_nm = {1} update prgst_cd fail >> please retrying'.format(
                rcdg_id, rcdg_file_nm))
    logger.info('END..  prgst_cd change count = {0}'.format(update_count))
    logger.info('-' * 100)
    mysql.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(txt_path):
    """
    This is a program that update prgst cd
    :param      txt_path:       Path of text
    """
    global DT
    global ST
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        if not os.path.exists(txt_path):
            print 'txt is not exist -> {0}'.format(txt_path)
            print 'please check text path ( absolute path )'
            sys.exit(1)
        processing(txt_path)
    except Exception:
        exc_info = traceback.format_exc()
        print(exc_info)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print 'usage : python change_prgst_cd.py [txt_path (absolute path)]'
        print 'usage : python change_prgst_cd.py /app/prd/MindsVOC/CS_monitoring/test.txt'
        sys.exit(1)