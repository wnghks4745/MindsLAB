#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-09-29, modification: 2017-12-07"


###########
# imports #
###########
import sys
import argparse
import traceback
import cx_Oracle
import cfg.config
from lib.openssl import decrypt_string

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#############
# constants #
#############
DIR_PATH = ''
ORACLE_DB_CONFIG = {}
CONFIG_TYPE = ''


#########
# class #
#########
class ORACLE(object):
    def __init__(self):
        self.dsn_tns = ORACLE_DB_CONFIG['dsn']
        passwd = decrypt_string(ORACLE_DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def rows_to_dict_list(self):
        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor]

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def count_call_meta_group(self, call_type, date):
        """
        :return:
        """
        sql = """
            SELECT 
                TO_CHAR(DOCUMENT_DT, 'YYYY-MM-DD') AS DT
                , STT_PRGST_CD
                , COUNT(STT_PRGST_CD) AS CNT
            FROM CALL_META 
            WHERE 1=1
                AND PROJECT_CD = :1
                AND (REC_ID <> '' OR  REC_ID IS NOT NULL)
        """
        if date is not None:
            sql += "AND DOCUMENT_DT = '{0}'".format(date)
        sql += """            
            GROUP BY TO_CHAR(DOCUMENT_DT, 'YYYY-MM-DD'), STT_PRGST_CD
            ORDER BY TO_CHAR(DOCUMENT_DT, 'YYYY-MM-DD')
        """
        bind = (call_type,)
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        return rows

    def get_call_meta_by_dt_and_cd(self, call_type, document_dt, stt_prgst_cd):
        """
        :param document_dt:             ex: '2018-03-26'
        :param stt_prgst_cd:            ex: '05'
        :return:
        """
        sql = """
            SELECT 
                DOCUMENT_DT
                , DOCUMENT_ID
                , STT_PRGST_CD
                , REC_ID
            FROM CALL_META 
            WHERE 1=1
                AND PROJECT_CD = :1 
                AND DOCUMENT_DT = :2
                AND STT_PRGST_CD = :3 
        """
        bind = (call_type, document_dt, stt_prgst_cd,)
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        return rows


def oracle_connect():
    """
    Attempt to connect to oracle
    :param:     logger:     Logger
    :return:                ORACLE
    """
    cnt = 0
    while True:
        try:
            oracle = ORACLE()
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            print "Fail connect ORACLE, retrying count = {0}".format(cnt)
            if cnt > 10:
                break
            cnt += 1
            continue
    return oracle


def set_config(config_type):
    """
    active type setting
    :param config_type:
    :return:
    """
    global ORACLE_DB_CONFIG
    global CONFIG_TYPE

    CONFIG_TYPE = config_type
    ORACLE_DB_CONFIG = cfg.config.ORACLE_DB_CONFIG[config_type]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    parser.add_argument('-t', action='store', dest='call_type', default='CS', type=str, help='CS or TM or CD',
                        choices=['CS', 'TM', 'CD'])
    parser.add_argument('-date', action='store', dest='date', default=None, type=str, help='YYYY-MM-DD')
    parser.add_argument('-detail', action='store', dest='detail', type=str, help='Y or N',  choices=['Y', 'N'])
    arguments = parser.parse_args()
    config_type = arguments.config_type
    call_type = arguments.call_type
    date = arguments.date
    detail = arguments.detail
    print 'start!!  -ct : config_type = {0}, -t : call_type = {1}, -t : date = {2}, -t : detail = {3}'\
        .format(config_type, call_type, date, detail)

    set_config(config_type)
    oracle = oracle_connect()
    for item in oracle.count_call_meta_group(call_type, date):
        if item['STT_PRGST_CD'] == '03':
            print '{DT}\t[{STT_PRGST_CD}]\tCount: {CNT}\t[ERROR]'.format(**item)
            if detail == 'Y':
                print '=' * 50, '{DT} Error Detail Info '.format(**item), '=' * 50
                print 'index : DOCUMENT_DT | DOCUMENT_ID | REC_ID | STT_PRGST_CD'
                idx = 1
                for error_item in oracle.get_call_meta_by_dt_and_cd(call_type, item['DT'], '03'):
                    print '{0} : {DOCUMENT_DT}\t{DOCUMENT_ID}\t{REC_ID}\t{STT_PRGST_CD}'.format(idx, **error_item)
                    idx += 1

        else:
            print '{DT}\t[{STT_PRGST_CD}]\tCount: {CNT}'.format(**item)

    oracle.disconnect()
