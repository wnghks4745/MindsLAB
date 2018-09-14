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
DB_CONFIG = {}
CONFIG_TYPE = ''


#########
# class #
#########
class ORACLE(object):
    def __init__(self):
        self.dsn_tns = DB_CONFIG['dsn']
        passwd = decrypt_string(DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
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

    def count_tb_tm_cntr_info_group(self, date):
        """
        :return:
        """
        sql = """
            SELECT
                PRPS_DATE AS DT
                , QA_STTA_PRGST_CD
                , COUNT(*) AS CNT
            FROM
                TB_TM_CNTR_INFO
            WHERE 1=1
            
        """
        if date is not None:
            sql += "AND PRPS_DATE = '{0}'".format(date)
        sql += """            
            GROUP BY PRPS_DATE, QA_STTA_PRGST_CD
            ORDER BY PRPS_DATE, QA_STTA_PRGST_CD
        """
        self.cursor.execute(sql)
        rows = self.rows_to_dict_list()
        return rows

    def get_tb_tm_cntr_info_by_dt_and_cd(self, qa_stta_prgst_cd, prps_date):
        """
        :param qa_stta_prgst_cd:        qa_stta_prgst_cd
        :param prps_date:               YYYYMMDD  ( ex:20180326 )
        :return:
        """
        sql = """
            SELECT 
                ORG_FOF_C
                , EPL_ID
                , PRPS_DATE
                , CONT_NO
                , CNTC_CID
                , PRPS_CNTC_USID
                , SEX_TC
                , FETUS_YN
                , QA_STTA_PRGST_CD
                , IP_CD
                , AGE
            FROM TB_TM_CNTR_INFO 
            WHERE 1=1
                AND PRPS_DATE = :1 
                AND QA_STTA_PRGST_CD = :2
        """
        bind = (qa_stta_prgst_cd, prps_date,)
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
    global DB_CONFIG
    global CONFIG_TYPE

    CONFIG_TYPE = config_type
    DB_CONFIG = cfg.config.DB_CONFIG[config_type]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    parser.add_argument('-date', action='store', dest='date', default=None, type=str, help='YYYYMMDD')
    parser.add_argument('-detail', action='store', dest='detail', type=str, help='Y or N',  choices=['Y', 'N'])
    arguments = parser.parse_args()
    config_type = arguments.config_type
    date = arguments.date
    detail = arguments.detail
    print 'start!!  -ct : config_type = {0}, -date : date = {1}, -detail : detail = {2}, '.format(config_type, date, detail)

    set_config(config_type)
    oracle = oracle_connect()
    for item in oracle.count_tb_tm_cntr_info_group(date):
        if item['QA_STTA_PRGST_CD'] == '12':
            print '{DT}\t[{QA_STTA_PRGST_CD}]\tCount: {CNT}\t[ERROR]'.format(**item)
            if detail == 'Y':
                print '=' * 50, '{DT} Error Detail Info '.format(**item), '=' * 50
                print 'index : ORG_FOF_C | EPL_ID | PRPS_DATE | CONT_NO | IP_CD | CNTC_CID | PRPS_CNTC_USID | SEX_TC | AGE | FETUS_YN | QA_STTA_PRGST_CD'
                idx = 1
                for error_item in oracle.get_tb_tm_cntr_info_by_dt_and_cd(item['DT'], '12'):
                    print '{0} : {ORG_FOF_C}\t{EPL_ID}\t{PRPS_DATE}\t{CONT_NO}\t{IP_CD}\t{CNTC_CID}\t{PRPS_CNTC_USID}\t{SEX_TC}\t{AGE}\t{FETUS_YN}\t{QA_STTA_PRGST_CD}'\
                        .format(idx, **error_item)
                    idx += 1
        else:
            print '{DT}\t[{QA_STTA_PRGST_CD}]\tCount: {CNT}'.format(**item)

    oracle.disconnect()
