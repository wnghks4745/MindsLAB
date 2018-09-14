#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-04-12, modification: 2018-04-16"

###########
# imports #
###########
import os
import sys
import time
import argparse
import traceback
import cx_Oracle
from datetime import datetime
import cfg.config
from lib.openssl import decrypt_string


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#############
# constants #
#############
INSERT_CNT = 0
DELETE_CNT = 0
CONFIG = {}
DB_CONFIG = {}


#########
# class #
#########
class Oracle(object):
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
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def insert_sec_info(self, arguments):
        global INSERT_CNT
        try:
            sql = """
                INSERT INTO
                    TB_SCRT_SEC_INFO(  
                        IP_CD,
                        QA_SCRT_LCCD,
                        QA_SCRT_MCCD,
                        SEC_NO,
                        SEC_CONT,
                        AWST_SEC_CNT,
                        QUSTN_ASW_USE_YN,
                        SORT_SEQN_NO,
                        SCTN_SAVE_YN,
                        MDF_CRTN_YN,
                        QA_SCRT_TP,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                VALUES (
                    :1, :2, :3, :4, :5, 
                    :6, :7, :8, :9, :10,
                    :11, :12, :13, SYSDATE, :13,
                    :14, SYSDATE
                )
            """

            bind = (
                arguments[0].strip(),
                arguments[1].strip(),
                arguments[2].strip(),
                arguments[3].strip(),
                arguments[4].strip(),
                arguments[5].strip(),
                arguments[6].strip(),
                arguments[7].strip(),
                arguments[8].strip(),
                arguments[9].strip(),
                arguments[10].strip(),
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                INSERT_CNT += 1
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_sec_sntc_info(self, arguments):
        global INSERT_CNT
        try:
            sql = """
                INSERT INTO
                    TB_SCRT_SEC_SNTC_INFO(  
                        IP_CD,
                        QA_SCRT_LCCD,
                        QA_SCRT_MCCD,
                        SEC_NO,
                        SNTC_NO,
                        SCRT_SNTC_SORT_NO,
                        LINK_SNTC_YN,
                        USE_YN,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                VALUES (
                    :1, :2, :3, :4, :5, 
                    :6, :7, :8, :9, :10,
                    SYSDATE, :11, :12, SYSDATE
                )
            """

            bind = (
                arguments[0].strip(),
                arguments[1].strip(),
                arguments[2].strip(),
                arguments[3].strip(),
                arguments[4].strip(),
                arguments[5].strip(),
                arguments[6].strip(),
                arguments[7].strip(),
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                INSERT_CNT += 1
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_sntc_mst_info(self, arguments):
        global INSERT_CNT
        try:
            sql = """
                INSERT INTO
                    TB_SCRT_SNTC_MST_INFO(  
                        SNTC_ID,
                        SNTC_NO,
                        SNTC_CD,
                        DTC_SNTC_CD,
                        DTC_SNTC_NM,
                        SCRT_SNTC_CONT,
                        STRT_SNTC_YN,
                        CUST_ASW_YN,
                        ESTL_SNTC_YN,
                        KYWD_LIT,
                        DTC_BEF_LIT,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM,
                        SNTC_DCD
                    )
                VALUES (
                    :1, :2, :3, :4, :5, 
                    :6, :7, :8, :9, :10,
                    :11, :12, :13, SYSDATE, :14, 
                    :15, SYSDATE, :16
                )
            """
            bind = (
                arguments[0].strip(),
                arguments[1].strip(),
                arguments[2].strip(),
                arguments[3].strip(),
                arguments[4].strip(),
                arguments[5].strip(),
                arguments[6].strip(),
                arguments[7].strip(),
                arguments[8].strip(),
                arguments[9].strip(),
                arguments[10].strip(),
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
                arguments[11].strip(),
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                INSERT_CNT += 1
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_sntc_dtc_info(self, arguments):
        global INSERT_CNT
        try:
            sql = """
                INSERT INTO
                    TB_SCRT_SNTC_DTC_INFO(  
                        DTC_NO,
                        SNTC_NO,
                        DTC_SORT_NO,
                        DTC_CONT,
                        USE_YN,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                VALUES (
                    :1, :2, :3, :4, :5, 
                    :6, :7, SYSDATE, :8, :9,
                    SYSDATE
                )
            """

            bind = (
                arguments[0].strip(),
                arguments[1].strip(),
                arguments[2].strip(),
                arguments[3].strip(),
                arguments[4].strip(),
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
                'SCRIPT',
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                INSERT_CNT += 1
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def delete_query(self, table_name):
        global DELETE_CNT
        try:
            sql = """
                DELETE FROM
                    {0}
                WHERE 1=1
            """.format(table_name)
            self.cursor.execute(sql)
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


def connect_db(db):
    """
    Connect database
    :param      db:         Database
    :return                 SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = "Korean_Korea.KO16KSC5601"
                sql = Oracle()
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            if cnt < 3:
                print "Fail connect {0}, retrying count = {1}".format(db, cnt)
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def processing(type, args):
    """
    processing
    :param      type:                   Type( Insert, Delete )
    :param      args:                   Arguments
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    target_file_name = args.target_file_name
    delete_table_name = args.del_table_name
    if args.in_table_name:
        insert_table_name = args.in_table_name.upper()
    print "-" * 100
    print "Start Insert/Delete Script"
    target_file_path = '{0}/{1}'.format(os.getcwd(), target_file_name)
    if type == 'l':
        print "Target txt file : {0}".format(target_file_path)
    if not os.path.exists(target_file_path) and type == 'I':
        print "Target file is not exists"
        print "End Insert Script"
        sys.exit(1)
    oracle = ''
    try:
        oracle = connect_db('Oracle')
        if not oracle:
            print "---------- Can't connect db ----------"
            sys.exit(1)
        if type == 'I':
            target_file = open(target_file_path, 'r')
            for line in target_file:
                line_list = line.split('\t')
                if insert_table_name == 'TB_SCRT_SEC_INFO':
                    oracle.insert_sec_info(line_list)
                elif insert_table_name == 'TB_SCRT_SEC_SNTC_INFO':
                    oracle.insert_sec_sntc_info(line_list)
                elif insert_table_name == 'TB_SCRT_SNTC_MST_INFO':
                    oracle.insert_sntc_mst_info(line_list)
                elif insert_table_name == 'TB_SCRT_SNTC_DTC_INFO':
                    oracle.insert_sntc_dtc_info(line_list)

        elif type == 'D':
            if not oracle.delete_query(delete_table_name):
                print "Delete is Failed : {0}".format(delete_table_name)
        oracle.disconnect()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        oracle.disconnect()
    print "END.. Start time = {0}, The time required = {1}, insert count = {2}, delete count = {3}".format(st, elapsed_time(dt), INSERT_CNT, DELETE_CNT)


########
# main #
########
def main(args):
    """
    This is a program that Insert Script
    :param      args:       Arguments
    """
    try:
        global CONFIG
        global DB_CONFIG
        CONFIG = cfg.config.CONFIG
        DB_CONFIG = cfg.config.DB_CONFIG[args.config_type]
        if args.target_file_name and args.in_table_name and not args.del_table_name:
            processing('I', args)
        elif not args.target_file_name and not args.in_table_name and args.del_table_name:
            processing('D', args)
        else:
            print "Check Argument --help or -h"
            sys.exit(0)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', action='store', dest='target_file_name', default=False, type=str, help='Target file name')
    parser.add_argument('-delete', action='store', dest='del_table_name', default=False, type=str, help='Delete Table name')
    parser.add_argument('-insert', action='store', dest='in_table_name', default=False, type=str, help='Insert Table name')
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    arguments = parser.parse_args()
    if arguments.config_type == 'prd':
        ans = raw_input('Do you really want to modify prd(운영) database?  y or n >> ')
        if 'y' != ans.lower():
            print "Cancle complete."
            sys.exit(0)
    main(arguments)
