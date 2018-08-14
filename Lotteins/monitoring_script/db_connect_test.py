#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-28, modification: 2018-02-28"

###########
# imports #
###########
import os
import sys
import time
import pymssql
import cx_Oracle
import traceback
from __init__ import ORACLE_DB_CONFIG, MSSQL_DB_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class Oracle(object):
    def __init__(self):
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
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


class MSSQL(object):
    def __init__(self):
        self.conn = pymssql.connect(
            host=MSSQL_DB_CONFIG['host'],
            user=MSSQL_DB_CONFIG['user'],
            password=MSSQL_DB_CONFIG['password'],
            database=MSSQL_DB_CONFIG['database'],
            port=MSSQL_DB_CONFIG['port'],
            charset=MSSQL_DB_CONFIG['charset'],
            login_timeout=MSSQL_DB_CONFIG['login_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_cust_info(self, cti_call_id, start_time, r_user_number):
        query = """
            SELECT
                CallID,
                Tag2,
                Tag3,
                Tag8
            FROM
                t_callinfo WITH(NOLOCK)
            WHERE 1=1
                AND R_CallID = %s
                AND StartTime like %s
                AND Ext = %s
        """
        bind = (
            cti_call_id,
            "{0}%".format(start_time[:8]),
            r_user_number,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result


#######
# def #
#######
def connect_db(db):
    """
    Connect database
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    sql = False
    print "Connecting {0} ...".format(db)
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NAS_LANG"] = ".AL32UTF8"
                sql = Oracle()
            elif db == 'MsSQL':
                sql = MSSQL()
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


def main():
    """
    DB connect test script
    :return:
    """
    try:
        oracle = connect_db('Oracle')
        print "Success connect Oracle"
        oracle.disconnect()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "Fail connect Oracle"
    try:
        mssql = connect_db('MsSQL')
        print "Success connect MsSQL"
        mssql.disconnect()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "Fail connect MsSQL"

########
# main #
########
if __name__ == '__main__':
    main()
