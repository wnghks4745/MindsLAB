#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = 'creation: 2017-11-20, modification: 2017-11-20'

###########
# imports #
###########
import os
import sys
import time
import MySQLdb
import argparse
import traceback
from CS.Miraeasset.cfg.config import MYSQL_DB_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')

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

    def select_call_count(self, date):
        """
        Select call count
        :param:     date        Date
        :return:                Call count
        """
        sql = """
            SELECT
                PRGST_CD, COUNT(PRGST_CD)
            FROM
                TB_QA_STT_RECINFO
            WHERE 1=1
                AND {0} = date_format(REC_STDT, '%Y-%m-%d')
            GROUP BY
                PRGST_CD
        """.format(date)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        call_count_dic = dict()
        for row in rows:
            call_count_dic[row['PRGST_CD']] = row['COUNT(PRGST_CD)']
        return call_count_dic

    def select_process_information(self, date):
        """
        Select processing information
        :param      date:       Date
        :return:                List of Information dictionary
        """
        sql = """
            SELECT
                RCDG_ID, RCDG_FILE_NM
            FROM
                TB_QA_STT_RECINFO
            WHERE 1=1
                AND PRGST_CD = 02
                AND {0} = date_format(REC_STDT, '%Y-%m-%d')
        """.format(date)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        info_dic_list = list()
        for row in rows:
            info_dic = dict()
            info_dic['RCDG_ID'] = row['RCDG_ID']
            info_dic['RCDG_FILE_NM'] = row['RCDG_FILE_NM']
            info_dic_list.append(info_dic)
        return info_dic_list

    def select_failure_information(self, date):
        """
        Select failure information
        :param      date:       Date
        :return:                List of Information dictionary
        """
        sql = """
            SELECT
                RCDG_ID, RCDG_FILE_NM
            FROM
                TB_QA_STT_RECINFO
            WHERE 1=1
                AND PRGST_CD = 03
                AND {0} = date_format(REC_STDT, '%Y-%m-%d')
        """.format(date)
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        info_dic_list = list()
        for row in rows:
            info_dic = dict()
            info_dic['RCDG_ID'] = row['RCDG_ID']
            info_dic['RCDG_FILE_NM'] = row['RCDG_FILE_NM']
            info_dic_list.append(info_dic)
        return info_dic_list

    def select_success_information(self, before_list):
        sql = """
            SELECT
                RCDG_ID, RCDG_FILE_NM, PRGST_CD
            FROM
                TB_QA_STT_RECINFO
            WHERE 0=1
        """
        for info_dic in before_list:
            add_sql = 'OR (RCDG_ID = "{0}" AND RCDG_FILE_NM = "{1}")\n'.format(info_dic['RCDG_ID'], info_dic['RCDG_FILE_NM'])
            sql = sql + add_sql
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        success_list = list()
        for row in rows:
            if row['PRGST_CD'] == '05':
                info_dic = dict()
                info_dic['RCDG_ID'] = row['RCDG_ID']
                info_dic['RCDG_FILE_NM'] = row['RCDG_FILE_NM']
                success_list.append(info_dic)
        return success_list

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

#######
# def #
#######
def mysql_connect():
    """
    Attempt to connect to MySQL 4 times
    :return:    Mysql
    """
    mysql = False
    for cnt in range(0, 4):
        try:
            mysql = MySQL()
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            print 'Fail connect MySQL, retrying count = {0}'.format(cnt)
            cnt += 1
            continue
    return mysql


def processing(argv):
    """
    Processing
    :param      argv:       argument
    """
    mysql = False
    try:
        wait_count = 0
        process_count = 0
        fail_count = 0
        success_count = 0
        no_file_count = 0
        before_processing_list = list()
        success_list = list()
        while True:
            mysql = mysql_connect()
            call_count_dic = mysql.select_call_count(argv.date)
            processing_list = mysql.select_process_information(argv.date)
            failure_list = mysql.select_failure_information(argv.date)
            if not '01' in call_count_dic:
                call_count_dic['01'] = 0
            if not '02' in call_count_dic:
                call_count_dic['02'] = 0
            if not '03' in call_count_dic:
                call_count_dic['03'] = 0
            if not '05' in call_count_dic:
                call_count_dic['05'] = 0
            if not '90' in call_count_dic:
                call_count_dic['90'] = 0
            if (not (wait_count == call_count_dic['01'] and process_count == call_count_dic['02'] and
                             fail_count == call_count_dic['03'] and success_count == call_count_dic['05']
                     and no_file_count == call_count_dic['90'])):
                os.system('cls' if os.name == 'nt' else 'clear')
                print '=' * 100
                print 'COUNT'
                print ' CS Waits ("01")         : {0}'.format(call_count_dic['01'])
                print ' CS no files ("90")      : {0}'.format(call_count_dic['90'])
                print ' CS processing ("02")    : {0}'.format(call_count_dic['02'])
                print ' CS failures ("03")      : {0}'.format(call_count_dic['03'])
                print ' CS Success ("05")       : {0}'.format(call_count_dic['05'])
                print '-' * 100
                print 'Information'
                if not len(processing_list) == 0:
                    print ' CS processing information'
                    count = 0
                    for info_dic in processing_list:
                        count += 1
                        if count == 11:
                            break
                        print '     Recording ID : {0}      Recording File name : {1}'.format(
                            info_dic['RCDG_ID'], info_dic['RCDG_FILE_NM'])
                print ''
                if not len(failure_list) == 0:
                    print ' CS failure information'
                    for info_dic in failure_list:
                        print '     Recording ID : {0}      Recording File name : {1}'.format(
                            info_dic['RCDG_ID'], info_dic['RCDG_FILE_NM'])
                if not len(before_processing_list) == 0:
                    success_list = success_list + mysql.select_success_information(before_processing_list)
                if len(success_list) > 10:
                    del_len = len(success_list) - 10
                    success_list = success_list[del_len:]
                print ''
                if not len(success_list) == 0:
                    print ' CS success information'
                    for info_dic in success_list:
                        print '     Recording ID : {0}      Recording File name : {1}'.format(
                            info_dic['RCDG_ID'], info_dic['RCDG_FILE_NM'])
                print '=' * 100
                wait_count = call_count_dic['01']
                process_count = call_count_dic['02']
                fail_count = call_count_dic['03']
                success_count = call_count_dic['05']
                no_file_count = call_count_dic['90']
                before_processing_list = processing_list
            mysql.disconnect()
            if argv.delay:
                time.sleep(argv.delay)
    except Exception:
        mysql.disconnect()
        exc_info = traceback.format_exc()
        print exc_info

########
# main #
########
def main(argv):
    """
    This is a program that monitoring CS program
    :param      argv:       argument
    """
    try:
        if not 'current_date()' == argv.date:
            if not len(argv.date) == 8:
                print 'Unknown command'
                print 'usage: -date YYYYMMDD'
                sys.exit(2)
            argv.date = "'{0}-{1}-{2}'".format(argv.date[:4], argv.date[4:6], argv.date[6:8])
        processing(argv)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-date', action='store', dest='date', default='current_date()', type=str, help='Date')
    parser.add_argument('-delay', action='store', dest='delay', default=5, type=int, help='Delay time')
    arguments = parser.parse_args()
    main(arguments)