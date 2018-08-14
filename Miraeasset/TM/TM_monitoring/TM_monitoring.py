#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-11-30, modification: 2017-00-00"

###########
# imports #
###########
import os
import sys
import time
import MySQLdb
import argparse
import traceback
from cfg.config import MYSQL_DB_CONFIG

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
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_target_count(self, date):
        query = """
            SELECT
                STT_PRGST_CD,
                COUNT(STT_PRGST_CD)
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                AND {0} = DATE_FORMAT(STT_REQ_DTM, '%Y-%m-%d')
            GROUP BY
                STT_PRGST_CD
        """.format(date)
        self.cursor.execute(query, )
        result = self.cursor.fetchall()
        target_count_dic = dict()
        for row in result:
            target_count_dic[row['PRGST_CD']] = row['COUNT(STT_PRGST_CD)']
        return target_count_dic

    def select_process_information(self, date):
        query = """
            SELECT
                POLI_NO,
                CTRDT
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                AND STT_PRGST_CD = '02'
                AND {0} = DATE_FORMAT(STT_REQ_DTM, '%Y-%m-%d')
        """.format(date)
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        info_dict_list = list()
        for row in result:
            info_dict = dict()
            info_dict['POLI_NO'] = row['POLI_NO']
            info_dict['CTRDT'] = row['CTRDT']
            info_dict_list.append(info_dict)
        return info_dict_list

    def select_failure_information(self, date):
        query = """
            SELECT
                POLI_NO,
                CTRDT
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                AND PRGST_CD = '03'
                AND {0} = DATE_FORMAT(STT_REQ_DTM, '%Y-%m-%d')
        """.format(date)
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        info_dict_list = list()
        for row in result:
            info_dict = dict()
            info_dict['POLI_NO'] = row['POLI_NO']
            info_dict['CTRDT'] = row['CTRDT']
            info_dict_list.append(info_dict)
        return info_dict_list

    def select_success_information(self, before_list):
        query = """
            SELECT
                POLI_NO,
                CTRDT,
                STT_PRGST_CD
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 0=1
        """
        for info_dict in before_list:
            add_query = 'OR (POLI_NO = "{0}" AND CTRDT = "{1}")\n'.format(info_dict['POLI_NO'], info_dict['CTRDT'])
            query += add_query
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        success_list = list()
        for row in result:
            if row['STT_PRGST_CD'] == '05':
                info_dict = dict()
                info_dict['POLI_NO'] = row['POLI_NO']
                info_dict['CTRDT'] = row['CTRDT']
                success_list.append(info_dict)
        return success_list


#######
# def #
#######

def connect_db():
    """
    Connect database
    :return:                        SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            sql = MySQL()
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            if cnt < 3:
                print "Fail connect MySQL, retrying count = {0}".format(cnt)
            time.sleep(10)
            continue
    if not sql:
        err_str = "Fail connect MySQL"
        raise Exception(err_str)
    return sql


def processing(args):
    """
    Processing
    :param          args:        Arguments
    """
    mysql = False
    wait_count = 0
    fail_count = 0
    process_count = 0
    success_count = 0
    no_file_count = 0
    before_processing_list = list()
    success_list = list()
    try:
        while True:
            mysql = connect_db()
            target_count_dict = mysql.select_target_count(args.date)
            processing_list = mysql.select_process_information(args.date)
            failure_list = mysql.select_failure_information(args.date)
            if not target_count_dict or not processing_list or not failure_list:
                err_str = "Error MySQL select."
                raise Exception(err_str)
            if '01' not in target_count_dict:
                target_count_dict['01'] = 0
            if '02' not in target_count_dict:
                target_count_dict['02'] = 0
            if '03' not in target_count_dict:
                target_count_dict['03'] = 0
            if '05' not in target_count_dict:
                target_count_dict['05'] = 0
            if '90' not in target_count_dict:
                target_count_dict['90'] = 0
            if (not (wait_count == target_count_dict['01']
                     and process_count == target_count_dict['02']
                     and fail_count == target_count_dict['03']
                     and success_count == target_count_dict['05']
                     and no_file_count == target_count_dict['90'])):
                os.system('cls' if os.name == 'nt' else 'clear')
                print '=' * 100
                print 'COUNT'
                print ' Waiting      .. ("01")    : {0}'.format(target_count_dict['01'])
                print ' Processing   .. ("02")    : {0}'.format(target_count_dict['02'])
                print ' Failure      .. ("03")    : {0}'.format(target_count_dict['03'])
                print ' Success      .. ("05")    : {0}'.format(target_count_dict['05'])
                print ' No REC file  .. ("90")    : {0}'.format(target_count_dict['90'])
                print '\n\n'
                if len(processing_list) != 0:
                    print 'CS processing information'
                    for info_dict in processing_list:
                        print 'POLI_NO = {0}, CTRDT = {1}'.format(info_dict.get('POLI_NO'), info_dict.get('CTRDT'))
                if len(failure_list) != 0:
                    print 'CS failure information'
                    for info_dict in failure_list:
                        print 'POLI_NO = {0}, CTRDT = {1}'.format(info_dict.get('POLI_NO'), info_dict.get('CTRDT'))
                if len(before_processing_list) != 0:
                    success_list += mysql.select_success_information(before_processing_list)
                if len(success_list) > 10:
                    start_idx = len(success_list) - 10
                    success_list = success_list[start_idx:]
                if len(success_list) != 0:
                    print 'CS success information'
                    for info_dict in success_list:
                        print 'POLI_NO = {0}, CTRDT = {1}'.format(info_dict.get('POLI_NO'), info_dict.get('CTRDT'))
                print '=' * 100
                wait_count = target_count_dict['01']
                process_count = target_count_dict['02']
                fail_count = target_count_dict['03']
                success_count = target_count_dict['05']
                no_file_count = target_count_dict['90']
                before_processing_list = processing_list
            mysql.disconnect()
            if args.delay:
                time.sleep(args.delay)
    except Exception:
        if mysql:
            mysql.disconnect()
        exc_info = traceback.format_exc()
        raise Exception(exc_info)

########
# main #
########


def main(args):
    """
    This is a program that monitoring TM
    :param          args:            Arguments
    """
    try:
        if not 'current_date()' == args.date:
            if len(args.date) != 8:
                print 'Wrong date format.'
                print 'Date format = [YYYYMMDD]'
                sys.exit(1)
            args.date = "'{0}-{1}-{2}'".format(args.date[:4], args.date[4:6], args.date[6:8])
        processing(args)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-date', action='store', dest='date', default='current_date()', type=str,
                        help='Date [ex)YYYYMMDD)]')
    parser.add_argument('-delay', action='store', dest='delay', default=5, type=int, help='Delay time')
    arguments = parser.parse_args()
    main(arguments)
