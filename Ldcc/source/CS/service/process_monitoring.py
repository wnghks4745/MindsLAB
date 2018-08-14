#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = 'creation: 2017-12-29, modification: 2018-02-28'

###########
# imports #
###########
import os
import sys
import time
import MySQLdb
import traceback
import collections
from config import MYSQL_DB_CONFIG

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
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_count_stt_rcdg_info(self):
        query = """
            SELECT
                STT_PRGST_CD,
                COUNT(STT_PRGST_CD)
            FROM
                STT_RCDG_INFO
            WHERE 1=1
        """
        if len(sys.argv) == 2:
            date = sys.argv[1]
            try:
                int(date)
            except Exception:
                print "[usage] python {0} [YYYYMMDD]".format(sys.argv[0])
                print "ex) python {0} 20180803".format(sys.argv[0])
                sys.exit(0)
            query += "AND DATE = '{0}'".format(date)
        query += """
            GROUP BY
                STT_PRGST_CD
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
        count_dict = dict()
        for info_dict in results:
            count_dict[info_dict['STT_PRGST_CD']] = info_dict['COUNT(STT_PRGST_CD)']
        return count_dict

    def select_count_each_qa_tar_dcd_in_tb_qa_stt_tm_rcdg_info(self, host_name, cntr_proc_dcd):
        query = """
            SELECT
                NQA_STTA_PRGST_CD,
                COUNT(NQA_STTA_PRGST_CD)
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND STT_SERVER_ID = :1
                AND CNTR_PROC_DCD = :2
            GROUP BY
                NQA_STTA_PRGST_CD
        """
        bind = (
            host_name,
            cntr_proc_dcd,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return dict()
        if not results:
            return dict()
        count_dic = dict()
        for result in results:
            count_dic[result[0]] = result[1]
        return count_dic

    def select_stt_server_id(self):
        query = """
            SELECT
                STT_SERVER_ID
            FROM
                TB_TM_STT_RCDG_INFO
            GROUP BY
                STT_SERVER_ID
        """
        self.cursor.execute(query, )
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
        return results


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
    for cnt in range(1, 4):
        try:
            if db == 'MySQL':
                sql = MySQL()
            else:
                raise Exception("Unknown DB [{0}]".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            if cnt < 3:
                print "Fail connect {0}, retrying count = {1}".format(db, cnt)
            time.sleep(10)
            continue
    if not sql:
        err_str = "Fail connect {0}".format(db)
        raise Exception(err_str)
    return sql


def processing():
    """
    Processing
    """
    target_status_cd = ['00', '01', '02', '03', '90']
    target_print_dict = collections.OrderedDict()
    target_print_dict['90'] = '  녹취파일미탐지'
    target_print_dict['00'] = '  S T T     대기'
    target_print_dict['01'] = '  S T T   처리중'
    target_print_dict['02'] = '  S T T 변환오류'
    target_print_dict['03'] = '  S T T 처리완료'
    mysql = False
    try:
        while True:
            output_str = ""
            mysql = connect_db('MySQL')
            # CS monitoring
            cs_stt_dict = mysql.select_count_stt_rcdg_info()
            if cs_stt_dict:
                for cs_status_cd in target_status_cd:
                    if cs_status_cd not in cs_stt_dict:
                        cs_stt_dict[cs_status_cd] = 0
                output_str += '=' * 50 + "\n\n"
                output_str += "CS\n"
                output_str += '\033[32m'
                output_str += 'Status'.rjust(22)
                output_str += 'General'.rjust(22)
                output_str += '\033[0m'
                output_str += "\n\n"
                for cs_status_cd, name in target_print_dict.items():
                    if cs_status_cd.endswith('2'):
                        output_str += '\033[31m'
                    else:
                        output_str += ''
                    output_str += '{0}("{1}")'.format(name, cs_status_cd).rjust(22)
                    output_str += '{0}'.format(cs_stt_dict[cs_status_cd]).rjust(22)
                    output_str += "\n"
                    if cs_status_cd.endswith('2'):
                        output_str += '\033[0m'
                    else:
                        output_str += ''
            # Print output
            os.system('cls' if os.name == 'nt' else 'clear')
            print output_str
            mysql.disconnect()
            time.sleep(3)
    except Exception:
        mysql.disconnect()
        exc_info = traceback.format_exc()
        print exc_info


########
# main #
########
def main():
    """
    This is a program that monitoring priority processing
    """
    try:
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    main()
