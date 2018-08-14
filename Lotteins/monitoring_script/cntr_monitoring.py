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
import cx_Oracle
import traceback
import collections
from __init__ import ORACLE_DB_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


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
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_count_each_qa_tar_dcd_in_tb_qa_stt_tm_rcdg_info(self, cntr_proc_dcd):
        query = """
            SELECT
                QA_STTA_PRGST_CD,
                count(*)
            FROM
                TB_TM_CNTR_INFO
            WHERE 1=1
                AND CNTR_PROC_DCD = :1
            GROUP BY
                QA_STTA_PRGST_CD
        """
        bind = (
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
            if db == 'Oracle':
                os.environ["NAS_LANG"] = ".AL32UTF8"
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


def processing():
    """
    Processing
    """
    target_status_cd = ['00', '01', '02', '03', '11', '12', '13', '90']
    target_print_dict = collections.OrderedDict()
    target_print_dict['90'] = '  계약 매핑 실패'
    target_print_dict['00'] = '  S T T     대기'
    target_print_dict['01'] = '  S T T   처리중'
    target_print_dict['02'] = '  S T T 변환오류'
    target_print_dict['03'] = '  S T T 처리완료'
    target_print_dict['11'] = '  T   A   처리중'
    target_print_dict['12'] = '  T   A 변환오류'
    target_print_dict['13'] = '  T   A 처리완료'
    oracle = False
    try:
        while True:
            output_str = ""
            oracle = connect_db('Oracle')
            # CNTR monitoring
            first_cntr_dict = oracle.select_count_each_qa_tar_dcd_in_tb_qa_stt_tm_rcdg_info('01')
            second_cntr_dict = oracle.select_count_each_qa_tar_dcd_in_tb_qa_stt_tm_rcdg_info('02')
            if not first_cntr_dict and not second_cntr_dict:
                continue
            for status_cd in target_status_cd:
                if status_cd not in first_cntr_dict:
                    first_cntr_dict[status_cd] = 0
                if status_cd not in second_cntr_dict:
                    second_cntr_dict[status_cd] = 0
            output_str += '=' * 100 + "\n\n"
            output_str += '계약 처리 상태 [TM 서버]\n'
            output_str += '\033[32m'
            output_str += 'Status'.rjust(22)
            output_str += '1st QA'.rjust(22)
            output_str += '2nd QA'.rjust(22)
            output_str += '\033[0m'
            output_str += "\n\n"
            for status_cd, name in target_print_dict.items():
                if status_cd.endswith('2'):
                    output_str += '\033[31m'
                else:
                    output_str += ''
                output_str += '{0}("{1}")'.format(name, status_cd).rjust(22)
                output_str += '{0}'.format(first_cntr_dict[status_cd]).rjust(22)
                output_str += '{0}'.format(second_cntr_dict[status_cd]).rjust(22)
                output_str += "\n"
                if status_cd.endswith('2'):
                    output_str += '\033[0m'
                else:
                    output_str += ''
            # Print output
            os.system('cls' if os.name == 'nt' else 'clear')
            print output_str
            oracle.disconnect()
            time.sleep(3)
    except Exception:
        oracle.disconnect()
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
