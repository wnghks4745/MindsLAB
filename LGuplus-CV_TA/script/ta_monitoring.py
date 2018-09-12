#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MindsLAB"
__date__ = 'creation: 2018-09-06, modification: 0000-00-00'

###########
# imports #
###########
import os
import sys
import time
#import cx_Oracle
import traceback
import collections

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#########
# class #
#########
class PrdOracleConfig(object):
    host = '172.18.200.143'
    host_list = ['172.18.200.143', '172.18.200.144']
    user = 'STTAAPP'
    pd = 'sttasvc!@'
    port = 2525
    sid = 'PUSTTA'
    service_name = 'PUSTTA'
    reconnect_interval = 10
    con_db = 'prd'


class DevOracleConfig(object):
    host = '172.18.217.146'
    host_list = ['172.18.217.146']
    user = 'STTAADM'
    pd = 'sttasvc!@'
    port = 1525
    sid = 'DUSTTA'
    service_name = 'DUSTTA'
    reconnect_interval = 10
    con_db = 'dev'


class BColors(object):
    CEND = '\33[0m'
    CBOLD = '\33[1m'
    CITALIC = '\33[3m'
    CURL = '\33[4m'
    CBLINK = '\33[5m'
    CBLINK2 = '\33[6m'
    CSELECTED = '\33[7m'
    CBLACK = '\33[30m'
    CRED = '\33[31m'
    CGREEN = '\33[32m'
    CYELLOW = '\33[33m'
    CBLUE = '\33[34m'
    CVIOLET = '\33[35m'
    CBEIGE = '\33[36m'
    CWHITE = '\33[37m'
    CBLACKBG = '\33[40m'
    CREDBG = '\33[41m'
    CGREENBG = '\33[42m'
    CYELLOWBG = '\33[43m'
    CBLUEBG = '\33[44m'
    CVIOLETBG = '\33[45m'
    CBEIGEBG = '\33[46m'
    CWHITEBG = '\33[47m'
    CGREY = '\33[90m'
    CRED2 = '\33[91m'
    CGREEN2 = '\33[92m'
    CYELLOW2 = '\33[93m'
    CBLUE2 = '\33[94m'
    CVIOLET2 = '\33[95m'
    CBEIGE2 = '\33[96m'
    CWHITE2 = '\33[97m'
    CGREYBG = '\33[100m'
    CREDBG2 = '\33[101m'
    CGREENBG2 = '\33[102m'
    CYELLOWBG2 = '\33[103m'
    CBLUEBG2 = '\33[104m'
    CVIOLETBG2 = '\33[105m'
    CBEIGEBG2 = '\33[106m'
    CWHITEBG2 = '\33[107m'


class Oracle(object):
    def __init__(self, conf, host_reverse=False, fail_over=False, service_name=False):
        self.conf = conf
        os.environ["NLS_LANG"] = ".AL32UTF8"
        if fail_over:
            self.dsn_tns = '(DESCRIPTION = (ADDRESS_LIST= (FAILOVER = on)(LOAD_BALANCE = off)'
            if host_reverse:
                self.conf.host_list.reverse()
            for host in self.conf.host_list:
                self.dsn_tns += '(ADDRESS= (PROTOCOL = TCP)(HOST = {0})(PORT = {1}))'.format(host, self.conf.port)
            if service_name:
                self.dsn_tns += ')(CONNECT_DATA=(SERVICE_NAME={0})))'.format(self.conf.service_name)
            else:
                self.dsn_tns += ')(CONNECT_DATA=(SID={0})))'.format(self.conf.sid)
        else:
            if service_name:
                self.dsn_tns = cx_Oracle.makedsn(
                    self.conf.host,
                    self.conf.port,
                    service_name=self.conf.service_name
                )
            else:
                self.dsn_tns = cx_Oracle.makedsn(
                    self.conf.host,
                    self.conf.port,
                    sid=self.conf.sid
                )
        self.conn = cx_Oracle.connect(
            self.conf.user,
            self.conf.pd,
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_status_group_by_hostname(self, start_date):
        query = """
            SELECT
                TA_HOSTNAME,
                TA_STATUS,
                COUNT(TA_STATUS)
            FROM
                TB_TAKR_STTTARESULT_M
            WHERE 1=1
                AND START_DATE = :1
            GROUP BY
                TA_STATUS,
                TA_HOSTNAME
        """
        bind = (
            start_date,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
        count_dic = dict()
        for result in results:
            count_dic[(result[0], str(result[1]))] = result[2]
        return count_dic


#######
# def #
#######
def processing(con_db, start_date):
    """
    Processing
    """
    target_status_cd_list = list()
    hostname_list = ['pstawkr1', 'pstawkr2', 'pstawkr3']
    for hostname in hostname_list:
        for status in ['0', '1', '2', '3', '90', '91', '92', '93', '94', '95']:
            target_status_cd_list.append((hostname, status))
    target_print_dict = collections.OrderedDict()
    target_print_dict['0'] = 'TA     등록  완료'
    target_print_dict['1'] = 'TA     NLP 처리중'
    target_print_dict['2'] = 'TA     HMD 처리중'
    target_print_dict['3'] = 'TA     처리  완료'
    target_print_dict['90'] = '녹취 파일  미탐지'
    target_print_dict['91'] = 'NLP     처리 오류'
    target_print_dict['92'] = 'HMD     처리 오류'
    target_print_dict['93'] = 'NLP  DB 입력 오류'
    target_print_dict['94'] = 'HMD  DB 입력 오류'
    target_print_dict['95'] = '상태 DB 입력 오류'
    try:
        while True:
            try:
                if con_db.lower().strip() == 'dev':
#                    oracle = Oracle(DevOracleConfig, service_name=True)
                    pass
                else:
#                    oracle = Oracle(PrdOracleConfig, service_name=True)
                    pass
            except Exception:
                exc_info = traceback.format_exc()
                print exc_info
                print "[ERROR] Can't connect DB"
                sys.exit(1)
            output_str = ""
            # TA monitoring
#            status_dic = oracle.select_status_group_by_hostname(start_date)
            status_dic = {
                ('pstawkr1', '0'): 20,
                ('pstawkr1', '1'): 14,
                ('pstawkr1', '2'): 31,
                ('pstawkr1', '3'): 74,
                ('pstawkr1', '90'): 13,
                ('pstawkr1', '91'): 82,
                ('pstawkr2', '0'): 29,
                ('pstawkr2', '1'): 37,
                ('pstawkr2', '2'): 84,
                ('pstawkr2', '3'): 32,
                ('pstawkr2', '92'): 93,
                ('pstawkr2', '94'): 23,
                ('pstawkr3', '0'): 35,
                ('pstawkr3', '1'): 57,
                ('pstawkr3', '2'): 321,
                ('pstawkr3', '3'): 111,
                ('pstawkr3', '93'): 123,
                ('pstawkr3', '95'): 13,
            }
            if status_dic:
                for item in target_status_cd_list:
                    if item not in status_dic:
                        status_dic[item] = 0
                output_str += '=' * 100 + "\n"
                output_str += BColors.CBOLD + "TA monitoring" + '\n'
                output_str += BColors.CBEIGE + 'Status'.rjust(22)
                output_str += 'General'.rjust(32) + BColors.CEND + '\n\n'
                for status_cd, comment in target_print_dict.items():
                    if status_cd == '3':
                        total_count = 0
                        for hostname in hostname_list:
                            if status_dic[(hostname, status_cd)] > 0:
                                total_count += status_dic[(hostname, status_cd)]
                        output_str += '\t{0}'.format(comment).ljust(20) + '("{0}")'.format(status_cd).rjust(6)
                        output_str += BColors.CBOLD + BColors.CGREEN + str(total_count).rjust(20) + BColors.CEND
                        for hostname in hostname_list:
                            for key, value in status_dic.items():
                                if value > 0:
                                    if hostname == key[0] and status_cd == key[1]:
                                        output_str += '\n' + BColors.CVIOLET + key[0].rjust(31) + BColors.CEND
                                        output_str += BColors.CVIOLET + str(value).rjust(20) + BColors.CEND
                    elif status_cd.startswith('9'):
                        total_count = 0
                        for hostname in hostname_list:
                            if status_dic[(hostname, status_cd)] > 0:
                                total_count += status_dic[(hostname, status_cd)]
                        if total_count < 1:
                            continue
                        output_str += '\t{0}'.format(comment).ljust(20) + '("{0}")'.format(status_cd).rjust(6)
                        output_str += BColors.CBOLD + BColors.CRED + str(total_count).rjust(20) + BColors.CEND
                        for hostname in hostname_list:
                            for key, value in status_dic.items():
                                if value > 0:
                                    if hostname == key[0] and status_cd == key[1]:
                                        output_str += '\n' + BColors.CVIOLET + key[0].rjust(31) + BColors.CEND
                                        output_str += BColors.CVIOLET + str(value).rjust(20) + BColors.CEND
                    else:
                        total_count = 0
                        for hostname in hostname_list:
                            if status_dic[(hostname, status_cd)] > 0:
                                total_count += status_dic[(hostname, status_cd)]
                        output_str += '\t{0}'.format(comment).ljust(20) + '("{0}")'.format(status_cd).rjust(6)
                        output_str += BColors.CBOLD + str(total_count).rjust(20) + BColors.CEND
                        for hostname in hostname_list:
                            for key, value in status_dic.items():
                                if value > 0:
                                    if hostname == key[0] and status_cd == key[1]:
                                        output_str += '\n' + BColors.CVIOLET + key[0].rjust(31) + BColors.CEND
                                        output_str += BColors.CVIOLET + str(value).rjust(20) + BColors.CEND
                    output_str += '\n'
            # Print output
            os.system('cls' if os.name == 'nt' else 'clear')
            print output_str
            break
#            oracle.disconnect()
            time.sleep(3)
    except KeyboardInterrupt:
        print 'Stopped by interrupt'
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info


########
# main #
########
def main(target_date, con_db):
    """
    This is a program that monitoring priority processing
    """
    try:
        if not con_db.lower().strip() in ['prd', 'dev']:
            print "[ERROR] Choose dev or prd"
            sys.exit(1)
        processing(con_db, target_date)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print "usage : python {0} [Input target date] [PRD or DEV]".format(sys.argv[0])
        print "ex) python {0} 20180906 dev".format(sys.argv[0])
        sys.exit(1)
