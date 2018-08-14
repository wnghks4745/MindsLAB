#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = 'creation: 2018-02-21, modification: 0000-00-00'

###########
# imports #
###########
import os
import sys
import time
import paramiko
import traceback
from datetime import datetime
from __init__ import CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#######
# def #
#######
def extract_error_json_count(ssh, project_cd, rec_dir_path_list, target_date_dir_path, output_str):
    """
    Extract error json count
    :param      ssh:                        SSH
    :param      project_cd:                 Project code (CS or TM)
    :param      rec_dir_path_list:          Record directory path list
    :param      target_date_dir_path:       Target date directory path
    :param      output_str:                 Output string
    :return:                                Output string
    """
    output_str += '\033[32m' + "  " + project_cd + '\033[0m' + "\n"
    for rec_dir_path in rec_dir_path_list:
        rec_target_date_dir_path = os.path.join(rec_dir_path, target_date_dir_path)
        cmd = 'ls {0}/*json | wc -l'.format(rec_target_date_dir_path)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        for item in stdout:
            item = item.strip()
            if item != "0":
                output_str += '\033[31m'
                output_str += "    녹취파일 경로 = "
                output_str += str(rec_dir_path.rjust(20))
                output_str += "    오류 Json 파일 개수 = "
                output_str += str(item.rjust(5))
                output_str += '\033[0m'
                output_str += "\n"
            else:
                output_str += "    녹취파일 경로 = "
                output_str += str(rec_dir_path.rjust(20))
                output_str += "    오류 Json 파일 개수 = "
                output_str += str(item.rjust(5))
                output_str += "\n"
    return output_str


def processing(target_date):
    """
    Processing
    """
    year = target_date[:4].strip()
    month = target_date[4:6]
    day = target_date[6:].strip()
    date_dir_path = "error_data/{y}/{m}/{d}".format(y=year, m=month, d=day)
    try:
        while True:
            output_str = ""
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            for server_host in CONFIG['server_list']:
                try:
                    ssh.connect(server_host, username=CONFIG['username'], password=CONFIG['password'], timeout=5)
                except Exception:
                    exc_info = traceback.format_exc()
                    print exc_info
                    print "Can't connect {0}".format(server_host)
                    continue
                output_str += '=' * 100 + "\n"
                output_str += "\n" + server_host + " 서버" + "\n\n"
                output_str = extract_error_json_count(ssh, 'CS', CONFIG['cs_rec_dir_path'], date_dir_path, output_str)
                output_str += "\n"
                output_str = extract_error_json_count(ssh, 'TM', CONFIG['tm_rec_dir_path'], date_dir_path, output_str)
                ssh.close()
            os.system('cls' if os.name == 'nt' else 'clear')
            print output_str
            time.sleep(3)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info


########
# main #
########
def main(target_date):
    """
    This is a program that monitoring script
    """
    try:
        processing(target_date)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        if len(sys.argv[1]) < 8:
            print 'Error target date. ex) 20170705'
            sys.exit(1)
        main(sys.argv[1])
    elif len(sys.argv) == 1:
        main(datetime.fromtimestamp(time.time()).strftime('%Y%m%d'))
    else:
        print "usage : python upload_json_monitoring.py [target_date, default=NOW]"
        print "Ex) python upload_json_monitoring.py or upload_json_monitoring.py 20170704"
        sys.exit(1)
