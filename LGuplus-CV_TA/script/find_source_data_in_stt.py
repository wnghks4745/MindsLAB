#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-10-02, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import json
import shutil
import argparse
from datetime import datetime

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def main(args):
    """
    This program that split line
    :param      args:       Arguments
    """
    try:
        max_duration = int(args.max)
        min_duration = int(args.min)
    except Exception:
        print 'Error duration'
        sys.exit(1)
    try:
        datetime.strptime(args.date, '%Y%m%d')
        target_date = args.date
        year = args.date[:4]
        mon = args.date[4:6]
        day = args.date[6:8]
    except Exception:
        print 'Error date'
        sys.exit(1)
    try:
        if len(args.st) > 2 or len(args.et) > 2:
            print 'Error start, end time'
            print 'ex) 01, 02, 03, ...., 18, 19, 20...'
            sys.exit(1)
        int(args.st)
        temp_et = str(int(int(args.et) - 1))
        st = args.st if len(args.st) == 2 else '0' + args.st
        et = temp_et if len(temp_et) == 2 else '0' + temp_et
    except Exception:
        print 'Error duration'
        sys.exit(1)
    copy_cnt = 0
    output_dir_path = '/data1/MindsVOC/STT/STT_out/duration/{0}_{1}_{2}'.format(target_date, args.min, args.max)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    base_dir_path = '/data1/MindsVOC/STT/STT_out/{0}/{1}/{2}/{3}'.format(year, mon, day, target_date)
    source_dir_path = os.path.join(base_dir_path, 'source')
    trx_dir_path = os.path.join(base_dir_path, 'processed_db_upload')
    w_ob = os.walk(source_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if file_name.endswith('json'):
                base_file_name = os.path.splitext(file_name)[0]
                json_file_path = os.path.join(dir_path, file_name)
                output_json_file_path = os.path.join(output_dir_path, file_name)
                trx_file_name = base_file_name + '_trx.txt'
                trx_file_path = os.path.join(trx_dir_path, trx_file_name)
                output_trx_file_path = os.path.join(output_dir_path, trx_file_name)
                if not os.path.exists(trx_file_path):
                    continue
                json_file = open(json_file_path)
                json_data = json.load(json_file)
                talk_time = json_data['talk_time']
                start_time = json_data['start_time']
                end_time = json_data['end_time']
                json_file.close()
                try:
                    talk_time = int(talk_time)
                except Exception:
                    continue
                if start_time.startswith(st) and end_time.startswith(et):
                    if min_duration <= talk_time <= max_duration:
                        if os.path.exists(output_json_file_path):
                            os.remove(output_json_file_path)
                        if os.path.exists(output_trx_file_path):
                            os.remove(output_trx_file_path)
                        shutil.copy(json_file_path, output_dir_path)
                        trx_file = open(trx_file_path)
                        output_trx_file = open(output_trx_file_path, 'w')
                        for line in trx_file:
                            utf8_line = line.strip().decode('euc-kr').encode('utf-8')
                            print >> output_trx_file, utf8_line
                        trx_file.close()
                        output_trx_file.close()
                        copy_cnt += 1
    print '{0} copy file'.format(copy_cnt)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-d', nargs='?', action='store', dest='date', required=True,
                        help="Input target date\n[ ex) 20180914 ]")
    parser.add_argument('-s', nargs='?', action='store', dest='st', required=True,
                        help="Input wav start time\n[ ex) 14]")
    parser.add_argument('-e', nargs='?', action='store', dest='et', required=True,
                        help="Input wav end time\n[ ex) 16]")
    parser.add_argument('-max', nargs='?', action='store', dest='max', required=True,
                        help="Input wav maximum duration(/s)\n[ ex) 500]")
    parser.add_argument('-min', nargs='?', action='store', dest='min', required=True,
                        help="Input wav minimum duration(/s)\n[ ex) 300]")
    arguments = parser.parse_args()
    main(arguments)
