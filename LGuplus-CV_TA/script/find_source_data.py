#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astraea@mindslab.ai"
__date__ = "creation: 2018-09-19, modification: 0000-00-00"
__copyright__ = 'All Rights Reserved by MINDsLAB'

###########
# imports #
###########
import os
import sys
import json
import shutil
import argparse

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
        duration = int(args.duration)
    except Exception:
        print 'Error duration'
        sys.exit(1)
    copy_cnt = 0
    flag = True
    output_dir_path = '/data/maum/duration_' + args.duration
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    w_ob = os.walk('/data/maum/processed')
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if file_name.endswith('json'):
                base_file_name = os.path.splitext(file_name)[0]
                json_file_path = os.path.join(dir_path, file_name)
                output_json_file_path = os.path.join(output_dir_path, file_name)
                trx_file_name = base_file_name + '_trx.txt'
                trx_file_path = os.path.join(dir_path, trx_file_name)
                output_trx_file_path = os.path.join(output_dir_path, trx_file_name)
                json_file = open(json_file_path)
                json_data = json.load(json_file)
                talk_time = json_data['talk_time']
                json_file.close()
                try:
                    talk_time = int(talk_time)
                except Exception:
                    continue
                if duration - 5 < talk_time < duration + 5:
                    if os.path.exists(output_json_file_path):
                        os.remove(output_json_file_path)
                    if os.path.exists(output_trx_file_path):
                        os.remove(output_trx_file_path)
                    shutil.copy(json_file_path, output_dir_path)
                    shutil.copy(trx_file_path, output_dir_path)
                    copy_cnt += 1
            if copy_cnt == 4200:
                flag = False
                break
        if not flag:
            break
    print '{0} copy file'.format(copy_cnt)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-d', nargs='?', action='store', dest='duration', required=True,
                        help="Input wav duration(/s)\n[ ex) 300]")
    arguments = parser.parse_args()
    main(arguments)
