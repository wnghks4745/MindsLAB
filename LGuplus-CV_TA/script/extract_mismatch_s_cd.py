#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-10-10, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import argparse
import traceback
from datetime import datetime

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def extract_tag_frequency(target_date, target_dir_path):
    """
    Extract tag frequency
    :param      target_date:            Target date
    :param      target_dir_path:        Target directory path
    """
    s_cdnm_dict = dict()
    for dir_path, sub_dirs, files in os.walk(target_dir_path):
        for file_name in files:
            if not file_name.endswith('.log'):
                continue
            log_file_path = os.path.join(dir_path, file_name)
            try:
                log_file = open(log_file_path)
                for line in log_file:
                    if "Can't find S_CD" not in line:
                        continue
                    s_cdnm_line = line.strip().split("Can't find S_CD")[1].strip()
                    s_cdnm = s_cdnm_line.replace('[S_CDNM =', '')[:-1].strip()
                    s_cdnm_dict[s_cdnm] = 1
                log_file.close()
            except Exception:
                print traceback.format_exc()
                print "[ERROR] Can't analyze {0}".format(log_file_path)
                continue
    output_file = open('{0}_mismatch_s_cd.txt'.format(target_date), 'w')
    for s_cdnm in s_cdnm_dict.keys():
        print >> output_file, s_cdnm
    output_file.close()


def main(args):
    """
    This program that extract mismatch S_CD
    :param      args:       Arguments
    """
    target_dir_path = os.path.join('/logs/maum/cvta', args.date)
    try:
        datetime.strptime(args.date, '%Y%m%d')
    except Exception:
        print '[ERROR] Wrong date. [ ex) 20180914 ]'
        sys.exit(1)
    if not os.path.exists(target_dir_path):
        print "[ERROR] Can't find {0} directory".format(target_dir_path)
        sys.exit(1)
    extract_tag_frequency(args.date, target_dir_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-d', nargs='?', action='store', dest='date', required=True,
                        help="Input target date\n[ ex) 20180914 ]")
    arguments = parser.parse_args()
    main(arguments)
