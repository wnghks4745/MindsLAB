#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astraea@mindslab.ai"
__date__ = "creation: 2018-07-11, modification: 0000-00-00"
__copyright__ = 'All Rights Reserved by MINDsLAB'

###########
# imports #
###########
import os
import sys
import argparse
import traceback
import subprocess

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def sub_process(cmd):
    """
    Execute subprocess
    :param      cmd:        Command
    """
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        print response_out
    if len(response_err) > 0:
        print response_err


def execute_convert(args, target_file_path):
    """
    Execute convert file
    :param      args:                   Arguments
    :param      target_file_path:       Target file path
    """
    target_dir_path = os.path.dirname(target_file_path)
    output_file_name = '{0}_{1}'.format(args.con_format.replace('-', ''), os.path.basename(target_file_path))
    output_file_path = os.path.join(target_dir_path, output_file_name)
    cmd = 'iconf -c -f {0} -t {1} {2} > {3}'.format(
        args.src_format, args.con_format, target_file_path, output_file_path)
    sub_process(cmd)


def main(args):
    """
    This program that convert file
    :param      args:       Arguments
    """
    try:
        if args.file_path:
            target_file_path = os.path.abspath(args.file_path)
            if os.path.exists(target_file_path):
                execute_convert(args, target_file_path)
            else:
                print "[ERROR] Can't find {0} file".format(args.file_path)
        elif args.dir_path:
            target_dir_path = os.path.abspath(args.dir_path)
            if os.path.exists(target_dir_path):
                for dir_path, sub_dirs, files in os.walk(target_dir_path):
                    for file_name in files:
                        target_file_path = os.path.join(dir_path, file_name)
                        try:
                            execute_convert(args, target_file_path)
                        except Exception:
                            print traceback.format_exc()
                            print "[ERROR] Can't convert {0}".format(target_file_path)
                            continue
            else:
                print "[ERROR] Can't find {0} directory".format(args.dir_path)
        else:
            print "[ERROR] Input target file or directory"
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-sf', nargs='?', action='store', dest='src_format', required=True, type=str,
                        help="Input source file format\n[ ex) euc-kr ]")
    parser.add_argument('-cf', nargs='?', action='store', dest='con_format', required=True, type=str,
                        help="Input convert file format\n[ ex) utf-8 ]")
    parser.add_argument('-f', nargs='?', action='store', dest='file_path', type=str,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-d', nargs='?', action='store', dest='dir_path', type=str,
                        help="Input target directory path\n[ ex) /app/maum/test ]")
    arguments = parser.parse_args()
    main(arguments)
