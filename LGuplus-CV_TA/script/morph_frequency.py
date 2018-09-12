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

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def execute_frequency(target_file_path):
    """
    Execute frequency morph
    :param      target_file_path:       Target file path
    """
    target_file = open(target_file_path)
    output_dict = dict()
    for line in target_file:
        line = line.strip()
        line_list = line.split('\t')
        if len(line_list) < 3:
            continue
        morph_sent = line_list[2]
        morph_sent_list = morph_sent.split()
        for morph_word in morph_sent_list:
            if morph_word in output_dict:
                output_dict[morph_word] += 1
            else:
                output_dict[morph_word] = 1
    target_dir_path = os.path.dirname(target_file_path)
    output_file_name = 'frequency_{0}'.format(os.path.basename(target_file_path))
    output_file_path = os.path.join(target_dir_path, output_file_name)
    output_file = open(output_file_path, 'w')
    for morph_word, frequency in output_dict.items():
        if len(morph_word.split('/')) < 2:
            continue
        word = morph_word.split('/')[0]
        morph = morph_word.split('/')[1]
        print >> output_file, '{0}\t{1}\t{2}\t{3}'.format(
            os.path.basename(target_file_path), word, morph, frequency)
    target_file.close()
    output_file.close()


def main(args):
    """
    This program that morph frequency
    :param      args:       Arguments
    """
    try:
        if args.file_path:
            target_file_path = os.path.abspath(args.file_path)
            if os.path.exists(target_file_path):
                execute_frequency(target_file_path)
            else:
                print "[ERROR] Can't find {0} file".format(args.file_path)
        elif args.dir_path:
            target_dir_path = os.path.abspath(args.dir_path)
            if os.path.exists(target_dir_path):
                for dir_path, sub_dirs, files in os.walk(target_dir_path):
                    for file_name in files:
                        target_file_path = os.path.join(dir_path, file_name)
                        try:
                            execute_frequency(target_file_path)
                        except Exception:
                            print traceback.format_exc()
                            print "[ERROR] Can't execute frequency {0}".format(target_file_path)
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
    parser.add_argument('-f', nargs='?', action='store', dest='file_path', type=str,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-d', nargs='?', action='store', dest='dir_path', type=str,
                        help="Input target directory path\n[ ex) /app/maum/test ]")
    arguments = parser.parse_args()
    main(arguments)
