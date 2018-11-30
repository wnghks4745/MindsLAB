#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astraea@mindslab.ai"
__date__ = "creation: 2018-09-14, modification: 0000-00-00"
__copyright__ = 'All Rights Reserved by MINDsLAB'

###########
# imports #
###########
import os
import sys
import argparse
import traceback
from flashtext.keyword import KeywordProcessor

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def extract_tag_frequency(target_date, target_file_path, target_dir_path):
    """
    Extract tag frequency
    :param      target_date:            Target date
    :param      target_file_path:       Target file path
    :param      target_dir_path:        Target directory path
    """
    target_tag_freq_dict = dict()
    target_tag_file_dict = dict()
    keyword_processor = KeywordProcessor()
    target_file = open(target_file_path)
    for tag in target_file:
        tag = ' ' + tag.strip() + ' '
        keyword_processor.add_keyword(tag)
    target_file.close()
    for dir_path, sub_dirs, files in os.walk(target_dir_path):
        for file_name in files:
            nlp_file_path = os.path.join(dir_path, file_name)
            try:
                nlp_file = open(nlp_file_path)
                for line in nlp_file:
                    line_list = line.strip().split('\t')
                    tag_sent = ' ' + line_list[2] + ' '
                    keywords_found = keyword_processor.extract_keywords(tag_sent)
                    for keyword in keywords_found:
                        keyword = keyword.strip()
                        if keyword not in target_tag_freq_dict:
                            target_tag_freq_dict[keyword] = 1
                        else:
                            target_tag_freq_dict[keyword] += 1
                        if keyword not in target_tag_file_dict:
                            target_tag_file_dict[keyword] = [nlp_file_path]
                        else:
                            if nlp_file_path not in target_tag_file_dict[keyword]:
                                target_tag_file_dict[keyword].append(nlp_file_path)
                nlp_file.close()
            except Exception:
                print traceback.format_exc()
                print "[ERROR] Can't analyze {0}".format(nlp_file_path)
                continue
    frequency_output_file = open('{0}_frequency.txt'.format(target_date), 'w')
    sorted_tag_list = sorted(target_tag_freq_dict)
    for tag in sorted_tag_list:
        print >> frequency_output_file, '{0}\t{1}'.format(tag, target_tag_freq_dict[tag])
    frequency_output_file.close()
    file_list_output_file = open('{0}_file_list.txt'.format(target_date), 'w')
    sorted_file_list = sorted(target_tag_file_dict)
    for tag in sorted_file_list:
        for file_nm in target_tag_file_dict[tag]:
            print >> file_list_output_file, '{0}\t{1}'.format(tag, file_nm)
    file_list_output_file.close()


def main(args):
    """
    This program that split line
    :param      args:       Arguments
    """
    target_file_path = os.path.abspath(args.file_path)
    target_dir_path = os.path.join('/data/maum/nlp_output', args.date)
    if not os.path.exists(target_file_path):
        print "[ERROR] Can't find {0} file".format(target_file_path)
        sys.exit(1)
    if not os.path.exists(target_dir_path):
        print "[ERROR] Can't find {0} directory".format(target_dir_path)
        sys.exit(1)
    extract_tag_frequency(args.date, target_file_path, target_dir_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i', nargs='?', action='store', dest='file_path', required=True,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-d', nargs='?', action='store', dest='date', required=True,
                        help="Input target date\n[ ex) 20180914 ]")
    arguments = parser.parse_args()
    main(arguments)
