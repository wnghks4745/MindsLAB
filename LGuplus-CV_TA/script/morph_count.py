#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-30, modification: 0000-00-00"

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
def execute_morph_count(keyword_file_path, target_dir_path, output_dir_path):
    """
    Execute morph count
    :param      keyword_file_path:      Keyword file path
    :param      target_dir_path:        Target directory path
    :param      output_dir_path:        Output directory path
    """
    nlp_word_tag_dict = dict()
    nlp_word_tag_freq_dict = dict()
    keyword_file = open(keyword_file_path)
    for keyword in keyword_file:
        keyword = keyword.strip()
        w_ob = os.walk(target_dir_path)
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                if file_name.endswith('nlp'):
                    nlp_file = open(os.path.join(dir_path, file_name))
                    for line in nlp_file:
                        line = line.strip()
                        if keyword in line:
                            sent, nlp_sent, tag_sent = line.split('\t')
                            tag_sent_list = tag_sent.split()
                            for target_tag_word in tag_sent_list:
                                if keyword in target_tag_word:
                                    if target_tag_word in nlp_word_tag_dict:
                                        if target_tag_word in nlp_word_tag_freq_dict:
                                            nlp_word_tag_freq_dict[target_tag_word] += 1
                                        else:
                                            nlp_word_tag_freq_dict[target_tag_word] = 0
                                            nlp_word_tag_dict[target_tag_word].append(line)
                                    else:
                                        if target_tag_word in nlp_word_tag_freq_dict:
                                            nlp_word_tag_freq_dict[target_tag_word] += 1
                                        else:
                                            nlp_word_tag_freq_dict[target_tag_word] = 0
                                            nlp_word_tag_dict[target_tag_word] = [line]
                    nlp_file.close()
    keyword_file.close()
    cnt = 1
    freq_output_file_name = os.path.join(
        os.path.dirname(keyword_file_path), 'freq_' + os.path.basename(keyword_file_path)
    )
    freq_output_file = open(freq_output_file_name, 'w')
    for tag_word, freq in nlp_word_tag_freq_dict.items():
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
        print >> freq_output_file, "{0}\t{1}\t{2}".format(cnt, tag_word, freq)
        line_output_file = open(os.path.join(output_dir_path, str(cnt)), 'w')
        for line in nlp_word_tag_dict[tag_word]:
            print >> line_output_file, line
        line_output_file.close()
        cnt += 1
    freq_output_file.close()


def main(args):
    """
    This program that morph count
    :param      args:       Arguments
    """
    try:
        keyword_file_path = os.path.abspath(args.keyword_file)
        target_dir_path = os.path.abspath(args.dir_path)
        output_dir_path = os.path.abspath(args.output_dir_path)
        if os.path.exists(keyword_file_path) and os.path.exists(target_dir_path):
            execute_morph_count(keyword_file_path, target_dir_path, output_dir_path)
        else:
            print "[ERROR] Can't find {0} or {1} file".format(args.keyword_file, args.dir_path)
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-k', nargs='?', action='store', dest='keyword_file', type=str, required=True,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-d', nargs='?', action='store', dest='dir_path', type=str, required=True,
                        help="Input target directory path\n[ ex) /app/maum/test ]")
    parser.add_argument('-o', nargs='?', action='store', dest='output_dir_path', type=str, required=True,
                        help="Input output directory path\n[ ex) /app/maum/test/output ]")
    arguments = parser.parse_args()
    main(arguments)
