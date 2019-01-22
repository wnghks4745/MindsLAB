#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astraea@mindslab.ai"
__date__ = "creation: 2018-07-11, modification: 0000-00-00"
__copyright__ = "All Rights Reserved by MINDsLAB"

###########
# imports #
###########
import os
import sys
import argparse
import traceback
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.brain.hmd import hmd_pb2

###########
# options #
###########
reload(sys)
sys.setdefaultencodig("utf-8")


#######
# def #
#######
def find_loc(space_idx_list, pos, plus_num, len_nlp_sent):
    """
    Find word location
    :param      space_idx_list:     List of space index
    :param      pos:                Position
    :param      plus_num:           Add index integer
    :param      len_nlp_sent:       Length of NLP sentence
    :return:                        Start and end index
    """
    s_i = 0
    len_vs = len(space_idx_list)
    for s_i in range(len_vs):
        if space_idx_list[s_i] > pos:
            break
    key_pos = space_idx_list[s_i]
    if s_i != -1:
        e_i = s_i + plus_num
        if e_i >= len_vs:
            e_i = len_vs - 1
        end_pos = space_idx_list[e_i] + 1
    else:
        end_pos = len_nlp_sent
    return key_pos, end_pos


def find_hmd(dtc_word_list, sentence, tmp_nlp_sent, space_idx_list):
    """
    Find HMD
    :param      dtc_word_list:          Detect keyword list
    :param      sentence:               Sentence
    :param      tmp_nlp_sent:           NLP sentence
    :param      space_idx_list:         List of space index
    :return:                            NLP sentence and True/False
    """
    pos = 0
    output_nlp_sent = ''
    tmp_nlp_sent = tmp_nlp_sent
    for dtc_word in dtc_word_list:
        if len(dtc_word) == 0:
            continue
        b_pos = True
        b_sub = False
        b_neg = False
        key_pos = 0
        end_pos = len(tmp_nlp_sent)
        # Sentence Special Command
        w_loc = -1
        while True:
            w_loc += 1
            if w_loc == len(dtc_word):
                return tmp_nlp_sent.strip(), False
            if dtc_word[w_loc] == '!':
                if b_pos:
                    b_pos = False
                else:
                    b_neg = True
            elif dtc_word[w_loc] == '@':
                key_pos = pos
            elif dtc_word[w_loc] == '+':
                w_loc += 1
                try:
                    plus_num = int(dtc_word[w_loc]) + len(dtc_word.strip().split(' ')) - 1
                    key_pos, end_pos = find_loc(space_idx_list, pos, plus_num, len(tmp_nlp_sent))
                except ValueError:
                    print 'ERR: + next number Check Dictionary [Keyword rule = {0}]'.format(dtc_word)
                    return tmp_nlp_sent.strip(), False
            elif dtc_word[w_loc] == '%':
                b_sub = True
            else:
                k_str = dtc_word[w_loc:]
                break
        # Searching lemma
        t_pos = 0
        if b_pos:
            if b_sub:
                pos = tmp_nlp_sent[key_pos:end_pos].find(k_str)
            else:
                pos = tmp_nlp_sent[key_pos:end_pos].find(' ' + k_str + ' ')
            if pos != -1:
                pos = pos + key_pos
        else:
            t_pos = key_pos
            if b_neg:
                pos = sentence.find(k_str)
            elif b_sub:
                pos = tmp_nlp_sent[key_pos:end_pos].find(k_str)
            else:
                pos = tmp_nlp_sent[key_pos:end_pos].find(' ' + k_str + ' ')
        # Checking Result
        if b_pos:
            if pos > -1:
                if b_sub:
                    replace_str = ''
                    for item in k_str:
                        replace_str += ' ' if item == ' ' else '_'
                    output_nlp_sent = tmp_nlp_sent[:key_pos]
                    output_nlp_sent += tmp_nlp_sent[key_pos:end_pos].replace(k_str, replace_str)
                    output_nlp_sent += tmp_nlp_sent[end_pos:]
                else:
                    target_str = ' ' + k_str + ' '
                    replace_str = ''
                    for item in k_str:
                        replace_str += ' ' if item == ' ' else '_'
                    replace_str = ' ' + replace_str + ' '
                    output_nlp_sent = tmp_nlp_sent[:key_pos]
                    output_nlp_sent += tmp_nlp_sent[key_pos:end_pos].replace(target_str, replace_str)
                    output_nlp_sent += tmp_nlp_sent[end_pos:]
            else:
                return tmp_nlp_sent.strip(), False
        else:
            if pos > -1:
                return tmp_nlp_sent.strip(), False
            else:
                pos = t_pos
    return output_nlp_sent.strip(), True


def vec_word_combine(tmp_list, category, dtc_keyword, dtc_keyword_list, level, hmd_rule):
    """
    Vec word combine
    :param      tmp_list:               Temp list
    :param      category:               Category
    :param      dtc_keyword:            HMD keyword rule
    :param      dtc_keyword_list:       HMD keyword rule list
    :param      level:                  Index of HMD keyword rule list
    :param      hmd_rule:               HMD rule
    :return:                            HMD matrix list
    """
    if level == len(dtc_keyword_list):
        tmp_list.append((category, dtc_keyword, hmd_rule))
    elif level == 0:
        for idx in range(len(dtc_keyword_list[level])):
            tmp_dtc_keyword = dtc_keyword_list[level][idx]
            vec_word_combine(tmp_list, category, tmp_dtc_keyword, dtc_keyword_list, level + 1, hmd_rule)
    else:
        for idx in range(len(dtc_keyword_list[level])):
            if dtc_keyword[-1] == '@':
                tmp_dtc_keyword = dtc_keyword[:-1] + '$@' + dtc_keyword_list[level][idx]
            elif dtc_keyword[-1] == '%':
                tmp_dtc_keyword = dtc_keyword[:-1] + '$%' + dtc_keyword_list[level][idx]
            elif len(dtc_keyword) > 1 and dtc_keyword[-2] == '+' and ('0' <= dtc_keyword[-1] <= '9'):
                tmp_dtc_keyword = dtc_keyword[:-1] + '$+' + dtc_keyword[-1] + dtc_keyword_list[level][idx]
            elif dtc_keyword[-1] == '#':
                tmp_dtc_keyword = dtc_keyword[:-1] + '$#' + dtc_keyword_list[level][idx]
            else:
                tmp_dtc_keyword = dtc_keyword + '$' + dtc_keyword_list[level][idx]
            vec_word_combine(tmp_list, category, tmp_dtc_keyword, dtc_keyword_list, level + 1, hmd_rule)
    return tmp_list


def split_hmd_rule(rule):
    """
    Split HMD rule
    :param      rule:       HMD rule
    :return:                HMD rule list
    """
    flag = False
    detect_rule = str()
    rule_list = list()
    for idx in range(len(rule)):
        if rule[idx] == '(':
            flag = True
        elif rule[idx] == ')' and len(detect_rule) != 0:
            rule_list.append(detect_rule)
            detect_rule = str()
            flag = False
        elif flag:
            detect_rule += rule[idx]
    return rule_list


def load_hmd_model(category_delimiter, model_name):
    """
    Load HMD model
    :param      category_delimiter:         Category delimiter
    :param      model_name:                 Model name
    :return:                                HMD dictionary
    """
    conf = Config()
    conf.init('brain-ta.conf')
    model_path = '{0}/{1}__0.hmdmodel'.format(conf.get('brain-ta.hmd.model.dir'), model_name)
    if not os.path.exists(model_path):
        raise Exception('[ERROR] Not existed HMD model [{0}]'.format(model_path))
    try:
        in_file = open(model_path, 'rb')
        hm = hmd_pb2.HmdModel()
        hm.ParseFromString(in_file.read())
        in_file.close()
        # Make HMD matrix list
        matrix_list = list()
        for rules in hm.rules:
            dtc_keyword_list = list()
            rule_list = split_hmd_rule(rules.rule)
            for idx in range(len(rule_list)):
                dtc_keyword = rule_list[idx].split('|')
                dtc_keyword_list.append(dtc_keyword)
            tmp_list = list()
            category = category_delimiter.join(rules.categories)
            matrix_list += vec_word_combine(tmp_list, category, '', dtc_keyword_list, 0, rules.rule)
        # Make HMD matrix dictionary
        hmd_dict = dict()
        for category, dtc_keyword, hmd_rule in matrix_list:
            if len(category) < 1 or category.startswith('#') or len(dtc_keyword) < 1:
                continue
            if dtc_keyword not in hmd_dict:
                hmd_dict[dtc_keyword] = [[category, hmd_rule]]
            else:
                hmd_dict[dtc_keyword].append([category, hmd_rule])
        return hmd_dict
    except Exception:
        raise Exception(traceback.format_exc())


def execute_hmd(sentence, nlp_sentence, hmd_dict):
    """
    Execute HMD
    :param      sentence:           Sentence
    :param      nlp_sentence:       NLP sentence
    :param      hmd_dict:           HMD dictionary
    :return:                        Detected category dictionary
    """
    # Execute HMD
    detect_category_dict = dict()
    tmp_nlp_sent = ' {0} '.format(nlp_sentence).decode('utf-8')
    space_idx_list = [idx for idx in range(len(tmp_nlp_sent)) if tmp_nlp_sent.startswith(' ', idx)]
    if hmd_dict:
        for dtc_keyword in hmd_dict.keys():
            dtc_word_list = dtc_keyword.split('$')
            output_nlp_sent, b_print = find_hmd(dtc_word_list, sentence, tmp_nlp_sent, space_idx_list)
            if b_print:
                for category, hmd_rule in hmd_dict[dtc_keyword]:
                    if category not in detect_category_dict:
                        detect_category_dict[category] = [(dtc_keyword, hmd_rule, output_nlp_sent)]
                    else:
                        detect_category_dict[category].append((dtc_keyword, hmd_rule, output_nlp_sent))
    return detect_category_dict


def execute_file_hmd(args, target_file_path, hmd_dict):
    """
    Execute file HMD
    :param      args:                   Arguments
    :param      target_file_path:       Target file absolute path
    :param      hmd_dict:               HMD dictionary
    """
    output_list = list()
    line_no = 0
    with open(target_file_path) as target_file:
        for line in target_file:
            line = line.strip()
            line_list = line.split(args.file_delimiter)
            if len(line_list) + 1 < args.txt_idx or len(line_list) + 1 < args.nlp_idx:
                print 'idx is wrong : {0}'.format(line)
                continue
            sentence = line_list[args.txt_idx]
            nlp_sentence = line_list[args.nlp_idx]
            try:
                detect_category_dict = execute_hmd(sentence, nlp_sentence, hmd_dict)
            except Exception:
                print "[ERROR] Can't execute HMD [{0}]".format(target_file_path)
                print traceback.format_exc()
                continue
            if args.tm_print:
                if detect_category_dict:
                    for category, value in detect_category_dict.items():
                        for dtc_keyword, hmd_rule, output_nlp_sent in value:
                            print '{0}\t{1}\t{2}'.format(category, dtc_keyword, sentence)
                else:
                    print "{0}\t{1}\t{2}".format('None', 'None', sentence)
            output_list.append((sentence, detect_category_dict, line_no, nlp_sentence))
            line_no += 1
    if args.output:
        output_file = open('{0}_hmd'.format(target_file_path), 'w')
        for sentence, detect_category_dict, line_no, nlp_sentence in output_list:
            if detect_category_dict:
                for category, value in detect_category_dict.items():
                    for dtc_keyword, hmd_rule, output_nlp_sent in value:
                        print >> output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                            line_no, category, dtc_keyword, sentence, nlp_sentence)
            else:
                print >> output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(line_no, 'None', 'None', sentence, nlp_sentence)
        output_file.close()


def main(args):
    """
    This program that execute HMD
    :param      args:       Arguments
    """
    try:
        # Load HMD model
        hmd_dict = load_hmd_model(args.cate_delimiter, args.model_name)
        if args.text:
            if args.nlp_text:
                detect_category_dict = execute_hmd(args.text, args.nlp_sent, hmd_dict)
                if detect_category_dict:
                    for category, value in detect_category_dict.items():
                        for dtc_keyword, hmd_rule, output_nlp_sent in value:
                            print 'Category --> ', category
                            print 'Detect keyword --> ', dtc_keyword
                            print 'Sentence --> ', args.text
                else:
                    print 'Category --> ', 'None'
                    print 'Detect keyword --> ', 'None'
                    print 'Sentence --> ', args.text
            else:
                print '[ERROR] You have to input NLP sentence'
        if args.file_path:
            target_file_path = os.path.abspath(args.file_path)
            if os.path.exists(target_file_path):
                execute_file_hmd(args, target_file_path, hmd_dict)
            else:
                print "[ERROR] Can't find {0} file".format(args.file_path)
        if args.dir_path:
            target_dir_path = os.path.abspath(args.dir_path)
            if os.path.exists(target_dir_path):
                for dir_path, sub_dirs, files in os.walk(target_dir_path):
                    for file_name in files:
                        execute_file_hmd(args, os.path.join(dir_path, file_name), hmd_dict)
            else:
                print "[ERROR] Can't find {0} directory".format(args.dir_path)
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-t', nargs='?', action='store', dest='text', type=str,
                        help="Input target text\n[ ex) '[A]드라마 다시보기 결제하다' ]")
    parser.add_argument('-nt', nargs='?', action='store', dest='nlp_text', type=str,
                        help="Input target NLP text\n[ ex) '[ A ] 드라마 다시 보다 기 결제 하다' ]")
    parser.add_argument('-n', nargs='?', action='store', dest='model_name', required=True, type=str,
                        help="Input HMD model name\n[ ex) test ]")
    parser.add_argument('-f', nargs='?', action='store', dest='file_path', type=str,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-d', nargs='?', action='store', dest='dir_path', type=str,
                        help="Input target directory path\n[ ex) /app/maum/test ]")
    parser.add_argument('-fd', nargs='?', action='store', dest='file_delimiter', default='\t', type=str,
                        help="Input target file delimiter\n[ default = '\\t' ]")
    parser.add_argument('-cd', nargs='?', action='store', dest='cate_delimiter', default='|', type=str,
                        help="Input category delimiter\n[ default = '|' ]")
    parser.add_argument('-ti', nargs='?', action='store', dest='txt_idx', default=0, type=int,
                        help="Input target sentence index\n[ default = 0 ]")
    parser.add_argument('-ni', nargs='?', action='store', dest='nlp_idx', default=1, type=int,
                        help="Input target NLP sentence index\n[ default = 1 ]")
    parser.add_argument('-o', nargs='?', action='store', dest='output', default=True, type=bool,
                        help="Do you want output file? [ default = True ]\n[ True / False]")
    parser.add_argument('-p', nargs='?', action='store', dest='tm_print', default=False, type=bool,
                        help="Do you want print terminal? [ default = False ]\n[ True / False]")
    arguments = parser.parse_args()
    main(arguments)
