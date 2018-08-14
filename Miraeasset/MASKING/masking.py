#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-00-00, modification: 2017-00-00"

###########
# imports #
###########
import os
import re
import sys
import time
import shutil
import argparse
import traceback
import collections
from datetime import datetime
from cfg.config import MASKING_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DT = ""
ST = ""
NUMBER_RULE = MASKING_CONFIG['number_rule']
BIRTH_RULE = MASKING_CONFIG['birth_rule']
ETC_RULE = MASKING_CONFIG['etc_rule']
EMAIL_RULE = MASKING_CONFIG['email_rule']
ADDRESS_RULE = MASKING_CONFIG['address_rule']
NAME_RULE = MASKING_CONFIG['name_rule']


#######
# def #
#######

def elapsed_time(sdate):
    """
    elapsed time
    :param          sdate:          date object
    :return                         Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param          logger:                     Logger
    :param          delete_file_path:           Input path
    """
    if os.path.exists(delete_file_path):
        try:
            if os.path.isfile(delete_file_path):
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def masking(str_idx, input_line_list):
    """
    Masking
    :param          str_idx:                String index (Split by tab)
    :param          input_line_list:        Input line list
    :return:                                Output dictionary
    """
    line_cnt = 0
    number_rule = MASKING_CONFIG['number_rule']
    birth_rule = MASKING_CONFIG['birth_rule']
    etc_rule = MASKING_CONFIG['etc_rule']
    email_rule = MASKING_CONFIG['email_rule']
    address_rule = MASKING_CONFIG['address_rule']
    name_rule = MASKING_CONFIG['name_rule']
    next_line_cnt = int(MASKING_CONFIG['next_line_cnt'])
    line_dict = collections.OrderedDict()
    for line in input_line_list:
        line = line.strip()
        line_list = line.split("\t")
        sent = line_list[str_idx].strip()
        line_dict[line_cnt] = sent
        line_dict[line_cnt] = sent.decode('euc-kr')
        line_cnt += 1
    line_re_rule_dict = collections.OrderedDict()
    for line_num, line in line_dict.items():
        re_rule_dict = dict()
        if u'성함' in line or u'이름' in line:
            if u'확인' in line or u'어떻게' in line or u'여쭤' in line or u'맞으' in line or u'부탁' in line:
                if 'name_rule' not in re_rule_dict:
                    re_rule_dict['name_rule'] = name_rule
        if u'핸드폰' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'휴대폰' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'전화' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'팩스' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'카드' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'card_number_rule' not in re_rule_dict:
                    re_rule_dict['card_number_rule'] = number_rule
        if u'주민' in line and u'번호' in line and u'앞자리' in line:
            if 'id_number_rule' not in re_rule_dict:
                re_rule_dict['id_number_rule'] = birth_rule
        if u'주민' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'id_number_rule' not in re_rule_dict:
                    re_rule_dict['id_number_rule'] = number_rule
        if u'계좌' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'account_number_rule' not in re_rule_dict:
                    re_rule_dict['account_number_rule'] = number_rule
        if u'사업자' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'id_number_rule' not in re_rule_dict:
                    re_rule_dict['id_number_rule'] = number_rule
        if u'이메일' in line and u'주소' in line:
            if 'email_rule' not in re_rule_dict:
                re_rule_dict['email_rule'] = email_rule
        if u'주소' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'address_rule' not in re_rule_dict:
                    re_rule_dict['address_rule'] = address_rule
        if u'서울' in line or u'경기' in line or u'부산' in line or u'광주' in line or u'대구' in line or u'울산' in line or u'대전' in line or u'충청' in line or u'충북' in line or u'충남' in line or u'경상' in line or u'경북' in line or u'경남' in line or u'제주' in line:
            if 'address_rule' not in re_rule_dict:
                re_rule_dict['address_rule'] = address_rule
        if u'생년월일' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line or u'구요' in line:
                if 'birth_rule' not in re_rule_dict:
                    re_rule_dict['birth_rule'] = birth_rule
        else:
            if 'etc_rule' not in re_rule_dict:
                re_rule_dict['etc_rule'] = etc_rule

        if line_num in line_re_rule_dict:
            line_re_rule_dict[line_num].update(re_rule_dict)
        else:
            line_re_rule_dict[line_num] = re_rule_dict

        for cnt in range(1, next_line_cnt + 1):
            next_line_num = line_num + cnt
            if next_line_num in line_dict:
                if next_line_num in line_re_rule_dict:
                    line_re_rule_dict[next_line_num].update(re_rule_dict)
                else:
                    line_re_rule_dict[next_line_num] = re_rule_dict
    output_dict = collections.OrderedDict()
    for re_line_num, re_rule_dict in line_re_rule_dict.items():
        output_str = ""
        output_json = ""
        if len(line_dict[re_line_num].decode('utf-8')) < int(MASKING_CONFIG['minimum_length']):
            continue
        for rule_name, re_rule in re_rule_dict.items():
            if rule_name == 'name_rule':
                masking_code = "10"
            elif rule_name == 'birth_rule':
                masking_code = "20"
            elif rule_name == 'id_number_rule':
                masking_code = "30"
            elif rule_name == 'card_number_rule':
                masking_code = "40"
            elif rule_name == 'account_number_rule':
                masking_code = "50"
            elif rule_name == 'tel_number_rule':
                masking_code = "60"
            elif rule_name == 'address_rule':
                masking_code = "70"
            elif rule_name == 'email_rule':
                masking_code = "100"
            else:
                masking_code = "110"
            p = re.compile(re_rule.decode('euc-kr'))
            re_result = p.finditer(line_dict[re_line_num].decode('utf-8'))
            if len(output_str) < 1:
                output_str = line_dict[re_line_num].decode('utf-8')
            for item in re_result:
                idx_tuple = item.span()
                start = idx_tuple[0]
                end = idx_tuple[1]
                masking_part = ""
                for idx in output_str[start:end]:
                    if idx == " ":
                        masking_part += " "
                        continue
                    masking_part += "*"
                output_str = output_str.replace(output_str[start:end], masking_part)
        output_dict[re_line_num] = output_str
    return output_dict


def processing(args):
    """
    """
    if args.input_file_path:
        try:
            input_file = open(args.input_file_path, 'r')
            input_line_list = input_file.readlines()
            input_file.close()
        except Exception:
            print "Can't open file."
            exc_info = traceback.format_exc()
            raise Exception(exc_info)
        output_dict = masking(args.str_idx, input_line_list)
        if args.output_file_name:
            output_file = open(args.output_file_name, 'w')
        else:
            output_file = open("{0}_masking_output".format(args.input_file_path), 'w')
        for line_num, line in output_dict.items():
            print line_num, line
        for cnt in range(0, len(input_line_list)):
            print >> output_file, "{0}\t{1}".format(input_line_list[cnt].strip(), output_dict[cnt])
        output_file.close()
    elif args.input_dir_path:
        if os.path.isabs(args.input_dir_path):
            input_dir_path = args.input_dir_path
        else:
            input_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), args.input_dir_path)
        if not os.path.exists(input_dir_path):
            print "Can't find {0}".format(args.input_dir_path)
            sys.exit(1)
        w_ob = os.walk(input_dir_path)
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                try:
                    input_file = open(os.path.join(dir_path, file_name), 'r')
                    input_line_list = input_file.readlines()
                    input_file.close()
                except Exception:
                    print "Can't open file."
                    exc_info = traceback.format_exc()
                    raise Exception(exc_info)
                output_dict = masking(args.str_idx, input_line_list)
                if args.output_file_name:
                    output_file = open(args.output_file_name, 'w')
                else:
                    output_file = open("{0}_masking_output".format(os.path.join(dir_path, file_name)), 'w')
                for cnt in range(0, len(input_line_list)):
                    print >> output_file, "{0}\t{1}".format(input_line_list[cnt].strip(), output_dict[cnt])
                output_file.close()


########
# main #
########


def main(args):
    """
    This is a program that
    """
    global DT
    global ST
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(args)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', action='store', dest='input_dir_path', default=False, type=str,
                        help='Input directory path')
    parser.add_argument('-f', action='store', dest='input_file_path', default=False, type=str,
                        help='Input file path')
    parser.add_argument('-o', action='store', dest='output_file_name', default=False, type=str,
                        help='Output file name')
    parser.add_argument('-m', action='store', dest='merge', default='n', type=str,
                        help='You want merge output file? "y/n" or "Y/N" [default = "n/N"]')
    parser.add_argument('-i', action='store', dest='str_idx', default=0, type=int,
                        help='Index sentence of line split by tab [default = 0]')
    arguments = parser.parse_args()
    main(arguments)
