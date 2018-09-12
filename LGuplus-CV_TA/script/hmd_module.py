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
import grpc
import argparse
import traceback
from google.protobuf import json_format
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.common import lang_pb2
from maum.brain.hmd import hmd_pb2
from maum.brain.hmd import hmd_pb2_grpc

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class HmdClient(object):
    def __init__(self, args):
        self.args = args
        self.conf = Config()
        self.conf.init('brain-ta.conf')
        remote = 'localhost:{0}'.format(self.conf.get('brain-ta.hmd.front.port'))
        channel = grpc.insecure_channel(remote)
        self.stub = hmd_pb2_grpc.HmdClassifierStub(channel)

    def load_hmd_model(self, model_name):
        model_path = '{0}/{1}__0.hmdmodel'.format(self.conf.get('brain-ta.hmd.model.dir'), model_name)
        if not os.path.exists(model_path):
            raise Exception('[ERROR] Not existed HMD model. [{0}]'.format(model_path))
        try:
            in_file = open(model_path, 'rb')
            hm = hmd_pb2.HmdModel()
            hm.ParseFromString(in_file.read())
            in_file.close()
            return hm
        except Exception:
            raise Exception(traceback.format_exc())

    def set_model(self, model_name, target_file_path):
        model = hmd_pb2.HmdModel()
        model.lang = lang_pb2.kor
        model.model = model_name
        rules_list = list()
        with open(target_file_path) as target_file:
            for line in target_file:
                line = line.strip()
                line_list = line.split(self.args.file_delimiter)
                if len(line_list) < 2:
                    print '[ERROR] Line field count at least two [{0}]'.format(line)
                    continue
                hmd_client = hmd_pb2.HmdRule()
                hmd_client.rule = line_list[-1]
                hmd_client.categories.extend(line_list[:-1])
                rules_list.append(hmd_client)
        model.rules.extend(rules_list)
        self.stub.SetModel(model)
        model_key = hmd_pb2.ModelKey()
        model_key.lang = lang_pb2.kor
        model_key.model = model_name
#        ret_model = self.stub.GetModel(model_key)
#        print json_format.MessageToJson(ret_model)


#######
# def #
#######
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
    :param          rule:       HMD rule
    :return:                    HMD rule list
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
    :param      category_delimiter:     HMD category delimiter
    :param      model_name:             Model name
    :return:                            HMD dictionary
    """
    conf = Config()
    conf.init('brain-ta.conf')
    model_path = '{0}/{1}__0.hmdmodel'.format(conf.get('brain-ta.hmd.model.dir'), model_name)
    if not os.path.exists(model_path):
        raise Exception('Not existed HMD model [{0}]'.format(model_path))
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


def main(args):
    """
    This program that make HMD model
    :param      args:       Arguments
    """
    try:
        hmd_client = HmdClient(args)
        if args.mode == 0:
            if args.file_path:
                target_file_path = os.path.abspath(args.file_path)
                if os.path.exists(target_file_path):
                    hmd_client.set_model(args.model_name, target_file_path)
                else:
                    print "[ERROR] Can't find {0} file".format(args.file_path)
            else:
                print '[ERROR] You have to input target'
        elif args.mode == 1:
            hm = hmd_client.load_hmd_model(args.model_name)
            for rules in hm.rules:
                print '{0}\t{1}'.format(args.cate_delimiter.join(rules.categories), rules.rule)
        elif args.mode == 2:
            hmd_dict = load_hmd_model(args.cate_delimiter, args.model_name)
            if hmd_dict:
                print 'Success load hmd'
            else:
                print 'Error hmd empty'
        else:
            print "[ERROR] Wrong script mode ['0' or '1']"
    except Exception:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-m', nargs='?', action='store', dest='mode', required=True, type=int,
                        help='''Choose script mode
    0 : Make HMD model
    1 : View HMD model
    2 : Load HMD model'''
                        )
    parser.add_argument('-n', nargs='?', action='store', dest='model_name', required=True, type=str,
                        help="Input HMD model name\n[ ex) test ]")
    parser.add_argument('-f', nargs='?', action='store', dest='file_path', type=str,
                        help="Input target file path\n[ ex) /app/maum/test/test.txt ]")
    parser.add_argument('-fd', nargs='?', action='store', dest='file_delimiter', default='\t', type=str,
                        help="Input target file field delimiter\n[ default = '\\t' ]")
    parser.add_argument('-cd', nargs='?', action='store', dest='cate_delimiter', default='|', type=str,
                        help="Input category delimiter\n[ default = '|' ]")
    arguments = parser.parse_args()
    main(arguments)
