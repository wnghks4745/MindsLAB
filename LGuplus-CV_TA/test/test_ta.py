#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
import time
import traceback
from lib import logger
from cfg import test_config
from datetime import datetime
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config
from maum.common import lang_pb2
from maum.brain.hmd import hmd_pb2
from maum.brain.nlp import nlp_pb2
from maum.brain.nlp import nlp_pb2_grpc

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class NlpClient(object):
    def __init__(self, engine):
        self.conf = Config()
        self.conf.init('brain-ta.conf')
        if engine.lower() == 'nlp1':
            self.remote = 'localhost:{0}'.format(self.conf.get('brain-ta.nlp.1.kor.port'))
            channel = grpc.insecure_channel(self.remote)
            self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)
        elif engine.lower() == 'nlp2':
            self.remote = 'localhost:{0}'.format(self.conf.get('brain-ta.nlp.2.kor.port'))
            channel = grpc.insecure_channel(self.remote)
            self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)
        elif engine.lower() == 'nlp3':
            self.remote = 'localhost:{0}'.format(self.conf.get('brain-ta.nlp.3.kor.port'))
            channel = grpc.insecure_channel(self.remote)
            self.stub = nlp_pb2_grpc.NaturalLanguageProcessingServiceStub(channel)
        else:
            print 'Not existed Engine'
            raise Exception('Not existed Engine')

    def analyze(self, target_text):
        in_text = nlp_pb2.InputText()
        try:
            in_text.text = target_text
        except Exception:
            in_text.text = unicode(target_text, 'euc-kr').encode('utf-8')
            target_text = unicode(target_text, 'euc-kr').encode('utf-8')
        in_text.lang = lang_pb2.kor
        in_text.split_sentence = True
        in_text.use_tokenizer = False
        in_text.use_space = True
        in_text.level = 1
        in_text.keyword_frequency_level = 0
        ret = self.stub.Analyze(in_text)
        result_list = list()
        for idx in range(len(ret.sentences)):
            nlp_word_list = list()
            morph_word_list = list()
#            text = ret.sentences[idx].text
            analysis = ret.sentences[idx].morps
            for ana_idx in range(len(analysis)):
                morphs_word = analysis[ana_idx].lemma
                morphs_type = analysis[ana_idx].type
                if morphs_type in ['VV', 'VA', 'VX', 'VCP', 'VCN']:
                    nlp_word_list.append('{0}다'.format(morphs_word))
                    morph_word_list.append('{0}다/{1}'.format(morphs_word, morphs_type))
                elif ana_idx > 0 and morph_word_list[-1].split('/')[1] == 'NNG' and morphs_type == 'XSN':
                    before_word = nlp_word_list.pop()
                    morph_word_list.pop()
                    nlp_word_list.append('{0}{1}'.format(before_word, morphs_word))
                    morph_word_list.append('{0}{1}/NNG'.format(before_word, morphs_word))
                elif ana_idx > 0 and morph_word_list[-1].split('/')[1] == 'SL' and morphs_type in ['SN', 'SW']:
                    before_word = nlp_word_list.pop()
                    morph_word_list.pop()
                    nlp_word_list.append('{0}{1}'.format(before_word, morphs_word))
                    morph_word_list.append('{0}{1}/NNG'.format(before_word, morphs_word))
                elif ana_idx > 0 and morph_word_list[-1].split('/')[1] == 'SN' and morphs_type in ['SL', 'SW']:
                    before_word = nlp_word_list.pop()
                    morph_word_list.pop()
                    nlp_word_list.append('{0}{1}'.format(before_word, morphs_word))
                    morph_word_list.append('{0}{1}/NNG'.format(before_word, morphs_word))
                elif ana_idx > 2 and morphs_type == 'SN':
                    if morph_word_list[-2].split('/')[1] == 'SN' and morph_word_list[-1].split('/')[1] == 'SP':
                        middle_word = nlp_word_list.pop()
                        head_word = nlp_word_list.pop()
                        morph_word_list.pop()
                        morph_word_list.pop()
                        nlp_word_list.append('{0}{1}{2}'.format(head_word, middle_word, morphs_word))
                        morph_word_list.append('{0}{1}{2}/NNG'.format(head_word, middle_word, morphs_word))
                    else:
                        nlp_word_list.append('{0}'.format(morphs_word))
                        morph_word_list.append('{0}/{1}'.format(morphs_word, morphs_type))
                else:
                    nlp_word_list.append('{0}'.format(morphs_word))
                    morph_word_list.append('{0}/{1}'.format(morphs_word, morphs_type))
            nlp_sent = ' '.join(nlp_word_list).encode('utf-8').strip()
            morph_sent = ' '.join(morph_word_list).encode('utf-8').strip()
            result_list.append((target_text, nlp_sent, morph_sent))
        return result_list


#######
# def #
#######
def elapsed_time(start_time):
    """
    elapsed time
    :param          start_time:          date object
    :return                              Required time (type : datetime)
    """
    end_time = datetime.fromtimestamp(time.time())
    required_time = end_time - start_time
    return required_time


def find_loc(space_idx_list, pos, plus_num, len_nlp_sent):
    """
    Find word location
    :param      space_idx_list:         List of space index
    :param      pos:                    Position
    :param      plus_num:               Add index integer
    :param      len_nlp_sent:           Length of NLP sentence
    :return:                            Start and end index
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
    tmp_nlp_sent = tmp_nlp_sent.decode('utf-8')
    for dtc_word in dtc_word_list:
        if len(dtc_word) == 0:
            continue
        b_pos = True
        b_sub = False
        b_neg = False
        key_pos = 0
        end_pos = len(tmp_nlp_sent)
        # Searching Special Command
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
                    plus_num = int(dtc_word[w_loc])
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


def execute_hmd(sent, nlp_sent, hmd_dict):
    """
    Execute HMD
    :param      sent:               Sentence
    :param      nlp_sent:           NLP sentence
    :param      hmd_dict:           HMD matrix dictionary
    :return:                        Detected category dictionary
    """
    # Execute HMD
    detect_category_dict = dict()
    tmp_nlp_sent = ' {0} '.format(nlp_sent)
    space_idx_list = [idx for idx in range(len(tmp_nlp_sent)) if tmp_nlp_sent.startswith(' ', idx)]
    if hmd_dict:
        for dtc_keyword in hmd_dict.keys():
            dtc_word_list = dtc_keyword.split('$')
            output_nlp_sent, b_print = find_hmd(dtc_word_list, sent, tmp_nlp_sent, space_idx_list)
            if b_print:
                for category, hmd_rule in hmd_dict[dtc_keyword]:
                    if category not in detect_category_dict:
                        detect_category_dict[category] = [(dtc_keyword, hmd_rule, output_nlp_sent)]
                    else:
                        detect_category_dict[category].append((dtc_keyword, hmd_rule, output_nlp_sent))
    return detect_category_dict


def execute_hmd_analyze(log, conf, file_name, nlp_output_list):
    """
    Execute HMD analyze
    :param      log:                    Logger
    :param      conf:                   Config
    :param      file_name:              File name without extension
    :param      nlp_output_list:        NLP output list
    """
    log.info('Start HMD')
    dt = datetime.fromtimestamp(time.time())
    # Load HMD model
    hmd_dict = load_hmd_model(conf.hmd_cate_delimiter, conf.hmd_model_name)
    # 1. HMD analyze
    hmd_output_list = list()
    for result_list in nlp_output_list:
        for sent, nlp_sent, morph_sent in result_list:
            try:
                detect_category_dict = execute_hmd(sent, nlp_sent, hmd_dict)
            except Exception:
                log.error(traceback.format_exc())
                log.error("Can't execute HMD")
                log.error("Sentence -> {0}".format(sent))
                log.error("NLP sentence -> {0}".format(nlp_sent))
                continue
            hmd_output_list.append((sent, detect_category_dict))
    # 2. Keep category in dictionary
    cate_idx_dict = dict()
    for sentence, detect_category_dict in hmd_output_list:
        if detect_category_dict:
            for category, value in detect_category_dict.items():
                output_cate = conf.hmd_cate_delimiter.join(category.split(conf.hmd_cate_delimiter)[:-3])
                idx_id = category.split(conf.hmd_cate_delimiter)[-3]
                end_idx = category.split(conf.hmd_cate_delimiter)[-2]
                idx = category.split(conf.hmd_cate_delimiter)[-1]
                if (output_cate, idx_id) not in cate_idx_dict:
                    cate_idx_dict[(output_cate, idx_id)] = (end_idx, [idx])
                else:
                    cate_idx_dict[(output_cate, idx_id)][1].append(idx)
    # 3. Overlap check category
    hmd_output_check_dict = dict()
    overlap_check_cate_dict = dict()
    for key, value in cate_idx_dict.items():
        flag = True
        end_idx = int(value[0])
        idx_list = value[1]
        for cnt in range(1, end_idx + 1):
            if str(cnt) in idx_list:
                continue
            else:
                flag = False
        if flag:
            hmd_output_check_dict[key[0] + conf.hmd_cate_delimiter + key[1]] = 1
            overlap_check_cate_dict[key[0]] = 1
    log.info('Done HMD, the time required = {0}'.format(elapsed_time(dt)))
    # Make output file
    if not os.path.exists(conf.modified_hmd_output_dir_path):
        os.makedirs(conf.modified_hmd_output_dir_path)
    modified_hmd_output_file = open('{0}/{1}.hmd'.format(conf.modified_hmd_output_dir_path, file_name), 'w')
    if overlap_check_cate_dict:
        for category in overlap_check_cate_dict.keys():
            print >> modified_hmd_output_file, '{0}\t{1}'.format(file_name, category)
    else:
        print >> modified_hmd_output_file, '{0}\tNone'.format(file_name)
    modified_hmd_output_file.close()
    if not os.path.exists(conf.hmd_output_dir_path):
        os.makedirs(conf.hmd_output_dir_path)
    hmd_output_file = open('{0}/{1}.hmd'.format(conf.hmd_output_dir_path, file_name), 'w')
    for sentence, detect_category_dict in hmd_output_list:
        if detect_category_dict:
            for category, value in detect_category_dict.items():
                check_cate = conf.hmd_cate_delimiter.join(category.split(conf.hmd_cate_delimiter)[:-2])
                if check_cate in hmd_output_check_dict:
                    for dtc_keyword, hmd_rule, output_nlp_sent in value:
                        print >> hmd_output_file, '{0}\t{1}\t{2}'.format(category, dtc_keyword, sentence)
                else:
                    print >> hmd_output_file, '{0}\t{1}\t{2}'.format('Dele', 'Dele', sentence)
        else:
            print >> hmd_output_file, '{0}\t{1}\t{2}'.format('None', 'None', sentence)
    hmd_output_file.close()


def execute_nlp_analyze(log, conf, file_name, nlp_client, target_file_path):
    """
    Execute NLP analyze
    :param      log:                    Logger
    :param      conf:                   Config
    :param      file_name:              Target file name without extension
    :param      nlp_client:             NLP client object
    :param      target_file_path:       Target file absolute path
    """
    log.info('Start NLP')
    nlp_output_list = list()
    dt = datetime.fromtimestamp(time.time())
    with open(target_file_path) as target_file:
        for line in target_file:
            line = line.strip()
            try:
                result_list = nlp_client.analyze(line)
            except Exception:
                log.error("Can't analyze line")
                log.error("Line --> ", line)
                log.error(traceback.format_exc())
                continue
            nlp_output_list.append(result_list)
    log.info('Done NLP, the time required = {0}'.format(elapsed_time(dt)))
    if not os.path.exists(conf.nlp_output_dir_path):
        os.makedirs(conf.nlp_output_dir_path)
    nlp_output_file_path = os.path.join(conf.nlp_output_dir_path, "{0}.nlp".format(file_name))
    nlp_output_file = open(nlp_output_file_path, 'w')
    for result_list in nlp_output_list:
        for target_text, nlp_sent, morph_sent in result_list:
            print >> nlp_output_file, '{0}\t{1}\t{2}'.format(target_text, nlp_sent, morph_sent)
    nlp_output_file.close()
    return nlp_output_list


def main(target_file_name):
    """
    This program that execute CV TA
    :param      target_file_name:       Target file name
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts)
    conf = test_config.TAConfig
    file_name = os.path.splitext(target_file_name)[0]
    target_file_path = os.path.join(conf.processed_dir_path, target_file_name)
    log = logger.set_logger(
        logger_name=conf.logger_name,
        log_dir_path=os.path.join(conf.log_dir_path, 'test_' + datetime.fromtimestamp(ts).strftime('%Y%m%d')),
        log_file_name='{0}.log'.format(file_name),
        log_level='debug'
    )
    try:
        log.info("[START] Execute TA ..")
        nlp_client = NlpClient(conf.nlp_engine)
        if os.path.exists(target_file_path):
            nlp_output_list = execute_nlp_analyze(log, conf, file_name, nlp_client, target_file_path)
            execute_hmd_analyze(log, conf, file_name, nlp_output_list)
        else:
            log.error("Can't find {0}".format(target_file_path))
        log.info("[E N D] Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
    except Exception:
        log.error(traceback.format_exc())
        #TODO: ERROR PROCESS
        sys.exit(1)
