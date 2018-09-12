#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-24, modification: 2018-09-12"

###########
# imports #
###########
import os
import sys
import time
import signal
import shutil
import traceback
import collections
from datetime import datetime
from flashtext.keyword import KeywordProcessor
from cfg import config
from lib import logger, util, nlp, hmd

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


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


def error_process(**kwargs):
    """
    Execute error process
    :param          kwargs:         Arguments
    """
    log = kwargs['log']
    status = kwargs['status']
    rest_send_key = kwargs['rest_send_key']
    processed_dir = kwargs['processed_dir']
    error_dir = kwargs['error_dir']
    json_file_name = kwargs['json_file_name']
    trx_file_name = kwargs['trx_file_name']
    log.error("Execute error process")
    util.update_status(log, status, rest_send_key)
    if not os.path.exists(error_dir):
        os.makedirs(error_dir)
    if os.path.exists(os.path.join(error_dir, json_file_name)):
        os.remove(os.path.join(error_dir, json_file_name))
    if os.path.exists(os.path.join(error_dir, trx_file_name)):
        os.remove(os.path.join(error_dir, trx_file_name))
    if os.path.exists(os.path.join(processed_dir, json_file_name)):
        shutil.move(os.path.join(processed_dir, json_file_name), error_dir)
    if os.path.exists(os.path.join(processed_dir, trx_file_name)):
        shutil.move(os.path.join(processed_dir, trx_file_name), error_dir)


def load_replace_keyword():
    """
    Load replace keyword
    :return:            Replace keyword trie dictionary
    """
    keyword_processor = KeywordProcessor()
    replace_keyword_file = open('/sorc/maum/cvta/replace.txt', 'r')
    for line in replace_keyword_file:
        line = line.strip()
        line_list = line.split("\t")
        if len(line_list) == 1:
            match_keyword = ' ' + line_list[0].strip()
            display_keyword = ' '
        elif len(line_list) == 2:
            match_keyword = ' ' + line_list[0].strip()
            display_keyword = ' ' + line_list[1].strip()
        else:
            continue
        keyword_processor.add_keyword(match_keyword, display_keyword)
    replace_keyword_file.close()
    return keyword_processor


def execute_hmd_analyze(**kwargs):
    """
    Execute HMD analyze
    :param      kwargs:         Arguments
    :return                     HMD output list
    """
    log = kwargs['log']
    conf = kwargs['conf']
    rest_send_key = kwargs['rest_send_key']
    start_date = kwargs['start_date']
    start_time = kwargs['start_time']
    file_name = kwargs['file_name']
    svc_type = kwargs['svc_type']
    nlp_output_list = kwargs['nlp_output_list']
    log.info('2. Start HMD')
    dt = datetime.fromtimestamp(time.time())
    # Load HMD model
    log.debug('Load HMD model')
    hmd_model_name = conf.hmd_home_model_name if svc_type == 'H' else conf.hmd_mobile_model_name
    hmd_dict = hmd.load_hmd_model(conf.hmd_cate_delimiter, hmd_model_name)
    # 1. HMD analyze
    log.info('--> HMD analyzing ...')
    hmd_output_list = list()
    for result_list in nlp_output_list:
        for sent, nlp_sent, morph_sent in result_list:
            try:
                detect_category_dict = hmd.execute_hmd(sent, nlp_sent, hmd_dict)
            except Exception:
                log.error(traceback.format_exc())
                log.error("Can't execute HMD")
                log.error("Sentence -> {0}".format(sent))
                log.error("NLP sentence -> {0}".format(nlp_sent))
                continue
            hmd_output_list.append((sent, detect_category_dict))
    log.info('--> Modifying HMD output')
    # 2. Modify HMD output
    cate_idx_dict = dict()
    for sentence, detect_category_dict in hmd_output_list:
        if detect_category_dict:
            for detected_cate, value in detect_category_dict.items():
                category = conf.hmd_cate_delimiter.join(detected_cate.split(conf.hmd_cate_delimiter)[:-3])
                idx_id = detected_cate.split(conf.hmd_cate_delimiter)[-3]
                end_idx = detected_cate.split(conf.hmd_cate_delimiter)[-2]
                idx = detected_cate.split(conf.hmd_cate_delimiter)[-1]
                if (category, idx_id) not in cate_idx_dict:
                    cate_idx_dict[(category, idx_id)] = (end_idx, [idx])
                else:
                    cate_idx_dict[(category, idx_id)][1].append(idx)
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
    log.info('Done HMD(detected count = {0}), the time required = {1}'.format(
        len(overlap_check_cate_dict), elapsed_time(dt)))
    # Make HMD result bind list
    hmd_bind_list = list()
    for category in overlap_check_cate_dict.keys():
        category_list = category.split(conf.hmd_cate_delimiter)
        if len(category_list) != 6:
            continue
        bind = (rest_send_key, start_date, svc_type) + tuple(category_list)
        hmd_bind_list.append(bind)
    # Make output file
    if conf.output_file:
        # Make category output file
        base_dir_path = "{0}/{1}".format(start_date, start_time[:2])
        modified_hmd_output_dir = os.path.join(conf.modified_hmd_output_dir_path, base_dir_path)
        if not os.path.exists(modified_hmd_output_dir):
            os.makedirs(modified_hmd_output_dir)
        modified_hmd_output_file = open('{0}/{1}.hmd'.format(modified_hmd_output_dir, file_name), 'w')
        if overlap_check_cate_dict:
            for category in overlap_check_cate_dict.keys():
                print >> modified_hmd_output_file, '{0}\t{1}'.format(file_name, category)
        else:
            print >> modified_hmd_output_file, '{0}\tNone'.format(file_name)
        modified_hmd_output_file.close()
        # Make hmd output file
        hmd_output_dir = os.path.join(conf.hmd_output_dir_path, base_dir_path)
        if not os.path.exists(hmd_output_dir):
            os.makedirs(hmd_output_dir)
        hmd_output_file = open('{0}/{1}.hmd'.format(hmd_output_dir, file_name), 'w')
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
    return hmd_bind_list


def execute_nlp_analyze(**kwargs):
    """
    Execute NLP analyze
    :param          kwargs:         Arguments
    :return                         NLP keyword list and NLP output list
    """
    log = kwargs['log']
    conf = kwargs['conf']
    rest_send_key = kwargs['rest_send_key']
    start_date = kwargs['start_date']
    start_time = kwargs['start_time']
    file_name = kwargs['file_name']
    svc_type = kwargs['svc_type']
    target_file_path = kwargs['target_file_path']
    log.info('1. Start NLP')
    line_cnt = 0
    nlp_output_list = list()
    # Load NLP client
    log.debug('Load NLP client')
    nlp_client = nlp.NlpClient(conf.nlp_engine)
    dt = datetime.fromtimestamp(time.time())
    replace_processor = load_replace_keyword()
    with open(target_file_path) as target_file:
        log.info('--> NLP analyzing ...')
        for line in target_file:
            line_cnt += 1
            line = line.strip()
            line = line.replace('[A]', '[A] ').replace('[C]', '[C] ')
            line = replace_processor.replace_keywords(line)
            line = line.replace('[A] ', '[A]').replace('[C] ', '[C]')
            try:
                result_list = nlp_client.analyze(line)
            except Exception:
                log.error("Can't analyze line")
                log.error("Line --> ", line)
                log.error(traceback.format_exc())
                continue
            nlp_output_list.append(result_list)
    log.info('Done NLP(line count = {0}), the time required = {1}'.format(line_cnt, elapsed_time(dt)))
    dt = datetime.fromtimestamp(time.time())
    # 브랜드명 keyword
    brand_keyword_dict = util.select_brand_keyword(log)
    keyword_processor = KeywordProcessor()
    for brand_keyword in brand_keyword_dict.keys():
        keyword_processor.add_keyword(brand_keyword)
    # Make NLP keyword data
    nlp_keyword_dict = collections.OrderedDict()
    for result_list in nlp_output_list:
        for target_text, nlp_sent, morph_sent in result_list:
            morph_sent_list = morph_sent.split()
            for morph in morph_sent_list:
                if len(morph.split('/')) == 2:
                    word, tag = morph.split('/')
                else:
                    word = '/'
                    tag = morph.split('/')[-1]
                if (word, tag) in nlp_keyword_dict:
                    if target_text.startswith('[A]'):
                        nlp_keyword_dict[(word, tag)]['tx'] += 1
                    else:
                        nlp_keyword_dict[(word, tag)]['rx'] += 1
                else:
                    if target_text.startswith('[A]'):
                        nlp_keyword_dict[(word, tag)] = {'tx': 1, 'rx': 0}
                    else:
                        nlp_keyword_dict[(word, tag)] = {'tx': 0, 'rx': 1}
            # Check brand keyword
            keywords_found = keyword_processor.extract_keywords(target_text)
            for keyword in keywords_found:
                if (keyword, 'NB') in nlp_keyword_dict:
                    if target_text.startswith('[A]'):
                        nlp_keyword_dict[(keyword, 'NB')]['tx'] += 1
                    else:
                        nlp_keyword_dict[(keyword, 'NB')]['rx'] += 1
                else:
                    if target_text.startswith('[A]'):
                        nlp_keyword_dict[(keyword, 'NB')] = {'tx': 1, 'rx': 0}
                    else:
                        nlp_keyword_dict[(keyword, 'NB')] = {'tx': 0, 'rx': 1}
    # 불용어 keyword
    del_keyword_dict = util.select_del_keyword(log)
    # 감성 keyword
    senti_keyword_dict = util.select_senti_keyword(log)
    # Make NLP output
    nlp_seq = 0
    insert_nlp_keyword_list = list()
    for key, freq in nlp_keyword_dict.items():
        word = key[0]
        tag = key[1]
        if tag not in conf.upload_tag_dict:
            continue
        if word in del_keyword_dict:
            continue
        nlp_seq += 1
        entity_cd = senti_keyword_dict[word] if word in senti_keyword_dict else 'A0'
        bind = (
            rest_send_key,
            nlp_seq,
            'f',
            word,
            start_date,
            tag,
            entity_cd,
            '',
            freq['tx'] + freq['rx'],
            freq['tx'],
            freq['rx'],
            svc_type
        )
        insert_nlp_keyword_list.append(bind)
    log.info('Done modify NLP output, the time required = {1}'.format(line_cnt, elapsed_time(dt)))
    # Make output file
    if conf.output_file:
        base_dir_path = "{0}/{1}".format(start_date, start_time[:2])
        nlp_output_dir = os.path.join(conf.nlp_output_dir_path, base_dir_path)
        if not os.path.exists(nlp_output_dir):
            os.makedirs(nlp_output_dir)
        nlp_output_file_path = os.path.join(nlp_output_dir, "{0}.nlp".format(file_name))
        nlp_output_file = open(nlp_output_file_path, 'w')
        for result_list in nlp_output_list:
            for target_text, nlp_sent, morph_sent in result_list:
                print >> nlp_output_file, '{0}\t{1}\t{2}'.format(target_text, nlp_sent, morph_sent)
        nlp_output_file.close()
    return insert_nlp_keyword_list, nlp_output_list


def main(job):
    """
    This program that execute TA
    :param      job:       Job
    """
    # Ignore kill signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    st = datetime.fromtimestamp(time.time())
    # Job -> (rest_send_key, start_date, start_time, file_name, svc_type)
    rest_send_key, start_date, start_time, file_name, svc_type = job
    # Set logger
    base_dir_path = "{0}/{1}".format(start_date, start_time[:2])
    log = logger.set_logger(
        logger_name=config.TAConfig.logger_name,
        log_dir_path=os.path.join(config.TAConfig.log_dir_path, base_dir_path),
        log_file_name='{0}.log'.format(file_name),
        log_level=config.TAConfig.log_level
    )
    # Setup meta file name and directory
    json_file_name = "{0}.json".format(file_name)
    trx_file_name = "{0}_trx.txt".format(file_name)
    processed_dir = os.path.join(config.CollectorConfig.processed_dir_path, base_dir_path)
    target_file_path = os.path.join(processed_dir, trx_file_name)
    error_dir = os.path.join(config.CollectorConfig.error_dir_path, base_dir_path)
    # Execute TA
    log.info("[START] Execute TA ..")
    log_str = 'rest_send_key = {0},'.format(rest_send_key)
    log_str += ' start_date = {0},'.format(start_date)
    log_str += ' start_time = {0},'.format(start_time)
    log_str += ' file_name = {0},'.format(file_name)
    log_str += ' svc_type = {0}'.format(svc_type)
    log.info(log_str)
    process_id = '1'
    try:
        if os.path.exists(target_file_path):
            # Execute NLP analyze
            insert_nlp_keyword_list, nlp_output_list = execute_nlp_analyze(
                log=log,
                conf=config.TAConfig,
                rest_send_key=rest_send_key,
                start_date=start_date,
                start_time=start_time,
                file_name=file_name,
                svc_type=svc_type,
                target_file_path=target_file_path
            )
            process_id = '2'
            # Execute HMD analyze
            util.update_status(log, '2', rest_send_key)
            hmd_bind_list = execute_hmd_analyze(
                log=log,
                conf=config.TAConfig,
                rest_send_key=rest_send_key,
                start_date=start_date,
                start_time=start_time,
                file_name=file_name,
                svc_type=svc_type,
                nlp_output_list=nlp_output_list
            )
            process_id = '3'
            # Insert NLP keyword
            util.delete_nlp_keyword_info(log, rest_send_key)
            util.insert_nlp_keyword_info(log, insert_nlp_keyword_list)
            process_id = '4'
            # Insert HMD result
            util.delete_hmd_result(log, rest_send_key)
            util.insert_hmd_result(log, hmd_bind_list)
            process_id = '5'
            # Update status to done
            util.update_status(log, '3', rest_send_key)
        else:
            process_id = '5'
            util.update_status(log, '90', rest_send_key)
            log.error("Not existed {0}".format(target_file_path))
    except Exception:
        log.error(traceback.format_exc())
        error_process(
            log=log,
            status='9' + process_id,
            rest_send_key=rest_send_key,
            processed_dir=processed_dir,
            error_dir=error_dir,
            json_file_name=json_file_name,
            trx_file_name=trx_file_name
        )
    finally:
        log.info("[E N D] Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
