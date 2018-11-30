#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-24, modification: 2018-11-14"

###########
# imports #
###########
import os
import sys
import time
import signal
import shutil
import traceback
import cx_Oracle
import collections
from datetime import datetime
from cfg import config
from lib import logger, util, nlp, hmd, db_connection
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), 'lib/python'))
from common.config import Config

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
    oracle = kwargs['oracle']
    status = kwargs['status']
    rest_send_key = kwargs['rest_send_key']
    processed_dir = kwargs['processed_dir']
    error_dir = kwargs['error_dir']
    json_file_name = kwargs['json_file_name']
    trx_file_name = kwargs['trx_file_name']
    log.error("Execute error process")
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
    try:
        util.update_status(log, oracle, status, rest_send_key)
    except Exception:
        log.error(traceback.format_exc())
        log.error("Can't update status")


def make_hmd_insert_data(log, conf, rest_send_key, start_date, svc_type, overlap_check_cate_dict):
    """
    Make HMD insert DB data
    :param      log:                        Logger
    :param      conf:                       Config
    :param      rest_send_key:              REST_SEND_KEY
    :param      start_date:                 START_DATE
    :param      svc_type:                   SVC_TYPE
    :param      overlap_check_cate_dict:    Overlap checked category dictionary
    :return:                                HMD insert DB data
    """
    # Make HMD insert DB data
    log.info('--> Make HMD insert DB data')
    hmd_bind_list = list()
    for category in overlap_check_cate_dict.keys():
        category_list = category.split(conf.hmd_cate_delimiter)
        if len(category_list) != 10:
            continue
        s_cd_type, s_cd_type_nm, service_cd, service_nm, l_cd, l_cdnm, m_cd, m_cdnm, s_cd, s_cdnm = category_list
        bind = (
            rest_send_key,
            start_date,
            svc_type,
            s_cd_type.strip(),
            service_cd.strip(),
            l_cd.strip(),
            m_cd.strip(),
            s_cd.strip(),
            s_cd_type_nm.strip(),
            service_nm.strip(),
            l_cdnm.strip(),
            m_cdnm.strip(),
            s_cdnm.strip()
        )
        hmd_bind_list.append(bind)
    return hmd_bind_list


def hmd_post_processing(log, conf, hmd_output_list, category_rank_dict):
    """
    HMD post processing
    :param      log:                    Logger
    :param      conf:                   Config
    :param      hmd_output_list:        HMD output list
    :param      category_rank_dict:     Category rank dictionary
    :return:                            Checked category
    """
    # 2. Setup category dictionary
    log.info('--> Setup category dictionary')
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
    # 3. Check category
    log.info('--> Check category')
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
    # 4. Overlap check category
    log.info('--> Overlap check category')
    for group, cate_rank_dict in category_rank_dict.items():
        temp_cat = ''
        temp_rank = 0
        for cat, rank in cate_rank_dict.items():
            if cat in overlap_check_cate_dict:
                if temp_rank == 0:
                    temp_cat = cat
                    temp_rank = rank
                elif temp_rank > rank:
                    del overlap_check_cate_dict[temp_cat]
                else:
                    del overlap_check_cate_dict[cat]
    return hmd_output_check_dict, overlap_check_cate_dict


def hmd_analyze(log, nlp_output_list, hmd_model):
    """
    HMD analyze
    :param      log:                Logger
    :param      nlp_output_list:    NLP output list
    :param      hmd_model:          HMD model
    :return:                        HMD output list
    """
    # 1. HMD analyze
    log.info('--> HMD analyzing')
    hmd_output_list = list()
    for result_list in nlp_output_list:
        for sent, nlp_sent, morph_sent in result_list:
            try:
                if len(sent) < 7:
                    continue
                detect_category_dict = hmd.execute_hmd(sent, nlp_sent, hmd_model)
            except Exception:
                log.error("Can't analyze HMD")
                log.error("Sentence -> {0}".format(sent))
                log.error("NLP sentence -> {0}".format(nlp_sent))
                raise Exception(traceback.format_exc())
            hmd_output_list.append((sent, detect_category_dict))
    return hmd_output_list


def load_hmd_model(log, conf, team_id, svc_type):
    """
    Load HMD model
    :param      log:                Logger
    :param      conf:               Config
    :param      team_id:            TEAM_ID
    :param      svc_type:           SVC_TYPE
    :return:                        HMD model
    """
    # Load HMD model
    log.info('--> Load HMD model')
    hmd_conf = Config()
    hmd_conf.init('brain-ta.conf')
    model_path = '{0}/{1}__0.hmdmodel'.format(hmd_conf.get('brain-ta.hmd.model.dir'), team_id)
    if os.path.exists(model_path):
        hmd_model_name = team_id
    else:
        if svc_type == 'H':
            hmd_model_name = conf.hmd_home_model_name
        elif svc_type == 'M':
            hmd_model_name = conf.hmd_mobile_model_name
        else:
            raise Exception("Wrong format svc_type('H' or 'M') [{0}]".format(svc_type))
        model_path = '{0}/{1}__0.hmdmodel'.format(hmd_conf.get('brain-ta.hmd.model.dir'), hmd_model_name)
        if not os.path.exists(model_path):
            raise Exception('Not existed HMD model [{0}]'.format(model_path))
    hmd_model = hmd.load_hmd_model(conf.hmd_cate_delimiter, hmd_model_name)
    return hmd_model


def execute_hmd_analyze(**kwargs):
    """
    Execute HMD analyze
    :param      kwargs:         Arguments
    :return                     HMD run time(seconds), HMD output list
    """
    log = kwargs['log']
    conf = kwargs['conf']
    rest_send_key = kwargs['rest_send_key']
    start_date = kwargs['start_date']
    start_time = kwargs['start_time']
    file_name = kwargs['file_name']
    svc_type = kwargs['svc_type']
    team_id = kwargs['team_id']
    nlp_output_list = kwargs['nlp_output_list']
    category_rank_dict = kwargs['category_rank_dict']
    log.info('2. Start HMD')
    hmd_st = datetime.fromtimestamp(time.time())
    # Load HMD model
    hmd_model = load_hmd_model(log, conf, team_id, svc_type)
    # HMD analyze
    hmd_output_list = hmd_analyze(log, nlp_output_list, hmd_model)
    # HMD post processing
    hmd_output_check_dict, overlap_check_cate_dict = hmd_post_processing(log, conf, hmd_output_list, category_rank_dict)
    # Make HMD insert DB data
    hmd_bind_list = make_hmd_insert_data(
        log,
        conf,
        rest_send_key,
        start_date,
        svc_type,
        overlap_check_cate_dict
    )
    hmd_run_time = float("{0:.2f}".format(elapsed_time(hmd_st).total_seconds()))
    log.info('Done HMD process(detected count = {0}), the time required = {1}'.format(
        len(overlap_check_cate_dict), elapsed_time(hmd_st)))
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
    return hmd_run_time, hmd_bind_list


def make_nlp_insert_data(log, conf, rest_send_key, start_date, svc_type, nlp_dict, del_dict, senti_dict):
    """
    Make nlp DB insert data
    :param      log:                    Logger
    :param      conf:                   Config
    :param      rest_send_key:          REST_SEND_KEY
    :param      start_date:             START_DATE
    :param      svc_type:               SVC_TYPE
    :param      nlp_dict:               NLP keyword dictionary
    :param      del_dict:               Delete keyword dictionary
    :param      senti_dict:             Sensitivity keyword dictionary
    :return:                            NLP DB insert data
    """
    # Make nlp DB insert data
    log.info('--> Make nlp DB insert data')
    nlp_seq = 0
    insert_nlp_keyword_list = list()
    for key, freq in nlp_dict.items():
        word = key[0]
        tag = key[1]
        if tag not in conf.upload_tag_dict:
            continue
        if word in del_dict:
            continue
        if tag != 'NB' and len(word) < 4:
            continue
        nlp_seq += 1
        entity_cd = senti_dict[word] if word in senti_dict else 'A0'
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
    return insert_nlp_keyword_list


def nlp_post_processing(log, nlp_output_list, brand_keyword_processor):
    """
    NLP post processing
    :param      log:                        Logger
    :param      nlp_output_list:            NLP output list
    :param      brand_keyword_processor:    Flashtext
    :return:                                NLP keyword dictionary
    """
    # Make NLP keyword data
    log.info('--> Make NLP keyword data')
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
            keywords_found = brand_keyword_processor.extract_keywords(target_text)
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
    return nlp_keyword_dict


def nlp_analyze(log, conf, target_file_path):
    """
    Execute NLP engine
    :param      log:                Logger
    :param      conf:               Config
    :param      target_file_path:   Target file path
    :return:                        NLP output list
    """
    line_cnt = 0
    nlp_output_list = list()
    # Load NLP client
    log.info('--> Load NLP client')
    nlp_client = nlp.NlpClient(conf.nlp_engine)
    nlp_analyze_st = datetime.fromtimestamp(time.time())
    with open(target_file_path) as target_file:
        log.info('--> NLP analyzing ...')
        for line in target_file:
            try:
                line_cnt += 1
                line = line.strip()
                result_list = nlp_client.analyze(line)
            except Exception:
                log.error("Can't analyze NLP")
                log.error("Sentence --> ", line)
                raise Exception(traceback.format_exc())
            nlp_output_list.append(result_list)
    log.info('--> Done NLP(line count = {0}), the time required = {1}'.format(line_cnt, elapsed_time(nlp_analyze_st)))
    return nlp_output_list


def execute_nlp_analyze(**kwargs):
    """
    Execute NLP analyze
    :param          kwargs:         Arguments
    :return                         NLP run time(seconds), NLP keyword list and NLP output list
    """
    log = kwargs['log']
    conf = kwargs['conf']
    rest_send_key = kwargs['rest_send_key']
    start_date = kwargs['start_date']
    start_time = kwargs['start_time']
    file_name = kwargs['file_name']
    svc_type = kwargs['svc_type']
    target_file_path = kwargs['target_file_path']
    brand_keyword_processor = kwargs['brand_keyword_processor']
    del_keyword_dict = kwargs['del_keyword_dict']
    senti_keyword_dict = kwargs['senti_keyword_dict']
    log.info('1. Start NLP')
    nlp_st = datetime.fromtimestamp(time.time())
    # Execute nlp engine
    nlp_output_list = nlp_analyze(log, conf, target_file_path)
    # NLP post processing
    nlp_keyword_dict = nlp_post_processing(log, nlp_output_list, brand_keyword_processor)
    # Make NLP insert DB data
    insert_nlp_keyword_list = make_nlp_insert_data(
        log,
        conf,
        rest_send_key,
        start_date,
        svc_type,
        nlp_keyword_dict,
        del_keyword_dict,
        senti_keyword_dict
    )
    nlp_run_time = float("{0:.2f}".format(elapsed_time(nlp_st).total_seconds()))
    log.info('Done NLP process, the time required = {0}'.format(elapsed_time(nlp_st)))
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
    return nlp_run_time, insert_nlp_keyword_list, nlp_output_list


def main(job, brand_keyword_processor, del_keyword_dict, senti_keyword_dict, category_rank_dict):
    """
    This program that execute TA
    :param      job:                         Job
    :param      brand_keyword_processor:     Brand keyword flash text object
    :param      del_keyword_dict:            Delete keyword dictionary
    :param      senti_keyword_dict:          Sensitivity keyword dictionary
    :param      category_rank_dict:          Category rank dictionary
    """
    ta_st = datetime.fromtimestamp(time.time())
    # Ignore kill signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # Job -> (rest_send_key, start_date, start_time, file_name, svc_type, team_id)
    rest_send_key, start_date, start_time, file_name, svc_type, team_id = job
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
    log_str += ' svc_type = {0},'.format(svc_type)
    log_str += ' team_id = {0}'.format(team_id)
    log.info(log_str)
    # Connect Oracle
    try:
        oracle = db_connection.Oracle(config.OracleConfig, failover=True, service_name=True)
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        if error.code == 1033:
            time.sleep(config.OracleConfig.reconnect_interval)
            oracle = db_connection.Oracle(config.OracleConfig, failover=True, service_name=True)
        else:
            error_process(
                log=log,
                oracle='',
                status='99',
                rest_send_key=rest_send_key,
                processed_dir=processed_dir,
                error_dir=error_dir,
                json_file_name=json_file_name,
                trx_file_name=trx_file_name
            )
            log.error(traceback.format_exc())
            log.error("Can't connect db")
            log.info("[E N D] Start time = {0}, The time required = {1}".format(ta_st, elapsed_time(ta_st)))
            sys.exit(1)
    except Exception:
        error_process(
            log=log,
            oracle='',
            status='99',
            rest_send_key=rest_send_key,
            processed_dir=processed_dir,
            error_dir=error_dir,
            json_file_name=json_file_name,
            trx_file_name=trx_file_name
        )
        log.error(traceback.format_exc())
        log.error("Can't connect db")
        log.info("[E N D] Start time = {0}, The time required = {1}".format(ta_st, elapsed_time(ta_st)))
        sys.exit(1)
    # Execute TA
    process_id = '1'
    try:
        if os.path.exists(target_file_path):
            # Execute NLP analyze
            nlp_run_time, insert_nlp_keyword_list, nlp_output_list = execute_nlp_analyze(
                log=log,
                conf=config.TAConfig,
                rest_send_key=rest_send_key,
                start_date=start_date,
                start_time=start_time,
                file_name=file_name,
                svc_type=svc_type,
                target_file_path=target_file_path,
                brand_keyword_processor=brand_keyword_processor,
                del_keyword_dict=del_keyword_dict,
                senti_keyword_dict=senti_keyword_dict
            )
            util.update_status_with_required_time(
                log=log,
                oracle=oracle,
                status='2',
                rest_send_key=rest_send_key,
                nlp_run_time=nlp_run_time,
                hmd_run_time='',
                ta_run_time=''
            )
            # Execute HMD analyze
            process_id = '2'
            hmd_run_time, hmd_bind_list = execute_hmd_analyze(
                log=log,
                conf=config.TAConfig,
                rest_send_key=rest_send_key,
                start_date=start_date,
                start_time=start_time,
                file_name=file_name,
                svc_type=svc_type,
                team_id=team_id,
                nlp_output_list=nlp_output_list,
                category_rank_dict=category_rank_dict
            )
            # Insert NLP keyword
            process_id = '3'
            util.delete_nlp_keyword_info(log, oracle, rest_send_key)
            util.insert_nlp_keyword_info(log, oracle, insert_nlp_keyword_list)
            # Insert HMD result
            process_id = '4'
            util.delete_hmd_result(log, oracle, rest_send_key)
            util.insert_hmd_result(log, oracle, hmd_bind_list)
            # Update status to done
            process_id = '5'
            ta_run_time = float("{0:.2f}".format(elapsed_time(ta_st).total_seconds()))
            util.update_status_with_required_time(
                log=log,
                oracle=oracle,
                status='3',
                rest_send_key=rest_send_key,
                nlp_run_time=nlp_run_time,
                hmd_run_time=hmd_run_time,
                ta_run_time=ta_run_time
            )
        else:
            process_id = '5'
            util.update_status(log, oracle, '90', rest_send_key)
            log.error("Not existed {0}".format(target_file_path))
    except Exception:
        log.error(traceback.format_exc())
        error_process(
            log=log,
            oracle=oracle,
            status='9' + process_id,
            rest_send_key=rest_send_key,
            processed_dir=processed_dir,
            error_dir=error_dir,
            json_file_name=json_file_name,
            trx_file_name=trx_file_name
        )
    finally:
        log.info("[E N D] Start time = {0}, The time required = {1}".format(ta_st, elapsed_time(ta_st)))
