#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-12-12, modification: 2019-01-08"

###########
# imports #
###########
import os
import sys
import time
import signal
import traceback
import collections
from datetime import datetime
from cfg import config
from lib import logger, util, nlp, hmd, openssl

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
START_DATE = ''


#######
# def #
#######
def elapsed_time(start_time):
    """
    elapsed time
    :param      start_time:         date object
    :return:                        Required time (type : datetime)
    """
    end_time = datetime.fromtimestamp(time.time())
    required_time = end_time - start_time
    return required_time


def sync_hmd_add(**kwargs):
    model_list = kwargs['model_list']
    syn_keyword_list = kwargs['syn_keyword_list']
    sync_list = list()
    syn_keyword_dict = dict()
    for syn_keyword in syn_keyword_list:
        if syn_keyword['SYNONYM_CD'] not in syn_keyword_dict:
            syn_keyword_dict[syn_keyword['SYNONYM_CD']] = list()
        syn_keyword_dict[syn_keyword['SYNONYM_CD']].append(syn_keyword['SYNONYM_BODY'])
    for item_list in syn_keyword_dict.values():
        for keyword in item_list:
            modified_syn_keyword = '|'.join(list(set(item_list) - set(keyword)))
            sync_list.append([keyword, modified_syn_keyword])
    return hmd.new_hmd_add(model_list, sync_list)


def make_reply_dict(stt_info_dict_list):
    """
    Make REPLY Dictionary (응답내용 사전)
    :param      stt_info_dict_list:         STT Information Dictionary List
    :return:                                REPLY Dictionary
    """
    reply_dict = dict()
    line_num = -1
    for stt_info_dict in stt_info_dict_list:
        line_num += 1
        if stt_info_dict['SPEAKER_KIND'] == 'C' or stt_info_dict['SPEAKER_KIND'] == 'M':
            reply_dict[line_num] = stt_info_dict['SENTENCE']
    return reply_dict


def make_reply(stt_info_dict_list, cfg, line_num):
    """
    Make REPLY(응답내용)
    :param      stt_info_dict_list:         STT Information Dictionary List
    :param      cfg:                        Config
    :param      line_num:                   Line Number
    :return:                                REPLY(응답내용)
    """
    reply_dict = make_reply_dict(stt_info_dict_list)
    reply = ''
    count = cfg.reply_count
    first_flag = True
    for cnt in range(1, cfg.reply_range + 1):
        target_line_num = line_num + cnt
        if target_line_num in reply_dict:
            if not first_flag:
                reply += '|||'
            first_flag = False
            reply += '{0} '.format(reply_dict[target_line_num])
            count -= 1
        if count == 0:
            break
    return reply.strip()


def update_tb_based_offer_info(log, job, script_meta_dict, offer_info, oracle, cfg):
    """
    Update TB_BASED_OFFER_INFO ( 청약정보 테이블 )
    :param      log:                    Logger
    :param      job:                    Job
    :param      script_meta_dict:       Script Meta Information Dictionary
    :param      offer_info:             Offer Information Dictionary
    :param      oracle:                 Oracle
    :param      cfg:                    Config
    """
    log.info('8. Update TB_BASED_OFFER_INFO')
    # Update data 정제
    insrps_cmp_id = job['INSRPS_CMP_ID']
    target_evlu_chart_cd = ''
    for goods_cd, goods_dict in script_meta_dict.items():
        for evlu_chart_cd, evlu_chart_cd_dict in goods_dict['DICT'].items():
            target_evlu_chart_cd = evlu_chart_cd
    ta_score = offer_info['TA_SCORE']
    rec_count = offer_info['REC_COUNT']
    target_evlu_yn = offer_info['TARGET_EVLU_YN']
    tm_insr_pd_nm = offer_info['TM_INSR_PD_NM']
    insrco_nm = offer_info['INSRCO_NM']
    sum_duration_min = offer_info['SUM_DURATION_MIN']
    # Update dict 생성
    update_dict = dict()
    update_dict['INSRPS_CMP_ID'] = insrps_cmp_id
    update_dict['TARGET_EVLU_CHART_CD'] = target_evlu_chart_cd
    update_dict['TA_SCORE'] = ta_score
    update_dict['REC_COUNT'] = rec_count
    update_dict['TARGET_EVLU_YN'] = target_evlu_yn
    update_dict['TM_INSR_PD_NM'] = tm_insr_pd_nm
    update_dict['INSRCO_NM'] = insrco_nm
    update_dict['SUM_DURATION_MIN'] = sum_duration_min
    util.update_data_to_tb_based_offer_info(log, update_dict, oracle, cfg)
    util.update_status(log, '04', job, oracle=oracle, cfg=cfg)


def insert_tb_ta_result_evluation(log, job, script_meta_dict, offer_info, oracle, cfg):
    """
    Insert TB_TA_RESULT_EVLUATION ( QA 심사결과 테이블 )
    :param      log:                    Logger
    :param      job:                    Job
    :param      script_meta_dict:       Script Meta Information Dictionary
    :param      offer_info:             Offer Information Dictionary
    :param      oracle:                 Oracle
    :param      cfg:                    Config
    :return
    """
    log.info('7. Insert TB_TA_RESULT_EVLUATION')
    # insert data 정제
    insrps_cmp_id = job['INSRPS_CMP_ID']
    insrps_cst_mngt_no = job['INSRPS_CST_MNGT_NO']
    insr_sbscrp_dt = job['INSR_SBSCRP_DT']
    tm_mngt_bzs_nm = job['TM_MNGT_BZS_NM']
    tm_cnr_nm = job['TM_CNR_NM']
    goods_cd = ''
    goods_nm = ''
    for goods_cd, goods_cd_dict in script_meta_dict.items():
        goods_nm = goods_cd_dict['META']['GOODS_NM']
    insr_sbscrp_rgr_empno = job['INSR_SBSCRP_RGR_EMPNO']
    insr_sbscrp_rgr_fnm = job['INSR_SBSCRP_RGR_FNM']
    last_result_cd = offer_info['LAST_RESULT_CD']
    measure_result_cd = offer_info['MEASURE_RESULT_CD']
    insrco_nm = offer_info['INSRCO_NM']
    qa_score = offer_info['TA_SCORE']
    ta_score = offer_info['TA_SCORE']
    # insert dict 생성
    insert_dict = dict()
    insert_dict['INSRPS_CMP_ID'] = insrps_cmp_id
    insert_dict['INSRPS_CST_MNGT_NO'] = insrps_cst_mngt_no
    insert_dict['INSR_SBSCRP_DT'] = insr_sbscrp_dt
    insert_dict['TM_MNGT_BZS_NM'] = tm_mngt_bzs_nm
    insert_dict['TM_CNR_NM'] = tm_cnr_nm
    insert_dict['GOODS_NM'] = goods_nm
    insert_dict['GOODS_CD'] = goods_cd
    insert_dict['QA_RESULT_CD'] = last_result_cd
    insert_dict['TA_RESULT_CD'] = last_result_cd
    insert_dict['MEASURE_RESULT_CD'] = measure_result_cd
    insert_dict['INSR_SBSCRP_RGR_EMPNO'] = insr_sbscrp_rgr_empno
    insert_dict['INSR_SBSCRP_RGR_FNM'] = insr_sbscrp_rgr_fnm
    insert_dict['LAST_RESULT_CD'] = last_result_cd
    insert_dict['MEASURE_RESULT_CD'] = measure_result_cd
    insert_dict['INSRCO_NM'] = insrco_nm
    insert_dict['QA_SCORE'] = qa_score
    insert_dict['TA_SCORE'] = ta_score
    util.delete_tb_ta_result_evluation(log, insrps_cmp_id, oracle, cfg)
    util.insert_data_to_tb_ta_result_evluation(log, insert_dict, oracle, cfg)


def make_tb_result_evlu_script(log, cfg, call_info_dict, job, script_meta_dict):
    """
    Make TB_RESULT_EVLU_SCRIPT ( 스크립트 결과 테이블 )
    :param      log:                    Logger
    :param      cfg:                    Config
    :param      call_info_dict:         Call Information Dictionary
    :param      job:                    Job
    :param      script_meta_dict:       Script Meta Information Dictionary
    :return:                            TB_RESULT_EVLU_SCRIPT Dictionary
                                            Detect Criteria Code Dictionary
    """
    log.info('6-1. Make TB_RESULT_EVLU_SCRIPT Dictionary')
    tb_result_evlu_script_dict = dict()
    detect_criteria_cd_dict = dict()
    for ucid, info_dict in call_info_dict.items():
        for hmd_result_dict in info_dict['HMD_SECTION_RESULT_LIST']:
            # 미탐지인 경우 Insert 하지 않음
            if hmd_result_dict['category'] == 'None' or hmd_result_dict['category'] == 'New_None':
                continue
            category_list = hmd_result_dict['category'].strip().split('!@#$')
            # insert data 정제
            insrps_cmp_id = job['INSRPS_CMP_ID']
            insrps_cst_mngt_no = job['INSRPS_CST_MNGT_NO']
            evlu_criteria_cd = ''
            script_cd = category_list[0].encode('utf-8')
            priority_num = 0
            reply = ''
            if len(category_list) > 4:
                goods_cd, evlu_chart_cd, evlu_criteria_cd, script_cd = category_list[:4]
                if evlu_criteria_cd not in detect_criteria_cd_dict:
                    detect_criteria_cd_dict[evlu_criteria_cd] = dict()
                if script_cd not in detect_criteria_cd_dict[evlu_criteria_cd]:
                    detect_criteria_cd_dict[evlu_criteria_cd][script_cd] = dict()
                if hmd_result_dict['line_num'] not in detect_criteria_cd_dict[evlu_criteria_cd][script_cd]:
                    detect_criteria_cd_dict[evlu_criteria_cd][script_cd][hmd_result_dict['line_num']] = True
                if script_meta_dict[goods_cd]['DICT'][evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT'][script_cd]['META']['REPLY_YN'] == 'Y':
                    reply = make_reply(info_dict['STT_INFO_DICT_LIST'], cfg, hmd_result_dict['line_num'])
                if not str(script_meta_dict[goods_cd]['DICT'][evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT'][script_cd]['META']['PRIORITY_NUM']) == 'None':
                    priority_num = script_meta_dict[goods_cd]['DICT'][evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT'][script_cd]['META']['PRIORITY_NUM']
            ucid_gkey = info_dict['UCID_GKEY']
            armsoffset = info_dict['STT_INFO_DICT_LIST'][hmd_result_dict['line_num']]['ARMSOFFSET']
            rxtx_kind = info_dict['STT_INFO_DICT_LIST'][hmd_result_dict['line_num']]['RXTX_KIND']
            speaker_kind = info_dict['STT_INFO_DICT_LIST'][hmd_result_dict['line_num']]['SPEAKER_KIND']
            sentence_line = hmd_result_dict['line_num']
            reply_yn = 'N' if len(reply) < 1 else 'Y'
            tm_cal_dt = info_dict['TM_CAL_DT']
            # insert_dict 생성
            insert_dict = dict()
            insert_dict['INSRPS_CMP_ID'] = insrps_cmp_id
            insert_dict['INSRPS_CST_MNGT_NO'] = insrps_cst_mngt_no
            insert_dict['EVLU_CRITERIA_CD'] = evlu_criteria_cd
            insert_dict['SCRIPT_CD'] = script_cd
            insert_dict['PRIORITY_NUM'] = priority_num
            insert_dict['UCID_GKEY'] = ucid_gkey
            insert_dict['ARMSOFFSET'] = armsoffset
            insert_dict['RXTX_KIND'] = rxtx_kind
            insert_dict['SPEAKER_KIND'] = speaker_kind
            insert_dict['SENTENCE_LINE'] = sentence_line
            insert_dict['REPLY_YN'] = reply_yn
            insert_dict['REPLY'] = reply
            insert_dict['SENTENCE_SEQ'] = 0
            insert_dict['TM_CAL_DT'] = tm_cal_dt
            while True:
                key = '{0}_{1}_{2}_{3}'.format(insrps_cmp_id, evlu_criteria_cd, script_cd, insert_dict['SENTENCE_SEQ'])
                if key not in tb_result_evlu_script_dict:
                    tb_result_evlu_script_dict[key] = insert_dict
                    break
                insert_dict['SENTENCE_SEQ'] += 1
    return tb_result_evlu_script_dict, detect_criteria_cd_dict


def insert_tb_result_evlu_criteria(log, cfg, call_info_dict, job, script_meta_dict, oracle, ora_cfg):
    """
    Insert TB_RESULT_EVLU_CRITERIA ( 평가항목 결과 테이블 )
    :param      log:                    Logger
    :param      cfg:                    Config
    :param      call_info_dict:         Call Information Dictionary
    :param      job:                    Job
    :param      script_meta_dict:       Script Meta Information Dictionary
    :param      oracle:                 Oracle
    :param      ora_cfg:                Oracle Config
    :return:                            Offer Information Dictionary
    """
    # Make 스크립트 결과
    evlu_script_dict, detect_criteria_cd_dict = make_tb_result_evlu_script(
        log, cfg, call_info_dict, job, script_meta_dict)
    log.info('6-2. Insert TB_RESULT_EVLU_CRITERIA Dictionary')
    tb_result_evlu_criteria_dict = dict()
    offer_output = dict()
    insrco_nm = ''
    tm_insr_pd_nm = ''
    goods_match_list = util.select_goods_match(log, job, oracle, cfg)
    for goods_match_dict in goods_match_list:
        goods_list = util.select_goods(log, goods_match_dict, oracle, cfg)
        for goods_dict in goods_list:
            tm_insr_pd_nm = goods_dict['GOODS_NM']
            tb_based_insurer_list = util.select_tb_based_insurer(log, goods_dict['INSRCO_C'], oracle, cfg)
            for tb_based_insurer_dict in tb_based_insurer_list:
                insrco_nm = tb_based_insurer_dict['INSRCO_NM']
    offer_output['LAST_RESULT_CD'] = '01'
    offer_output['MEASURE_RESULT_CD'] = '01'
    offer_output['TA_SCORE'] = 0
    offer_output['REC_COUNT'] = len(call_info_dict.keys())
    offer_output['TARGET_EVLU_YN'] = 'N'
    offer_output['SUM_DURATION_MIN'] = 0
    offer_output['INSRCO_NM'] = insrco_nm
    offer_output['TM_INSR_PD_NM'] = tm_insr_pd_nm
    for call_info in call_info_dict.values():
        if str(call_info['DURATION_MINUTE']) != 'None':
            offer_output['SUM_DURATION_MIN'] += int(call_info['DURATION_MINUTE'])
    setting_dict_list = util.select_setting(log, oracle, ora_cfg)
    setting_dict = dict()
    for item_dict in setting_dict_list:
        setting_dict[item_dict['SETTING_TYPE']] = item_dict
    detected_n_check = 100
    last_result_02_check = 100
    if '03' in setting_dict:
        detected_n_check -= 1
        if setting_dict['03']['OPTION3_VALUE'] == 'Y':
            detected_n_check = int(setting_dict['03']['OPTION4_VALUE'])
        last_result_02_check = -1
        if setting_dict['03']['OPTION5_VALUE'] == 'Y':
            last_result_02_check = int(setting_dict['03']['OPTION6_VALUE'])
    for goods_cd, goods_cd_dict in script_meta_dict.items():
        for evlu_chart_cd, evlu_chart_cd_dict in goods_cd_dict['DICT'].items():
            for evlu_criteria_cd, evlu_criteria_cd_dict in evlu_chart_cd_dict['DICT'].items():
                # insert data 정제
                insrps_cmp_id = job['INSRPS_CMP_ID']
                insrps_cst_mngt_no = job['INSRPS_CST_MNGT_NO']
                ta_score = float(evlu_criteria_cd_dict['META']['ITEM_SCORE'])
                # 탐지기준
                detected_yn = 'N'
                success_yn = 'N'
                score_rule_cd = evlu_criteria_cd_dict['META']['SCORE_RULE_CD']
                if score_rule_cd == 'D':
                    if evlu_criteria_cd in detect_criteria_cd_dict:
                        detect_script_cd_list = detect_criteria_cd_dict[evlu_criteria_cd].keys()
                        target_script_cd_list = list()
                        for script_cd, script_cd_dict in evlu_criteria_cd_dict['DICT'].items():
                            if script_cd_dict['META'].get('GRADING_YN') == 'Y':
                                target_script_cd_list.append(script_cd)
                        log.debug('EVLU_CRITERIA_CD : {0}'.format(evlu_criteria_cd))
                        log.debug('detect_script_cd_list : {0}'.format(set(detect_script_cd_list)))
                        log.debug('target_script_cd_list : {0}'.format(set(target_script_cd_list)))
                        if set(target_script_cd_list) == set(detect_script_cd_list) & set(target_script_cd_list):
                            detected_yn = 'Y'
                            success_yn = 'Y'
                        else:
                            detected_yn = 'P'
                            success_yn = 'P'
                elif score_rule_cd == 'ND':
                    success_yn = 'Y'
                    if evlu_criteria_cd in detect_criteria_cd_dict:
                        detect_line_num_list = list()
                        for script_cd, line_num_dict in detect_criteria_cd_dict[evlu_criteria_cd].items():
                            for line_num in line_num_dict.keys():
                                detect_line_num_list.append('{0}_{1}'.format(script_cd, line_num))
                        if len(set(detect_line_num_list)) > 0:
                            detected_yn = 'P'
                            success_yn = 'P'
                        if len(set(detect_line_num_list)) > 5:
                            detected_yn = 'Y'
                            success_yn = 'N'
                if success_yn == 'P':
                    ta_score *= 0.5
                elif success_yn == 'N':
                    ta_score *= 0
                reply_yn = 'X'
                target_reply_y_list = list()
                for script_cd, script_cd_dict in evlu_criteria_cd_dict['DICT'].items():
                    if script_cd_dict['META'].get('REPLY_YN') == 'Y':
                        target_reply_y_list.append(script_cd)
                if len(set(target_reply_y_list)) > 0:
                    reply_yn = 'N'
                    detect_reply_y_list = list()
                    for info_dict in evlu_script_dict.values():
                        if info_dict['INSRPS_CMP_ID'] == insrps_cmp_id and \
                            info_dict['EVLU_CRITERIA_CD'] == evlu_criteria_cd and info_dict['REPLY_YN'] == 'Y':
                            detect_reply_y_list.append(info_dict['SCRIPT_CD'])
                    if set(target_reply_y_list) == set(detect_reply_y_list) & set(target_reply_y_list):
                        reply_yn = 'Y'
                last_result_cd = '01'
                if success_yn != 'Y':
                    if evlu_criteria_cd_dict['META']['LAST_RESULT_CD'] in ['02', '08']:
                        if evlu_criteria_cd_dict['META']['LAST_RESULT_CD'] == '02':
                            last_result_02_check -= 1
                        last_result_cd = evlu_criteria_cd_dict['META']['LAST_RESULT_CD']
                if int(offer_output['LAST_RESULT_CD']) < int(last_result_cd):
                    offer_output['LAST_RESULT_CD'] = last_result_cd
                measure_cd = '01'
                if success_yn != 'Y':
                    detected_n_check -= 1
                    if evlu_criteria_cd_dict['META']['MEASURE_CD'] in ['02', '03', '04']:
                        measure_cd = evlu_criteria_cd_dict['META']['MEASURE_CD']
                if int(offer_output['MEASURE_RESULT_CD']) < int(measure_cd):
                    offer_output['MEASURE_RESULT_CD'] = measure_cd
                offer_output['TA_SCORE'] += ta_score
                # insert_dict 생성
                insert_dict = dict()
                insert_dict['INSRPS_CMP_ID'] = insrps_cmp_id
                insert_dict['INSRPS_CST_MNGT_NO'] = insrps_cst_mngt_no
                insert_dict['EVLU_CRITERIA_CD'] = evlu_criteria_cd
                insert_dict['TA_SCORE'] = ta_score
                insert_dict['QA_SCORE'] = ta_score
                insert_dict['SUCCESS_YN'] = success_yn
                insert_dict['DETECTED_YN'] = detected_yn
                insert_dict['REPLY_YN'] = reply_yn
                insert_dict['SCORE_RULE_CD'] = score_rule_cd
                key = '{0}_{1}'.format(insrps_cmp_id, evlu_criteria_cd)
                if key not in tb_result_evlu_criteria_dict:
                    tb_result_evlu_criteria_dict[key] = insert_dict
    if '03' in setting_dict:
        ta_score_check = True
        if setting_dict['03']['OPTION1_VALUE'] == 'Y':
            ta_score_check = False
            if offer_output['TA_SCORE'] < int(setting_dict['03']['OPTION2_VALUE']):
                ta_score_check = True
        if ta_score_check and detected_n_check < 1 and last_result_02_check < 1:
            offer_output['LAST_RESULT_CD'] = '08'
    if '01' in setting_dict:
        if setting_dict['01']['OPTION1_VALUE'] == 'Y' and offer_output['LAST_REULST_CD'] == '02':
            offer_output['TARGET_EVLU_YN'] = 'Y'
        if setting_dict['01']['OPTION3_VALUE'] == 'Y' and offer_output['LAST_RESULT_CD'] == '08':
            offer_output['TARGET_EVLU_YN'] = 'Y'
        if setting_dict['01']['OPTION2_VALUE'] == 'Y' and job['INSR_QA_YN'] == 'Y':
            offer_output['TARGET_EVLU_YN'] = 'N'
    util.delete_data_to_tb_result_evlu_criteria(log, job, oracle, ora_cfg)
    util.insert_data_to_tb_result_evlu_criteria(log, tb_result_evlu_criteria_dict, oracle, ora_cfg)
    util.delete_data_to_tb_ta_result_evlu_script(log, job, oracle, ora_cfg)
    util.insert_data_to_tb_ta_result_evlu_script(log, evlu_script_dict, oracle, ora_cfg)
    return offer_output


def detect_sect_hmd_output(log, conf, call_info_dict, job, script_meta_dict):
    """
    Detect Section from HMD output
    :param      log:                    Logger
    :param      conf:                   Config
    :param      call_info_dict:         Call Information Dictionary
    :param      job:                    Job
    :param      script_meta_dict:       Script Meta Information Dictionary
    :return:                            Modify HMD output in Call Information Dictionary
    """
    log.info('5. Detect Section from HMD output')
    temp_dict = collections.OrderedDict()
    base_dir_name = job['INSRPS_CMP_ID']
    dedup_dict = dict()
    for ucid, info_dict in call_info_dict.items():
        if ucid not in temp_dict:
            temp_dict[ucid] = collections.OrderedDict()
        for line_num, category, dtc_keyword, sentence, nlp_sent in info_dict['HMD_RESULT_LIST']:
            category_list = category.strip().split('!@#$')
            dedup_key = '{0}_{1}_{2}'.format(ucid, line_num, '!@#$'.join(category_list[:4]))
            if dedup_key in dedup_dict:
                continue
            dedup_dict[dedup_key] = True
            # Check the line num dictionary
            if line_num not in temp_dict[ucid]:
                temp_dict[ucid][line_num] = {'mother': list(), 'baby': list()}
            # Make the mother Check
            mother_check = False
            if len(category_list) > 5:
                goods_cd, evlu_chart_cd, evlu_criteria_cd, script_cd = category_list[:4]
                if evlu_criteria_cd.endswith('17') and len(info_dict['STT_INFO_DICT_LIST']) - 30 > line_num:
                    category = 'New_None'
                    dtc_keyword = 'New_None'
                if (evlu_criteria_cd.endswith('19') or evlu_criteria_cd('30')) and line_num > 30:
                    category = 'New_None'
                    dtc_keyword = 'New_None'
                if script_meta_dict[goods_cd]['DICT'][evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT'][script_cd]['META']['STRT_SCRIPT_YN'] == 'Y':
                    mother_check = True
            # mother category check
            if mother_check:
                temp_dict[ucid][line_num]['mother'].append(
                    {
                        'line_num': line_num,
                        'category': category,
                        'dtc_keyword': dtc_keyword,
                        'sentence': sentence,
                        'nlp_sent': nlp_sent
                    }
                )
            else:
                temp_dict[ucid][line_num]['baby'].append(
                    {
                        'line_num': line_num,
                        'category': category,
                        'dtc_keyword': dtc_keyword,
                        'sentence': sentence,
                        'nlp_sent': nlp_sent
                    }
                )
    # Make the detect section output
    current_category_list = list()
    current_category_dict = dict()
    total_hmd_output_file = object
    total_hmd_output_file_path = ''
    if conf.output_file:
        total_hmd_output_dir = os.path.join(os.path.join(conf.hmd_section_output_dir_path, START_DATE), base_dir_name)
        if not os.path.exists(total_hmd_output_dir):
            os.makedirs(total_hmd_output_dir)
        total_hmd_output_file_path = '{0}/{1}.hmd'.format(total_hmd_output_dir, base_dir_name)
        total_hmd_output_file = open(total_hmd_output_file_path, 'w')
    mother_life = 0
    for ucid, line_num_dict in temp_dict.items():
        detect_sect_hmd_list = list()
        hmd_output_file = object
        hmd_output_file_path = ''
        if conf.output_file:
            hmd_output_dir = os.path.join(os.path.join(conf.hmd_section_output_dir_path, START_DATE), base_dir_name)
            if not os.path.exists(hmd_output_dir):
                os.makedirs(hmd_output_dir)
            hmd_output_file_path = '{0}/{1}.hmd'.format(hmd_output_dir, ucid)
            hmd_output_file = open(hmd_output_file_path, 'w')
        for detect_line_num, detect_line_dict in line_num_dict.items():
            # 엄마문장 수명 체크
            mother_life -= 1
            if mother_life < 0:
                current_category_list = list()
            # Check the mother script
            if len(detect_line_dict['mother']) > 0:
                mother_life = 5
                current_category_list = list()
                for category_dict in detect_line_dict['mother']:
                    category = category_dict['category']
                    category_list = category.split('!@#$')
                    current_category_list.append('!@#$'.join(category_list[:3]))
                    detect_sect_hmd_list.append(category_dict)
                    if conf.output_file:
                        print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                            category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
                        print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                            ucid, category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
            # Save the current category
            current_category_dict[detect_line_num] = current_category_list
            # Before one line category
            last_detect_line_num = int(detect_line_num)-1 if int(detect_line_num) > 0 else 0
            for category_dict in detect_line_dict['baby']:
                category = category_dict['category']
                category_list = category.split('!@#$')
                script_cd = category_list[-1]
                if len(category_list) > 2:
                    script_cd = category_list[-2]
                # output baby category
                flag = False
                for current_category in current_category_list:
                    if category.startswith(current_category):
                        detect_sect_hmd_list.append(category_dict)
                        if conf.output_file:
                            print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                                category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                                category_dict['sentence'], category_dict['nlp_sent'])
                            print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                                ucid, category_dict['line_num'], category_dict['category'],
                                category_dict['dtc_keyword'], category_dict['sentence'], category_dict['nlp_sent'])
                        flag = True
                        break
                if flag:
                    continue
                # output last baby category
                for last_category in current_category_dict[last_detect_line_num]:
                    if category.startswith(last_category):
                        detect_sect_hmd_list.append(category_dict)
                        if conf.output_file:
                            print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                                category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                                category_dict['sentence'], category_dict['nlp_sent'])
                            print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                                ucid, category_dict['line_num'], category_dict['category'],
                                category_dict['dtc_keyword'], category_dict['sentence'], category_dict['nlp_sent'])
                        flag = True
                        break
                if flag:
                    continue
                # output 공통 스크립트 category
                if script_cd.startswith('CO'):
                    detect_sect_hmd_list.append(category_dict)
                    if conf.output_file:
                        print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                            category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
                        print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                            ucid, category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
                elif len(category_list) < 5:
                    detect_sect_hmd_list.append(category_dict)
                    if conf.output_file:
                        print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                            category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
                        print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                            ucid, category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
                else:
                    category_dict['category'] = 'New_None'
                    category_dict['dtc_keyword'] = 'New_None'
                    detect_sect_hmd_list.append(category_dict)
                    if conf.output_file:
                        print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                            category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
                        print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                            ucid, category_dict['line_num'], category_dict['category'], category_dict['dtc_keyword'],
                            category_dict['sentence'], category_dict['nlp_sent'])
        if conf.output_file:
            hmd_output_file.close()
            openssl.encrypt_file([hmd_output_file_path])
        call_info_dict[ucid]['HMD_SECTION_RESULT_LIST'] = detect_sect_hmd_list
    if conf.output_file:
        total_hmd_output_file.close()
        openssl.encrypt_file([total_hmd_output_file_path])
    return call_info_dict


def execute_hmd_analyze(log, conf, call_info_dict, job, oracle, cfg):
    """
    Execute HMD analyze
    :param      log:                    Logger
    :param      conf:                   Config
    :param      call_info_dict:         Call Information Dictionary
    :param      job:                    Job
    :param      oracle:                 Oracle
    :param      cfg:                    Config
    :return:                            Call Information Dictionary with HMD Result
                                            Mother Script List
    """
    base_dir_name = job['INSRPS_CMP_ID']
    log.info('4. Start HMD')
    # Make HMD model
    hmd_model_name = job['INSRPS_CMP_ID']
    make_hmd_dict = dict()
    # 1. 비 스크립트 사전 준비
    banned_terms_list = util.select_banned_terms(log, oracle, cfg)
    for banned_terms_dict in banned_terms_list:
        if banned_terms_dict['SCRIPT_CD'] in make_hmd_dict:
            continue
        make_hmd_dict[banned_terms_dict['SCRIPT_CD']] = banned_terms_dict
    # 2. 스크립트 사전 준비
    goods_match_list = util.select_goods_match(log, job, oracle, cfg)
    log.debug('goods match count : {0}'.format(len(goods_match_list)))
    if len(goods_match_list) < 1:
        st = job['st']
        util.update_status(log, '91', job, oracle=oracle, cfg=cfg)
        util.delete_tb_ta_result_evluation(log, job['INSRPS_CMP_ID'], oracle, cfg)
        log.error("Target Offer is not target data : {0}".format(job['INSRPS_CMP_ID']))
        log.info("[E N D] Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
        sys.exit(1)
    script_meta_dict = dict()
    for goods_match_dict in goods_match_list:
        if str(goods_match_dict['GOODS_CD']) == 'None':
            st = job['st']
            util.update_status(log, '91', job, oracle=oracle, cfg=cfg)
            util.delete_tb_ta_result_evluation(log, job['INSRPS_CMP_ID'], oracle, cfg)
            log.error("Target Offer is not target data : {0}".format(job['INSRPS_CMP_ID']))
            log.info("[E N D] Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
            sys.exit(1)
        # goods_list = util.select_goods(log, goods_match_dict, oracle, cfg)
        goods_list = util.select_evlu_chart(log, goods_match_dict, oracle, cfg, job)
        log.debug('\tgoods count : {0}'.format(len(goods_list)))
        if len(goods_list) < 1:
            st = job['st']
            util.update_status(log, '92', job, oracle=oracle, cfg=cfg)
            util.delete_tb_ta_result_evluation(log, job['INSRPS_CMP_ID'], oracle, cfg)
            log.error("Target Offer is not evlu_chart_cd data : {0}".format(job['INSRPS_CMP_ID']))
            log.info("[E N D] Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
            sys.exit(1)
        for goods_dict in goods_list:
            goods_cd = goods_dict['GOODS_CD']
            if goods_cd not in script_meta_dict:
                temp_dict = dict()
                temp_dict['DICT'] = dict()
                temp_dict['META'] = goods_dict
                script_meta_dict[goods_cd] = temp_dict
            evlu_match_list = util.select_evlu_match(log, goods_dict, oracle, cfg)
            log.debug('\t\tevlu match count : {0}'.format(len(evlu_match_list)))
            for evlu_match_dict in evlu_match_list:
                # evlu_criteria_list = util.select_evlu_criteria(log, evlu_match_dict)
                use_evlu_chart_cd = goods_dict['USE_EVLU_CHART_CD']
                if use_evlu_chart_cd not in script_meta_dict[goods_cd]['DICT']:
                    temp_dict = dict()
                    temp_dict['DICT'] = dict()
                    temp_dict['META'] = evlu_match_dict
                    script_meta_dict[goods_cd]['DICT'][use_evlu_chart_cd] = temp_dict
                # log.debug('\t\t\tevlu criteria count : {0}'.format(len(evlu_criteria_list)))
                # for evlu_criteria_dict in evlu_criteria_list:
                script_evlu_match_list = util.select_script_evlu_match(log, evlu_match_dict, oracle, cfg)
                evlu_criteria_cd = evlu_match_dict['EVLU_CRITERIA_CD']
                if evlu_criteria_cd not in script_meta_dict[goods_cd]['DICT'][use_evlu_chart_cd]['DICT']:
                    temp_dict = dict()
                    temp_dict['DICT'] = dict()
                    temp_dict['META'] = evlu_match_dict
                    script_meta_dict[goods_cd]['DICT'][use_evlu_chart_cd]['DICT'][evlu_criteria_cd] = temp_dict
                log.debug('\t\t\t\tscript evlu match count : {0}'.format(len(script_evlu_match_list)))
                for script_evlu_match_dict in script_evlu_match_list:
                    script_list = util.select_script(log, script_evlu_match_dict, oracle, cfg)
                    script_cd = script_evlu_match_dict['SCRIPT_CD']
                    if script_cd not in script_meta_dict[goods_cd]['DICT'][use_evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT']:
                        temp_dict = dict()
                        temp_dict['DICT'] = dict()
                        temp_dict['META'] = script_evlu_match_dict
                        script_meta_dict[goods_cd]['DICT'][use_evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT'][script_cd] = temp_dict
                    log.debug('\t\t\t\t\tscript count : {0}'.format(len(script_list)))
                    for script_dict in script_list:
                        script_dict['HMD'] = script_dict['HMD'].replace('&amp;', '&').replace('&lt;', '<').replace(
                            '&gt;', '>').replace('&quot', '"').replace('&#039', "'")
                        script_meta_dict[goods_cd]['DICT'][use_evlu_chart_cd]['DICT'][evlu_criteria_cd]['DICT'][script_cd]['META'].update(script_dict)
                        script_seq = script_dict['SCRIPT_SEQ']
                        key = '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                            goods_cd, use_evlu_chart_cd, evlu_criteria_cd, script_cd, script_seq)
                        if key in make_hmd_dict:
                            continue
                        make_hmd_dict[key] = script_dict
    # sync HMD add
    hmd_target_list = list()
    syn_keyword_list = util.select_syn_keyword(log, oracle, cfg)
    for key, value in make_hmd_dict.items():
        sync_hmd = sync_hmd_add(model_list=[value['HMD']], syn_keyword_list=syn_keyword_list)
        hmd_target_list.append('{0}\t{1}'.format(key, sync_hmd[0]).strip())
    hmd.set_model(hmd_model_name, hmd_target_list, '\t')
    # Load HMD model
    log.debug('Load HMD model')
    hmd_dict = hmd.load_hmd_model(conf.hmd_cate_delimiter, hmd_model_name)
    # 1. HMD analyze
    total_hmd_output_file = object
    total_hmd_output_file_path = ''
    if conf.output_file:
        total_hmd_output_dir = os.path.join(os.path.join(conf.hmd_output_dir_path, START_DATE), base_dir_name)
        if not os.path.exists(total_hmd_output_dir):
            os.makedirs(total_hmd_output_dir)
        total_hmd_output_file_path = '{0}/{1}.hmd'.format(total_hmd_output_dir, base_dir_name)
        total_hmd_output_file = open(total_hmd_output_file_path, 'w')
    for ucid, info_dict in call_info_dict.items():
        log.info('--> HMD analyzing ...')
        hmd_output_list = list()
        line_num = -1
        for sent, nlp_sent, morph_sent in info_dict['NLP_RESULT_LIST']:
            line_num += 1
            try:
                detect_category_dict = hmd.execute_hmd(sent, nlp_sent, hmd_dict)
            except Exception:
                log.error(traceback.format_exc())
                log.error("Can't execute HMD")
                log.error("Sentence -> {0}".format(sent))
                log.error("NLP sentence -> {0}".format(nlp_sent))
                continue
            hmd_output_list.append((sent, detect_category_dict, line_num, nlp_sent))
        log.info('--> Make HMD output')
        # Make hmd output file
        hmd_output_file = object
        hmd_output_file_path = ''
        if conf.output_file:
            hmd_output_dir = os.path.join(os.path.join(conf.hmd_output_dir_path, START_DATE), base_dir_name)
            if not os.path.exists(hmd_output_dir):
                os.makedirs(hmd_output_dir)
            hmd_output_file_path = '{0}/{1}.hmd'.format(hmd_output_dir, ucid)
            hmd_output_file = open(hmd_output_file_path, 'w')
        hmd_result_list = list()
        for sentence, detect_category_dict, line_num, nlp_sent in hmd_output_list:
            if detect_category_dict:
                for category, value in detect_category_dict.items():
                    category = category.encode('utf-8')
                    for dtc_keyword, hmd_rule, output_nlp_sent in value:
                        if conf.output_file:
                            print >> hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}'.format(
                                line_num, category, dtc_keyword, sentence, nlp_sent)
                            print >> total_hmd_output_file, '{0}\t{1}\t{2}\t{3}\t{4}\t{5}'.format(
                                ucid, line_num, category, dtc_keyword, sentence, nlp_sent)
                        hmd_result_list.append((line_num, category, dtc_keyword, sentence, nlp_sent))
                        # 중복제거를 위해 한번 출력 후 break
                        break
            else:
                if conf.output_file:
                    print >> hmd_output_file, '{0}\tNone\tNone\t{1}\t{2}'.format(line_num, sentence, nlp_sent)
                    print >> total_hmd_output_file, '{0}\t{1}\tNone\tNone\t{2}\t{3}'.format(
                        ucid, line_num, sentence, nlp_sent)
                    hmd_result_list.append((line_num, 'None', 'None', sentence, nlp_sent))
        if conf.output_file:
            hmd_output_file.close()
            openssl.encrypt_file([hmd_output_file_path])
        call_info_dict[ucid]['HMD_RESULT_LIST'] = hmd_result_list
    if conf.output_file:
        total_hmd_output_file.close()
        openssl.encrypt_file([total_hmd_output_file_path])
    hmd_model_path = '{0}/{1}__0.hmdmodel'.format(os.path.join(os.getenv('MAUM_ROOT'), 'trained/hmd'), hmd_model_name)
    if os.path.exists(hmd_model_path):
        os.remove(hmd_model_path)
    return call_info_dict, script_meta_dict


def execute_nlp_analyze(log, conf, call_info_dict, job):
    """
    Execute NLP analyze
    :param      log:                    Logger
    :param      conf:                   Config
    :param      call_info_dict:         Call Information Dictionary
    :param      job:                    Job
    :return:                            Call Information Dictionary with NLP Result
    """
    log.info('3. Start NLP')
    line_cnt = 0
    # Load NLP client
    log.debug('Load NLP client')
    nlp_client = nlp.NlpClient(conf.nlp_engine)
    for ucid, info_dict in call_info_dict.items():
        nlp_output_list = list()
        dt = datetime.fromtimestamp(time.time())
        log.info('--> NLP analyzing ... UCID : {0}'.format(ucid))
        for stt_info_dict in info_dict['STT_INFO_DICT_LIST']:
            line = '[{0}] {1}'.format(stt_info_dict['SPEAKER_KIND'].strip(), stt_info_dict['SENTENCE'].strip())
            try:
                result_list = nlp_client.analyze(line)
            except Exception:
                log.error("Can't analyze line")
                log.error("Line --> ", line)
                log.error(traceback.format_exc())
                continue
            nlp_output_list.append(result_list)
            line_cnt += 1
        log.info('Done NLP(line count = {0}), the time required = {1}'.format(line_cnt, elapsed_time(dt)))
        dt = datetime.fromtimestamp(time.time())
        # Make NLP output
        nlp_output_file = object
        nlp_output_file_path = ''
        if conf.output_file:
            nlp_output_dir = os.path.join(os.path.join(conf.nlp_output_dir_path, START_DATE), job['INSRPS_CMP_ID'])
            if not os.path.exists(nlp_output_dir):
                os.makedirs(nlp_output_dir)
            nlp_output_file_path = os.path.join(nlp_output_dir, "{0}.nlp".format(ucid))
            nlp_output_file = open(nlp_output_file_path, 'w')
        modified_nlp_output_list = list()
        for result_list in nlp_output_list:
            line_target_text = ''
            line_nlp_sent = ''
            line_morph_sent = ''
            for target_text, nlp_sent, morph_sent in result_list:
                line_target_text += '{0} '.format(target_text)
                line_nlp_sent += '{0} '.format(nlp_sent)
                line_morph_sent += '{0} '.format(morph_sent)
            modified_nlp_output_list.append((line_target_text, line_nlp_sent, line_morph_sent))
            if conf.output_file:
                print >> nlp_output_file, '{0}\t{1}\t{2}'.format(line_target_text, line_nlp_sent, line_morph_sent)
        if conf.output_file:
            nlp_output_file.close()
            openssl.encrypt_file([nlp_output_file_path])
        log.info('Done modify NLP output, the time required = {0}'.format(elapsed_time(dt)))
        call_info_dict[ucid]['NLP_RESULT_LIST'] = modified_nlp_output_list
    return call_info_dict


def select_stt_info(log, call_info_dict, oracle, cfg):
    """
    Select STT Information
    :param      log:                Logger
    :param      call_info_dict      Call Information Dictionary
    :param      oracle              Oracle
    :param      cfg                 Config
    :return                         Call Information Dictionary with Stt Information
    """
    log.info("2. Select STT Information")
    for ucid, info_dict in call_info_dict.items():
        stt_info_list = util.select_stt_info(log, info_dict['UCID_GKEY'], oracle, cfg)
        call_info_dict[ucid]['STT_INFO_DICT_LIST'] = stt_info_list
    return call_info_dict


def select_call_info(log, job, oracle, cfg):
    """
    Select call Information
    :param	    log:	        Logger
    :param	    job:	        Target job
                                   INSRPS_CMP_ID(캠페인ID)
    :param	    oracle:	        Oracle
    :param      cfg:		    Config
    :return:		            call information dictionary
    """
    log.info("1. Select Call Information")
    call_info_dict = collections.OrderedDict()
    while True:
        ready_call_list = util.select_call_info(
            log, job['INSRPS_CMP_ID'], job['INSR_SBSCRP_DT'], oracle, cfg, status='01')
        process_call_list = util.select_call_info(
            log, job['INSRPS_CMP_ID'], job['INSR_SBSCRP_DT'], oracle, cfg, status='02')
        if not ready_call_list and not process_call_list:
            call_info_list = util.select_call_info(log, job['INSRPS_CMP_ID'], job['INSR_SBSCRP_DT'], oracle, cfg)
            break
        time.sleep(2)
    if not call_info_list:
        log.error("Not Exist Call Info [INSRPS_CMP_ID : {0}]".format(job['INSRPS_CMP_ID']))
        util.update_status(log, '90', job, oracle=oracle, cfg=cfg)
        util.delete_tb_ta_result_evluation(log, job['INSRPS_CMP_ID'], oracle, cfg)
        sys.exit(1)
    for info_dict in call_info_list:
        util.update_mapping_yn(log, oracle, cfg, info_dict)
        call_info_dict[info_dict['UCID']] = info_dict
    return call_info_dict


########
# main #
########
def main(job):
    """
    This program that execute TA
    :param      job:        Job
    """
    global START_DATE
    # Ignore Kill signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, signal.SIG_IGN)
    st = datetime.fromtimestamp(time.time())
    job['st'] = st
    START_DATE = datetime.strftime(st, '%Y%m%d')
    # Set logger
    log = logger.set_logger(
        logger_name=config.TAConfig.logger_name,
        log_dir_path=os.path.join(config.TAConfig.log_dir_path, START_DATE),
        log_file_name='{0}.log'.format(job['INSRPS_CMP_ID']),
        log_level=config.TAConfig.log_level
    )
    # Execute TA
    log.info("[START] Execute TA ..")
    log_str = 'INSRPS_CMP_ID = {0},'.format(job['INSRPS_CMP_ID'])
    log.info(log_str)
    oracle = False
    cfg = False
    try:
        try:
            oracle, cfg = util.db_connect(user='mlsta_dev')
        except Exception:
            oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
        # 1. select REC file
        rec_info_dict = select_call_info(log, job, oracle, cfg)
        # 2. Select STT result
        rec_info_dict = select_stt_info(log, rec_info_dict, oracle, cfg)
        # Execute NLP analyze
        rec_info_dict = execute_nlp_analyze(log, config.TAConfig, rec_info_dict, job, oracle, cfg)
        # Execute HMD analyze
        rec_info_dict, script_meta_dict = execute_hmd_analyze(log, config.TAConfig, rec_info_dict, job, oracle, cfg)
        # Detect Section HMD output
        rec_info_dict = detect_sect_hmd_output(log, config.TAConfig, rec_info_dict, job, script_meta_dict)
        # Insert 평가항목 결과
        offer_dict = insert_tb_result_evlu_criteria(
            log, config.TAConfig, rec_info_dict, job, script_meta_dict, oracle, cfg)
        # Insert QA 심사결과
        insert_tb_ta_result_evluation(log, job, script_meta_dict, offer_dict, oracle, cfg)
        # Update 청약정보
        update_tb_based_offer_info(log, job, script_meta_dict, offer_dict, oracle, cfg)
    except Exception:
        log.error(traceback.format_exc())
        util.update_status(log, '03', job, oracle=oracle, cfg=cfg)
        util.delete_tb_ta_result_evluation(log, job['INSRPS_CMP_ID'], oracle, cfg)
    finally:
        try:
            oracle.disconnect()
        except Exception:
            log.error('ERROR oracle disconnect')
        log.info("[E N D] Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
