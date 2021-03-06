#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-21, modification: 2018-03-06"

###########
# imports #
###########
import os
import re
import sys
import time
import glob
import json
import shutil
import traceback
import workerpool
import subprocess
import collections
from argparse import Namespace
from datetime import datetime
from datetime import timedelta
from operator import itemgetter
from cfg.config import STT_CONFIG
from cfg.config import DB_CONFIG
from cfg.config import MASKING_CONFIG
from lib.iLogger import set_logger
from lib.openssl import encrypt, encrypt_file

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
WAV_CNT = 0
RESULT_CNT = 0
RCDG_INFO_DICT = dict()
TOTAL_WAV_TIME = 0
DELETE_FILE_LIST = list()
STT_TEMP_DIR_NAME = ""
STT_TEMP_DIR_PATH = ""
TB_TM_STT_SLCD_DTC_RST_DICT = dict()


#########
# class #
#########


#######
# def #
#######
def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param      logger:                 Logger
    :param      delete_file_path:       Input path
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


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger      Logger
    :param      cmd:        Command
    :return                 Response out
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


def elapsed_time(sdate):
    """
    elapsed time
    :param      sdate:      date object
    :return:                Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def error_process(logger, rfile_name, biz_cd, project_cd):
    """
    Error process
    :param      logger:                 Logger
    :param      rfile_name:             RFILE_NAME(녹취파일명)
    :param      biz_cd:                 BIZ_CD(업체구분코드)
    :param      project_cd:             PROJECT_CD(프로젝트 코드)
    """
    logger.error("Error process")
    logger.error("RFILE_NAME = {0}, BIZ_CD = {1}, PROJECT_CD = {2}".format(
        rfile_name, biz_cd, project_cd))
    rec_path = '{0}/{1}/{2}'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd)
    target_rec_path_list = glob.glob('{0}/{1}.*'.format(rec_path, rfile_name))
    target_rec_path_list += glob.glob('{0}/{1}_*'.format(rec_path, rfile_name))
    rec_proc_path = '{0}/{1}/{2}/processed/{3}/{4}/{5}'.format(
        STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8])
    target_rec_path_list += glob.glob('{0}/{1}.*'.format(rec_proc_path, rfile_name))
    target_rec_path_list += glob.glob('{0}/{1}_*'.format(rec_proc_path, rfile_name))
    error_dir_path = '{0}/error_data/{1}/{2}/{3}'.format(rec_path, DT[:4], DT[4:6], DT[6:8])
    if not os.path.exists(error_dir_path):
        os.makedirs(error_dir_path)
    for target_path in target_rec_path_list:
        logger.error('encrypt {0}'.format(target_path))
        encrypt_file([target_path])
        if not target_path.endswith('.enc'):
            target_path += '.enc'
        target_name = os.path.basename(target_path)
        move_path = '{0}/{1}'.format(error_dir_path, target_name)
        if os.path.exists(move_path):
            del_garbage(logger, move_path)
        logger.error('move file {0} -> {1}'.format(target_path, move_path))
        shutil.move(target_path, error_dir_path)


def statistical_data(logger):
    """
    Statistical data print
    :param      logger:             Logger
    """
    global RESULT_CNT
    required_time = elapsed_time(DT)
    end_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M.%S')
    total_wav_duration = timedelta(seconds=TOTAL_WAV_TIME)
    total_wav_average_duration = timedelta(seconds=TOTAL_WAV_TIME / float(WAV_CNT))
    xrt = (int(timedelta(seconds=TOTAL_WAV_TIME).total_seconds() / required_time.total_seconds()))
    logger.info("18. Statistical data print")
    logger.info("\tStart time                   = {0}".format(ST))
    logger.info("\tEnd time                     = {0}".format(end_time))
    logger.info("\tThe time required            = {0}".format(required_time))
    logger.info("\tWAV count                    = {0}".format(WAV_CNT))
    logger.info("\tResult count                 = {0}".format(RESULT_CNT))
    logger.info("\tTotal WAV duration           = {0}".format(total_wav_duration))
    logger.info("\tTotal WAV average duration   = {0}".format(total_wav_average_duration))
    logger.info("\txRT                          = {0} xRT".format(xrt))
    logger.info("Done CS")
    logger.info("Remove logger handler")
    logger.info("CS END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info("17. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception:
            continue


def move_output(logger):
    """
    Move output to CS output path
    :param      logger:             Logger
    """
    global RCDG_INFO_DICT
    logger.info("15. Move output to CS output path")
    for info_dict in RCDG_INFO_DICT.values():
        rfile_name = info_dict['RFILE_NAME']
        chn_tp = 'M'
        biz_cd = info_dict['BIZ_CD']
        output_dir_path = '{0}/{1}/{2}/{3}/{4}/{5}'.format(
            STT_CONFIG['stt_output_path'], rfile_name[:4],
            rfile_name[4:6], rfile_name[6:8], biz_cd, rfile_name)
        output_dict = {
            'mlf': {'ext': 'mlf', 'merge': 'N'},
            'unseg': {'ext': 'stt', 'merge': 'N'},
            'do_space': {'ext': 'stt', 'merge': 'N'},
            'txt': {'ext': 'txt', 'merge': 'Y'},
            'detail': {'ext': 'detail', 'merge': 'Y'},
            'result': {'ext': 'result', 'merge': 'N'},
            'modified_nlp_line_number': {'ext': 'hmd.txt', 'merge': 'Y'},
            'JSON': {'ext': 'json', 'merge': 'Y'},
            'JSON2': {'ext': 'json2', 'merge': 'Y'},
            'HMD': {'ext': 'hmd.txt', 'merge': 'Y'},
            'MCNT': {'ext': 'morph.cnt', 'merge': 'Y'},
            'NCNT': {'ext': 'ne.cnt', 'merge': 'Y'},
            'IDX': {'ext': 'idx', 'merge': 'Y'},
            'IDXVP': {'ext': 'idxvp', 'merge': 'Y'},
            'W2V': {'ext': 'w2v.txt', 'merge': 'Y'},
            'masking': {'ext': 'detail', 'merge': 'Y'},
            'META_JSON': {'ext': 'json', 'merge': 'Y'}
        }
        # Move the file
        for target, target_option in output_dict.items():
            path_list = list()
            ext = target_option['ext']
            output_target_path = '{0}/{1}'.format(output_dir_path, target)
            if not os.path.exists(output_target_path):
                os.makedirs(output_target_path)
            if chn_tp == 'S':
                if target_option['merge'] == 'Y':
                    path_list.append('{0}/{1}/{2}_trx.{3}'.format(STT_TEMP_DIR_PATH, target, rfile_name, ext))
                else:
                    path_list.append('{0}/{1}/{2}_rx.{3}'.format(STT_TEMP_DIR_PATH, target, rfile_name, ext))
                    path_list.append('{0}/{1}/{2}_tx.{3}'.format(STT_TEMP_DIR_PATH, target, rfile_name, ext))
            else:
                path_list.append('{0}/{1}/{2}.{3}'.format(STT_TEMP_DIR_PATH, target, rfile_name, ext))
            for path in path_list:
                file_name = os.path.basename(path)
                if os.path.exists('{0}/{1}'.format(output_target_path, file_name)):
                    del_garbage(logger, '{0}/{1}'.format(output_target_path, file_name))
                if not os.path.exists(path):
                    logger.error('File not created -> {0}'.format(path))
                    continue
                logger.debug('move file {0} -> {1}'.format(path, output_target_path))
                shutil.move(path, output_target_path)
                #logger.info('delete file {0}'.format(path))
                #del_garbage(logger, path)
            encrypt(output_target_path)
            logger.info('encrypt {0}'.format(output_target_path))


def time_to_seconds(input_time):
    """
    Time ex) HH:MM:SS.SSSSSS to seconds
    :param          input_time:         Input time
    :return:                            Float seconds
    """
    time_list = str(input_time).split(":")
    hours_to_second = float(time_list[0]) * 3600
    minutes_to_second = float(time_list[1]) * 60
    seconds = float(time_list[2])
    total_seconds = hours_to_second + minutes_to_second + seconds
    return total_seconds


def extract_silence(start_time_idx, end_time_idx, total_duration, delimiter, input_line_list, silence_seconds):
    """
    Extract silence section
    :param          start_time_idx:         Index start time of line split by delimiter
    :param          end_time_idx:           Index end time of line split by delimiter
    :param          total_duration:         Total record duration
    :param          delimiter:              Line delimiter
    :param          input_line_list:        Input line list
    :param          silence_seconds:        Target silence seconds
    :return:                                Output dictionary
    """
    output_dict = collections.OrderedDict()
    for idx in range(0, len(input_line_list)):
        front_line = input_line_list[idx].strip()
        front_line_list = front_line.split(delimiter)
        end_time = front_line_list[int(end_time_idx)]
        end_time_seconds = time_to_seconds(end_time)
        if idx + 1 == len(input_line_list):
            hours_to_second = float(total_duration[:2]) * 3600
            minutes_to_second = float(total_duration[2:4]) * 60
            seconds = float(total_duration[4:6])
            start_time_seconds = hours_to_second + minutes_to_second + seconds
        else:
            back_line = input_line_list[idx + 1].strip()
            back_line_list = back_line.split(delimiter)
            start_time = back_line_list[int(start_time_idx)]
            start_time_seconds = time_to_seconds(start_time)
        duration = start_time_seconds - end_time_seconds
        if duration > float(silence_seconds):
            key = "{0}_{1}".format(idx, idx) if idx + 1 == len(input_line_list) else "{0}_{1}".format(idx, idx + 1)
            output_dict[key] = round(duration, 2)
    return output_dict


def modify_nlp_output_line_number(logger):
    """
    Modify NLF output file line number
    :param      logger:     Logger
    """
    logger.info("13. Modify NLP output file line number")
    hmd_result_dir_path = "{0}/HMD".format(STT_TEMP_DIR_PATH)
    modified_nlp_line_number_dir_path = "{0}/modified_nlp_line_number".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(modified_nlp_line_number_dir_path):
        os.makedirs(modified_nlp_line_number_dir_path)
    w_ob = os.walk(hmd_result_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            hmd_result_file = open(os.path.join(dir_path, file_name), 'r')
            hmd_result_file_list = hmd_result_file.readlines()
            modified_line_number_file = open(os.path.join(modified_nlp_line_number_dir_path, file_name), 'w')
            merge_temp_num = 0
            merge_temp_sent = ""
            merge_temp_nlp_sent = ""
            merge_temp_list = list()
            # Merge sentence
            for idx in range(0, len(hmd_result_file_list)):
                line = hmd_result_file_list[idx].strip()
                line_list = line.split("\t")
                merge_sent = line_list[3].strip()
                try:
                    merge_nlp_sent = line_list[4].strip()
                except Exception:
                    logger.error('line -> {0}'.format(line))
                    logger.error('line_list -> {0}'.format(line_list))
                    raise Exception(traceback.format_exc())
                if idx < 1:
                    merge_temp_sent = merge_sent
                    merge_temp_nlp_sent = merge_nlp_sent
                elif not merge_sent.startswith("["):
                    merge_temp_sent += " " + merge_sent
                    merge_temp_nlp_sent += " " + merge_nlp_sent
                else:
                    line_list[1] = str(merge_temp_num).strip()
                    line_list[3] = merge_temp_sent.strip()
                    line_list[4] = merge_temp_nlp_sent.strip()
                    merge_temp_list.append(line_list[:])
                    merge_temp_sent = merge_sent
                    merge_temp_nlp_sent = merge_nlp_sent
                    merge_temp_num += 1
                if idx == len(hmd_result_file_list) - 1:
                    line_list[1] = str(merge_temp_num).strip()
                    line_list[3] = merge_temp_sent.strip()
                    line_list[4] = merge_temp_nlp_sent.strip()
                    merge_temp_list.append(line_list[:])
            # Separate sentence
            line_number = 0
            for merged_line_list in merge_temp_list:
                sent = merged_line_list[3].strip()
                temp_sent = sent.replace("[", "\r\n[")
                temp_sent_list = temp_sent.split("\r\n")
                modified_sent_list = temp_sent_list if len(temp_sent_list[0]) > 1 else temp_sent_list[1:]
                nlp_sent = merged_line_list[4].strip()
                nlp_temp_sent = nlp_sent.replace("[", "\r\n[")
                nlp_temp_sent_list = nlp_temp_sent.split("\r\n")
                nlp_sent_list = nlp_temp_sent_list if len(nlp_temp_sent_list[0]) > 1 else nlp_temp_sent_list[1:]
                for idx in range(0, len(modified_sent_list)):
                    merged_line_list[1] = str(line_number)
                    merged_line_list[3] = modified_sent_list[idx].strip()
                    merged_line_list[4] = nlp_sent_list[idx].strip()
                    print >> modified_line_number_file, "\t".join(merged_line_list)
                    line_number += 1
            hmd_result_file.close()
            modified_line_number_file.close()


def make_ne_cnt_file():
    """
    Make ne.cnt file
    """
    ne_file_list = glob.glob("{0}/NCNT/*.ne.cnt".format(STT_TEMP_DIR_PATH))
    ne_output_dict = dict()
    for ne_file_path in ne_file_list:
        ne_file = open(ne_file_path, 'r')
        for ne_line in ne_file:
            ne_line = ne_line.strip()
            ne_line_list = ne_line.split("\t")
            if len(ne_line_list) != 4:
                continue
            word = ne_line_list[1]
            ne = ne_line_list[2]
            word_freq_cnt = int(ne_line_list[3])
            key = "{0}/{1}".format(word, ne)
            if ne_line_list[0] != '$$$':
                continue
            if key not in ne_output_dict:
                ne_output_dict[key] = [word_freq_cnt, 1, word, ne]
            else:
                ne_output_dict[key][0] += word_freq_cnt
                ne_output_dict[key][1] += 1
        ne_file.close()
    sorted_ne_output = sorted(ne_output_dict.iteritems(), key=itemgetter(1), reverse=True)
    ne_output_file = open("{0}/{1}.ne.cnt".format(STT_TEMP_DIR_PATH, STT_TEMP_DIR_NAME), 'w')
    print >> ne_output_file, "개체명\t개체유형\t단어 빈도\t문서 빈도"
    for item in sorted_ne_output:
        print >> ne_output_file, "{0}\t{1}\t{2}\t{3}".format(item[1][2], item[1][3], item[1][0], item[1][1])
    ne_output_file.close()


def make_morph_cnt_file(logger):
    """
    Make morph.cnt file
    :param      logger:     Logger
    """
    morph_file_list = glob.glob("{0}/MCNT/*.morph.cnt".format(STT_TEMP_DIR_PATH))
    # Load freq_except.txt file
    freq_except_dic = dict()
    freq_except_file_path = "{0}/LA/rsc/freq_except.txt".format(STT_CONFIG['ta_path'])
    if os.path.exists(freq_except_file_path):
        freq_except_file = open(freq_except_file_path, 'r')
        for line in freq_except_file:
            line = line.strip()
            if line in freq_except_dic:
                continue
            freq_except_dic[line] = 1
    else:
        logger.error("Can't load freq_except.txt file -> [{0}]".format(freq_except_file_path))
    morph_output_dict = dict()
    for morph_file_path in morph_file_list:
        morph_file = open(morph_file_path, 'r')
        for morph_line in morph_file:
            morph_line = morph_line.strip()
            morph_line_list = morph_line.split("\t")
            if len(morph_line_list) != 4:
                continue
            word = morph_line_list[1]
            morph = morph_line_list[2]
            word_freq_cnt = int(morph_line_list[3])
            key = "{0}/{1}".format(word, morph)
            if morph_line_list[0] != '$$$' or word in freq_except_dic:
                continue
            if key not in morph_output_dict:
                morph_output_dict[key] = [word_freq_cnt, 1, word, morph]
            else:
                morph_output_dict[key][0] += word_freq_cnt
                morph_output_dict[key][1] += 1
        morph_file.close()
    sorted_morph_output = sorted(morph_output_dict.iteritems(), key=itemgetter(1), reverse=True)
    morph_output_file = open("{0}/{1}.morph.cnt".format(STT_TEMP_DIR_PATH, STT_TEMP_DIR_NAME), 'w')
    print >> morph_output_file, "형태소\t품사\t단어 빈도\t문서 빈도"
    for item in sorted_morph_output:
        print >> morph_output_file, "{0}\t{1}\t{2}\t{3}".format(item[1][2], item[1][3], item[1][0], item[1][1])
    morph_output_file.close()


def make_statistics_file(logger):
    """
    Make statistics file
    :param      logger:     Logger
    """
    logger.info("12. Make statistics file")
    logger.info("12-1. Make morph.cnt file")
    make_morph_cnt_file(logger)
    logger.info("12-2. Make ne.cnt file")
    make_ne_cnt_file()


def pool_sub_process(cmd):
    """
    Execute subprocess
    :param      cmd:        Command
    """
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    sub_pro.communicate()


def execute_new_lang(logger):
    """
    Execute new_lang.exe [ make nlp result file ]
    :param      logger:     Logger
    """
    global DELETE_FILE_LIST
    logger.info("11. Execute new_lang.exe")
    start = 0
    end = 0
    cmd_list = list()
    os.chdir(STT_CONFIG['ta_bin_path'])
    target_list = glob.glob("{0}/txt/*".format(STT_TEMP_DIR_PATH))
    thread = len(target_list) if len(target_list) < int(STT_CONFIG['nl_thread']) else int(STT_CONFIG['nl_thread'])
    output_dir_list = ['JSON', 'JSON2', 'HMD', 'MCNT', 'NCNT', 'IDX', 'IDXVP', 'W2V']
    for dir_name in output_dir_list:
        output_dir_path = "{0}/{1}".format(STT_TEMP_DIR_PATH, dir_name)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    temp_new_lang_dir_path = '{0}/{1}'.format(STT_CONFIG['ta_bin_path'], STT_TEMP_DIR_NAME)
    DELETE_FILE_LIST.append(temp_new_lang_dir_path)
    if not os.path.exists(temp_new_lang_dir_path):
        os.makedirs(temp_new_lang_dir_path)
    # Make list file
    for cnt in range(thread):
        end += len(target_list) / thread
        if (len(target_list) % thread) > cnt:
            end += 1
        list_file_path = "{0}/{1}_{2}.list".format(temp_new_lang_dir_path, STT_TEMP_DIR_NAME, cnt)
        list_file = open(list_file_path, 'w')
        for idx in range(start, end):
            print >> list_file, target_list[idx]
        list_file.close()
        start = end
        cmd = "./new_lang.exe -DJ {0} txt {1}".format(list_file_path, DT[:8])
        cmd_list.append(cmd)
    pool = workerpool.WorkerPool(thread)
    pool.map(pool_sub_process, cmd_list)
    pool.shutdown()
    pool.wait()


def masking(str_idx, delimiter, encoding, input_line_list):
    """
    Masking
    :param          str_idx:                Index sentence of line split by delimiter
    :param          delimiter:              Line delimiter
    :param          encoding:               Encoding
    :param          input_line_list:        Input line list
    :return:                                Output dictionary and Index output dictionary
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
        line_list = line.split(delimiter)
        if str_idx >= len(line_list):
            sent = ''
        else:
            sent = line_list[str_idx].strip()
        try:
            line_dict[line_cnt] = sent.decode(encoding)
        except Exception:
            if sent[-1] == '\xb1':
                line_dict[line_cnt] = sent[:-1].decode(encoding)
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
        if (u'주민' in line and u'번호' in line) or (u'면허' in line and u'번호' in line) or (u'외국인' in line and u'등록' in line and u'번호' in line):
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'id_number_rule' not in re_rule_dict:
                    re_rule_dict['id_number_rule'] = number_rule
        if u'계좌' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'account_number_rule' not in re_rule_dict:
                    re_rule_dict['account_number_rule'] = number_rule
        if u'신한' in line or u'농협' in line or u'우리' in line or u'하나' in line or u'기업' in line or u'국민' in line or u'외환' in line or u'씨티' in line or u'수협' in line or u'대구' in line or u'부산' in line or u'광주' in line or u'제주' in line or u'전북' in line or u'경남' in line or u'케이' in line or u'카카오' in line:
            if u'은행' in line or u'뱅크' in line:
                if 'account_number_rule' not in re_rule_dict:
                    re_rule_dict['account_number_rule'] = number_rule
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
    index_output_dict = collections.OrderedDict()
    for re_line_num, re_rule_dict in line_re_rule_dict.items():
        output_str = ""
        if len(line_dict[re_line_num]) < int(MASKING_CONFIG['minimum_length']):
            output_dict[re_line_num] = line_dict[re_line_num].encode(encoding)
            index_output_dict[re_line_num] = list()
            continue
        for rule_name, re_rule in re_rule_dict.items():
            if rule_name == 'name_rule':
                masking_code = "10"
                masking_cnt = 2
            elif rule_name == 'birth_rule':
                masking_code = "20"
                masking_cnt = 2
            elif rule_name == 'id_number_rule':
                masking_code = "30"
                masking_cnt = 3
            elif rule_name == 'card_number_rule':
                masking_code = "40"
                masking_cnt = 3
            elif rule_name == 'account_number_rule':
                masking_code = "50"
                masking_cnt = 3
            elif rule_name == 'tel_number_rule':
                masking_code = "60"
                masking_cnt = 3
            elif rule_name == 'address_rule':
                masking_code = "70"
                masking_cnt = 3
            elif rule_name == 'email_rule':
                masking_code = "100"
                masking_cnt = 3
            else:
                masking_code = "110"
                masking_cnt = 3
            p = re.compile(re_rule.decode('euc-kr'))
            re_result = p.finditer(line_dict[re_line_num].decode('utf-8'))
            if len(output_str) < 1:
                output_str = line_dict[re_line_num].decode('utf-8')
            index_info = list()
            for item in re_result:
                idx_tuple = item.span()
                start = idx_tuple[0]
                end = idx_tuple[1]
                masking_part = ""
                index_info.append({"start_idx": start, "end_idx": end, "masking_code": masking_code})
                cnt = 0
                for idx in output_str[start:end]:
                    if idx == " ":
                        masking_part += " "
                        continue
                    cnt += 1
                    if cnt % masking_cnt == 0:
                        masking_part += idx
                    else:
                        masking_part += "*"
                output_str = output_str.replace(output_str[start:end], masking_part)
            if re_line_num not in index_output_dict:
                index_output_dict[re_line_num] = index_info
            else:
                for data in index_info:
                    index_output_dict[re_line_num].append(data)
        output_dict[re_line_num] = output_str.encode(encoding)
    return output_dict, index_output_dict


def execute_masking(logger):
    """
    Execute masking
    :param      logger:                 Logger
    """
    logger.info("10. Execute masking")
    target_file_list = glob.glob('{0}/detail/*'.format(STT_TEMP_DIR_PATH))
    masking_dir_path = '{0}/masking'.format(STT_TEMP_DIR_PATH)
    if not os.path.exists(masking_dir_path):
        os.makedirs(masking_dir_path)
    for target_file_path in target_file_list:
        try:
            target_file = open(target_file_path, 'r')
            line_list = target_file.readlines()
            sent_list = masking(3, '\t', 'euc-kr', line_list)
            masking_file = open(os.path.join(masking_dir_path, os.path.basename(target_file_path)), 'w')
            line_num = 0
            for line in line_list:
                line_split = line.split('\t')
                new_line = line_split[:3]
                if line_num in sent_list[0]:
                    new_line.append(sent_list[0][line_num].strip())
                else:
                    new_line.append(line_split[3].strip())
                print >> masking_file, '\t'.join(new_line)
                line_num += 1
            masking_file.close()
        except Exception:
            logger.error('masking Failed -> {0}'.format(target_file_path))
            raise Exception(traceback.format_exc())


def set_output(logger):
    """
    Set output directory
    :param      logger:     Logger
    """
    global RESULT_CNT
    logger.info("9. Set output directory")
    result_dir_path = "{0}/result".format(STT_TEMP_DIR_PATH)
    json_dir_path = "{0}/META_JSON".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(result_dir_path):
        os.makedirs(result_dir_path)
    if not os.path.exists(json_dir_path):
        os.makedirs(json_dir_path)
    file_path_list = glob.glob("{0}/*".format(STT_TEMP_DIR_PATH))
    for file_path in file_path_list:
        if file_path.endswith(".result"):
            shutil.move(file_path, result_dir_path)
        if file_path.endswith(".json"):
            shutil.move(file_path, json_dir_path)
    RESULT_CNT = len(glob.glob("{0}/*.result".format(result_dir_path)))


def print_detail_file(txt_output_file, detail_output_file, speaker, st_time, ed_time, sent):
    """
    Print detail file
    :param          txt_output_file:                    Txt output file
    :param          detail_output_file:                 Detail output file
    :param          speaker:                            Speaker
    :param          st_time:                            Start time
    :param          ed_time:                            End time
    :param          sent:                               Sentence
    """
    print >> txt_output_file, "{0}{1}".format(speaker, sent.encode('euc-kr'))
    print >> detail_output_file, "{0}\t{1}\t{2}\t{3}".format(
        speaker, str(timedelta(seconds=float(st_time) / 100)),
        str(timedelta(seconds=float(ed_time) / 100)), sent.encode('euc-kr'))


def split_sent_use_time(**kwargs):
    """
    Split sentence use time information
    :param          kwargs:         arguments
    """
    speaker = kwargs.get('speaker')
    pre_speaker = kwargs.get('pre_speaker')
    st_time = kwargs.get('st_time')
    pre_st_time = kwargs.get('pre_st_time')
    ed_time = kwargs.get('ed_time')
    pre_ed_time = kwargs.get('pre_ed_time')
    sent = kwargs.get('sent')
    pre_sent = kwargs.get('pre_sent')
    mlf_info_dict = kwargs.get('mlf_info_dict')
    txt_output_file = kwargs.get('txt_output_file')
    detail_output_file = kwargs.get('detail_output_file')
    for cnt, mlf_info in mlf_info_dict.items():
        mlf_st_time = mlf_info[0]
        mlf_word = mlf_info[2].decode('euc-kr')
        if int(pre_st_time) < int(mlf_st_time):
            front_mlf_word = mlf_info_dict[cnt - 1][2].decode('euc-kr') if cnt - 1 in mlf_info_dict else ""
            back_mlf_word = mlf_info_dict[cnt + 1][2].decode('euc-kr') if cnt + 1 in mlf_info_dict else ""
            search_word_fir = front_mlf_word + mlf_word
            search_word_sec = front_mlf_word + " " + mlf_word
            search_word_thi = mlf_word + back_mlf_word
            search_word_fou = mlf_word + " " + back_mlf_word
            if pre_sent.find(search_word_fir) > -1:
                temp_idx = pre_sent.find(search_word_fir)
                pre_sent_idx = temp_idx + search_word_fir.find(mlf_word)
            elif pre_sent.find(search_word_sec) > -1:
                temp_idx = pre_sent.find(search_word_sec)
                pre_sent_idx = temp_idx + search_word_sec.find(mlf_word)
            elif pre_sent.find(search_word_thi) > -1:
                temp_idx = pre_sent.find(search_word_thi)
                pre_sent_idx = temp_idx + search_word_thi.find(mlf_word)
            elif pre_sent.find(search_word_fou) > -1:
                temp_idx = pre_sent.find(search_word_fou)
                pre_sent_idx = temp_idx + search_word_fou.find(mlf_word)
            else:
                pre_sent_idx = pre_sent.find(mlf_word)
                if pre_sent_idx == -1:
                    pre_sent_idx = len(pre_sent)
            if pre_sent_idx == 0:
                pre_sent_idx = len(mlf_word)
            print_detail_file(txt_output_file, detail_output_file, pre_speaker, pre_st_time, mlf_st_time,
                              pre_sent[:pre_sent_idx].strip())
            print_detail_file(txt_output_file, detail_output_file, speaker, st_time, ed_time, sent)
            if len(pre_sent[pre_sent_idx:].strip()) > 0:
                print_detail_file(txt_output_file, detail_output_file, pre_speaker, mlf_st_time, pre_ed_time,
                                  pre_sent[pre_sent_idx:].strip())
            break


def make_mlf_info(mlf_file_path):
    """
    Make mlf info
    :param          mlf_file_path:          Mlf file path
    :return:                                Mlf output dictionary
    """
    output_dict = collections.OrderedDict()
    mlf_file = open(mlf_file_path, 'r')
    mlf_file_lines = mlf_file.readlines()[2:-1]
    mlf_file.close()
    cnt = 0
    zero_cnt = -1
    for line in mlf_file_lines:
        line = line.strip()
        line_list = line.split()
        if int(line_list[0].strip()) == 0:
            zero_cnt += 1
        st_time = str(30050 * zero_cnt + int(line_list[0].strip()))
        ed_time = str(30050 * zero_cnt + int(line_list[1].strip()))
        sent = line_list[2].replace("#", "").strip()
        if '<s>' == sent or '</s>' == sent:
            continue
        output_dict[cnt] = (st_time, ed_time, sent)
        cnt += 1
    return output_dict


def make_stt_info(logger, speaker, file_name, output_dict):
    """
    Make stt information
    :param      logger:             Logger
    :param      speaker:            Speaker
    :param      file_name:          File name
    :param      output_dict:        Output dict
    :return:                        Output dict
    """
    for line in file_name:
        try:
            line_list = line.split(",")
            if len(line_list) != 3:
                continue
            st = line_list[0].strip()
            et = line_list[1].strip()
            sent = line_list[2].strip()
            modified_st = st.replace("ts=", "").strip()
            modified_et = et.replace("te=", "").strip()
            key = float(modified_st)
            while True:
                if key not in output_dict:
                    output_dict[key] = "{0}\t{1}\t{2}\t{3}".format(speaker, modified_st, modified_et, sent)
                    break
                else:
                    key += 0.1
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Error make stt information")
            logger.error(line)
            logger.error(exc_info)
            continue
    return output_dict


def make_output(logger, target_dir_path):
    """
    Make txt file and detail file
    :param      logger:                 Logger
    :param      target_dir_path:        Output directory path
    """
    global RCDG_INFO_DICT
    logger.info("8. Make output [txt file and detailed file]")
    txt_dir_path = "{0}/txt".format(STT_TEMP_DIR_PATH)
    detail_dir_path = "{0}/detail".format(STT_TEMP_DIR_PATH)
    output_dir_list = [txt_dir_path, detail_dir_path]
    for output_dir_path in output_dir_list:
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    # Create txt & detail file
    logger.info('Create txt & detail file')
    for key, info_dict in RCDG_INFO_DICT.items():
        rfile_name = info_dict['RFILE_NAME']
        chn_tp = 'M'
        biz_cd = info_dict['BIZ_CD']
        project_cd = info_dict['PROJECT_CD']
        # Mono
        if chn_tp == 'M':
            file_path = '{0}/{1}.stt'.format(target_dir_path, rfile_name)
            if os.path.exists(file_path):
                stt_file = open(file_path, 'r')
                txt_output_file = open('{0}/{1}.txt'.format(txt_dir_path, rfile_name), 'w')
                detail_output_file = open('{0}/{1}.detail'.format(detail_dir_path, rfile_name), 'w')
                for line in stt_file:
                    line_list = line.split(",")
                    if len(line_list) != 3:
                        continue
                    st = line_list[0].strip()
                    et = line_list[1].strip()
                    start_time = str(timedelta(seconds=float(st.replace("ts=", "")) / 100))
                    end_time = str(timedelta(seconds=float(et.replace("te=", "")) / 100))
                    sent = line_list[2].strip()
                    speaker = '[M]'
                    print >> txt_output_file, "{0}{1}".format(speaker, sent)
                    print >> detail_output_file, "{0}\t{1}\t{2}\t{3}".format(speaker, start_time, end_time, sent)
                stt_file.close()
                txt_output_file.close()
                detail_output_file.close()
            else:
                logger.error("{0} don't have stt file.".format(rfile_name))
                error_process(logger, rfile_name, biz_cd, project_cd)
                del RCDG_INFO_DICT[key]
                continue
        # Stereo
        else:
            rx_file_path = '{0}/{1}_rx.stt'.format(target_dir_path, rfile_name)
            tx_file_path = '{0}/{1}_tx.stt'.format(target_dir_path, rfile_name)
            if os.path.exists(rx_file_path) and os.path.exists(tx_file_path):
                rx_file = open(rx_file_path, 'r')
                tx_file = open(tx_file_path, 'r')
                output_dict = dict()
                output_dict = make_stt_info(logger, '[A]', tx_file, output_dict)
                output_dict = make_stt_info(logger, '[C]', rx_file, output_dict)
                tx_file.close()
                rx_file.close()
            else:
                logger.error("{0} don't have stt file.".format(rfile_name))
                error_process(logger, rfile_name, biz_cd, project_cd)
                del RCDG_INFO_DICT[key]
                continue
            # Detailed txt & detail file creation.
            sorted_stt_info_output_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
            # Make mlf info
            rx_mlf_file_path = "{0}/mlf/{1}_rx.mlf".format(STT_TEMP_DIR_PATH, rfile_name)
            tx_mlf_file_path = "{0}/mlf/{1}_tx.mlf".format(STT_TEMP_DIR_PATH, rfile_name)
            if not os.path.exists(rx_mlf_file_path) or not os.path.exists(tx_mlf_file_path):
                logger.error("{0} don't have mlf file.".format(rfile_name))
                error_process(logger, rfile_name, biz_cd, project_cd)
                del RCDG_INFO_DICT[key]
                continue
            rx_mlf_info_dict = make_mlf_info(rx_mlf_file_path)
            tx_mlf_info_dict = make_mlf_info(tx_mlf_file_path)
            # Merge .stt file and make detail file
            pre_speaker = ""
            pre_st_time = ""
            pre_ed_time = ""
            pre_sent = ""
            txt_output_file = open('{0}/{1}_trx.txt'.format(txt_dir_path, rfile_name), 'w')
            detail_output_file = open('{0}/{1}_trx.detail'.format(detail_dir_path, rfile_name), 'w')
            for idx in range(0, len(sorted_stt_info_output_list)):
                detail_line = sorted_stt_info_output_list[idx][1]
                detail_line_list = detail_line.split("\t")
                speaker = detail_line_list[0]
                st_time = detail_line_list[1]
                ed_time = detail_line_list[2]
                sent = detail_line_list[3].decode('euc-kr')
                # 맨 마지막 라인일 경우
                if idx + 1 == len(sorted_stt_info_output_list):
                    if len(pre_speaker) > 0:
                        print_detail_file(
                            txt_output_file, detail_output_file, pre_speaker, pre_st_time, pre_ed_time, pre_sent)
                    print_detail_file(txt_output_file, detail_output_file, speaker, st_time, ed_time, sent)
                    continue
                # 이전 라인 정보가 없을 경우
                if len(pre_speaker) < 1:
                    pre_speaker = speaker
                    pre_st_time = st_time
                    pre_ed_time = ed_time
                    pre_sent = sent
                    continue
                # 현재 라인의 종료 시간이 이전 라인의 종료 시간보다 빠르고 현재 라인이 공백 기준으로 10 음정 이상일 경우
                if int(ed_time) < int(pre_ed_time) and len(sent.replace(" ", "")) > 9:
                    if speaker == '[A]':
                        split_sent_use_time(
                            speaker=speaker,
                            pre_speaker=pre_speaker,
                            st_time=st_time,
                            pre_st_time=pre_st_time,
                            ed_time=ed_time,
                            pre_ed_time=pre_ed_time,
                            sent=sent,
                            pre_sent=pre_sent,
                            mlf_info_dict=rx_mlf_info_dict,
                            txt_output_file=txt_output_file,
                            detail_output_file=detail_output_file,
                        )
                        pre_speaker = ""
                    else:
                        split_sent_use_time(
                            speaker=speaker,
                            pre_speaker=pre_speaker,
                            st_time=st_time,
                            pre_st_time=pre_st_time,
                            ed_time=ed_time,
                            pre_ed_time=pre_ed_time,
                            sent=sent,
                            pre_sent=pre_sent,
                            mlf_info_dict=tx_mlf_info_dict,
                            txt_output_file=txt_output_file,
                            detail_output_file=detail_output_file,
                        )
                        pre_speaker = ""
                # 정상 적인 경우
                else:
                    print_detail_file(
                        txt_output_file, detail_output_file, pre_speaker, pre_st_time, pre_ed_time, pre_sent)
                    pre_speaker = speaker
                    pre_st_time = st_time
                    pre_ed_time = ed_time
                    pre_sent = sent
            txt_output_file.close()
            detail_output_file.close()


def execute_unseg_and_do_space(logger):
    """
    Execute unseg.exe and do_space.exe
    :param      logger:             Logger
    :return:                        Output directory path
    """
    global RCDG_INFO_DICT
    logger.info("7. Execute unseg.exe and do_space.exe")
    # Check the mlf file
    mlf_cnt = 0
    for info_dict in RCDG_INFO_DICT.values():
        chn_tp = 'M'
        if chn_tp == 'S':
            mlf_cnt += 2
            continue
        else:
            mlf_cnt += 1
            continue
    mlf_list = glob.glob("{0}/*.mlf".format(STT_TEMP_DIR_NAME))
    if len(mlf_list) != mlf_cnt:
        logger.error("mt_long Engine error occurred")
        logger.info("CS END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error("----------    CS ERROR   ----------")
        delete_garbage_file(logger)
        for key, info_dict in RCDG_INFO_DICT.items():
            rfile_name = info_dict['RFILE_NAME']
            biz_cd = info_dict['BIZ_CD']
            project_cd = info_dict['PROJECT_CD']
            error_process(logger, rfile_name, biz_cd, project_cd)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # MLF 복사
    mlf_dir_path = "{0}/mlf".format(STT_TEMP_DIR_PATH)
    unseg_dir_path = "{0}/unseg".format(STT_TEMP_DIR_PATH)
    do_space_dir_path = "{0}/do_space".format(STT_TEMP_DIR_PATH)
    output_dir_list = [mlf_dir_path, unseg_dir_path, do_space_dir_path]
    for output_dir_path in output_dir_list:
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    for mlf_path in mlf_list:
        try:
            shutil.move(mlf_path, mlf_dir_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't move mlf file {0} -> {1}".format(mlf_path, mlf_dir_path))
            logger.error(exc_info)
            continue
    os.chdir(STT_CONFIG['stt_tool_path'])
    # Execute unseg.exe
    unseg_cmd = './unseg.exe -d {mp} {up} 300'.format(mp=mlf_dir_path, up=unseg_dir_path)
    sub_process(logger, unseg_cmd)
    do_space_cmd = './do_space.exe {up} {dp}'.format(up=unseg_dir_path, dp=do_space_dir_path)
    sub_process(logger, do_space_cmd)
    return do_space_dir_path


def execute_dnn(logger, thread_cnt):
    """
    Execute DNN (mt_long_utt_dnn_support.gpu.exe)
    :param      logger:         Logger
    :param      thread_cnt:     Thread count
    """
    logger.info("6. Execute DNN (mt_long_utt_dnn_support.gpu.exe)")
    os.chdir(STT_CONFIG['stt_path'])
    dnn_thread = thread_cnt if thread_cnt < STT_CONFIG['thread'] else STT_CONFIG['thread']
    cmd = "./mt_long_utt_dnn_support.gpu.exe {tn} {th} 1 1 {gpu} 128 0.8".format(
        tn=STT_TEMP_DIR_NAME, th=dnn_thread, gpu=STT_CONFIG['gpu'])
    sub_process(logger, cmd)


def make_pcm_list_file(logger):
    """
    Make PCM list file
    :param      logger:     Logger
    :return:                Thread count
    """
    global WAV_CNT
    global TOTAL_WAV_TIME
    global DELETE_FILE_LIST
    logger.info("5. Do make list file")
    list_file_cnt = 0
    max_list_file_cnt = 0
    w_ob = os.walk(STT_TEMP_DIR_PATH)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if file_name.endswith(".pcm"):
                # List 파일에 PCM 파일명을 입력한다.
                list_file_path = "{sp}/{tn}_n{cnt}.list".format(
                    sp=STT_CONFIG['stt_path'], tn=STT_TEMP_DIR_NAME, cnt=list_file_cnt)
                curr_list_file_path = "{sp}/{tn}_n{cnt}_curr.list".format(
                    sp=STT_CONFIG['stt_path'], tn=STT_TEMP_DIR_NAME, cnt=list_file_cnt)
                DELETE_FILE_LIST.append(list_file_path)
                DELETE_FILE_LIST.append(curr_list_file_path)
                output_file_div = open(list_file_path, 'a')
                print >> output_file_div, "{tn}/{sn}".format(tn=STT_TEMP_DIR_PATH, sn=file_name)
                output_file_div.close()
                WAV_CNT += 1
                TOTAL_WAV_TIME += os.stat("{tp}/{sn}".format(tp=STT_TEMP_DIR_PATH, sn=file_name))[6] / 16000.0
                if list_file_cnt > max_list_file_cnt:
                    max_list_file_cnt = list_file_cnt
                if list_file_cnt + 1 == STT_CONFIG['thread']:
                    list_file_cnt = 0
                    continue
                list_file_cnt += 1
    if max_list_file_cnt == 0:
        thread_cnt = 1
    else:
        thread_cnt = max_list_file_cnt + 1
    return thread_cnt


def make_pcm_file(logger):
    """
    Make pcm file
    :param      logger:             Logger
    """
    logger.info("4. Make pcm file")
    for key, info_dict in RCDG_INFO_DICT.items():
        rfile_name = info_dict['RFILE_NAME']
        biz_cd = info_dict['BIZ_CD']
        project_cd = info_dict['PROJECT_CD']
        try:
            file_sprt = 'N'
            rec_ext = info_dict['REC_EXT']
            chn_tp = 'M'
            file_path = '{0}/{1}.{2}'.format(STT_TEMP_DIR_PATH, rfile_name, rec_ext)
            rx_file_path = '{0}/{1}_rx.{2}'.format(STT_TEMP_DIR_PATH, rfile_name, rec_ext)
            tx_file_path = '{0}/{1}_tx.{2}'.format(STT_TEMP_DIR_PATH, rfile_name, rec_ext)
            # separation rec file
            if chn_tp == 'S' and file_sprt == 'N':
                logger.debug("\tSeparation rec file")
                if rec_ext == 'pcm':
                    wav_file_path = '{0}/{1}.wav'.format(STT_TEMP_DIR_PATH, rfile_name)
                    rx_file_path = '{0}/{1}_rx.wav'.format(STT_TEMP_DIR_PATH, rfile_name)
                    tx_file_path = '{0}/{1}_tx.wav'.format(STT_TEMP_DIR_PATH, rfile_name)
                    sox_cmd = './sox -t raw -b 16 -e signed-integer -r 8000 -B -c2 {0} {1}'.format(
                        file_path, wav_file_path)
                    sub_process(logger, sox_cmd)
                    file_path = wav_file_path
                    rec_ext = 'wav'
                # If rx or tx wav file is already existed remove file
                if os.path.exists(rx_file_path):
                    del_garbage(logger, rx_file_path)
                if os.path.exists(tx_file_path):
                    del_garbage(logger, tx_file_path)
                os.chdir(STT_CONFIG['stt_tool_path'])
                cmd = './ffmpeg -i {0} -filter_complex "[0:0]pan=1c|c0=c0[left];[0:0]pan=1c|c0=c1[right]"'.format(
                    file_path)
                cmd += ' -map "[left]" {0} -map "[right]" {1}'.format(rx_file_path, tx_file_path)
                sub_process(logger, cmd)
                # Delete stereo wav file
                if not os.path.exists(rx_file_path) or not os.path.exists(tx_file_path):
                    err_str = "ffmpeg Failed"
                    raise Exception(err_str)
            target_file_list = [rx_file_path, tx_file_path] if chn_tp == 'S' else [file_path]
            # make pcm file
            for target_file in target_file_list:
                file_name_path = os.path.splitext(target_file)[0]
                logger.debug("\ttarget file : {0}".format(target_file))
                logger.debug("\textension : {0}".format(rec_ext))
                if rec_ext == 'm4a':
                    logger.debug("\t m4a -> wav")
                    wav_file_path = "{0}.wav".format(file_name_path)
                    if os.path.exists(wav_file_path):
                        del_garbage(logger, wav_file_path)
                    os.chdir(STT_CONFIG['stt_tool_path'])
                    cmd = "./ffmpeg -i {0} -f wav -ac 1 {1}".format(target_file, wav_file_path)
                    sub_process(logger, cmd)
                    rec_ext = 'wav'
                    target_file = wav_file_path
                if rec_ext == 'wav':
                    logger.debug("\t wav -> pcm")
                    pcm_file_path = "{0}.pcm".format(file_name_path)
                    if os.path.exists(pcm_file_path):
                        del_garbage(logger, pcm_file_path)
                    os.chdir(STT_CONFIG['stt_tool_path'])
                    cmd = "./sox -t wav {0} -r 8000 -b 16 -t raw {1}".format(target_file, pcm_file_path)
                    sub_process(logger, cmd)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("---------- make pcm error ----------")
            error_process(logger, rfile_name, biz_cd, project_cd)
            del RCDG_INFO_DICT[key]
            continue
    if len(RCDG_INFO_DICT.keys()) < 1:
        logger.info("CS END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error("----------    Job is ZERO(0)   ----------")
        delete_garbage_file(logger)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


def copy_data(logger):
    """
    Copy source data
    :param      logger:             Logger
    """
    global RCDG_INFO_DICT
    logger.info("3. Copy data")
    for key, info_dict in RCDG_INFO_DICT.items():
        rfile_name = info_dict['RFILE_NAME']
        biz_cd = info_dict['BIZ_CD']
        chn_tp = 'M'
        rec_ext = info_dict['REC_EXT']
        project_cd = info_dict['PROJECT_CD']
        file_sprt = 'N'
        try:
            target_dir = '{0}/{1}/{2}/processed/{3}/{4}/{5}'.format(
                STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8])
            # Mono
            if chn_tp == 'M' or (chn_tp == 'S' and file_sprt == 'N'):
                file_path = '{0}/{1}.{2}'.format(target_dir, rfile_name, rec_ext)
                if not os.path.exists(file_path):
                    logger.error("{0} file is not exist -> {1}".format(rec_ext, file_path))
                    error_process(logger, rfile_name, biz_cd, project_cd)
                    del RCDG_INFO_DICT[key]
                    continue
                logger.debug('\t{0} -> {1}'.format(file_path, STT_TEMP_DIR_PATH))
                shutil.copy(file_path, STT_TEMP_DIR_PATH)
            # Stereo
            elif chn_tp == 'S' and file_sprt == 'Y':
                rx_file_path = '{0}/{1}_rx.{2}'.format(target_dir, rfile_name, rec_ext)
                tx_file_path = '{0}/{1}_tx.{2}'.format(target_dir, rfile_name, rec_ext)
                if not os.path.exists(rx_file_path) or not os.path.exists(tx_file_path):
                    logger.error("{0} file is not exists -> {0} or {1}".format(rec_ext, rx_file_path, tx_file_path))
                    error_process(logger, rfile_name, biz_cd, project_cd)
                    del RCDG_INFO_DICT[key]
                    continue
                logger.debug('\t{0} -> {1}'.format(rx_file_path, STT_TEMP_DIR_PATH))
                shutil.copy(rx_file_path, STT_TEMP_DIR_PATH)
                logger.debug('\t{0} -> {1}'.format(tx_file_path, STT_TEMP_DIR_PATH))
                shutil.copy(tx_file_path, STT_TEMP_DIR_PATH)
            else:
                logger.error("CHN_TP ERROR {0} : {1}".format(key, chn_tp))
                error_process(logger, rfile_name, biz_cd, project_cd)
                del RCDG_INFO_DICT[key]
                continue
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("---------- copy data error ----------")
            error_process(logger, rfile_name, biz_cd, project_cd)
            del RCDG_INFO_DICT[key]
            continue
    if len(os.listdir(STT_TEMP_DIR_PATH)) < 1:
        logger.error("No such file -> {0}".format(RCDG_INFO_DICT.keys()))
        delete_garbage_file(logger)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


def check_json_data(logger, json_data):
    """
    Check json data
    :param          logger:             Logger
    :param          json_data:          Json data
    :return:                            True or False
    """
    project_cd = str(json_data['ProjectCD']).strip()
    if len(project_cd) < 1:
        logger.error("Error ProjectCD length is 0")
        return False
    rec_id = str(json_data['RecID']).strip()
    if len(rec_id) < 1:
        logger.error("Error RecID length is 0")
        return False
    cti_call_id = str(json_data['CTICallID']).strip()
    if len(cti_call_id) < 1:
        logger.error("Error CTICallID length is 0")
        return False
    if cti_call_id == 'None':
        cti_call_id = None
    call_type = str(json_data['CallType']).strip()
    if len(call_type) < 1:
        logger.error("Error CallType length is 0")
        return False
    start_time = str(json_data['StartTime']).strip()
    if len(start_time) != 14:
        logger.error("Error StartTime length is not 14")
        return False
    end_time = str(json_data['EndTime']).strip()
    if len(end_time) != 14:
        logger.error("Error EndTime length is not 14")
        return False
    duration = str(json_data['Duration']).strip()
    if len(duration) != 6:
        logger.error("Error Duration length is not 6")
        return False
    ani = str(json_data['ANI']).strip()
    if len(ani) < 1:
        logger.error("Error ANI length is 0")
        return False
    r_user_id = str(json_data['RUserID']).strip()
    if len(r_user_id) < 1:
        logger.error("Error RUserID length is 0")
        return False
    r_user_name = str(json_data['RUserName']).strip()
    if len(r_user_name) < 1:
        logger.error("Error RUserName length is 0")
        return False
    r_user_number = str(json_data['RUserNumber']).strip()
    if len(r_user_number) < 1:
        logger.error("Error RUserNumber length is 0")
        return False
    r_file_name = str(json_data['RFileName']).strip()
    if len(r_file_name) < 1:
        logger.error("Error RFileName length is 0")
        return False
    cust_name = str(json_data['CuName']).strip()
    if len(cust_name) < 1:
        logger.error("Error CuName length is 0")
        return False
    cust_number = str(json_data['CuNumber']).strip()
    if len(cust_number) < 1:
        logger.error("Error CuNumber length is 0")
        return False
    cust_id = str(json_data['CuID']).strip()
    if len(cust_id) < 1:
        logger.error("Error CuID length is 0")
        return False
    biz = str(json_data['Biz']).strip()
    if len(biz) < 1:
        logger.error("Error Biz length is 0")
        return False
    chn_tp = str(json_data['Chn_tp']).strip().upper()
    if chn_tp not in ['S', 'M']:
        logger.error("Error Chn_tp is not 'S' or 'M'")
        return False
    sprt = str(json_data['Sprt']).strip().upper()
    if sprt not in ['Y', 'N']:
        logger.error("Error Sprt is not 'Y' or 'N'")
        return False
    rec_ext = str(json_data['Rec_ext']).strip()
    if len(rec_ext) < 1:
        logger.error("Error Rec_ext length is 0")
        return False
    return True


def checked_json(logger, file_path, rfile_name, biz_cd, project_cd):
    """
    Check record file and json data
    :param              logger                          Logger
    :param              file_path                       JSON_PATH
    :param              rfile_name                      RFILE_NAME
    :param              biz_cd                          BIZ_CD
    :param              project_cd                      PROJECT_CD
    :return:                                            Checked json data
    """
    logger.debug("Check raw data (JSON, RECORD)")
    try:
        logger.debug("Open json file. [{0}]".format(file_path))
        try:
            json_file = open(file_path, 'r')
            logger.debug("Load json data. [encoding = euc-kr]")
            json_data = json.load(json_file, encoding='euc-kr')
            json_file.close()
        except Exception:
            logger.error("Retry load json data.")
            time.sleep(1)
            json_file = open(file_path, 'r')
            logger.debug("Load json data. [encoding = euc-kr]")
            json_data = json.load(json_file, encoding='euc-kr')
            json_file.close()
        # Check json data
        logger.debug("Check json data.")
        checked_json_data = check_json_data(logger, json_data)
        if not checked_json_data:
            error_process(logger, rfile_name, biz_cd, project_cd)
        # Check record file
        logger.debug("Check record file.")
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        error_process(logger, rfile_name, biz_cd, project_cd)


def update_status_and_select_rec_file(logger, job_list):
    """
    Update status and select rec file
    :param      logger:         Logger
    :param      job_list:       List of JOB
    """
    global RCDG_INFO_DICT
    global DELETE_FILE_LIST
    logger.info("2. Get recording information dictionary")
    logger.info("\tload job list -> {0}".format(job_list))
    # Creating recording file dictionary
    for job in job_list:
        rfile_name = job['RFILE_NAME']
        biz_cd = job['BIZ_CD']
        project_cd = job['PROJECT_CD']
        ext = job['REC_EXT']
        json_biz_cd = ''
        try:
            if biz_cd == 'CN_6M':
                json_biz_cd = 'cnet'
            elif biz_cd == 'VT_6M':
                json_biz_cd = 'voistore'
            json_path = '{0}/{1}/{2}/{3}.json'.format(STT_CONFIG['rec_dir_path'], json_biz_cd, project_cd, rfile_name)
            rec_path = '{0}/{1}/{2}/{3}.{4}'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name, ext)
            if not os.path.exists(json_path):
                logger.error('First json check Fail')
                logger.error("Not exist JSON FILE = {0}".format(json_path))
                json_path = '{0}/{1}/{2}/{3}.json'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name)
                if not os.path.exists(json_path):
                    raise Exception("Not exist JSON FILE = {0}".format(json_path))
            checked_json(logger, json_path, rfile_name, biz_cd, project_cd)
            shutil.copy(json_path, STT_TEMP_DIR_PATH)
            processed_dir_path = '{0}/{1}/{2}/processed/{3}/{4}/{5}'.format(
                STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8])
            if not os.path.exists(processed_dir_path):
                os.makedirs(processed_dir_path)
            processed_file_path = '{0}/{1}.{2}'.format(processed_dir_path, rfile_name, ext)
            processed_json_path = '{0}/{1}.json'.format(processed_dir_path, rfile_name)
            if os.path.exists(processed_file_path):
                del_garbage(logger, processed_file_path)
            if os.path.exists(processed_json_path):
                del_garbage(logger, processed_json_path)
            shutil.move(rec_path, processed_dir_path)
            shutil.move(json_path, processed_dir_path)
            DELETE_FILE_LIST.append(processed_file_path)
            DELETE_FILE_LIST.append(processed_json_path)
            key = '{0}'.format(rfile_name)
            RCDG_INFO_DICT[key] = job
        except Exception as e:
            logger.error(e)
            error_process(logger, rfile_name, biz_cd, project_cd)
            continue


def setup_data():
    """
    Setup target directory
    :return:                Logger, cnt
    """
    global STT_TEMP_DIR_PATH
    global STT_TEMP_DIR_NAME
    global DELETE_FILE_LIST
    # Determine temp directory name to be used in script
    cnt = 0
    while True:
        STT_TEMP_DIR_PATH = "{0}/stt_temp_directory_{1}".format(STT_CONFIG['stt_path'], cnt)
        if not os.path.exists(STT_TEMP_DIR_PATH):
            os.makedirs(STT_TEMP_DIR_PATH)
            STT_TEMP_DIR_NAME = os.path.basename(STT_TEMP_DIR_PATH)
            DELETE_FILE_LIST.append(STT_TEMP_DIR_PATH)
            break
        cnt += 1
    # Determining log name
    log_name = '{0}_{1}.log'.format(DT[:8], cnt)
    # Add logging
    logger_args = {
        'base_path': STT_CONFIG['log_dir_path'],
        'log_file_name': log_name,
        'log_level': STT_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    return logger, cnt


def processing(job_list):
    """
    CS processing
    :param      job_list:        Job(REC_ID, RFILE_NAME, NQA_STTA_PRGST_CD)
    """
    # 0. Setup data
    logger, cnt = setup_data()
    logger.info("-" * 100)
    logger.info('Start CS')
    try:
        # 2. Update status and Get recording information dictionary using job list
        update_status_and_select_rec_file(logger, job_list)
        # 3. Copy data
        copy_data(logger)
        # 4. Make pcm file
        make_pcm_file(logger)
        # 5. Make list file
        thread_cnt = make_pcm_list_file(logger)
        # 6. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 7. Execute unseg.exe and do_space.exe
        do_space_dir_path = execute_unseg_and_do_space(logger)
        # 8. Make output
        make_output(logger, do_space_dir_path)
        # 9. Set output
        set_output(logger)
        # 10. Execute masking
        execute_masking(logger)
        # 11. Execute TA
        execute_new_lang(logger)
        # 12. Make statistics file
        make_statistics_file(logger)
        # 13. Modify nlp output
        modify_nlp_output_line_number(logger)
        # 15. Move output
        move_output(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("CS END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    CS ERROR   ----------")
        for info_dict in RCDG_INFO_DICT.values():
            rfile_name = info_dict['RFILE_NAME']
            biz_cd = info_dict['BIZ_CD']
            project_cd = info_dict['PROJECT_CD']
            error_process(logger, rfile_name, biz_cd, project_cd)
        delete_garbage_file(logger)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    if len(RCDG_INFO_DICT.keys()) > 0:
        # 17. Delete garbage list
        delete_garbage_file(logger)
        # 18. Print statistical data
        statistical_data(logger)
        # 19. Update status
    for info_dict in RCDG_INFO_DICT.values():
        chn_tp = 'M'
        biz_cd = info_dict['BIZ_CD']
        file_sprt = 'N'
        project_cd = info_dict['PROJECT_CD']
        rec_ext = info_dict['REC_EXT']
        rfile_name = info_dict['RFILE_NAME']
        if chn_tp == 'M' or (chn_tp == 'S' and file_sprt == 'N'):
            target_file_list = [
                '{0}/{1}/{2}/processed/{3}/{4}/{5}/{6}.{7}'.format(
                    STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8], rfile_name, rec_ext)
            ]
        else:
            target_file_list = [
                '{0}/{1}/{2}/processed/{3}/{4}/{5}/{6}_rx.{7}'.format(
                    STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8], rfile_name, rec_ext),
                '{0}/{1}/{2}/processed/{3}/{4}/{5}/{6}_tx.{7}'.format(
                    STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8], rfile_name, rec_ext)
            ]
#        target_file_list += [
#            '{0}/{1}/{2}/processed/{3}/{4}/{5}/{6}.json'.format(
#                STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name[:4], rfile_name[4:6], rfile_name[6:8], rfile_name),
#        for target_file in target_file_list:
#            del_garbage(logger, target_file)
    logger.info("TOTAL END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job_list):
    """
    This is a program that execute CS
    :param      job_list:        JOB list
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        if len(job_list) > 0:
            processing(job_list)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)
