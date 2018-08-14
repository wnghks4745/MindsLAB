#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-21, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
import json
import time
import logging
import traceback
from datetime import datetime
from operator import itemgetter
from google.protobuf import json_format
from biz.client import hmd_client
from biz.common import util, db_connection, biz_worker
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from maum.biz.proc import nlp_pb2
from maum.biz.common import common_pb2
import maum.brain.hmd.hmd_pb2 as hmd_pb2

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class HmdProc(biz_worker.BizWorker):
    def __init__(self, index, frontend_port, backend_port, logq, hmd_addr):
        biz_worker.BizWorker.__init__(self, frontend_port, backend_port, logq)
        self.index = index
        self.hmd_addr = hmd_addr

    def initialize(self):
        """ callback """
        pass

    def do_work(self, frames):
        """ BizWorker 에서 호출하는 callback 함수"""
        header, body = frames
        hdr = common_pb2.Header()
        oracle = db_connection.Oracle()
        st = datetime.now()
        try:
            hdr.ParseFromString(header)
            hmd_client_obj = hmd_client.HmdClient(self.hmd_addr)
            project_code = hdr.call_metadata.project_code
            call_type_code = hdr.call_metadata.call_type_code
            speaker_code = hdr.call_metadata.speaker_code
            if speaker_code == 'ST0001':
                # RX(LEFT) 파일 고객
                file_dcd = 'FS0001'
            elif speaker_code == 'ST0002':
                # TX(RIGHT) 파일 상담사
                file_dcd = 'FS0002'
            else:
                file_dcd = 'FS0003'
            # PROJECT_CODE(CS OR TM), CALL_TYPE_CODE(IN OR OUTBOUND CALL), FILE_DCD(화자)
            # 기준으로 TA_HMD_MODEL_TB 에서 수행해야 할 HMD 목록을 가져온다.
            hmd_list = select_hmd_list(oracle, project_code, call_type_code, file_dcd)
            sentence_cnt = 0
            if hdr.model_params in hmd_list:
                nlp_result_detail = nlp_pb2.nlpResultDetail()
                nlp_result_detail.ParseFromString(body)
                # 관심 키워드
                if hdr.model_params in ['interest_keyword_cs_hmd', 'interest_keyword_tm_hmd']:
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            insert_interest_hmd_result(oracle, hmd_result, document, hdr)
                # 호전환 콜
                elif hdr.model_params == 'calltransfer_hmd':
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            insert_transfer_call_analysis_hmd_result(oracle, hmd_result, hdr, document)
                            break
                # 관심 구간
                elif hdr.model_params in ['interest_section_cs_hmd', 'interest_section_tm_hmd']:
                    hmd_result_list = list()
                    start_point_dict = dict()
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            hmd_result_list.append((hmd_result, document))
                            for cls in hmd_result.keys():
                                cat_list = cls.split('_')
                                if len(cat_list) != 3:
                                    continue
                                section_code, section_number, cust_yn = cat_list
                                if section_number == '001':
                                    if not section_code in start_point_dict:
                                        start_point_dict[section_code] = [document.sequence, document.start_time]
                    insert_interest_section_hmd_result(oracle, hmd_result_list, hdr, start_point_dict, self.log)
                # 상담 유형 분류, 상담 유형별 스크립트 미준수율
                elif hdr.model_params in ['call_driver_classify_cs_hmd', 'call_driver_classify_tm_hmd']:
                    total_category_dict = dict()
                    if hdr.model_params == 'call_driver_classify_cs_hmd':
                        name = 'call_driver_classify_cs'
                    else:
                        name = 'call_driver_classify_tm'
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            # 상담 유형 분류
                            category_dict = insert_call_driver_classify_hmd_result(
                                oracle, hmd_result, document, hdr, name)
                            for category in category_dict.keys():
                                if category not in total_category_dict:
                                    total_category_dict[category] = 1
                    if total_category_dict:
                        # 상담 유형별 스크립트 미준수율
                        category_list = total_category_dict.keys()
                        execute_call_driver_classify_script_quality(
                            oracle, category_list, nlp_result_detail, hdr, hmd_client_obj)
                # 에러 콜
                elif hdr.model_params == 'error_call_hmd':
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            insert_error_call_hmd_result(oracle, hmd_result, hdr, document)
                            break
                # 상담 품질 필수 문장
                elif hdr.model_params == 'require_sentence_hmd':
                    hmd_result_list = list()
                    if hdr.call_metadata.call_type_code == 'CT0001':
                        model_params = 'require_sentence_in_hmd'
                    else:
                        model_params = 'require_sentence_out_hmd'
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            hmd_result_list.append((hmd_result, document))
                    insert_require_sentence_hmd_result(oracle, hmd_result_list, hdr)
                # 부정/민원/불만
                elif hdr.model_params in ['negative_keyword_cs_hmd', 'negative_keyword_tm_hmd']:
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            insert_negative_keyword_hmd_result(oracle, hmd_result, document, hdr)
                self.log(logging.INFO, "{0} {1} HMD Process REQUIRED TIME = {2}".format(
                    sentence_cnt, hdr.model_params, str(datetime.now() - st)))
            hdr.status_id = util.ProcStatus.PS_COMPLETED
            util.insert_proc_result(oracle, self.logger, hdr)
            oracle.conn.commit()
        except grpc.RpcError as e:
            oracle.conn.rollback()
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, e)
        except Exception:
            oracle.conn.rollback()
            hdr.status_id = util.ProcStatus.PS_FAILED
            util.insert_proc_result(oracle, self.logger, hdr)
            self.log(logging.ERROR, traceback.format_exc())
        finally:
            oracle.disconnect()


#######
# def #
#######
def find_loc(vec_space, pos, plus_num, len_line):
    """
    Find loc
    :param      vec_space:
    :param      pos:
    :param      plus_num:
    :param      len_line:
    :return:
    """
    s_i = 0
    len_vs = len(vec_space)
    for s_i in range(len_vs):
        if vec_space[s_i] > pos:
            break
    key_pos = vec_space[s_i]
    if s_i != -1:
        e_i = s_i + plus_num
        if e_i >= len_vs:
            e_i = len_vs - 1
        end_pos = vec_space[e_i] + 1
    else:
        end_pos = len_line
    return key_pos, end_pos


def find_hmd(vec_key, sent, tmp_line, vec_space):
    """
    Find hmd
    :param      vec_key:
    :param      sent:
    :param      tmp_line:
    :param      vec_space:
    :return:
    """
    pos = 0
    not_line = tmp_line
    len_line = len(tmp_line)
    for i in range(len(vec_key)):
        if len(vec_key[i]) == 0:
            continue
        b_pos = True
        b_sub = False
        b_neg = False
        k_str = ""
        key_pos = 0
        end_pos = len_line
        # Searching Special Command
        w_loc = -1
        while True:
            w_loc += 1
            if w_loc == len(vec_key[i]):
                return not_line, False
            if vec_key[i][w_loc] == '!':
                if b_pos:
                    b_pos = False
                else:
                    b_neg = True
            elif vec_key[i][w_loc] == '@':
                key_pos = pos
            elif vec_key[i][w_loc] == '+':
                w_loc += 1
                try:
                    plus_num = int(vec_key[i][w_loc])
                    key_pos, end_pos = find_loc(vec_space, pos, plus_num, len_line)
                except ValueError:
                    print 'ERR: + next number Check Dictionary' + vec_key[i]
                    return not_line, False
            elif vec_key[i][w_loc] == '%':
                b_sub = True
            elif vec_key[i][w_loc] == '#':
                b_ne = True
            else:
                k_str = vec_key[i][w_loc:]
                break
        # Searching lemma
        t_pos = 0
        if b_pos:
            if b_sub:
                pos = tmp_line[key_pos:end_pos].find(k_str)
            else:
                pos = tmp_line[key_pos:end_pos].find(' ' + k_str + ' ')
            if pos != -1:
                pos = pos + key_pos
        else:
            t_pos = key_pos
            if b_neg:
                pos = sent.find(k_str)
            elif b_sub:
                pos = not_line[key_pos:end_pos].find(k_str)
            else:
                pos = not_line[key_pos:end_pos].find(' ' + k_str + ' ')
        # Checking Result
        if b_pos:
            if pos > -1:
                if b_sub:
                    tmp_line = tmp_line[:key_pos] + tmp_line[key_pos:end_pos].replace(
                        k_str, '_' * len(k_str)) + tmp_line[end_pos:]
                else:
                    tmp_line = tmp_line[:key_pos] + tmp_line[key_pos:end_pos].replace(
                        ' ' + k_str + ' ', ' ' + '_' * len(k_str) + ' ') + tmp_line[end_pos:]
            else:
                return not_line, False
        else:
            if pos > -1:
                return not_line, False
            else:
                pos = t_pos
    return tmp_line, True


def vec_word_combine(tmp_result, output, strs_list, ws, level):
    """
    Vec word combine
    :param      tmp_result:     Temp result
    :param      output:         Output
    :param      strs_list:      Str list
    :param      ws:             Ws
    :param      level:          Level
    :return:                    Temp result
    """
    if level == len(strs_list):
        tmp_result.append(output + ws)
    elif level == 0:
        for i in range(len(strs_list[level])):
            tmp = output + strs_list[level][i]
            vec_word_combine(tmp_result, tmp, strs_list, ws, level + 1)
    else:
        for i in range(len(strs_list[level])):
            if output[-1] == '@':
                tmp = output[:-1] + '$@' + strs_list[level][i]
            elif output[-1] == '%':
                tmp = output[:-1] + '$%' + strs_list[level][i]
            elif output[-2] == '+' and ('0' <= output[-1] <= '9'):
                tmp = output[:-1] + '$+' + output[-1] + strs_list[level][i]
            elif output[-1] == '#':
                tmp = output[:-1] + '$#' + strs_list[level][i]
            else:
                tmp = output + '$' + strs_list[level][i]
            vec_word_combine(tmp_result, tmp, strs_list, ws, level + 1)
    return tmp_result


def split_input(detect_keyword):
    """
    Split input
    :param      detect_keyword:     Detect keyword
    :return:                        Detect keyword list
    """
    detect_keyword_list = list()
    cnt = 0
    tmp = ''
    for idx in range(len(detect_keyword)):
        if detect_keyword[idx] == '(':
            cnt = 1
        elif detect_keyword[idx] == ')' and len(tmp) != 0:
            detect_keyword_list.append(tmp)
            tmp = ''
            cnt = 0
        elif cnt == 1:
            tmp += detect_keyword[idx]
    return detect_keyword_list


def load_model(model_path):
    """
    Load Model
    :param      model_path:     Model Path
    :return:                    Model Value
    """
    if not model_path.endswith('.hmdmodel'):
        raise Exception('model extension is not .hmdmodel : {0}'.format(model_path))
    try:
        in_file = open(model_path, 'rb')
        hm = hmd_pb2.HmdModel()
        hm.ParseFromString(in_file.read())
        in_file.close()
        return hm
    except Exception:
        raise Exception(traceback.format_exc())


def execute_hmd(document, model_name):
    """
    HMD
    :param      document:        document
    :param      model_name:         Model Name
    :return
    """
    hmd_dict = dict()
    model_path = '{0}/trained/hmd/{1}__0.hmdmodel'.format(os.getenv('MAUM_ROOT'), model_name)
    if not os.path.exists(model_path):
        raise Exception('model is not exists : {0}'.format(model_path))
    model_value = load_model(model_path)
    output_list = list()
    detect_category_dict = dict()
    for rules in model_value.rules:
        strs_list = list()
        category = rules.categories[0]
        dtc_cont = rules.rule
        detect_keyword_list = split_input(dtc_cont)
        for idx in range(len(detect_keyword_list)):
            detect_keyword = detect_keyword_list[idx].split("|")
            strs_list.append(detect_keyword)
        ws = ''
        output = ''
        tmp_result = []
        output += '{0}\t'.format(category)
        output_list += vec_word_combine(tmp_result, output, strs_list, ws, 0)
    for item in output_list:
        item = item.strip()
        if len(item) < 1 or item[0] == '#':
            continue
        item_list = item.split("\t")
        t_loc = len(item_list) - 1
        cate = ''
        for idx in range(t_loc):
            if idx != 0:
                cate += '_'
            cate += item_list[idx]
            if item_list[t_loc] not in hmd_dict:
                hmd_dict[item_list[t_loc]] = [[cate]]
            else:
                hmd_dict[item_list[t_loc]].append([cate])
    json_data = json.loads(json_format.MessageToJson(document, True))
    word_list = list()
    for sentence in json_data['document']['sentences']:
        for words in sentence['words']:
            tagged_text = words['taggedText']
            tagged_text_list = tagged_text.split()
            for tagged_word in tagged_text_list:
                word = tagged_word.split("/")[0]
                tag = tagged_word.split("/")[1]
                if tag in ['VV', 'VA', 'VX', 'VCP', 'VCN']:
                    word += u"\ub2e4"
                word_list.append(word)
    nlp_sent = " ".join(word_list)
    s_tmp = " {0} ".format(nlp_sent)
    vec_space = list()
    vec_ne = list()
    rmv_ne = list()
    cnt = 0
    flag = False
    for idx in range(len(s_tmp)):
        if s_tmp[idx] == ' ':
            vec_space.append(cnt)
        elif (idx + 1 != len(s_tmp)) and s_tmp[idx:idx + 2] == '__':
            if flag:
                t_word = s_tmp[idx + 2:s_tmp[idx + 2:].find(' ') + idx + 2]
                tmp_ne.append(cnt)
                tmp_ne.append(t_word)
                vec_ne.append(tmp_ne)
                cnt = cnt - 2 - len(t_word)
                r_tmp_ne.append(idx)
                r_tmp_ne.append(idx + 2 + len(t_word))
                rmv_ne.append(r_tmp_ne)
                flag = False
            else:
                tmp_ne = list()
                tmp_ne.append(cnt)
                r_tmp_ne = list()
                r_tmp_ne.append(idx)
                flag = True
                cnt -= 2
        cnt += 1
    rmv_ne.reverse()
    for ne in rmv_ne:
        s_tmp = s_tmp[:ne[0]] + s_tmp[ne[0] + 2:ne[1]] + s_tmp[ne[2]:]
    b_check = False
    for key in hmd_dict.keys():
        tmp_line = s_tmp
        vec_key = key.split('$')
        tmp, b_print = find_hmd(vec_key, document.sentence, tmp_line, vec_space)
        if b_print:
            b_check = True
            for item in hmd_dict[key]:
                if item[0] not in detect_category_dict:
                    detect_category_dict[item[0]] = [key]
                else:
                    detect_category_dict[item[0]].append(key)
    return detect_category_dict


def insert_negative_keyword_hmd_result(oracle, hmd_result, document, hdr):
    """
    Inert interest HMD result to CS_NEGATIVE_KEYWORD_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          document:               NLP Document
    :param          hdr:                    Header
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for cls in hmd_result.keys():
        cat_list = cls.split('_')
        if len(cat_list) != 2:
            continue
        category, keyword_name = cat_list
        query = """
        INSERT INTO CS_NEGATIVE_KEYWORD_TB
        (
            PIPELINE_EVENT_ID,
            NLP_RESULT_ID,
            CATEGORY,
            KEYWORD_NAME,
            CALL_DATE,
            SPEAKER_CODE,
            PROJECT_CODE,
            BUSINESS_DCD,
            CALL_ID,
            STT_RESULT_DETAIL_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3, :4,
            TO_DATE(:5, 'YYYY/MM/DD'),
            :6, :7, :8, :9, :10, SYSDATE,
            SYSDATE, :11, :12
        )
        """
        bind = (
            hdr.pipeline_event_id,
            document.nlp_result_id,
            category,
            keyword_name,
            call_date,
            hdr.call_metadata.speaker_code,
            hdr.call_metadata.project_code,
            hdr.call_metadata.business_dcd,
            hdr.call_id,
            document.stt_result_detail_id,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
        break
    oracle.conn.commit()


def insert_require_sentence_hmd_result(oracle, hmd_result_list, hdr):
    """
    Inert require sentence HMD result to CS_REQUIRE_SENTENCE_TB
    :param          oracle:                 DB
    :param          hmd_result_list:        HMD result list
    :param          hdr:                    Header
    """
    check_category_dict = {
        'open_mention':['N', '', ''],
        'close_mention':['N', '', ''],
        'extra_question':['N', '', ''],
        'react_mention':['N', '', '']
    }
    for hmd_result, document in hmd_result_list:
        for cls in hmd_result.keys():
            if cls == 'OPEN_MENTION':
                if check_category_dict['open_mention'][0] == 'N':
                    check_category_dict['open_mention'] = [
                        'Y', document.nlp_result_id, document.stt_result_detail_id]
            elif cls == 'CLOSE_MENTION':
                if check_category_dict['close_mention'][0]  == 'N':
                    check_category_dict['close_mention'] = [
                        'Y', document.nlp_result_id, document.stt_result_detail_id]
            elif cls == 'EXTRA_QUESTION':
                if check_category_dict['extra_question'][0] == 'N':
                    check_category_dict['extra_question'] = [
                        'Y', document.nlp_result_id, document.stt_result_detail_id]
            elif cls == 'REACT_MENTION':
                if check_category_dict['react_mention'][0] == 'N':
                    check_category_dict['react_mention'] = [
                        'Y', document.nlp_result_id, document.stt_result_detail_id]
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    query = """
    INSERT INTO CS_REQUIRE_SENTENCE_TB
    (
        PIPELINE_EVENT_ID,
        CALL_ID,
        CALL_DATE,
        BUSINESS_DCD,
        PROJECT_CODE,
        OPEN_MENTION_DTC_YN,
        OM_NLP_RESULT_ID,
        OM_STT_RESULT_DETAIL_ID,
        CLOSE_MENTION_DTC_YN,
        CM_NLP_RESULT_ID,
        CM_STT_RESULT_DETAIL_ID,
        EXTRA_QUESTION_DTC_YN,
        EQ_NLP_RESULT_ID,
        EQ_STT_RESULT_DETAIL_ID,
        REACT_MENTION_DTC_YN,
        RM_NLP_RESULT_ID,
        RM_STT_RESULT_DETAIL_ID,
        CREATED_DTM,
        UPDATED_DTM,
        CREATOR_ID,
        UPDATOR_ID
    )
    VALUES
    (
        :1, :2, TO_DATE(:3, 'YYYY/MM/DD'),
        :4, :5, :6, :7, :8, :9, :10, :11,
        :12, :13, :14, :15, :16, :17, 
        SYSDATE, SYSDATE, :18, :19
    )
    """
    bind = (
        hdr.pipeline_event_id,
        hdr.call_id,
        call_date,
        hdr.call_metadata.business_dcd,
        hdr.call_metadata.project_code,
        check_category_dict['open_mention'][0],
        check_category_dict['open_mention'][1],
        check_category_dict['open_mention'][2],
        check_category_dict['close_mention'][0],
        check_category_dict['close_mention'][1],
        check_category_dict['close_mention'][2],
        check_category_dict['extra_question'][0],
        check_category_dict['extra_question'][1],
        check_category_dict['extra_question'][2],
        check_category_dict['react_mention'][0],
        check_category_dict['react_mention'][1],
        check_category_dict['react_mention'][2],
        hdr.creator_id,
        hdr.creator_id,
    )
    oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def insert_error_call_hmd_result(oracle, hmd_result, hdr, document):
    """
    Inert error call HMD result to CS_ERROR_CALL_ANALYSIS_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          hdr:                    Header
    :param          document:               NLP Document
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for cls in hmd_result.keys():
        query = """
        INSERT INTO CS_ERROR_CALL_ANALYSIS_TB
        (
            PIPELINE_EVENT_ID,
            CALL_ID,
            CATEGORY,
            CALL_DATE,
            PROJECT_CODE,
            SENTENCE,
            STT_RESULT_DETAIL_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY/MM/DD'),
            :5, :6, :7, SYSDATE,
            SYSDATE, :8, :9
        )
        """
        bind = (
            hdr.pipeline_event_id,
            hdr.call_id,
            cls,
            call_date,
            hdr.call_metadata.project_code,
            document.sentence,
            document.stt_result_detail_id,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def insert_script_quality_non_result(oracle, quality_type_code, hdr, non_dtc_category_list):
    """
    Inert script quality HMD none result to CS_SCRIPT_QUALITY_RESULT_TB
    :param          oracle:                         DB
    :param          quality_type_code:              Quality type code
    :param          hdr:                            Header
    :param          non_dtc_category_list:          Target category list
    """
    overlap_check_dict = dict()
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for item in non_dtc_category_list:
        if item in overlap_check_dict:
            continue
        overlap_check_dict[item] = 1
        query = """
        INSERT INTO CS_SCRIPT_QUALITY_RESULT_TB
        (
            QUALITY_TYPE_CODE,
            BUSINESS_DCD,
            CALL_ID,
            CALL_DATE,
            CATEGORY_1DEPTH_ID,
            CATEGORY_2DEPTH_ID,
            CATEGORY_3DEPTH_ID,
            SNTC_SORT_NO,
            SNTC_DTC_YN,
            NLP_RESULT_ID,
            STT_RESULT_DETAIL_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY/MM/DD'),
            :5, :6, :7,
            :8, :9, :10, :11,
            SYSDATE, SYSDATE,
            :12, :13
        )
        """
        bind = (
            quality_type_code,
            hdr.call_metadata.business_dcd,
            hdr.call_id,
            call_date,
            item.split("_")[0],
            item.split("_")[1],
            item.split("_")[2],
            item.split("_")[3],
            "N",
            '',
            '',
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def insert_script_quality_result(oracle, hmd_result, quality_type_code, document, hdr):
    """
    Inert script quality HMD result to CS_SCRIPT_QUALITY_RESULT_TB
    :param          oracle:                     DB
    :param          hmd_result:                 HMD result
    :param          quality_type_code:          Quality type code
    :param          document:                   NLP Document
    :param          hdr:                        Header
    :return                                     Target category list
    """
    dtc_category_list = list()
    overlap_check_dict = dict()
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for cls in hmd_result.keys():
        if cls in overlap_check_dict:
            continue
        overlap_check_dict[cls] = 1
        query = """
        INSERT INTO CS_SCRIPT_QUALITY_RESULT_TB
        (
            QUALITY_TYPE_CODE,
            BUSINESS_DCD,
            CALL_ID,
            CALL_DATE,
            CATEGORY_1DEPTH_ID,
            CATEGORY_2DEPTH_ID,
            CATEGORY_3DEPTH_ID,
            SNTC_SORT_NO,
            SNTC_DTC_YN,
            NLP_RESULT_ID,
            STT_RESULT_DETAIL_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY/MM/DD'),
            :5, :6, :7,
            :8, :9, :10, :11,
            SYSDATE, SYSDATE,
            :12, :13
        )
        """
        bind = (
            quality_type_code,
            hdr.call_metadata.business_dcd,
            hdr.call_id,
            call_date,
            cls.split("_")[0],
            cls.split("_")[1],
            cls.split("_")[2],
            cls.split("_")[3],
            "Y",
            document.nlp_result_id,
            document.stt_result_detail_id,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
        dtc_category_list.append(cls)
    oracle.conn.commit()
    return dtc_category_list


def select_dtc_cont(oracle, scrt_sntc_info_id):
    """
    Select SCRT_SNTC_INFO_ID from CM_SCRT_SNTC_INFO_TB
    :param          oracle:                     DB
    :param          scrt_sntc_info_id:          SCRT_SNTC_INFO_ID
    :return:                                    DTC_CONT
    """
    query = """
        SELECT
            DTC_CONT
        FROM
            CM_SCRT_SNTC_DTC_INFO_TB
        WHERE 1=1
            AND SCRT_SNTC_INFO_ID = :1
    """
    bind = (
        scrt_sntc_info_id,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return False
    if not result:
        return False
    return result


def select_script_quality_scrt_sntc_info_id(oracle, category_1depth_id, category_2depth_id, category_3depth_id):
    """
    Select SCRT_SNTC_INFO_ID from CM_SCRT_SNTC_INFO_TB
    :param          oracle:                     DB
    :param          category_1depth_id:     CATEGORY_1DEPTH_ID
    :param          category_2depth_id:     CATEGORY_2DEPTH_ID
    :param          category_3depth_id:     CATEGORY_3DEPTH_ID
    :return:                                SCRT_SNTC_INFO_ID, SNTC_SORT_NO
    """
    query = """
        SELECT
            SCRT_SNTC_INFO_ID,
            SNTC_SORT_NO
        FROM
            CM_SCRT_SNTC_INFO_TB
        WHERE 1=1
            AND QUALITY_TYPE_CODE = 'QT0001'
            AND CATEGORY_1DEPTH_ID = :1
            AND CATEGORY_2DEPTH_ID = :2
            AND CATEGORY_3DEPTH_ID = :3
    """
    bind = (
        category_1depth_id,
        category_2depth_id,
        category_3depth_id,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return False
    if not result:
        return False
    return result


def execute_call_driver_classify_script_quality(oracle, category_list, nlp_result_detail, hdr, hmd_client_obj):
    """
    Execute call driver classify script quality
    :param          oracle:                     DB
    :param          category_list:              Category list
    :param          nlp_result_detail:          NLP result
    :param          hdr:                        Header
    :param          hmd_client_obj:             HMD cline object
    """
    for category_key in category_list:
        category_list = category_key.split("_")
        category_1depth_id = category_list[0]
        category_2depth_id = category_list[1]
        category_3depth_id = category_list[2]
        result = select_script_quality_scrt_sntc_info_id(
            oracle, category_1depth_id, category_2depth_id, category_3depth_id)
        model_name = "{0}{1}".format(hdr.call_id, hdr.call_metadata.speaker_code)
        target_category_dict = dict()
        if result:
            category_dtc_list = list()
            for item in result:
                scrt_sntc_info_id = item[0]
                sntc_sort_no = str(item[1])
                dtc_cont_result = select_dtc_cont(oracle, scrt_sntc_info_id)
                if not dtc_cont_result:
                    continue
                for dtc_cont in dtc_cont_result:
                    dtc_cont = dtc_cont[0]
                    category = [category_1depth_id, category_2depth_id, category_3depth_id, sntc_sort_no]
                    category_list = '_'.join(category)
                    category_dtc_list.append(([category_list], dtc_cont))
                    if category_list not in target_category_dict:
                        target_category_dict[category_list] = 1
            hmd_client_obj.make_hmd_model(category_dtc_list, model_name)
        else:
            continue
        total_dtc_category_dict = dict()
        for document in nlp_result_detail.documentList:
            script_quality_result = execute_hmd(document, model_name)
            if len(script_quality_result) > 0:
                dtc_category_list = insert_script_quality_result(
                    oracle, script_quality_result, 'QT0001', document, hdr)
                for dtc_category in dtc_category_list:
                    if dtc_category not in total_dtc_category_dict:
                        total_dtc_category_dict[dtc_category] = 1
        model_path = os.path.join(os.getenv('MAUM_ROOT'), "trained/hmd/{0}__0.hmdmodel".format(model_name))
        try:
            os.remove(model_path)
        except Exception:
            pass
        non_dtc_category_list = list(set(target_category_dict.keys()) - set(total_dtc_category_dict.keys()))
        if len(non_dtc_category_list) > 0:
            insert_script_quality_non_result(oracle, 'QT0001', hdr, non_dtc_category_list)


def select_category_id(oracle, name, id):
    """
    Select category ID
    :param          oracle:     Oracle
    :param          name:       Name
    :param          id:         ID
    :return:                    ID
    """
    query = """
        SELECT
            ID
        FROM
            CM_CATEGORY_TB
        WHERE 1=1
            AND NAME = :1
            AND PARENT_ID = :2
    """
    bind = (
        name,
        id,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return False
    if not result:
        return False
    return result[0]


def select_call_driver_classify_category_id(oracle, name):
    """
    Select call driver classify category ID
    :param          oracle:         Oracle
    :param          name:           Name
    :return:                        ID
    """
    query = """
        SELECT
            ID
        FROM
            CM_CATEGORY_TB
        WHERE 1=1
            AND NAME = :1
    """
    bind = (
        name,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return False
    if not result:
        return False
    return result[0]


def insert_call_driver_classify_hmd_result(oracle, hmd_result, document, hdr, name):
    """
    Inert call driver classify HMD result to CS_CALL_DRIVER_CLASSIFY_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          document:               Document
    :param          hdr:                    Header
    :param          name:                   CM_CATEGORY_TB NAME
    :return                                 Category dictionary
    """
    category_dict = dict()
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for cls in hmd_result.keys():
        cat_list = cls.split('_')
        if len(cat_list) != 3:
            continue
        id = select_call_driver_classify_category_id(oracle, name)
        if not id:
            continue
        category_1depth, category_2depth, category_3depth = cat_list
        category_1depth_id = select_category_id(oracle, category_1depth, id)
        if not category_1depth_id:
            continue
        category_2depth_id = select_category_id(oracle, category_2depth, category_1depth_id)
        if not category_2depth_id:
            continue
        category_3depth_id = select_category_id(oracle, category_3depth, category_2depth_id)
        if not category_3depth_id:
            continue
        query = """
        INSERT INTO CS_CALL_DRIVER_CLASSIFY_TB
        (
            PIPELINE_EVENT_ID,
            PROJECT_CODE,
            BUSINESS_DCD,
            CALL_DATE,
            CALL_ID,
            RUSER_ID,
            NLP_RESULT_ID,
            STT_RESULT_DETAIL_ID,
            CATEGORY_1DEPTH_ID,
            CATEGORY_2DEPTH_ID,
            CATEGORY_3DEPTH_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY/MM/DD'),
            :5, :6, :7, :8,
            :9, :10, :11,
            SYSDATE, SYSDATE,
            :12, :13
        )
        """
        bind = (
            hdr.pipeline_event_id,
            hdr.call_metadata.project_code,
            hdr.call_metadata.business_dcd,
            call_date,
            hdr.call_id,
            hdr.call_metadata.ruser_id,
            document.nlp_result_id,
            document.stt_result_detail_id,
            category_1depth_id,
            category_2depth_id,
            category_3depth_id,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
        category_key = "{0}_{1}_{2}".format(category_1depth_id, category_2depth_id, category_3depth_id)
        if category_key not in category_dict:
            category_dict[category_key] = 1
    oracle.conn.commit()
    return category_dict


def select_interest_section_hmd_cust_sentences(oracle, call_id, end_time):
    """
    Select interest section hmd customer sentence
    :param          oracle:             DB
    :param          call_id:            CALL_ID
    :param          end_time:           End time
    :return:                            Sentences
    """
    query = """
        SELECT 
            BB.SENTENCE,
            CC.NLP_SENTENCE
        FROM
            (
                SELECT
                    STT_RESULT_ID
                FROM
                    STT_RESULT_TB
                WHERE 1=1
                    AND CALL_ID = :1
                    AND SPEAKER_CODE = 'ST0001'
            ) AA
        LEFT JOIN
            STT_RESULT_DETAIL_TB BB
        ON
            AA.STT_RESULT_ID = BB.STT_RESULT_ID
        LEFT JOIN
            TA_NLP_RESULT_TB CC
        ON
            BB.STT_RESULT_DETAIL_ID = CC.STT_RESULT_DETAIL_ID
        WHERE 1=1
            AND (BB.START_TIME BETWEEN :2 AND :3)
    """
    bind = (
        call_id,
        end_time,
        end_time + 200,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return False
    if not result:
        return False
    return result


def extract_answer_status_and_sentence(oracle, cust_yn, call_id, start_time):
    """
    Extract customer answer sentence and status
    :param          oracle:                 DB
    :param          cust_yn:                CUST_YN
    :param          call_id:                CALL_ID
    :param          start_time:             START_TIME
    :return:                                Answer status and sentence
    """
    # 고객 응답
    answer_status = ''
    cust_sentence = ''
    normal_answer_list = [
        '네', '너 의', '네 네', '네네', '예', '예예', '아예', '그러다 시 어요', '동의 하 ㄹ 게요'
    ]
    if cust_yn.upper() == 'Y':
        cust_result = select_interest_section_hmd_cust_sentences(oracle, call_id, start_time)
        if cust_result:
            for item in cust_result:
                sentence = item[0]
                nlp_sentence = item[1] if item[1] else ''
                cust_sentence += " " + sentence
                answer_status = 'I'
                for word in normal_answer_list:
                    if word in nlp_sentence:
                        answer_status = 'Y'
                        break
            cust_sentence = "\r\n[C]" + cust_sentence.strip()
        else:
            cust_sentence = "\r\n[C] (응답 없음)"
            answer_status = 'N'
    return answer_status, cust_sentence


def insert_interest_section_hmd_result(oracle, hmd_result_list, hdr, start_point_dict, log):
    """
    Insert interest section hmd result to CS_INTEREST_SECTION_TB
    :param          oracle:                     DB
    :param          hmd_result_list:            HMD result list
    :param          hdr:                        Header
    :param          start_point_dict:           Start point list
    """
    output_dict = dict()
    for hmd_result, document in hmd_result_list:
        for cls in hmd_result.keys():
            cat_list = cls.split('_')
            if len(cat_list) != 3:
                continue
            section_code, section_number, cust_yn  = cat_list
            if section_code in start_point_dict and document.sequence >= start_point_dict[section_code][0]:
                if start_point_dict[section_code][1] + 3000 >= document.start_time:
                    answer_status, cust_sentence = extract_answer_status_and_sentence(
                        oracle, cust_yn, hdr.call_id, document.end_time)
                    if section_code in output_dict:
                        if output_dict[section_code]['end_time'] < document.end_time:
                            output_dict[section_code]['end_time'] = document.end_time
                        if not document.sequence in output_dict[section_code]['sentence']:
                            output_dict[section_code]['sentence'].update(
                                {document.sequence: "[A]" + document.sentence + cust_sentence})
                            output_dict[section_code]['answer_status'] = answer_status
                        if len(answer_status) > 0:
                            if output_dict[section_code]['answer_status'] == 'Y':
                                if answer_status in ['N', 'I']:
                                    output_dict[section_code]['answer_status'] = 'I'
                            elif output_dict[section_code]['answer_status'] == 'N':
                                if answer_status in ['Y', 'I']:
                                    output_dict[section_code]['answer_status'] = 'I'
                    else:
                        output_dict[section_code] = {
                            'start_time': start_point_dict[section_code][1],
                            'end_time': document.end_time,
                            'sentence': {document.sequence: "[A]" + document.sentence + cust_sentence},
                            'answer_status': answer_status
                        }
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for section_code, result in output_dict.items():
        query = """
        INSERT INTO CS_INTEREST_SECTION_TB
        (
            PIPELINE_EVENT_ID,
            CALL_ID,
            INTEREST_SECTION_CODE,
            CALL_DATE,
            PROJECT_CODE,
            SENTENCE,
            START_SENTENCE,
            CU_AN_DTC_YN,
            START_TIME,
            END_TIME,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY/MM/DD'),
            :5, :6, :7,
            :8, :9, :10,
            SYSDATE, SYSDATE, :11, :12
        )
        """
        sorted_sentence = sorted(result['sentence'].iteritems(), key=itemgetter(0), reverse=False)
        if hdr.call_metadata.project_code == 'PC0002':
            if len(result['answer_status']) < 1:
                answer_status = 'N'
            else:
                answer_status = result['answer_status']
        else:
            answer_status = ''
        sentence_list = list()
        for item in sorted_sentence:
            sentence_list.append(item[1])
        bind = (
            hdr.pipeline_event_id,
            hdr.call_id,
            section_code,
            call_date,
            hdr.call_metadata.project_code,
            "\r\n".join(sentence_list),
            sentence_list[0].replace("[A]", "").strip(),
            answer_status,
            result['start_time'],
            result['end_time'],
            hdr.creator_id,
            hdr.creator_id,
        )
        try:
            oracle.cursor.execute(query, bind)
        except Exception:
            log(logging.INFO, bind)
    oracle.conn.commit()


def insert_transfer_call_analysis_hmd_result(oracle, hmd_result, hdr, document):
    """
    Inert transfer call analysis table HMD result to CS_TRANSFER_CALL_ANALYSIS_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          hdr:                    Header
    :param          document:               NLP Document
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for cls in hmd_result.keys():
        query = """
            INSERT INTO CS_TRANSFER_CALL_ANALYSIS_TB
            (
                PIPELINE_EVENT_ID,
                CALL_ID,
                CATEGORY,
                CALL_DATE,
                PROJECT_CODE,
                SENTENCE,
                STT_RESULT_DETAIL_ID,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID
            )
            VALUES
            (
                :1, :2, :3,
                TO_DATE(:4, 'YYYY/MM/DD'),
                :5, :6, :7, SYSDATE,
                SYSDATE, :8, :9
            )
        """
        bind = (
            hdr.pipeline_event_id,
            hdr.call_id,
            cls,
            call_date,
            hdr.call_metadata.project_code,
            document.sentence,
            document.stt_result_detail_id,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
        break
    oracle.conn.commit()


def insert_interest_hmd_result(oracle, hmd_result, document, hdr):
    """
    Inert interest HMD result to CS_INTEREST_KEYWORD_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          document:               NLP Document
    :param          hdr:                    Header
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    for cls in hmd_result.keys():
        cat_list = cls.split('_')
        if len(cat_list) != 2:
            continue
        category, keyword_name = cat_list
        query = """
            INSERT INTO CS_INTEREST_KEYWORD_TB
            (
                PIPELINE_EVENT_ID,
                NLP_RESULT_ID,
                CATEGORY,
                KEYWORD_NAME,
                CALL_DATE,
                SPEAKER_CODE,
                PROJECT_CODE,
                BUSINESS_DCD,
                CALL_ID,
                STT_RESULT_DETAIL_ID,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID
            )
            VALUES
            (
                :1, :2, :3, :4,
                TO_DATE(:5, 'YYYY/MM/DD'),
                :6, :7, :8, :9, :10, SYSDATE,
                SYSDATE, :11, :12
            )
        """
        bind = (
            hdr.pipeline_event_id,
            document.nlp_result_id,
            category,
            keyword_name,
            call_date,
            hdr.call_metadata.speaker_code,
            hdr.call_metadata.project_code,
            hdr.call_metadata.business_dcd,
            hdr.call_id,
            document.stt_result_detail_id,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def select_hmd_list(oracle, project_code, call_type_code, file_dcd):
    """
    Select HMD list
    :param          oracle:                 DB
    :param          project_code:           Project code
    :param          call_type_code:         Call type code
    :param          file_dcd:               File dcd
    :return:                                HMD list
    """
    query = """
        SELECT
            MODEL_PARAMS
        FROM
            TA_HMD_MODEL_TB
        WHERE 1=1
            AND PROJECT_CODE = :1
            AND (CALL_TYPE_CODE = :2 OR CALL_TYPE_CODE = 'CT0003')
            AND (FILE_DCD = :3 OR FILE_DCD = 'FS0003')
    """
    bind = (
        project_code,
        call_type_code,
        file_dcd,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return list()
    if not result:
        return list()
    hmd_list = list()
    for item in result:
        hmd_list.append(item[0])
    return hmd_list
