#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import grpc
import json
import time
import logging
import cx_Oracle
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
                # 상담유형 조회
                cntc_info = list()
                for item in hdr.cntc_cls_list:
                    cntc_info.append((item.cntc_lcls_c, item.cntc_lcls_nm, item.cntc_md_clas_c, item.cntc_md_clas_nm))
                # 대표고객 인입현황
                if hdr.model_params == 'ksqi_hmd' and hdr.call_metadata.cu_name == '대표고객':
                    detect_yn = True
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            detect_yn = False
                            insert_ksqi_hmd_result(oracle, hdr, document.sentence, cntc_info, 'Y')
                    if detect_yn:
                        insert_ksqi_hmd_result(oracle, hdr, '', cntc_info, 'N')
                # 관심 키워드
                elif hdr.model_params == 'interest_keywords_hmd':
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, hdr.model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                            insert_interest_hmd_result(oracle, hmd_result, document.sentence, hdr, cntc_info)
                # 상담 유형 자동 분류[소비부 사용]
                elif hdr.model_params == 'VOC_hmd':
                    model_params = 'VOC_C_hmd' if hdr.call_metadata.speaker_code == 'ST0001' else 'VOC_A_hmd'
                    category_dict = dict()
                    for document in nlp_result_detail.documentList:
                        hmd_result = execute_hmd(document, model_params)
                        if len(hmd_result) > 0:
                            sentence_cnt += 1
                        category_dict = extract_call_driver_classify_hmd_result(oracle, hmd_result, 'CS_VOC', category_dict)
                    insert_call_driver_classify_hmd_result(oracle, hdr, category_dict, cntc_info)
                # 해피콜모니터링 (QT0002), 해피콜 고객질의 비단답형
                elif hdr.model_params == 'happy_call_hmd':
                    result = select_happy_call_info(oracle, hdr.call_metadata.poly_no)
                    if hdr.call_metadata.poly_no != 'None' and result:
                        fof_nm, mjy_nm, ch_nm, ho_nm, cont_dt = result
                        for item in cntc_info:
                            cntc_lcls_nm = item[1]
                            cntc_md_clas_c = item[2]
                            cntc_md_clas_nm = item[3]
                            if cntc_md_clas_c == 'O0102' or cntc_md_clas_c == 'O0104':
                                continue
                            if cntc_lcls_nm == '완판모니터링' and not cntc_md_clas_nm.endswith('_피보험자'):
                                sentence_cnt = set_up_happy_call_hmd(
                                    oracle=oracle,
                                    hdr=hdr,
                                    hmd_client_obj=hmd_client_obj,
                                    fof_nm=fof_nm,
                                    mjy_nm=mjy_nm,
                                    ch_nm=ch_nm,
                                    ho_nm=ho_nm,
                                    cont_dt=cont_dt,
                                    cntc_md_clas_c=cntc_md_clas_c,
                                    cntc_md_clas_nm=cntc_md_clas_nm,
                                    nlp_result_detail=nlp_result_detail
                                )
                # 상담사 품질 체크
                elif hdr.model_params == 'agent_quality_hmd':
                    total_category_dict = dict()
                    # 인바운드콜인 경우 첫인사, 끝인사 추가
                    if hdr.call_metadata.call_type_code == 'CT0001':
                        category_dict = select_category_dict(oracle, '공통', '첫인사', 'agent_quality')
                        category_dict.update(select_category_dict(oracle, '공통', '끝인사', 'agent_quality'))
                        if not category_dict:
                            category_dict = dict()
                        for category in category_dict.keys():
                            if category not in total_category_dict:
                                total_category_dict[category] = 1
                    before_len = len(total_category_dict)
                    # 상담유형 조회
                    for item in cntc_info:
                        cntc_lcls_nm = item[1]
                        cntc_md_clas_nm = item[3]
                        # 상담유형 별 hmd 생성
                        category_dict = select_category_dict(oracle, cntc_lcls_nm, cntc_md_clas_nm, 'agent_quality')
                        for category in category_dict.keys():
                            if category not in total_category_dict:
                                total_category_dict[category] = 1
                    after_len = len(total_category_dict)
                    # 상담유형 별 hmd가 생성되지 않는다면 종료
                    if before_len - after_len == 0:
                        total_category_dict = dict()
                    if total_category_dict:
                        category_list = total_category_dict.keys()
                        sentence_cnt = execute_agent_script_quality(
                            oracle, category_list, nlp_result_detail, hdr, hmd_client_obj, sentence_cnt)
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
    :param      document:           document
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
    tmp_ne = list()
    r_tmp_ne = list()
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
    for key in hmd_dict.keys():
        tmp_line = s_tmp
        vec_key = key.split('$')
        tmp, b_print = find_hmd(vec_key, document.sentence, tmp_line.decode('utf8'), vec_space)
        if b_print:
            for item in hmd_dict[key]:
                if item[0] not in detect_category_dict:
                    detect_category_dict[item[0]] = [key]
                else:
                    detect_category_dict[item[0]].append(key)
    return detect_category_dict


def select_category_dict(oracle, cntc_lcls_nm, cntc_md_clas_nm, model_name):
    """
    Select CNTC_LCLS_NM, CNTC_MD_CLAS_NM
    :param          oracle:                     DB
    :param          cntc_lcls_nm:               CNTC_LCLS_NM
    :param          cntc_md_clas_nm:            CNTC_MD_CLAS_NM
    :param          model_name:                 model name
    :return:                                    category dictionary
    """
    category_dict = dict()
    id = select_call_driver_classify_category_id(oracle, model_name)
    if not id:
        return category_dict
    category_1depth_id = select_category_id(oracle, cntc_lcls_nm, id)
    if not category_1depth_id:
        return category_dict
    category_2depth_id = select_category_id(oracle, cntc_md_clas_nm, category_1depth_id)
    if not category_2depth_id:
        return category_dict
    category_key = "QT0001!@#${0}!@#${1}".format(category_1depth_id, category_2depth_id)
    category_dict[category_key] = 1
    return category_dict


def select_category_parents_id(oracle, name):
    """
    Select category parents ID
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


def select_category_id(oracle, name, parent_id):
    """
    Select category ID
    :param          oracle:             Oracle
    :param          name:               Name
    :param          parent_id:          PARENT_ID
    :return:                            ID
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
        parent_id,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return False
    if not result:
        return False
    return result[0]


def select_dtc_cont(oracle, scrt_sntc_info_id):
    """
    Select SCRT_SNTC_INFO_ID from CM_SCRT_SNTC_DTC_INFO_TB
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


def select_scrt_sntc_cont(oracle, type_code='', fir_depth_id='', sec_depth_id='', thi_depth_id='', sntc_sort_no=''):
    """
    Select SCRT_SNTC_INFO_ID from CM_SCRT_SNTC_INFO_TB
    :param          oracle:                     DB
    :param          type_code:                  QUALITY_TYPE_CODE
    :param          fir_depth_id:               CATEGORY_1DEPTH_ID
    :param          sec_depth_id:               CATEGORY_2DEPTH_ID
    :param          thi_depth_id:               CATEGORY_3DEPTH_ID
    :param          sntc_sort_no:               SNTC_SORT_NO
    :return:                                    SCRT_SNTC_CONT
    """
    query = """
        SELECT
            SCRT_SNTC_CONT
        FROM
            CM_SCRT_SNTC_INFO_TB
        WHERE 1=1
    """
    bind = ()
    cnt = 1
    if len(type_code):
        query += "AND QUALITY_TYPE_CODE = :{0}\n".format(cnt)
        bind += (type_code,)
        cnt += 1
    if len(fir_depth_id):
        query += "AND CATEGORY_1DEPTH_ID = :{0}\n".format(cnt)
        bind += (fir_depth_id,)
        cnt += 1
    if len(sec_depth_id):
        query += "AND CATEGORY_2DEPTH_ID = :{0}\n".format(cnt)
        bind += (sec_depth_id,)
        cnt += 1
    if len(thi_depth_id):
        query += "AND CATEGORY_3DEPTH_ID = :{0}\n".format(cnt)
        bind += (thi_depth_id,)
        cnt += 1
    if len(sntc_sort_no):
        query += "AND SNTC_SORT_NO = :{0}\n".format(cnt)
        bind += (sntc_sort_no,)
        cnt += 1
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return list()
    if not result:
        return list()
    return result


def select_scrt_sntc_info_id(oracle, type_code='', fir_depth_id='', sec_depth_id='', thi_depth_id=''):
    """
    Select SCRT_SNTC_INFO_ID from CM_SCRT_SNTC_INFO_TB
    :param          oracle:                     DB
    :param          type_code:                  QUALITY_TYPE_CODE
    :param          fir_depth_id:               CATEGORY_1DEPTH_ID
    :param          sec_depth_id:               CATEGORY_2DEPTH_ID
    :param          thi_depth_id:               CATEGORY_3DEPTH_ID
    :return:                                    SCRT_SNTC_INFO_ID, SNTC_SORT_NO
    """
    query = """
        SELECT
            SCRT_SNTC_INFO_ID,
            SNTC_SORT_NO,
            CUST_ANS_YN
        FROM
            CM_SCRT_SNTC_INFO_TB
        WHERE 1=1
    """
    bind = ()
    cnt = 1
    if len(type_code):
        query += "AND QUALITY_TYPE_CODE = :{0}\n".format(cnt)
        bind += (type_code,)
        cnt += 1
    if len(fir_depth_id):
        query += "AND CATEGORY_1DEPTH_ID = :{0}\n".format(cnt)
        bind += (fir_depth_id,)
        cnt += 1
    if len(sec_depth_id):
        query += "AND CATEGORY_2DEPTH_ID = :{0}\n".format(cnt)
        bind += (sec_depth_id,)
        cnt += 1
    if len(thi_depth_id):
        query += "AND CATEGORY_3DEPTH_ID = :{0}\n".format(cnt)
        bind += (thi_depth_id,)
        cnt += 1
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return list()
    if not result:
        return list()
    return result


def insert_happy_call_hmd_non_result(oracle, happy_call_id, hdr, target_category_list, iitem_nm):
    """
    Insert happy call hmd result
    :param          oracle:                     DB
    :param          happy_call_id:              Happy call result
    :param          hdr:                        Header
    :param          target_category_list:       Target category list
    :param          iitem_nm:                   IITEM_NM
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    cntc_user_depart_c = '' if hdr.call_metadata.cntc_user_depart_c == 'None' else hdr.call_metadata.cntc_user_depart_c
    cntc_user_depart_nm = '' if hdr.call_metadata.cntc_user_depart_nm == 'None' else hdr.call_metadata.cntc_user_depart_nm
    cntc_user_part_c = '' if hdr.call_metadata.cntc_user_part_c == 'None' else hdr.call_metadata.cntc_user_part_c
    cntc_user_part_nm = '' if hdr.call_metadata.cntc_user_part_nm == 'None' else hdr.call_metadata.cntc_user_part_nm
    for item in target_category_list:
        query = """
            INSERT INTO CS_HAPPY_CALL_MT_DETAIL_TB
            (
                HAPPY_CALL_ID,
                PIPELINE_EVENT_ID,
                CALL_ID,
                CALL_DATE,
                PROJECT_CODE,
                CALL_TYPE_CODE,
                RUSER_ID,
                RUSER_NAME,
                CATEGORY_1DEPTH_ID,
                PRT_NO,
                SNTC_SORT_NO,
                SENTENCE,
                CUST_SENTENCE,
                START_TIME,
                IITEM_NM,
                SNTC_DTC_YN,
                CU_DTC_YN,
                LONG_CU_DTC_YN,
                CNTC_USER_DEPART_C,
                CNTC_USER_DEPART_NM,
                CNTC_USER_PART_C,
                CNTC_USER_PART_NM,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID
            )
            VALUES
            (
                :1, :2, :3, TO_DATE(:4, 'YYYY/MM/DD'),
                :5, :6, :7, :8, :9, :10, :11, :12, :13,
                TO_DATE(:14, 'YYYY/MM/DD HH24:MI:SS'), 
                :15, :16, :17, :18, :19, :20, :21, :22,
                SYSDATE, SYSDATE, :23, :24
            )
        """
        prt_no = item.split("_")[1]
        if prt_no == 'None' or prt_no == u'None':
            prt_no = 999
        bind = (
            happy_call_id,
            hdr.pipeline_event_id,
            hdr.call_id,
            call_date,
            hdr.call_metadata.project_code,
            hdr.call_metadata.call_type_code,
            ruser_id,
            ruser_name,
            item.split("_")[0],
            prt_no,
            item.split("_")[2],
            '',
            '',
            hdr.call_metadata.start_time,
            iitem_nm,
            "N",
            '',
            '',
            cntc_user_depart_c,
            cntc_user_depart_nm,
            cntc_user_part_c,
            cntc_user_part_nm,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def insert_happy_call_long_cu_dtc(**kwargs):
    """
    Insert happy call long cu dtc
    :param          kwargs:             Arguments
    """
    hdr = kwargs.get('hdr')
    oracle = kwargs.get('oracle')
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    poly_no = '' if hdr.call_metadata.poly_no == 'None' else hdr.call_metadata.poly_no
    cont_date = '' if kwargs.get('cont_dt') == 'None' else kwargs.get('cont_dt')
    cu_id = '' if hdr.call_metadata.cu_id == 'None' else hdr.call_metadata.cu_id
    cu_name = '' if hdr.call_metadata.cu_name == 'None' else hdr.call_metadata.cu_name
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    query = """
        INSERT INTO CS_HAPPY_CALL_LONG_CU_DTC_TB
        (
            PIPELINE_EVENT_ID,
            CALL_DATE,
            CALL_ID,
            PROJECT_CODE,
            CALL_TYPE_CODE,
            POLY_NO,
            RUSER_ID,
            RUSER_NAME,
            CU_ID,
            CU_NAME,
            CATEGORY_1DEPTH_ID,
            CONT_DATE,
            IITEM_NM,
            CNTC_MD_CLAS_C,
            CNTC_MD_CLAS_NM,
            FOF_NM,
            MJY_NM,
            CH_NM,
            HO_NM,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, TO_DATE(:2, 'YYYY/MM/DD'),
            :3, :4, :5, :6, :7, :8, :9, :10, :11,
            TO_DATE(:12, 'YYYYMMDD'), 
            :13, :14, :15, :16, :17, :18, :19,
            SYSDATE, SYSDATE, :20, :21
        )
    """
    bind = (
        hdr.pipeline_event_id,
        call_date,
        hdr.call_id,
        hdr.call_metadata.project_code,
        hdr.call_metadata.call_type_code,
        poly_no,
        ruser_id,
        ruser_name,
        cu_id,
        cu_name,
        kwargs.get('fir_depth_id'),
        cont_date,
        kwargs.get('iitem_nm'),
        kwargs.get('cntc_md_clas_c'),
        kwargs.get('cntc_md_clas_nm'),
        kwargs.get('fof_nm'),
        kwargs.get('mjy_nm'),
        kwargs.get('ch_nm'),
        kwargs.get('ho_nm'),
        hdr.creator_id,
        hdr.creator_id,
    )
    oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def select_cust_sentences(oracle, call_id, end_time):
    """
    Select customer sentences
    :param          oracle:             DB
    :param          call_id:            CALL_ID
    :param          end_time:           END_TIME
    :return:                            Result rows
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
        end_time + 500,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return False
    if not result:
        return False
    return result


def insert_happy_call_hmd_result(**kwargs):
    """
    Insert happy call hmd result
    :param          kwargs:             Arguments
    :return:                            Target category list
    """
    hdr = kwargs.get('hdr')
    oracle = kwargs.get('oracle')
    document = kwargs.get('document')
    happy_call_result = kwargs.get('happy_call_result')
    target_category_list = kwargs.get('target_category_list')
    dedup_dict = kwargs.get('dedup_dict')
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    cntc_user_depart_c = '' if hdr.call_metadata.cntc_user_depart_c == 'None' else hdr.call_metadata.cntc_user_depart_c
    cntc_user_depart_nm = '' if hdr.call_metadata.cntc_user_depart_nm == 'None' else hdr.call_metadata.cntc_user_depart_nm
    cntc_user_part_c = '' if hdr.call_metadata.cntc_user_part_c == 'None' else hdr.call_metadata.cntc_user_part_c
    cntc_user_part_nm = '' if hdr.call_metadata.cntc_user_part_nm == 'None' else hdr.call_metadata.cntc_user_part_nm
    for cls in happy_call_result.keys():
        category_1depth_id = cls.split("_")[0]
        sntc_sort_no = cls.split("_")[2]
        cust_ans_yn = cls.split("_")[3]
        scrt_sntc_cont = select_scrt_sntc_cont(
            oracle, type_code='QT0002', fir_depth_id=category_1depth_id, sntc_sort_no=sntc_sort_no)
        scrt_sntc_cont = scrt_sntc_cont[0]
        if cust_ans_yn.upper() == 'Y':
            cust_sentence = ''
            cust_result = select_cust_sentences(oracle, hdr.call_id, document.end_time)
            if cust_result:
                answer_status = 'Y'
                for item in cust_result:
                    sentence = item[0]
                    cust_sentence += " " + sentence
                cust_sentence = " " + cust_sentence.strip()
            else:
                cust_sentence = ''
                answer_status = 'N'
            long_cu_dtc_yn = 'Y' if len(cust_sentence.decode('utf-8').replace(" ", "")) > 14 else 'N'
        else:
            cust_sentence = ''
            answer_status = ''
            long_cu_dtc_yn = ''
        query = """
            INSERT INTO CS_HAPPY_CALL_MT_DETAIL_TB
            (
                HAPPY_CALL_ID,
                PIPELINE_EVENT_ID,
                CALL_ID,
                CALL_DATE,
                PROJECT_CODE,
                CALL_TYPE_CODE,
                RUSER_ID,
                RUSER_NAME,
                CATEGORY_1DEPTH_ID,
                PRT_NO,
                SNTC_SORT_NO,
                SENTENCE,
                CUST_SENTENCE,
                START_TIME,
                IITEM_NM,
                SNTC_DTC_YN,
                CU_DTC_YN,
                LONG_CU_DTC_YN,
                CNTC_USER_DEPART_C,
                CNTC_USER_DEPART_NM,
                CNTC_USER_PART_C,
                CNTC_USER_PART_NM,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID,
                SCRT_SNTC_CONT
            )
            VALUES
            (
                :1, :2, :3, TO_DATE(:4, 'YYYY/MM/DD'),
                :5, :6, :7, :8, :9, :10, :11, :12, :13,
                TO_DATE(:14, 'YYYY/MM/DD HH24:MI:SS'), 
                :15, :16, :17, :18, :19, :20, :21, :22,
                SYSDATE, SYSDATE, :23, :24, :25
            )
        """
        prt_no = cls.split("_")[1]
        if prt_no == 'None' or prt_no == u'None':
            prt_no = 999
        bind = (
            kwargs.get('happy_call_id'),
            hdr.pipeline_event_id,
            hdr.call_id,
            call_date,
            hdr.call_metadata.project_code,
            hdr.call_metadata.call_type_code,
            ruser_id,
            ruser_name,
            cls.split("_")[0],
            prt_no,
            cls.split("_")[2],
            document.sentence,
            cust_sentence,
            hdr.call_metadata.start_time,
            kwargs.get('iitem_nm'),
            "Y",
            answer_status,
            long_cu_dtc_yn,
            cntc_user_depart_c,
            cntc_user_depart_nm,
            cntc_user_part_c,
            cntc_user_part_nm,
            hdr.creator_id,
            hdr.creator_id,
            scrt_sntc_cont,
        )
        oracle.cursor.execute(query, bind)
        if cls in target_category_list:
            del target_category_list[target_category_list.index(cls)]
        if long_cu_dtc_yn == 'Y' and cls.split("_")[0] not in dedup_dict:
            dedup_dict[cls.split("_")[0]] = 1
            # 해피콜 고객질의 비단답형
            insert_happy_call_long_cu_dtc(
                oracle=oracle,
                hdr=hdr,
                fir_depth_id=cls.split("_")[0],
                iitem_nm=kwargs.get('iitem_nm'),
                cntc_md_clas_c=kwargs.get('cntc_md_clas_c'),
                cntc_md_clas_nm=kwargs.get('cntc_md_clas_nm'),
                fof_nm=kwargs.get('fof_nm'),
                mjy_nm=kwargs.get('mjy_nm'),
                ch_nm=kwargs.get('ch_nm'),
                ho_nm=kwargs.get('ho_nm'),
                cont_dt=kwargs.get('cont_dt')
            )
    oracle.conn.commit()
    return target_category_list, dedup_dict


def select_iitem_nm(oracle, org_c, cl_cid, list_id):
    """
    Select IITEM_NM
    :param          oracle:             DB
    :param          org_c:              ORG_C
    :param          cl_cid:             CL_CID
    :param          list_id:            LIST_ID
    :return:                            IITEM_NM
    """
    dev = ''
    # dev = '@D_KBLUAT_ZREAD'
    query = """
        SELECT
            MNG_ITEM_2
        FROM
            TCL_LIST_DTL
        WHERE 1=1
            AND ORG_C = :1
            AND LIST_ID = :2
            AND CL_CID = :3
    """
    bind = (
        org_c,
        list_id,
        cl_cid
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return ''
    if not result:
        return ''
    return result[0]


def execute_happy_call_monitoring_hmd(**kwargs):
    """
    Execute happy call monitoring hmd
    :param          kwargs:         Arguments
    :return                         sentence count
    """
    hdr = kwargs.get('hdr')
    oracle = kwargs.get('oracle')
    target_result = kwargs.get('target_result')
    hmd_client_obj = kwargs.get('hmd_client_obj')
    nlp_result_detail = kwargs.get('nlp_result_detail')
    overlap_check_dict = dict()
    category_dtc_list = list()
    target_category_list = list()
    model_name = "{0}{1}".format(hdr.call_id, hdr.call_metadata.speaker_code)
    for scri_id, prt_no in target_result:
        category_1depth_id = select_category_id(oracle, scri_id, kwargs.get('id'))
        if not category_1depth_id:
            continue
        key = "{0}!@#${1}".format(category_1depth_id, prt_no)
        overlap_check_dict[key] = 1
    for key in overlap_check_dict.keys():
        category_1depth_id, prt_no = key.split("!@#$")
        temp_result = select_scrt_sntc_info_id(oracle, 'QT0002', category_1depth_id)
        for scrt_sntc_info_id, sntc_sort_no, cust_ans_yn in temp_result:
            dtc_cont_result = select_dtc_cont(oracle, scrt_sntc_info_id)
            if not dtc_cont_result:
                continue
            category = [category_1depth_id, prt_no, sntc_sort_no, cust_ans_yn]
            category_list = '_'.join(category)
            target_category_list.append(category_list)
            for dtc_cont in dtc_cont_result:
                dtc_cont = dtc_cont[0]
                category_dtc_list.append(([category_list], dtc_cont))
    hmd_client_obj.make_hmd_model(category_dtc_list, model_name)
    iitem_nm = ''
    # iitem_nm = select_iitem_nm(oracle, hdr.call_metadata.org_c, hdr.call_metadata.cu_id, hdr.call_metadata.list_id)
    sentence_count = 0
    dedup_dict = dict()
    for document in nlp_result_detail.documentList:
        happy_call_result = execute_hmd(document, model_name)
        if len(happy_call_result) > 0:
            sentence_count += len(happy_call_result)
            target_category_list, dedup_dict = insert_happy_call_hmd_result(
                oracle=oracle,
                happy_call_id=kwargs.get('happy_call_id'),
                happy_call_result=happy_call_result,
                document=document,
                hdr=hdr,
                target_category_list=target_category_list,
                iitem_nm=iitem_nm,
                cntc_md_clas_c=kwargs.get('cntc_md_clas_c'),
                cntc_md_clas_nm=kwargs.get('cntc_md_clas_nm'),
                fof_nm=kwargs.get('fof_nm'),
                mjy_nm=kwargs.get('mjy_nm'),
                ch_nm=kwargs.get('ch_nm'),
                ho_nm=kwargs.get('ho_nm'),
                cont_db=kwargs.get('cont_dt'),
                dedup_dict=dedup_dict
            )
    model_path = os.path.join(os.getenv('MAUM_ROOT'), "trained/hmd/{0}__0.hmdmodel".format(model_name))
    try:
        os.remove(model_path)
    except Exception:
        pass
    if len(target_category_list) > 0:
        insert_happy_call_hmd_non_result(oracle, kwargs.get('happy_call_id'), hdr, target_category_list, iitem_nm)
    return sentence_count


def insert_happy_call_target(**kwargs):
    """
    Insert happy call monitoring hmd
    :param          kwargs:         Arguments
    :return:                        HAPPY_CALL_ID
    """
    oracle = kwargs.get('oracle')
    hdr = kwargs.get('hdr')
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    poly_no = '' if hdr.call_metadata.poly_no == 'None' else hdr.call_metadata.poly_no
    cont_date = '' if kwargs.get('cont_dt') == 'None' else kwargs.get('cont_dt')
    cu_id = '' if hdr.call_metadata.cu_id == 'None' else hdr.call_metadata.cu_id
    cu_name = '' if hdr.call_metadata.cu_name == 'None' else hdr.call_metadata.cu_name
    my_seq = oracle.cursor.var(cx_Oracle.NUMBER)
    query = """
        INSERT INTO CS_HAPPY_CALL_MT_TB
        (
            PIPELINE_EVENT_ID,
            CALL_DATE,
            CALL_ID,
            PROJECT_CODE,
            CALL_TYPE_CODE,
            POLY_NO,
            CONT_DATE,
            CU_ID,
            CU_NAME,
            CNTC_MD_CLAS_C,
            CNTC_MD_CLAS_NM,
            FOF_NM,
            MJY_NM,
            CH_NM,
            HO_NM,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, TO_DATE(:2, 'YYYY/MM/DD'), :3, :4, :5,
            :6, TO_DATE(:7, 'YYYYMMDD'), :8, :9, :10,
            :11, :12, :13, :14, :15, SYSDATE, SYSDATE, :16, :17
        )
        RETURNING HAPPY_CALL_ID INTO :18
    """
    bind = (
        hdr.pipeline_event_id,
        call_date,
        hdr.call_id,
        hdr.call_metadata.project_code,
        hdr.call_metadata.call_type_code,
        poly_no,
        cont_date,
        cu_id,
        cu_name,
        kwargs.get('cntc_md_clas_c'),
        kwargs.get('cntc_md_clas_nm'),
        kwargs.get('fof_nm'),
        kwargs.get('mjy_nm'),
        kwargs.get('ch_nm'),
        kwargs.get('ho_nm'),
        hdr.creator_id,
        hdr.creator_id,
        my_seq,
    )
    oracle.cursor.execute(query, bind)
    happy_call_id = my_seq.getvalue()
    oracle.conn.commit()
    return int(happy_call_id)


def update_happy_call_target_date(oracle, hdr, happy_call_id):
    """
    Update happy call target call date
    :param          oracle:             DB
    :param          hdr:                Header
    :param          happy_call_id:      HAPPY_CALL_ID
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    query = """
        UPDATE
            CS_HAPPY_CALL_MT_TB
        SET
            CALL_DATE = TO_DATE(:1, 'YYYY/MM/DD')
        WHERE 1=1
            AND HAPPY_CALL_ID = :2
    """
    bind = (status, call_date, happy_call_id)
    oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def check_happy_call_target(oracle, hdr, fof_nm):
    """
    Check happy call data
    :param          oracle:             Oracle
    :param          hdr:                Header
    :param          fof_nm:             FOF_NM
    :return:                            Happy call target hmd list
    """
    poly_no = '' if hdr.call_metadata.poly_no == 'None' else hdr.call_metadata.poly_no
    cont_date = '' if hdr.call_metadata.cont_date == 'None' else hdr.call_metadata.cont_date
    query = """
        SELECT
            HAPPY_CALL_ID,
            CALL_DATE
        FROM
            CS_HAPPY_CALL_MT_TB
        WHERE 1=1
            AND POLY_NO = :1
            AND CONT_DATE = TO_DATE(:2, 'YYYYMMDD')
            AND FOF_NM = :3
    """
    bind = (
        poly_no,
        cont_date,
        fof_nm,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return False, False
    if not result:
        return False, False
    return result


def select_happy_call_hmd_target(oracle, call_id):
    """
    Select happy call hmd target list
    :param          oracle:             Oracle
    :param          call_id:            Call ID
    :return:                            Happy call target hmd list
    """
    dev = ''
    # dev = '@D_KBLUAT_ZREAD'
    query = """
        SELECT
            A.SCRI_ID,
            B.PRT_NO
        FROM
            ZCS.TCL_HAPY_SCRI_RSLT{0} A,
            ZCS.TCL_HAPY_SCRI{0} B
        WHERE 1=1
            AND A.SCRI_ID = B.SCRI_ID
            AND (A.POLY_NO, A.LIST_ID, A.MKTN_ID, A.SEQ, A.CL_CID)
                IN (
                    SELECT
                        C.POLY_NO,
                        B.LIST_ID,
                        B.MKTN_ID,
                        C.SEQ,
                        B.CL_CID
                    FROM
                        ZCS.TCL_CS_BASE{0} A,
                        ZCS.TCL_MKTN_RSLT{0} B,
                        ZCS.TCL_LIST_DTL{0} C,
                        ZCS.TCL_HAPY_RSLT_MST{0} D
                    WHERE 1=1
                        AND B.ORG_C = '000001'
                        AND (B.MKTN_ID, B.LIST_ID, B.CL_CID, C.POLY_NO)
                            IN (
                                SELECT
                                    MKTN_ID,
                                    LIST_ID,
                                    CU_ID,
                                    POLY_NO
                                FROM
                                    CM_CALL_META_TB
                                WHERE 1=1          
                                    AND CALL_ID = :1
                                )
                        AND A.ORG_C = B.ORG_C
                        AND A.CL_CID = B.CL_CID
                        AND B.ORG_C = C.ORG_C
                        AND B.CL_CID = C.CL_CID
                        AND B.LIST_ID = C.LIST_ID
                        AND C.CL_CID = D.CL_CID
                        AND C.LIST_ID = D.LIST_ID
                        AND C.SEQ = D.SEQ
                    )
            AND A.HAPY_CNT = (
                                SELECT
                                    MAX(HAPY_CNT)
                                FROM
                                    ZCS.TCL_HAPY_SCRI_RSLT{0}
                                WHERE 1=1
                                    AND POLY_NO = A.POLY_NO
                                    AND MKTN_ID = A.MKTN_ID
                                )
        ORDER BY
            B.PRT_NO
    """.format(dev)
    bind = (
        call_id,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchall()
    if result is bool:
        return False
    if not result:
        return False
    return result


def set_up_happy_call_hmd(**kwargs):
    """
    Set up happy call hmd
    :param      kwargs:         Arguments
    :return                     Sentence count
    """
    oracle = kwargs.get('oracle')
    hdr = kwargs.get('hdr')
    hmd_client_obj = kwargs.get('hmd_client_obj')
    fof_nm = kwargs.get('fof_nm')
    mjy_nm = kwargs.get('mjy_nm')
    ch_nm = kwargs.get('ch_nm')
    ho_nm = kwargs.get('ho_nm')
    cont_dt = kwargs.get('cont_dt')
    cntc_md_clas_c = kwargs.get('cntc_md_clas_c')
    cntc_md_clas_nm = kwargs.get('cntc_md_clas_nm')
    nlp_result_detail = kwargs.get('nlp_result_detail')
    # CM_CATEGORY_TB 에서 happy_cal ID 검색
    id = select_category_parents_id(oracle, 'happy_call')
    # 현업 DB 에서 RECODE_KEY 기준 대상 HMD 목록 검색
    target_result = select_happy_call_hmd_target(oracle, hdr.call_id)
    sentence_cnt = 0
    if id and target_result:
        happy_call_id, last_call_date = check_happy_call_target(oracle, hdr, fof_nm)
        if not happy_call_id:
            happy_call_id = insert_happy_call_target(
                oracle=oracle,
                hdr=hdr,
                cntc_md_clas_c=cntc_md_clas_c,
                cntc_md_clas_nm=cntc_md_clas_nm,
                fof_nm=fof_nm,
                mjy_nm=mjy_nm,
                ch_nm=ch_nm,
                ho_nm=ho_nm,
                cont_dt=cont_dt
            )
        else:
            if datetime.strptime(hdr.call_metadata.call_date, '%Y-%m-%d') > last_call_date:
                update_happy_call_target_date(oracle, hdr, happy_call_id)
        sentence_cnt = execute_happy_call_monitoring_hmd(
            oracle=oracle,
            hmd_client_obj=hmd_client_obj,
            hdr=hdr,
            happy_call_id=happy_call_id,
            id=id,
            nlp_result_detail=nlp_result_detail,
            target_result=target_result,
            cntc_md_clas_c=cntc_md_clas_c,
            cntc_md_clas_nm=cntc_md_clas_nm,
            fof_nm=fof_nm,
            mjy_nm=mjy_nm,
            ch_nm=ch_nm,
            ho_nm=ho_nm,
            cont_dt=cont_dt
        )
    return sentence_cnt


def select_happy_call_info(oracle, poly_no):
    """
    Select FOF_NM, MJY_NM, CH_NM
    :param          oracle:             DB
    :param          poly_no:            POLY_NO
    :return:                            Result rows
    """
    dev = ''
    # dev = '@D_KBLUAT_ZREAD'
    query = """
        SELECT
            A.FOF_NM,
            A.MJY_NM,
            A.CH_NM,
            A.HO_NM,
            A.CONT_DT
        FROM
            ZCS.TCL_HAPY_RSLT_MST{0} A,
            ZCS.TCL_HAPY_CNT_INFO{0} B,
            ZCS.TCL_MKTN_RSLT{0} C
        WHERE 1=1
            AND A.POLY_NO = :1
            AND A.MKTN_ID = B.MKTN_ID(+)
            AND A.POLY_NO = B.POLY_NO(+)
            AND A.HAPY_CNT = B.HAPY_CNT(+)
            AND A.MKTN_ID = C.MKTN_ID(+)
            AND A.LIST_ID = C.LIST_ID(+)
            AND A.CL_CID = C.CL_CID(+)
            AND A.LAST_CNTC_ID IS NOT NULL
            AND C.ASIN_CNSL_USID IS NOT NULL
    """.format(dev)
    bind = (
        poly_no,
    )
    oracle.cursor.execute(query, bind)
    result = oracle.cursor.fetchone()
    if result is bool:
        return False
    if not result:
        return False
    return result


def insert_call_driver_classify_hmd_result(oracle, hdr, category_dict, cntc_info):
    """
    Inert call driver classify HMD result to CS_CALL_DRIVER_CLASSIFY_TB
    :param          oracle:                 DB
    :param          hdr:                    Header
    :param          category_dict:          Category dictionary
    :param          cntc_info:              CNTC result
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    cu_id = '' if hdr.call_metadata.cu_id == 'None' else hdr.call_metadata.cu_id
    cu_name = '' if hdr.call_metadata.cu_name == 'None' else hdr.call_metadata.cu_name
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    dedup_dict = dict()
    for cls in category_dict.keys():
        cat_list = cls.split('|')
        if len(cat_list) != 3 or cls in dedup_dict:
            continue
        dedup_dict[cls] = 1
        category_1depth_id = cat_list[0]
        category_2depth_id = cat_list[1]
        category_3depth_id = cat_list[2]
        for item in cntc_info:
            cntc_lcls_c = item[0]
            cntc_lcls_nm = item[1]
            if cntc_lcls_c.startswith('O'):
                continue
            cntc_md_clas_c = item[2]
            cntc_md_clas_nm = item[3]
            query = """
            INSERT INTO CS_CALL_DRIVER_CLASSIFY_TB
            (
                PIPELINE_EVENT_ID,
                PROJECT_CODE,
                CALL_DATE,
                CALL_ID,
                START_TIME,
                DURATION,
                CALL_TYPE_CODE,
                RUSER_ID,
                RUSER_NAME,
                CU_ID,
                CU_NAME,
                CATEGORY_1DEPTH_ID,
                CATEGORY_2DEPTH_ID,
                CATEGORY_3DEPTH_ID,
                CNTC_LCLS_C,
                CNTC_LCLS_NM,
                CNTC_MD_CLAS_C,
                CNTC_MD_CLAS_NM,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID
            )
            VALUES
            (
                :1, :2, TO_DATE(:3, 'YYYY/MM/DD'), :4,
                TO_DATE(:5, 'YYYY/MM/DD HH24:MI:SS'), :6,
                :7, :8, :9, :10, :11, :12, :13, :14, :15,
                :16, :17, :18, SYSDATE, SYSDATE, :19, :20
            )
            """
            bind = (
                hdr.pipeline_event_id,
                hdr.call_metadata.project_code,
                call_date,
                hdr.call_id,
                hdr.call_metadata.start_time,
                hdr.call_metadata.duration,
                hdr.call_metadata.call_type_code,
                ruser_id,
                ruser_name,
                cu_id,
                cu_name,
                category_1depth_id,
                category_2depth_id,
                category_3depth_id,
                cntc_lcls_c,
                cntc_lcls_nm,
                cntc_md_clas_c,
                cntc_md_clas_nm,
                hdr.creator_id,
                hdr.creator_id,
            )
            oracle.cursor.execute(query, bind)
        oracle.conn.commit()


def insert_agent_quality_non_result(oracle, quality_type_code, hdr, non_dtc_category_list):
    """
    Inert script quality HMD none result to CS_AGENT_QUALITY_RESULT_TB
    :param          oracle:                         DB
    :param          quality_type_code:              Quality type code
    :param          hdr:                            Header
    :param          non_dtc_category_list:          Target category list
    """
    overlap_check_dict = dict()
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    poly_no = '' if hdr.call_metadata.poly_no == 'None' else hdr.call_metadata.poly_no
    cu_id = '' if hdr.call_metadata.cu_id == 'None' else hdr.call_metadata.cu_id
    cu_name = '' if hdr.call_metadata.cu_name == 'None' else hdr.call_metadata.cu_name
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    cntc_user_depart_c = '' if hdr.call_metadata.cntc_user_depart_c == 'None' else hdr.call_metadata.cntc_user_depart_c
    cntc_user_depart_nm = '' if hdr.call_metadata.cntc_user_depart_nm == 'None' else hdr.call_metadata.cntc_user_depart_nm
    cntc_user_part_c = '' if hdr.call_metadata.cntc_user_part_c == 'None' else hdr.call_metadata.cntc_user_part_c
    cntc_user_part_nm = '' if hdr.call_metadata.cntc_user_part_nm == 'None' else hdr.call_metadata.cntc_user_part_nm
    for item in non_dtc_category_list:
        if item in overlap_check_dict:
            continue
        overlap_check_dict[item] = 1
        query = """
        INSERT INTO CS_AGENT_QUALITY_RESULT_TB
        (
            RECORD_KEY,
            QUALITY_TYPE_CODE,
            CALL_ID,
            CALL_DATE,
            PROJECT_CODE,
            CALL_TYPE_CODE,
            CATEGORY_1DEPTH_ID,
            CATEGORY_2DEPTH_ID,
            SNTC_SORT_NO,
            SNTC_DTC_YN,
            SENTENCE,
            POLY_NO,
            START_TIME,
            RUSER_ID,
            RUSER_NAME,
            CU_ID,
            CU_NAME,
            CNTC_USER_DEPART_C,
            CNTC_USER_DEPART_NM,
            CNTC_USER_PART_C,
            CNTC_USER_PART_NM,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3, TO_DATE(:4, 'YYYY/MM/DD'),
            :5, :6, :7, :8, :9, :10, :11, :12,
            TO_DATE(:13, 'YYYY/MM/DD HH24:MI:SS'),
            :14, :15, :16, :17, :18, :19, :20, :21,
            SYSDATE, SYSDATE, :22, :23
        )
        """
        bind = (
            hdr.call_metadata.record_key,
            quality_type_code,
            hdr.call_id,
            call_date,
            hdr.call_metadata.project_code,
            hdr.call_metadata.call_type_code,
            item.split("_")[0],
            item.split("_")[1],
            item.split("_")[2],
            "N",
            '',
            poly_no,
            hdr.call_metadata.start_time,
            ruser_id,
            ruser_name,
            cu_id,
            cu_name,
            cntc_user_depart_c,
            cntc_user_depart_nm,
            cntc_user_part_c,
            cntc_user_part_nm,
            hdr.creator_id,
            hdr.creator_id,
        )
        oracle.cursor.execute(query, bind)
    oracle.conn.commit()


def insert_agent_quality_result(oracle, hmd_result, quality_type_code, document, hdr, end_category, end_yn, start_category, start_yn):
    """
    Inert script quality HMD result to CS_AGENT_QUALITY_RESULT_TB
    :param          oracle:                     DB
    :param          hmd_result:                 HMD result
    :param          quality_type_code:          Quality type code
    :param          document:                   NLP Document
    :param          hdr:                        Header
    :param          end_category:               End Category
    :param          end_yn:                     End YN
    :param          start_category:             Start Category
    :param          start_yn:                   Start YN
    :return                                     Target category list
    """
    dtc_category_list = list()
    overlap_check_dict = dict()
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    poly_no = '' if hdr.call_metadata.poly_no == 'None' else hdr.call_metadata.poly_no
    cu_id = '' if hdr.call_metadata.cu_id == 'None' else hdr.call_metadata.cu_id
    cu_name = '' if hdr.call_metadata.cu_name == 'None' else hdr.call_metadata.cu_name
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    cntc_user_depart_c = '' if hdr.call_metadata.cntc_user_depart_c == 'None' else hdr.call_metadata.cntc_user_depart_c
    cntc_user_depart_nm = '' if hdr.call_metadata.cntc_user_depart_nm == 'None' else hdr.call_metadata.cntc_user_depart_nm
    cntc_user_part_c = '' if hdr.call_metadata.cntc_user_part_c == 'None' else hdr.call_metadata.cntc_user_part_c
    cntc_user_part_nm = '' if hdr.call_metadata.cntc_user_part_nm == 'None' else hdr.call_metadata.cntc_user_part_nm
    for cls in hmd_result.keys():
        category_1depth_id = cls.split("_")[0]
        category_2depth_id = cls.split("_")[1]
        sntc_sort_no = cls.split("_")[2]
        scrt_sntc_cont = select_scrt_sntc_cont(
            oracle, type_code='QT0001', fir_depth_id=category_1depth_id, sec_depth_id=category_2depth_id,
            sntc_sort_no=sntc_sort_no)
        scrt_sntc_cont = scrt_sntc_cont[0]
        if end_yn == 'N' and cls == end_category:
            continue
        if start_yn == 'N' and cls == start_category:
            continue
        if cls in overlap_check_dict:
            continue
        overlap_check_dict[cls] = 1
        query = """
            INSERT INTO CS_AGENT_QUALITY_RESULT_TB
            (
                RECORD_KEY,
                QUALITY_TYPE_CODE,
                CALL_ID,
                CALL_DATE,
                PROJECT_CODE,
                CALL_TYPE_CODE,
                CATEGORY_1DEPTH_ID,
                CATEGORY_2DEPTH_ID,
                SNTC_SORT_NO,
                SNTC_DTC_YN,
                SENTENCE,
                POLY_NO,
                START_TIME,
                RUSER_ID,
                RUSER_NAME,
                CU_ID,
                CU_NAME,
                CNTC_USER_DEPART_C,
                CNTC_USER_DEPART_NM,
                CNTC_USER_PART_C,
                CNTC_USER_PART_NM,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID,
                SCRT_SNTC_CONT
            )
            VALUES
            (
                :1, :2, :3, TO_DATE(:4, 'YYYY/MM/DD'),
                :5, :6, :7, :8, :9, :10, :11, :12,
                TO_DATE(:13, 'YYYY/MM/DD HH24:MI:SS'),
                :14, :15, :16, :17, :18, :19, :20, :21,
                SYSDATE, SYSDATE, :22, :23, :24
            )
        """
        bind = (
            hdr.call_metadata.record_key,
            quality_type_code,
            hdr.call_id,
            call_date,
            hdr.call_metadata.project_code,
            hdr.call_metadata.call_type_code,
            cls.split("_")[0],
            cls.split("_")[1],
            cls.split("_")[2],
            "Y",
            document.sentence,
            poly_no,
            hdr.call_metadata.start_time,
            ruser_id,
            ruser_name,
            cu_id,
            cu_name,
            cntc_user_depart_c,
            cntc_user_depart_nm,
            cntc_user_part_c,
            cntc_user_part_nm,
            hdr.creator_id,
            hdr.creator_id,
            scrt_sntc_cont,
        )
        oracle.cursor.execute(query, bind)
        dtc_category_list.append(cls)
    oracle.conn.commit()
    return dtc_category_list


def execute_agent_script_quality(oracle, category_list, nlp_result_detail, hdr, hmd_client_obj, sentence_cnt):
    """
    Execute agent script quality
    :param          oracle:                     DB
    :param          category_list:              Category list
    :param          nlp_result_detail:          NLP result
    :param          hdr:                        Header
    :param          hmd_client_obj:             HMD cline object
    :param          sentence_cnt:               Sentence count
    """
    # 첫인사, 끝인사 카테고리 조회
    start_category = ''
    end_category = ''
    start_category_dict = select_category_dict(oracle, '공통', '첫인사', 'agent_quality')
    end_category_dict = select_category_dict(oracle, '공통', '끝인사', 'agent_quality')
    if len(start_category_dict) > 0:
        for key in start_category_dict.keys():
            key_list = key.split("!@#$")
            key_no = select_scrt_sntc_info_id(oracle, key_list[0], key_list[1], key_list[2])
            if key_no:
                key_no = str(key_no[0][1])
                start_category = '{0}_{1}_{2}'.format(key_list[1], key_list[2], key_no)
    if len(end_category_dict) > 0:
        for key in end_category_dict.keys():
            key_list = key.split("!@#$")
            key_no = select_scrt_sntc_info_id(oracle, key_list[0], key_list[1], key_list[2])
            if key_no:
                key_no = str(key_no[0][1])
                end_category = '{0}_{1}_{2}'.format(key_list[1], key_list[2], key_no)
    # hmd 실행
    for category_key in category_list:
        category_list = category_key.split("!@#$")
        quality_type_code = category_list[0]
        category_1depth_id = category_list[1]
        category_2depth_id = category_list[2]
        result = select_scrt_sntc_info_id(oracle, quality_type_code, category_1depth_id, category_2depth_id)
        # model 생성
        model_name = "{0}{1}{2}".format(quality_type_code, hdr.call_id, hdr.call_metadata.speaker_code)
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
                    category = [category_1depth_id, category_2depth_id, sntc_sort_no]
                    category_list = '_'.join(category)
                    category_dtc_list.append(([category_list], dtc_cont))
                    if category_list not in target_category_dict:
                        target_category_dict[category_list] = 1
            hmd_client_obj.make_hmd_model(category_dtc_list, model_name)
        else:
            continue
        total_dtc_category_dict = dict()
        max_line_number = len(nlp_result_detail.documentList)
        line_number = 0
        for document in nlp_result_detail.documentList:
            line_number += 1
            start_yn = 'Y'
            if line_number > 15 and start_category in target_category_dict:
                start_yn = 'N'
            end_yn = 'Y'
            if max_line_number - 15 > line_number and end_category in target_category_dict:
                # del target_category_dict[end_category]
                end_yn = 'N'
            script_quality_result = execute_hmd(document, model_name)
            if len(script_quality_result) > 0:
                sentence_cnt += len(script_quality_result)
                dtc_category_list = insert_agent_quality_result(
                    oracle, script_quality_result, 'QT0001', document, hdr, end_category, end_yn, start_category, start_yn)
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
            insert_agent_quality_non_result(oracle, 'QT0001', hdr, non_dtc_category_list)
    return sentence_cnt


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


def extract_call_driver_classify_hmd_result(oracle, hmd_result, name, category_dict):
    """
    Insert call driver classify HMD result to CS_CALL_DRIVER_CLASSIFY_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          name:                   CM_CATEGORY_TB NAME
    :param          category_dict:          Category dictionary
    :return                                 Category dictionary
    """
    for cls in hmd_result.keys():
        cat_list = cls.split('|')
        if len(cat_list) != 3:
            continue
        parent_id = select_call_driver_classify_category_id(oracle, name)
        if not parent_id:
            continue
        category_1depth, category_2depth, category_3depth = cat_list
        category_1depth_id = select_category_id(oracle, category_1depth, parent_id)
        if not category_1depth_id:
            continue
        category_2depth_id = select_category_id(oracle, category_2depth, category_1depth_id)
        if not category_2depth_id:
            continue
        category_3depth_id = select_category_id(oracle, category_3depth, category_2depth_id)
        if not category_3depth_id:
            continue
        category_key = "{0}|{1}|{2}".format(category_1depth_id, category_2depth_id, category_3depth_id)
        if category_key not in category_dict:
            category_dict[category_key] = 1
    return category_dict


def insert_interest_hmd_result(oracle, hmd_result, sentence, hdr, cntc_info):
    """
    Inert interest HMD result to CS_INTEREST_KEYWORD_TB
    :param          oracle:                 DB
    :param          hmd_result:             HMD result
    :param          sentence:               Sentence
    :param          hdr:                    Header
    :param          cntc_info:              CNTC result
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    poly_no = '' if hdr.call_metadata.poly_no == 'None' else hdr.call_metadata.poly_no
    cu_id = '' if hdr.call_metadata.cu_id == 'None' else hdr.call_metadata.cu_id
    cu_name = '' if hdr.call_metadata.cu_name == 'None' else hdr.call_metadata.cu_name
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    cntc_user_depart_c = '' if hdr.call_metadata.cntc_user_depart_c == 'None' else hdr.call_metadata.cntc_user_depart_c
    cntc_user_depart_nm = '' if hdr.call_metadata.cntc_user_depart_nm == 'None' else hdr.call_metadata.cntc_user_depart_nm
    cntc_user_part_c = '' if hdr.call_metadata.cntc_user_part_c == 'None' else hdr.call_metadata.cntc_user_part_c
    cntc_user_part_nm = '' if hdr.call_metadata.cntc_user_part_nm == 'None' else hdr.call_metadata.cntc_user_part_nm
    for cls in hmd_result.keys():
        cat_list = cls.split('|')
        if len(cat_list) != 2:
            continue
        category, keyword_name = cat_list
        for item in cntc_info:
            cntc_lcls_c = item[0]
            cntc_lcls_nm = item[1]
            cntc_md_clas_c = item[2]
            cntc_md_clas_nm = item[3]
            query = """
                INSERT INTO CS_INTEREST_KEYWORD_TB
                (
                    PIPELINE_EVENT_ID,
                    CALL_DATE,
                    CALL_ID,
                    PROJECT_CODE,
                    START_TIME,
                    CALL_TYPE_CODE,
                    POLY_NO,
                    RUSER_ID,
                    RUSER_NAME,
                    CU_ID,
                    CU_NAME,
                    CNTC_LCLS_C,
                    CNTC_LCLS_NM,
                    CNTC_MD_CLAS_C,
                    CNTC_MD_CLAS_NM,
                    SENTENCE,
                    CATEGORY,
                    KEYWORD_NAME,
                    CNTC_USER_DEPART_C,
                    CNTC_USER_DEPART_NM,
                    CNTC_USER_PART_C,
                    CNTC_USER_PART_NM,
                    CREATED_DTM,
                    UPDATED_DTM,
                    CREATOR_ID,
                    UPDATOR_ID
                )
                VALUES
                (
                    :1, TO_DATE(:2, 'YYYY/MM/DD'), :3, :4,
                    TO_DATE(:5, 'YYYY/MM/DD HH24:MI:SS'),
                    :6, :7, :8, :9, :10, :11, :12, :13,
                    :14, :15, :16, :17, :18, :19, :20,
                    :21, :22, SYSDATE, SYSDATE, :23, :24
                )
            """
            bind = (
                hdr.pipeline_event_id,
                call_date,
                hdr.call_id,
                hdr.call_metadata.project_code,
                hdr.call_metadata.start_time,
                hdr.call_metadata.call_type_code,
                poly_no,
                ruser_id,
                ruser_name,
                cu_id,
                cu_name,
                cntc_lcls_c,
                cntc_lcls_nm,
                cntc_md_clas_c,
                cntc_md_clas_nm,
                sentence,
                category,
                keyword_name,
                cntc_user_depart_c,
                cntc_user_depart_nm,
                cntc_user_part_c,
                cntc_user_part_nm,
                hdr.creator_id,
                hdr.creator_id,
            )
            oracle.cursor.execute(query, bind)
        oracle.conn.commit()


def insert_ksqi_hmd_result(oracle, hdr, sentence, cntc_info, detect_yn):
    """
    Inert KSQI call analysis table HMD result to CS_KSQI_ANALYSIS_TB
    :param          oracle:                 DB
    :param          hdr:                    Header
    :param          sentence:               Sentence
    :param          cntc_info:              CNTC result
    :param          detect_yn:              DETECT_YN
    """
    call_date = '' if hdr.call_metadata.call_date == 'None' else hdr.call_metadata.call_date
    ruser_id = '' if hdr.call_metadata.ruser_id == 'None' else hdr.call_metadata.ruser_id
    ruser_name = '' if hdr.call_metadata.ruser_name == 'None' else hdr.call_metadata.ruser_name
    cntc_user_depart_c = '' if hdr.call_metadata.cntc_user_depart_c == 'None' else hdr.call_metadata.cntc_user_depart_c
    cntc_user_depart_nm = '' if hdr.call_metadata.cntc_user_depart_nm == 'None' else hdr.call_metadata.cntc_user_depart_nm
    cntc_user_part_c = '' if hdr.call_metadata.cntc_user_part_c == 'None' else hdr.call_metadata.cntc_user_part_c
    cntc_user_part_nm = '' if hdr.call_metadata.cntc_user_part_nm == 'None' else hdr.call_metadata.cntc_user_part_nm
    ivr_serv_nm = '' if hdr.call_metadata.ivr_serv_nm == 'None' else hdr.call_metadata.ivr_serv_nm
    for item in cntc_info:
        cntc_lcls_c = item[0]
        cntc_lcls_nm = item[1]
        cntc_md_clas_c = item[2]
        cntc_md_clas_nm = item[3]
        query = """
            INSERT INTO CS_KSQI_ANALYSIS_TB
            (
                PIPELINE_EVENT_ID,
                CALL_ID,
                CALL_DATE,
                PROJECT_CODE,
                CALL_TYPE_CODE,
                START_TIME,
                DURATION,
                RUSER_ID,
                RUSER_NAME,
                SENTENCE,
                CNTC_LCLS_C,
                CNTC_LCLS_NM,
                CNTC_MD_CLAS_C,
                CNTC_MD_CLAS_NM,
                CNTC_USER_DEPART_C,
                CNTC_USER_DEPART_NM,
                CNTC_USER_PART_C,
                CNTC_USER_PART_NM,
                IVR_SERV_NM,
                DETECT_YN,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID
            )
            VALUES
            (
                :1, :2, TO_DATE(:3, 'YYYY/MM/DD'), :4, :5,
                TO_DATE(:6, 'YYYY/MM/DD HH24:MI:SS'), :7,
                :8, :9, :10, :11, :12, :13, :14, :15, :16,
                :17, :18, :19, :20, SYSDATE, SYSDATE, :21, :22
            )
        """
        bind = (
            hdr.pipeline_event_id,
            hdr.call_id,
            call_date,
            hdr.call_metadata.project_code,
            hdr.call_metadata.call_type_code,
            hdr.call_metadata.start_time,
            hdr.call_metadata.duration,
            ruser_id,
            ruser_name,
            sentence,
            cntc_lcls_c,
            cntc_lcls_nm,
            cntc_md_clas_c,
            cntc_md_clas_nm,
            cntc_user_depart_c,
            cntc_user_depart_nm,
            cntc_user_part_c,
            cntc_user_part_nm,
            ivr_serv_nm,
            detect_yn,
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
