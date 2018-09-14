#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-15"

###########
# imports #
###########
import os
import re
import sys
import glob
import time
import shutil
import cx_Oracle
import traceback
import subprocess
import workerpool
import collections
import cfg.config
from datetime import datetime
from operator import itemgetter
from lib.iLogger import set_logger
from lib.openssl import decrypt_string
from lib.damo import scp_dec_file, dir_scp_enc_file

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
DB_CONFIG = dict()
TA_CONFIG = dict()
NLP_DIR_PATH = ""
NLP_INFO_DICT = dict()
MASKING_CONFIG = dict()
DETAIL_DIR_PATH = ""
OUTPUT_DIR_NAME = ""
DELETE_FILE_LIST = list()
TA_TEMP_DIR_PATH = ""
REC_INFO_DICT = dict()


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = DB_CONFIG['dsn']
        passwd = decrypt_string(DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def rows_to_dict_list(self):
        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor]

    def update_ta_prgst_cd(self, info_dict, status):
        try:
            query = """
                UPDATE
                    CALL_META
                SET
                    CHAT_TA_PRGST_CD = :1,
                    LST_CHGP_CD = 'CHAT_TA',
                    LST_CHG_PGM_ID = 'CHAT_TA',
                    LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND PROJECT_CD = 'CS'
                    AND DOCUMENT_ID = :2
                    AND DOCUMENT_DT = :3
            """
            bind = (
                status,
                info_dict.get('DOCUMENT_ID'),
                info_dict.get('DOCUMENT_DT'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def delete_tb_cs_ta_chat_dtc_rst(self, rst_list):
        try:
            query = """
                DELETE FROM
                    TB_CS_TA_CHAT_DTC_RST
                WHERE 1=1
                    AND DOCUMENT_ID = :1
                    AND DOCUMENT_DT = :2
            """
            document_id = rst_list[0]['DOCUMENT_ID']
            document_dt = rst_list[0]['DOCUMENT_DT']
            bind = (
                document_id,
                document_dt,
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            self.disconnect()
            raise Exception(traceback.format_exc())

    def insert_tb_cs_ta_chat_dtc_rst(self, rst_list):
        try:
            query = """
                INSERT INTO
                    TB_CS_TA_CHAT_DTC_RST
                    (
                        DOCUMENT_ID
                        , DOCUMENT_DT
                        , QA_SCRT_LCCD
                        , QA_SCRT_MCCD
                        , STT_SNTC_LIN_NO
                        , SNTC_CONT
                        , SNTC_STTM
                        , SNTC_ENDTM
                        , REC_ID
                        , RFILE_NAME
                        , REGP_CD
                        , RGST_PGM_ID
                        , RGST_DTM
                        , LST_CHGP_CD
                        , LST_CHG_PGM_ID
                        , LST_CHG_DTM
                    )
                VALUES (
                    :1, :2, :3, :4, :5,
                    :6, :7, :8, :9, :10,
                    'CHAT_TA', 'CHAT_TA', SYSDATE, 'CHAT_TA', 'CHAT_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in rst_list:
                document_id = insert_dict['DOCUMENT_ID']
                document_dt = insert_dict['DOCUMENT_DT']
                qa_scrt_lccd = insert_dict['QA_SCRT_LCCD']
                qa_scrt_mccd = insert_dict['QA_SCRT_MCCD']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                sntc_cont = insert_dict['SNTC_CONT']
                sntc_sttm = insert_dict['SNTC_STTM']
                sntc_endtm = insert_dict['SNTC_ENDTM']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                values_tuple = (document_id, document_dt, qa_scrt_lccd, qa_scrt_mccd, stt_sntc_lin_no,
                                sntc_cont, sntc_sttm, sntc_endtm, rec_id, rfile_name)
                values_list.append(values_tuple)
            self.cursor.executemany(query, values_list)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


#######
# def #
#######
def elapsed_time(sdate):
    """
    elapsed time
    :param      sdate:      date object
    :return:                Reqruied time (type : datetime)
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
    :param      logger:     Logger
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


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info("15. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception:
            continue


def move_output(logger):
    """
    Move output
    :param      logger:     Logger
    """
    logger.info("14. Move output")
    output_sub_dir_list = list()
    logger.debug(TA_TEMP_DIR_PATH)
    dir_scp_enc_file(TA_TEMP_DIR_PATH)
    for dir in os.listdir(TA_TEMP_DIR_PATH):
        ta_temp_sub_dir_path = '{0}/{1}'.format(TA_TEMP_DIR_PATH, dir)
        if os.path.isdir(ta_temp_sub_dir_path):
            logger.debug(dir)
            output_sub_dir_list.append(dir)
    for target, info_dict in REC_INFO_DICT.items():
        document_dt = str(info_dict['DOCUMENT_DT'])
        for output_sub_dir in output_sub_dir_list:
            target_file_list = glob.glob('{0}/{1}/{2}*'.format(TA_TEMP_DIR_PATH, output_sub_dir, target))
            logger.debug(target_file_list)
            target_output_path = '{0}/{1}/{2}/{3}/{4}'.format(
                TA_CONFIG['ta_output_path'], document_dt[:4], document_dt[5:7], document_dt[8:10], output_sub_dir)
            if not os.path.exists(target_output_path):
                os.makedirs(target_output_path)
            for target_file in target_file_list:
                file_name = os.path.basename(target_file)
                target_output_file_path = '{0}/{1}'.format(target_output_path, file_name)
                logger.debug(target_output_file_path)
                if os.path.exists(target_output_file_path):
                    del_garbage(logger, target_output_path)
                shutil.move(target_file, target_output_path)


def set_data_for_tb_cs_ta_chat_dtc_rst(logger, dir_path, file_name):
    """
    Set data for TB_TM_QA_TA_DTC_RST
    :param      logger:             Logger
    :param      dir_path:           Directory path
    :param      file_name:          File name
    """
    global REC_INFO_DICT
    # output 결과 파일 정리
    rst_list = list()
    final_output_file = open(os.path.join(dir_path, file_name), 'r')
    for line in final_output_file:
        line = line.strip()
        line_list = line.split('\t')
        if line_list[4] == 'none' or line_list[4] == 'new_none':
            continue
        logger.debug("## line_list start ##")
        logger.debug(line_list)
        logger.debug("## line_list end ##")
        category_list = line_list[4].split("_")
        if len(category_list) != 4:
            continue
        qa_scrt_lccd = category_list[0]
        qa_scrt_mccd = category_list[2]
        rfile_name = os.path.basename(line_list[1]).replace("_trx", "")
        document_dt = REC_INFO_DICT[rfile_name]['DOCUMENT_DT']
        rec_id = REC_INFO_DICT[rfile_name]['REC_ID']
        sntc_cont = line_list[6].replace("[C]", "").replace("[A]", "").strip()
        stt_sntc_lin_no = line_list[2].strip()
        scrt_sntc_sttm = line_list[7]
        scrt_sntc_endtm = line_list[8]
        temp_sntc_sttm = scrt_sntc_sttm.replace(":", "").split('.')[0]
        temp_sntc_endtm = scrt_sntc_endtm.replace(":", "").split('.')[0]
        modified_sntc_sttm = temp_sntc_sttm if len(temp_sntc_sttm) == 6 else "0" + temp_sntc_sttm
        modified_sntc_endtm = temp_sntc_endtm if len(temp_sntc_endtm) == 6 else "0" + temp_sntc_endtm
        tb_cs_ta_chat_dtc_rst_dict = dict()
        tb_cs_ta_chat_dtc_rst_dict['DOCUMENT_ID'] = rfile_name
        tb_cs_ta_chat_dtc_rst_dict['DOCUMENT_DT'] = document_dt
        tb_cs_ta_chat_dtc_rst_dict['QA_SCRT_LCCD'] = qa_scrt_lccd
        tb_cs_ta_chat_dtc_rst_dict['QA_SCRT_MCCD'] = qa_scrt_mccd
        tb_cs_ta_chat_dtc_rst_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
        tb_cs_ta_chat_dtc_rst_dict['SNTC_CONT'] = sntc_cont
        tb_cs_ta_chat_dtc_rst_dict['SNTC_STTM'] = modified_sntc_sttm
        tb_cs_ta_chat_dtc_rst_dict['SNTC_ENDTM'] = modified_sntc_endtm
        tb_cs_ta_chat_dtc_rst_dict['REC_ID'] = rec_id
        tb_cs_ta_chat_dtc_rst_dict['RFILE_NAME'] = rfile_name
        rst_list.append(tb_cs_ta_chat_dtc_rst_dict)
    final_output_file.close()
    return rst_list


def db_insert_tb_cs_ta_chat_dtc_rst(logger, oracle, final_output_dir_path):
    """
    DB upload to TB_TM_QA_TA_SEC_INFO
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      final_output_dir_path:      Final output directory path
    """
    logger.info("13. DB upload to TB_TM_QA_TA_SEC_INFO")
    w_ob = os.walk(final_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            logger.debug(file_name)
            rst_list = set_data_for_tb_cs_ta_chat_dtc_rst(logger, dir_path, file_name)
            logger.debug(rst_list)
            if rst_list:
                oracle.delete_tb_cs_ta_chat_dtc_rst(rst_list)
                oracle.insert_tb_cs_ta_chat_dtc_rst(rst_list)


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
    address_rule = MASKING_CONFIG['address_rule']
    name_rule = MASKING_CONFIG['name_rule']
    next_line_cnt = int(MASKING_CONFIG['next_line_cnt'])
    line_dict = collections.OrderedDict()
    for line in input_line_list:
        line = line.strip()
        line_list = line.split(delimiter)
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
        if (u'주민' in line and u'번호' in line) or (u'면허' in line and u'번호' in line) or (u'외국인' in line and u'등록' in line and u'번호' in line) or (u'여권' in line and u'번호' in line):
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


def execute_masking(logger, target_dir_path):
    """
    Execute masking
    :param      logger:                 Logger
    :param      target_dir_path:        Target directory path
    :return                             Masking directory path
    """
    logger.info("12. Execute masking")
    target_file_list = glob.glob('{0}/*'.format(target_dir_path))
    masking_dir_path = '{0}/masking'.format(TA_TEMP_DIR_PATH)
    if not os.path.exists(masking_dir_path):
        os.makedirs(masking_dir_path)
    for target_file_path in target_file_list:
        target_file = open(target_file_path, 'r')
        line_list = target_file.readlines()
        sent_list = masking(6, '\t', 'euc-kr', line_list)
        masking_file = open(os.path.join(masking_dir_path, os.path.basename(target_file_path)), 'w')
        line_num = 0
        for line in line_list:
            line_split = line.split('\t')
            if line_num in sent_list[0]:
                line_split[6] = sent_list[0][line_num].strip()
            print >> masking_file, '\t'.join(line_split).strip()
            line_num += 1
        masking_file.close()
    return masking_dir_path


def modify_hmd_output(logger, hmd_output_dir_path):
    """
    Modify HMD output
    :param      logger:                     Logger
    :param      hmd_output_dir_path:        HMD output directory path
    :return                                 Final output directory path
    """
    logger.info("11. Modify HMD output")
    final_output_dir_path = "{0}/final_output".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(final_output_dir_path):
        os.makedirs(final_output_dir_path)
    # Make time information dictionary
    w_ob = os.walk(DETAIL_DIR_PATH)
    time_info_dict = dict()
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            detail_file = open(os.path.join(dir_path, file_name), 'r')
            cnt = 0
            for line in detail_file:
                line = line.strip()
                line_list = line.split("\t")
                rfile_name = os.path.splitext(file_name)[0]
                start_time = line_list[1]
                end_time = line_list[2]
                if str(cnt) not in time_info_dict:
                    time_info_dict[str(cnt)] = {rfile_name: [start_time, end_time]}
                else:
                    time_info_dict[str(cnt)].update({rfile_name: [start_time, end_time]})
                cnt += 1
            detail_file.close()
    # Add NLP and time information
    w_ob = os.walk(hmd_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        files.sort()
        for file_name in files:
            hmd_output_file = open(os.path.join(dir_path, file_name), 'r')
            item_list = list()
            for line in hmd_output_file:
                line = line.strip()
                line_list = line.split("\t")
                rfile_name = line_list[1]
                stt_sntc_lin_no = line_list[2]
                time_info = time_info_dict[stt_sntc_lin_no][rfile_name]
                scrt_sntc_sttm = time_info[0]
                scrt_sntc_endtm = time_info[1]
                line_list.append(scrt_sntc_sttm)
                line_list.append(scrt_sntc_endtm)
                item_list.append(line_list)
            final_output_file = open("{0}/{1}".format(final_output_dir_path, file_name), 'w')
            for line_list in item_list:
                print >> final_output_file, '\t'.join(line_list)
            final_output_file.close()
    return final_output_dir_path


def dedup_hmd_output(logger, hmd_output_dir_path):
    """
    De-duplication HMD output
    :param      logger:                     Logger
    :param      hmd_output_dir_path:        HMD output directory path
    :return:                                De-duplication HMD output directory path
    """
    logger.info("10. De-duplication HMD output")
    dedup_hmd_output_dir_path = "{0}/HMD_dedup".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(dedup_hmd_output_dir_path):
        os.makedirs(dedup_hmd_output_dir_path)
    w_ob = os.walk(hmd_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            temp_dict = collections.OrderedDict()
            hmd_output_file = open(os.path.join(dir_path, file_name), 'r')
            dedup_hmd_output_file = open(os.path.join(dedup_hmd_output_dir_path, file_name), 'w')
            for line in hmd_output_file:
                line = line.strip()
                line_list = line.split("\t")
                line_num = line_list[2].strip()
                category = line_list[4].strip()
                check_idx = "{0}_{1}".format(line_num, category)
                if check_idx not in temp_dict:
                    temp_dict[check_idx] = line
            for dedup_check_idx in temp_dict.keys():
                print >> dedup_hmd_output_file, temp_dict[dedup_check_idx]
            hmd_output_file.close()
            dedup_hmd_output_file.close()
    return dedup_hmd_output_dir_path


def sort_hmd_output(logger, hmd_output_dir_path):
    """
    Sort HMD output
    :param      logger:                     Logger
    :param      hmd_output_dir_path:        HMD output directory path
    :return:                                Sorted HMD output directory path
    """
    logger.info("9. Sorted HMD output")
    sorted_hmd_output_dir_path = "{0}/HMD_sorted".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(sorted_hmd_output_dir_path):
        os.makedirs(sorted_hmd_output_dir_path)
    w_ob = os.walk(hmd_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            temp_dict = collections.OrderedDict()
            hmd_output_file = open(os.path.join(dir_path, file_name), 'r')
            sorted_hmd_output_file = open(os.path.join(sorted_hmd_output_dir_path, file_name), 'w')
            for line in hmd_output_file:
                line = line.strip()
                line_list = line.split("\t")
                line_num = line_list[2].strip()
                category = line_list[4].strip()
                category_keyword = line_list[5].strip()
                check_key = "{0}_{1}".format(category, category_keyword)
                if line_num not in temp_dict:
                    temp_dict[line_num] = {check_key: line}
                else:
                    temp_dict[line_num].update({check_key: line})
            for sorted_line_num in temp_dict.keys():
                sorted_output = sorted(temp_dict[sorted_line_num].iteritems(), key=itemgetter(0), reverse=False)
                for item in sorted_output:
                    print >> sorted_hmd_output_file, item[1]
            hmd_output_file.close()
            sorted_hmd_output_file.close()
    return sorted_hmd_output_dir_path


def pool_sub_process(cmd):
    """
    Execute subprocess
    :param      cmd:        Command
    """
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    sub_pro.communicate()


def execute_hmd(logger, matrix_file_path):
    """
    Execute HMD
    :param      logger:                             Logger
    :param      matrix_file_path:                   Matrix file path
    :return:                                        HMD output directory path
    """
    global DELETE_FILE_LIST
    logger.info("8. Execute HMD")
    os.chdir(TA_CONFIG['hmd_script_path'])
    hmd_file_list = glob.glob("{0}/*".format(NLP_DIR_PATH))
    hmd_output_dir_path = "{0}/HMD_result".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(hmd_output_dir_path):
        os.makedirs(hmd_output_dir_path)
    start = 0
    end = 0
    cmd_list = list()
    thread = len(hmd_file_list) if len(hmd_file_list) < int(TA_CONFIG['hmd_thread']) else int(
        TA_CONFIG['hmd_thread'])
    # Make list file
    for cnt in range(thread):
        end += len(hmd_file_list) / thread
        if (len(hmd_file_list) % thread) > cnt:
            end += 1
        list_file_path = "{0}/{1}_{2}.list".format(TA_CONFIG['hmd_script_path'], OUTPUT_DIR_NAME, cnt)
        DELETE_FILE_LIST.append(list_file_path)
        list_file = open(list_file_path, 'w')
        for idx in range(start, end):
            print >> list_file, hmd_file_list[idx]
        list_file.close()
        start = end
        cmd = "python {0}/hmd.py {1} {2} {3} {4}".format(
            TA_CONFIG['hmd_script_path'], OUTPUT_DIR_NAME, list_file_path, matrix_file_path, hmd_output_dir_path)
        cmd_list.append(cmd)
    pool = workerpool.WorkerPool(thread)
    pool.map(pool_sub_process, cmd_list)
    pool.shutdown()
    pool.wait()
    return hmd_output_dir_path


def nlp_output(logger):
    """
    Copy STT output
    :param      logger:                 Logger
    """
    global NLP_INFO_DICT
    logger.info("7. NLP output")
    w_ob = os.walk(NLP_DIR_PATH)
    for dir_path, sub_dir, files in w_ob:
        for file_name in files:
            nlp_file = open(os.path.join(dir_path, file_name), 'r')
            for line in nlp_file:
                line_list = line.split('\t')
                file_nm = line_list[0]
                line_num = line_list[1]
                sent = line_list[3]
                nlp_sent = line_list[4].replace('[ A ]', '').replace('[ C ]', '').strip()
                key = '{0}|{1}'.format(file_nm, line_num)
                NLP_INFO_DICT[key] = [sent, nlp_sent]


def modify_nlp_output_line_number(logger):
    """
    Modify NLF output file line number
    :param      logger:     Logger
    """
    logger.info("6. Modify NLP output file line number")
    hmd_result_dir_path = "{0}/HMD".format(TA_TEMP_DIR_PATH)
    modify_nlp_line_number_dir_path = "{0}/modify_nlp_line_number".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(modify_nlp_line_number_dir_path):
        os.makedirs(modify_nlp_line_number_dir_path)
    w_ob = os.walk(hmd_result_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            hmd_result_file = open(os.path.join(dir_path, file_name), 'r')
            hmd_result_file_list = hmd_result_file.readlines()
            modified_line_number_file = open(os.path.join(modify_nlp_line_number_dir_path, file_name), 'w')
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
                    # 변경
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
    ne_file_list = glob.glob("{0}/NCNT/*.ne.cnt".format(TA_TEMP_DIR_PATH))
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
    ne_output_file = open("{0}/{1}.ne.cnt".format(TA_TEMP_DIR_PATH, OUTPUT_DIR_NAME), 'w')
    print >> ne_output_file, "개체명\t개체유형\t단어 빈도\t문서 빈도"
    for item in sorted_ne_output:
        print >> ne_output_file, "{0}\t{1}\t{2}\t{3}".format(item[1][2], item[1][3], item[1][0], item[1][1])
    ne_output_file.close()


def make_morph_cnt_file(logger):
    """
    Make morph.cnt file
    :param      logger:     Logger
    """
    morph_file_list = glob.glob("{0}/MCNT/*.morph.cnt".format(TA_TEMP_DIR_PATH))
    # Load freq_except.txt file
    freq_except_dic = dict()
    freq_except_file_path = "{0}/LA/rsc/freq_except.txt".format(TA_CONFIG['ta_path'])
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
    morph_output_file = open("{0}/{1}.morph.cnt".format(TA_TEMP_DIR_PATH, OUTPUT_DIR_NAME), 'w')
    print >> morph_output_file, "형태소\t품사\t단어 빈도\t문서 빈도"
    for item in sorted_morph_output:
        print >> morph_output_file, "{0}\t{1}\t{2}\t{3}".format(item[1][2], item[1][3], item[1][0], item[1][1])
    morph_output_file.close()


def make_statistics_file(logger):
    """
    Make statistics file
    :param      logger:     Logger
    """
    logger.info("5. Make statistics file")
    logger.info("5-1. Make morph.cnt file")
    make_morph_cnt_file(logger)
    logger.info("5-2. Make ne.cnt file")
    make_ne_cnt_file()


def execute_new_lang(logger):
    """
    Execute new_lang.exe [ make nlp result file ]
    :param      logger:             Logger
    """
    global DELETE_FILE_LIST
    logger.info("4. execute new lang")
    start = 0
    end = 0
    cmd_list = list()
    os.chdir(TA_CONFIG['ta_bin_path'])
    target_list = glob.glob("{0}/txt/*".format(TA_TEMP_DIR_PATH))
    thread = len(target_list) if len(target_list) < int(TA_CONFIG['nl_thread']) else int(TA_CONFIG['nl_thread'])
    output_dir_list = ['JSON', 'JSON2', 'HMD', 'MCNT', 'NCNT', 'IDX', 'IDXVP', 'W2V']
    for dir_name in output_dir_list:
        output_dir_path = "{0}/{1}".format(TA_TEMP_DIR_PATH, dir_name)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    temp_new_lang_dir_path = '{0}/{1}'.format(TA_CONFIG['ta_bin_path'], OUTPUT_DIR_NAME)
    DELETE_FILE_LIST.append(temp_new_lang_dir_path)
    if not os.path.exists(temp_new_lang_dir_path):
        os.makedirs(temp_new_lang_dir_path)
    # Make list file
    for cnt in range(thread):
        end += len(target_list) / thread
        if (len(target_list) % thread) > cnt:
            end += 1
        list_file_path = "{0}/{1}_{2}.list".format(temp_new_lang_dir_path, OUTPUT_DIR_NAME, cnt)
        list_file = open(list_file_path, 'w')
        for idx in range(start, end):
            print >> list_file, target_list[idx]
        list_file.close()
        start = end
        cmd = "./new_lang.exe -DJ {0} txt {1}".format(list_file_path, DT[:8])
        logger.debug("new_lang.exe cmd => {0}".format(cmd))
        cmd_list.append(cmd)
    pool = workerpool.WorkerPool(thread)
    pool.map(pool_sub_process, cmd_list)
    pool.shutdown()
    pool.wait()


def error_process(logger, oracle, target, status):
    """
    Error process
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      target:         Target
    :param      status:         Status
    """
    global REC_INFO_DICT
    logger.info("Error process REC_INFO_DICT[target] = {0}".format(REC_INFO_DICT[target]))
    oracle.update_ta_prgst_cd(REC_INFO_DICT[target], status)
    del REC_INFO_DICT[target]


def copy_stt_file(logger, oracle):
    """
    copy STT output txt, detail file to TA_TEMP_DIR_PATH
    :param      logger:             Logger
    :param      oracle:             Oracle DB
    :return:
    """
    logger.info("3. copy stt txt - copy STT output txt file to TA_TEMP_DIR_PATH")
    stt_target_list = ['txt', 'detail']
    for target, info_dict in REC_INFO_DICT.items():
        try:
            document_dt = str(info_dict['DOCUMENT_DT'])
            for target_name in stt_target_list:
                if info_dict['CHN_TP'] == 'S':
                    # Stereo
                    target_file_name = '{0}_trx.{1}.enc'.format(target, target_name)
                else:
                    # Mono
                    target_file_name = '{0}.{1}.enc'.format(target, target_name)
                target_stt_output_path = '{0}/{1}/{2}/{3}'.format(TA_CONFIG['stt_output_path'], document_dt[:4], document_dt[5:7], document_dt[8:10])
                target_stt_file_path = '{0}/{1}/{2}'.format(target_stt_output_path, target_name, target_file_name)
                temp_stt_path = '{0}/{1}'.format(TA_TEMP_DIR_PATH, target_name)
                temp_stt_file_path = '{0}/{1}.{2}'.format(temp_stt_path, target, target_name)
                if not os.path.exists(temp_stt_path):
                    os.makedirs(temp_stt_path)
                if not os.path.exists(target_stt_file_path):
                    logger.error('file is not exist -> {0}'.format(target_stt_file_path))
                    raise Exception("File is not exist [document_dt : {0}]".format(document_dt))
                if 0 != scp_dec_file(target_stt_file_path, temp_stt_file_path):
                    logger.error('scp_dec_file ERROR ==> '.format(target_stt_file_path))
                    raise Exception("File is not exist [document_dt : {0}]".format(document_dt))
                logger.debug("scp_dec_file {0} => {1}".format(target_stt_file_path, temp_stt_file_path))
        except Exception:
            error_process(logger, oracle, target, '12')
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            # raise Exception


def update_status(logger, oracle, job_list):
    """
    Update status and select rec file
    :param      logger:                 Logger
    :param      oracle:                 Oracle db
    :param      job_list:               Job list
    :return:                            STT output dictionary
    """
    global REC_INFO_DICT
    logger.info("2. Update status and select REC file")
    for job in job_list:
        # Update TA PRGST_CD status
        logger.debug("\tjob : {0}".format(job))
        oracle.update_ta_prgst_cd(job, '11')
        rfile_name = str(job['DOCUMENT_ID']).strip()
        REC_INFO_DICT[rfile_name] = job


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL object
    """
    # Connect DB
    logger.info('1. Connect {0} ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ['NLS_LANG'] = "Korean_Korea.KO16KSC5601"
                sql = Oracle(logger)
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
            logger.info("\tSuccess connect ".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        err_str = "Fail connect {0}".format(db)
        raise Exception(err_str)
    return sql


def setup_data():
    """
    Setup data and target directory
    """
    global NLP_DIR_PATH
    global DETAIL_DIR_PATH
    global OUTPUT_DIR_NAME
    global TA_TEMP_DIR_PATH
    global DELETE_FILE_LIST
    cnt = 0
    # Determine temp directory name to be used in script
    while True:
        OUTPUT_DIR_NAME = "temp_directory_{0}".format(cnt)
        TA_TEMP_DIR_PATH = "{0}/{1}".format(TA_CONFIG['ta_data_path'], OUTPUT_DIR_NAME)
        if not os.path.exists(TA_TEMP_DIR_PATH):
            os.makedirs(TA_TEMP_DIR_PATH)
            DELETE_FILE_LIST.append(TA_TEMP_DIR_PATH)
            break
        cnt += 1
    NLP_DIR_PATH = '{0}/modify_nlp_line_number'.format(TA_TEMP_DIR_PATH)
    DETAIL_DIR_PATH = '{0}/detail'.format(TA_TEMP_DIR_PATH)
    os.makedirs(NLP_DIR_PATH)
    os.makedirs(DETAIL_DIR_PATH)


def processing(job_list):
    """
    TA processing
    :param      job_list:
    """
    # 0. Setup data
    setup_data()
    # Add logging
    log_name = OUTPUT_DIR_NAME.replace('temp_directory', 'cs_ta_log')
    logger_args = {
        'base_path': TA_CONFIG['log_dir_path'],
        'log_file_name': "{0}.log".format(log_name),
        'log_level': TA_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start CS TA')
    # 1. Connect DB
    oracle = connect_db(logger, 'Oracle')
    try:
        # 2. Update status
        update_status(logger, oracle, job_list)
        # 3. copy stt txt
        copy_stt_file(logger, oracle)
        # 4. execute new lang
        execute_new_lang(logger)
        # 5. Make statistics file
        make_statistics_file(logger)
        # 6. Modify nlp output
        modify_nlp_output_line_number(logger)
        # 7. NLP output
        nlp_output(logger)
        # 8. Execute HMD
        hmd_output_dir_path = execute_hmd(logger, TA_CONFIG['matrix_file_path'])
        # 9. Sorted HMD output
        sorted_hmd_output_dir_path = sort_hmd_output(logger, hmd_output_dir_path)
        # 10. De-duplication HMD output
        dedup_hmd_output_dir_path = dedup_hmd_output(logger, sorted_hmd_output_dir_path)
        # 11. Modify HMD output
        final_output_dir_path = modify_hmd_output(logger, dedup_hmd_output_dir_path)
        # 12. Execute masking
        masking_dir_path = execute_masking(logger, final_output_dir_path)
        # 13. DB upload TB_CS_TA_CHAT_DTC_RST
        db_insert_tb_cs_ta_chat_dtc_rst(logger, oracle, masking_dir_path)
        # 14. Move output
        move_output(logger)
        # 15. Delete garbage file
        delete_garbage_file(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("CHATBOT TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    CHATBOT TA ERROR   ----------")
        for info_dict in REC_INFO_DICT.values():
            oracle.update_ta_prgst_cd(info_dict, '12')
        delete_garbage_file(logger)
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # Update status
    logger.info("END.. Update status to CHATBOT TA END (13)")
    for info_dict in REC_INFO_DICT.values():
        oracle.update_ta_prgst_cd(info_dict, '13')
    oracle.disconnect()
    logger.info("CHATBOT TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    logger.info("Remove logger handler")
    logger.info("----------     CHATBOT TA END      ----------")
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job_list, config_type):
    """
    This is a program that execute CS TA
    :param      job_list:           JOB list
    :param      config_type:        Config Type
    """
    global DT
    global ST
    global DB_CONFIG
    global TA_CONFIG
    global MASKING_CONFIG
    DB_CONFIG = cfg.config.DB_CONFIG[config_type]
    TA_CONFIG = cfg.config.TA_CONFIG[config_type]
    MASKING_CONFIG = cfg.config.MASKING_CONFIG
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
