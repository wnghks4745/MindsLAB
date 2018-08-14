#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-09, modification: 2018-03-12"

###########
# imports #
###########
import os
import re
import sys
import time
import glob
import shutil
import traceback
import cx_Oracle
import workerpool
import subprocess
import collections
from datetime import datetime
from cfg.config import IE_TA_CONFIG, DB_CONFIG, MASKING_CONFIG
from lib.openssl import encrypt, decrypt, encrypt_file

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
NLP_DIR_PATH = ""
TA_TEMP_DIR_PATH = ""
TA_TEMP_DIR_NAME = ""
DELETE_FILE_LIST = list()
RFILE_NAME_ID_DICT = dict()


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = cx_Oracle.makedsn(
            DB_CONFIG['host'],
            DB_CONFIG['port'],
            sid=DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
            DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_data_to_scrt_sntc_dtc_info(self, sntc_no):
        query = """
            SELECT
                DTC_CONT,
                SCRT_DCD
            FROM
                TB_SCRT_SNTC_DTC_INFO
            WHERE 1=1
                AND SNTC_NO = :1
        """
        bind = (
            sntc_no,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_int_key_sntc_no(self, sntc_dcd):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND SNTC_DCD = :1
        """
        bind = (
            sntc_dcd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_sntc_no(self, sntc_dcd):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND SNTC_DCD = :1
        """
        bind = (
            sntc_dcd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_dtc_cd_and_nm(self, sntc_no, sntc_dcd):
        query = """
            SELECT
                DTC_CD,
                DTC_CD_NM
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND SNTC_NO = :1
                AND SNTC_DCD = :2
        """
        bind = (
            sntc_no,
            sntc_dcd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def insert_tb_tm_nqa_ta_dtc_rst(self, upload_data_list):
        try:
            sql = """
                INSERT INTO
                    TB_TM_NQA_TA_DTC_RST(
                        REC_ID,
                        RFILE_NAME,
                        SNTC_NO,
                        SNTC_SEQ,
                        SNTC_DCD,
                        STT_SNTC_LIN_NO,
                        DTC_CD,
                        DTC_CD_NM,
                        SNTC_CONT,
                        SNTC_STTM,
                        SNTC_ENDTM,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                VALUES (
                    :1, :2, :3, :4, :5,
                    :6, :7, :8, :9, :10,
                    :11, 'CS_IE_TA', 'CS_IE_TA', SYSDATE, 'CS_IE_TA',
                    'CS_IE_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in upload_data_list:
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                sntc_no = insert_dict['SNTC_NO']
                sntc_seq = insert_dict['SNTC_SEQ']
                sntc_dcd = insert_dict['SNTC_DCD']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                dtc_cd = insert_dict['DTC_CD']
                dtc_cd_nm = insert_dict['DTC_CD_NM']
                sntc_cont = insert_dict['SNTC_CONT']
                sntc_sttm = insert_dict['SNTC_STTM']
                sntc_endtm = insert_dict['SNTC_ENDTM']
                values_tuple = (
                    rec_id, rfile_name, sntc_no, sntc_seq, sntc_dcd, stt_sntc_lin_no, dtc_cd, dtc_cd_nm, sntc_cont,
                    sntc_sttm, sntc_endtm
                )
                values_list.append(values_tuple)
            self.cursor.executemany(sql, values_list)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def update_cs_stta_prgst_cd(self, cs_stta_prgst_cd, rec_id, rfile_name):
        try:
            query = """
                UPDATE
                    TB_CS_STT_RCDG_INFO
                SET
                    CS_STTA_PRGST_CD = :1,
                    LST_CHGP_CD = 'CS_IE_TA',
                    LST_CHG_PGM_ID = 'CS_IE_TA',
                    LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND REC_ID = :2
                    AND RFILE_NAME = :3
            """
            bind = (
                cs_stta_prgst_cd,
                rec_id,
                rfile_name,
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
            self.logger.error(traceback.format_exc())
            raise Exception()

    def delete_tb_tm_nqa_ta_dtc_rst(self, rec_id, rfile_name):
        try:
            query = """
                DELETE FROM
                    TB_TM_NQA_TA_DTC_RST
                WHERE 1=1
                    AND REC_ID = :1
                    AND RFILE_NAME = :2
            """
            bind = (
                rec_id,
                rfile_name,
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


#######
# def #
#######
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


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info("10. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception as e:
            print e
            continue


def pool_sub_process(cmd):
    """
    Execute subprocess
    :param      cmd:        Command
    """
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    sub_pro.communicate()


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)


def move_output(logger, args):
    """
    Move output
    :param      logger:     Logger
    :param      args:       Arguments
    """
    logger.info("9. Move output")
    for info_dict in args.stt_tm_info_dict.values():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        call_start_time = str(info_dict['CALL_START_TIME']).strip()
        output_dir_path = '{0}/{1}/{2}/{3}/{4}-{5}'.format(
            IE_TA_CONFIG['ta_output_path'], call_start_time[:4], call_start_time[5:7], call_start_time[8:10],
            rec_id, rfile_name)
        output_list = ['HMD_result', 'final_output', 'IDX', 'IDXVP']
        # Move the file
        for target in output_list:
            output_target_path = '{0}/{1}'.format(output_dir_path, target)
            if not os.path.exists(output_target_path):
                os.makedirs(output_target_path)
            path_list = glob.glob('{0}/{1}/{2}*'.format(TA_TEMP_DIR_PATH, target, rfile_name))
            for path in path_list:
                file_name = os.path.basename(path)
                if os.path.exists('{0}/{1}'.format(output_target_path, file_name)):
                    del_garbage(logger, '{0}/{1}'.format(output_target_path, file_name))
                logger.debug('move file {0} -> {1}'.format(path, output_target_path))
                shutil.move(path, output_target_path)
            logger.info('encrypt {0}'.format(output_target_path))
            encrypt(output_target_path)


def set_data_for_tb_tm_nqa_ta_dtc_rst(logger, oracle, dir_path, file_name):
    """
    Set data for TB_TM_NQA_TA_DTC_RST
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      dir_path:                   Directory path
    :param      file_name:                  File name
    :return                                 Detect dictionary
    """
    # 관심 키워드 리스트
    int_key_list = list()
    int_key_result = oracle.select_sntc_no('02')
    for item in int_key_result:
        int_key_list.append(item[0])
    # 파일 탐색
    sntc_seq_cnt = 0
    overlap_check_dict = dict()
    hmd_output_file = open(os.path.join(dir_path, file_name), 'r')
    insert_data_dict = dict()
    insert_data_list = list()
    rec_id = ''
    rfile_name = ''
    for line in hmd_output_file:
        line = line.strip()
        line_list = line.split('\t')
        if line_list[4] == 'none':
            continue
        rfile_name = line_list[1].replace('_trx', '').strip()
        rec_id = RFILE_NAME_ID_DICT[rfile_name]
        category_list = line_list[4].split("_")
        sntc_no = category_list[1]
        sntc_dcd = '02'
        overlap_check_key = '{0}_{1}_{2}_{3}'.format(rec_id, rfile_name, sntc_no, sntc_dcd)
        if overlap_check_key not in overlap_check_dict:
            sntc_seq_cnt = 0
            sntc_seq = str(sntc_seq_cnt)
        else:
            sntc_seq_cnt += 1
            sntc_seq = str(sntc_seq_cnt)
        stt_sntc_lin_no = line_list[2].strip()
        sntc_dtc_lst_result = oracle.select_dtc_cd_and_nm(sntc_no, sntc_dcd)
        if not sntc_dtc_lst_result:
            logger.error("Can't select DTC_CD and DTC_CD_NM, SNTC_NO = {0}, SNTC_DCD = {1}".format(sntc_no, sntc_dcd))
            continue
        dtc_cd = sntc_dtc_lst_result[0]
        dtc_cd_nm = sntc_dtc_lst_result[1]
        sntc_cont = line_list[6].replace("[C]", "").replace("[A]", "").replace("[M]", "").strip()
        scrt_sntc_sttm = line_list[7]
        scrt_sntc_endtm = line_list[8]
        temp_sntc_sttm = scrt_sntc_sttm.replace(":", "").split('.')[0]
        temp_sntc_endtm = scrt_sntc_endtm.replace(":", "").split('.')[0]
        modified_sntc_sttm = temp_sntc_sttm if len(temp_sntc_sttm) == 6 else "0" + temp_sntc_sttm
        modified_sntc_endtm = temp_sntc_endtm if len(temp_sntc_endtm) == 6 else "0" + temp_sntc_endtm
        tb_tm_nqa_ta_dtc_rst_dict = dict()
        tb_tm_nqa_ta_dtc_rst_dict['REC_ID'] = rec_id
        tb_tm_nqa_ta_dtc_rst_dict['RFILE_NAME'] = rfile_name
        tb_tm_nqa_ta_dtc_rst_dict['SNTC_NO'] = sntc_no
        tb_tm_nqa_ta_dtc_rst_dict['SNTC_DCD'] = sntc_dcd
        tb_tm_nqa_ta_dtc_rst_dict['SNTC_SEQ'] = sntc_seq
        tb_tm_nqa_ta_dtc_rst_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
        tb_tm_nqa_ta_dtc_rst_dict['DTC_CD'] = dtc_cd
        tb_tm_nqa_ta_dtc_rst_dict['DTC_CD_NM'] = dtc_cd_nm
        tb_tm_nqa_ta_dtc_rst_dict['SNTC_CONT'] = unicode(sntc_cont, 'euc-kr')
        tb_tm_nqa_ta_dtc_rst_dict['SNTC_STTM'] = modified_sntc_sttm
        tb_tm_nqa_ta_dtc_rst_dict['SNTC_ENDTM'] = modified_sntc_endtm
        insert_data_list.append(tb_tm_nqa_ta_dtc_rst_dict)
    key = '{0}&{1}'.format(rec_id, rfile_name)
    if key not in insert_data_dict:
        insert_data_dict[key] = insert_data_list
    hmd_output_file.close()
    return insert_data_dict


def db_upload_tb_tm_nqa_ta_dtc_rst(logger, oracle, hmd_output_dir_path, args):
    """
    DB upload to TB_TM_NQA_TA_DTC_RST
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      hmd_output_dir_path:        HMD output directory path
    :param      args:                       Arguments
    """
    logger.info("8. DB upload to TB_TM_NQA_TA_DTC_RST")
    w_ob = os.walk(hmd_output_dir_path)
    update_dictionary = dict()
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            update_dictionary = set_data_for_tb_tm_nqa_ta_dtc_rst(logger, oracle, dir_path, file_name)
    for info_dict in args.stt_tm_info_dict.values():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        key = '{0}&{1}'.format(rec_id, rfile_name)
        if key not in update_dictionary:
            continue
        insert_list = update_dictionary[key]
        oracle.delete_tb_tm_nqa_ta_dtc_rst(rec_id, rfile_name)
        oracle.insert_tb_tm_nqa_ta_dtc_rst(insert_list)


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
        else :
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
            index_output_dict[re_line_num] = ''
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
            index_str = ""
            for item in re_result:
                idx_tuple = item.span()
                start = idx_tuple[0]
                end = idx_tuple[1]
                masking_part = ""
                index_str += "{0},{1},{2};".format(start, end, masking_code)
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
                index_output_dict[re_line_num] = index_str
            else:
                index_output_dict[re_line_num] += index_str
        output_dict[re_line_num] = output_str.encode(encoding)
    return output_dict, index_output_dict


def execute_masking(logger, target_dir_path):
    """
    Execute masking
    :param      logger:                 Logger
    :param      target_dir_path:        Target directory path
    :return                             Masking directory path
    """
    logger.info("7. Execute masking")
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
    logger.info("6. Modify HMD output")
    final_output_dir_path = "{0}/final_output".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(final_output_dir_path):
        os.makedirs(final_output_dir_path)
    # Make time information dictionary
    detail_dir_path = '{0}/detail'.format(TA_TEMP_DIR_PATH)
    w_ob = os.walk(detail_dir_path)
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


def execute_hmd(logger, matrix_file_path):
    """
    Execute HMD
    :param      logger:                             Logger
    :param      matrix_file_path:                   Matrix file path
    :return:                                        HMD output directory path
    """
    global DELETE_FILE_LIST
    logger.info("5. Execute HMD")
    os.chdir(IE_TA_CONFIG['hmd_script_path'])
    hmd_file_list = glob.glob("{0}/*".format(NLP_DIR_PATH))
    hmd_output_dir_path = "{0}/HMD_result".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(hmd_output_dir_path):
        os.makedirs(hmd_output_dir_path)
    start = 0
    end = 0
    cmd_list = list()
    hmd_thread = int(IE_TA_CONFIG['hmd_thread'])
    thread = len(hmd_file_list) if len(hmd_file_list) < hmd_thread else hmd_thread
    # Make list file
    for cnt in range(thread):
        end += len(hmd_file_list) / thread
        if (len(hmd_file_list) % thread) > cnt:
            end += 1
        list_file_path = "{0}/{1}_{2}.list".format(IE_TA_CONFIG['hmd_script_path'], TA_TEMP_DIR_NAME, cnt)
        DELETE_FILE_LIST.append(list_file_path)
        list_file = open(list_file_path, 'w')
        for idx in range(start, end):
            print >> list_file, hmd_file_list[idx]
        list_file.close()
        start = end
        cmd = "python {0}/hmd.py {1} {2} {3} {4}".format(
            IE_TA_CONFIG['hmd_script_path'], TA_TEMP_DIR_NAME, list_file_path, matrix_file_path, hmd_output_dir_path)
        cmd_list.append(cmd)
    pool = workerpool.WorkerPool(thread)
    pool.map(pool_sub_process, cmd_list)
    pool.shutdown()
    pool.wait()
    return hmd_output_dir_path


def vec_word_combine(tmp_result, output, strs_list, ws, level):
    """
    Vec word combine
    :param      tmp_result:     Temp result
    :param      output:         Output
    :param      strs_list:      Strs list
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


def make_matrix_file(logger, oracle):
    """
    Make matrix file
    :param      logger:     Logger
    :param      oracle:     Oracle DB
    :return:                Matrix file path
    """
    logger.info("4. Make matrix file")
    # Select SNTC_NO and DTC_CONT (문장 번호, 탐지 사전 내용)
    scrt_sntc_dtc_info_result_list = list()
    # 관심 키워드 matrix 조회
    int_key_result = oracle.select_int_key_sntc_no('02')
    for int_key_item in int_key_result:
        sntc_no = int_key_item[0]
        int_key_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no)
        if not int_key_scrt_sntc_dtc_info_result:
            continue
        for int_key_dtc_cont_result in int_key_scrt_sntc_dtc_info_result:
            dtc_cont = int_key_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(['INTKEY', sntc_no, dtc_cont])
    # Make matrix file
    matrix_dir_path = '{0}/HMD_matrix'.format(TA_TEMP_DIR_PATH)
    if not os.path.exists(matrix_dir_path):
        os.makedirs(matrix_dir_path)
    matrix_file_path = '{0}/{1}.matrix'.format(matrix_dir_path, DT)
    output_list = list()
    for item in scrt_sntc_dtc_info_result_list:
        strs_list = list()
        sntc_no = str(item[1]).strip()
        category = '{0}_{1}'.format(item[0], sntc_no)
        dtc_cont = str(item[2].encode("euc-kr")).strip()
        detect_keyword_list = split_input(dtc_cont)
        for idx in range(len(detect_keyword_list)):
            detect_keyword = detect_keyword_list[idx].split("|")
            strs_list.append(detect_keyword)
        ws = ""
        output = ""
        tmp_result = []
        output += '{0}\t'.format(category)
        output_list += vec_word_combine(tmp_result, output, strs_list, ws, 0)
    matrix_file = open(matrix_file_path, 'w')
    for item in output_list:
        print >> matrix_file, item
    matrix_file.close()
    return matrix_file_path


def update_status(logger, oracle, args):
    """
    Update status
    :param      logger:     Logger
    :param      oracle:     Oracle DB
    :param      args:       Arguments
    """
    logger.info("3. Update status")
    for info_dict in args.stt_tm_info_dict.values():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        # Update STT_PRGST_CD status
        oracle.update_cs_stta_prgst_cd('11', rec_id, rfile_name)


def error_process(logger, oracle, rec_id, rfile_name, cs_stta_prgst_cd, biz_cd):
    """
    Error process
    :param      logger:                 Logger
    :param      oracle:                 Oracle db
    :param      rec_id:                 REC_ID(녹취 ID)
    :param      rfile_name:             RFILE_NAME(녹취파일명)
    :param      cs_stta_prgst_cd:       CS_STTA_PRGST_CD(CS_STTA 상태코드)
    :param      biz_cd:                 BIZ_CD(업체구분코드)
    """
    logger.error("Error process")
    logger.error("REC_ID = {0}, RFILE_NAME = {1}, change CS_STTA_PRGST_CD = {2}".format(
        rec_id, rfile_name, cs_stta_prgst_cd))
    oracle.update_cs_stta_prgst_cd(cs_stta_prgst_cd, rec_id, rfile_name)
    if not cs_stta_prgst_cd == '00':
        rec_path = '{0}/CS/{1}'.format(IE_TA_CONFIG['rec_dir_path'], biz_cd)
        target_rec_path_list = glob.glob('{0}/{1}.*'.format(rec_path, rfile_name))
        target_rec_path_list += glob.glob('{0}/{1}_*'.format(rec_path, rfile_name))
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


def setup_temp_data(logger, oracle, args):
    """
    Copy source directory
    :param      logger:     Logger
    :param      oracle:     Oracle DB
    :param      args:       Arguments
    """
    global NLP_DIR_PATH
    global TA_TEMP_DIR_PATH
    global TA_TEMP_DIR_NAME
    global DELETE_FILE_LIST
    global RFILE_NAME_ID_DICT
    logger.info("2. Setup TEMP data")
    while True:
        TA_TEMP_DIR_PATH = "{0}/ie_ta_temp_directory_{1}".format(IE_TA_CONFIG['ta_data_path'], args.cnt)
        if not os.path.exists(TA_TEMP_DIR_PATH):
            try:
                os.makedirs(TA_TEMP_DIR_PATH)
            except Exception:
                continue
            TA_TEMP_DIR_NAME = os.path.basename(TA_TEMP_DIR_PATH)
            DELETE_FILE_LIST.append(TA_TEMP_DIR_PATH)
            break
        args.cnt += 1
    target_dir_path_dict = {
        'hmd.txt': "{0}/modified_nlp_line_number".format(TA_TEMP_DIR_PATH),
        'detail': '{0}/detail'.format(TA_TEMP_DIR_PATH),
        'idx': '{0}/IDX'.format(TA_TEMP_DIR_PATH),
        'idxvp': '{0}/IDXVP'.format(TA_TEMP_DIR_PATH)
    }
    NLP_DIR_PATH = "{0}/modified_nlp_line_number".format(TA_TEMP_DIR_PATH)
    if os.path.exists(TA_TEMP_DIR_PATH):
        del_garbage(logger, TA_TEMP_DIR_PATH)
    DELETE_FILE_LIST.append(TA_TEMP_DIR_PATH)
    for key, info_dict in args.stt_tm_info_dict.items():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        RFILE_NAME_ID_DICT[rfile_name] = rec_id
        call_start_time = str(info_dict['CALL_START_TIME']).strip()
        chn_tp = info_dict['CHN_TP']
        biz_cd = info_dict['BIZ_CD']
        stt_output_dir_path = '{0}/{1}/{2}/{3}/{4}-{5}'.format(
            IE_TA_CONFIG['stt_output_path'], call_start_time[:4], call_start_time[5:7], call_start_time[8:10],
            rec_id, rfile_name)
        flag = True
        for ext, target_dir_path in target_dir_path_dict.items():
            if not flag:
                continue
            if not os.path.exists(target_dir_path):
                os.makedirs(target_dir_path)
            dir_name = os.path.basename(target_dir_path)
            if chn_tp == 'S':
                target_file = '{0}/{1}/{2}_trx.{3}'.format(stt_output_dir_path, dir_name, rfile_name, ext)
            else:
                target_file = '{0}/{1}/{2}.{3}'.format(stt_output_dir_path, dir_name, rfile_name, ext)
            target_file += '.enc'
            if not os.path.exists(target_file):
                logger.error('{0} is not exists'.format(target_file))
                error_process(logger, oracle, rec_id, rfile_name, '12', biz_cd)
                del args.stt_tm_info_dict[key]
                flag = False
            else:
                logger.info('copy {0} -> {1}'.format(target_file, target_dir_path))
                shutil.copy(target_file, target_dir_path)
    logger.info('decrypt {0}'.format(TA_TEMP_DIR_PATH))
    decrypt(TA_TEMP_DIR_PATH)

def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.info('Connect {0} ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ['NAS_LANG'] = '.AL32UTF8'
                sql = Oracle(logger)
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
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


def processing(args):
    """
    TA processing
    :param      args:       Arguments
    """
    logger = args.logger
    logger.info('Start IE TA')
    # 1. Connect DB
    oracle = connect_db(logger, 'Oracle')
    try:
        # 2. Setup data
        setup_temp_data(logger, oracle, args)
        # 3. Update status
        update_status(logger, oracle, args)
        # 4. Make matrix file
        matrix_file_path = make_matrix_file(logger, oracle)
        # 5. Execute HMD
        hmd_output_dir_path = execute_hmd(logger, matrix_file_path)
        # 6. Modify HMD output
        final_output_dir_path = modify_hmd_output(logger, hmd_output_dir_path)
        # 7. Execute masking
        masking_dir_path = execute_masking(logger, final_output_dir_path)
        # 8. DB upload output
#        db_upload_tb_tm_nqa_ta_dtc_rst(logger, oracle, masking_dir_path, args)
        # 9. Move output
        move_output(logger, args)
        # 10. Delete garbage file
        delete_garbage_file(logger)
        oracle.disconnect()
        logger.info("Remove logger handler")
        logger.info("TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    except Exception:
        oracle.disconnect()
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("----------    TA ERROR    ----------")
        delete_garbage_file(logger)
        raise Exception(exc_info)


########
# main #
########
def main(args):
    """
    This is a program that execute information TA
    :param      args:       Arguments
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
        raise Exception(exc_info)
