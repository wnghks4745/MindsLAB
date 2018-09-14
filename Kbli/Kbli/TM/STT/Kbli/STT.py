#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-09-14, modification: 2018-01-12"

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
import subprocess
import collections
import cx_Oracle
from datetime import datetime, timedelta
from operator import itemgetter
import cfg.config
from lib.iLogger import set_logger
from lib.openssl import decrypt_string
from lib.damo import scp_enc_file, scp_dec_file

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DT = ''
ST = ''
TARGET_DIR_NAME = ''
TARGET_DIR_PATH = ''
DELETE_FILE_LIST = list()
PCM_CNT = 0
TOTAL_PCM_TIME = 0
RESULT_CNT = 0
ORACLE_DB_CONFIG = {}
CONFIG = {}
MASKING_CONFIG = {}
RCDG_ID_INFO_DICT = dict()


#########
# class #
#########
class Oracle(object):
    def __init__(self):
        self.dsn_tns = ORACLE_DB_CONFIG['dsn']
        passwd = decrypt_string(ORACLE_DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def rows_to_dict_list(self):
        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor]

    def commit(self, logger):
        try:
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            return False

    def find_call_meta(self, logger, document_id, document_dt):
        """
        Find Information of recording file using call_id
        :param      logger:                 Logger
        :param      document_id:            DOCUMENT_ID(녹취 ID)
        :param      document_dt:            DOCUMENT_DT(녹취 일자)
        :return:                            Information list of rec id
        """
        logger.debug('find recording id information : target -> {0} & {1}'.format(document_id, document_dt))
        sql = """
            SELECT
                *
            FROM
                CALL_META
            WHERE 1=1
                AND PROJECT_CD in  ('TM', 'CD')
                AND DOCUMENT_ID = :1
                AND DOCUMENT_DT = TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS')
        """
        logger.debug("{0} {1}".format(document_id, document_dt))
        bind = (document_id, document_dt, )
        self.cursor.execute(sql, bind)
        row = self.rows_to_dict_list()[0]
        if row is bool or not row:
            return False
        return row

    def update_prgst_cd_to_call_meta(self, logger, pk, prgst_cd):
        """
        Update progress code
        :param      logger:             Logger
        :param      pk:                 Primary key
        :param      prgst_cd:           Progress code
        :return                         Bool
        """
        global RCDG_ID_INFO_DICT
        document_id, document_dt, before_prgst_cd = pk.split('####')
        logger.info('Update progress code of Recording ID [{0}] File name [{1}]-> {2}'.format(
            document_id, document_id, prgst_cd))
        RCDG_ID_INFO_DICT[pk]['STT_PRGST_CD'] = prgst_cd
        project_cd = RCDG_ID_INFO_DICT[pk]['PROJECT_CD']
        try:
            sql = """
                UPDATE
                    CALL_META
                SET
                    STT_PRGST_CD = :1
                    , LST_CHGP_CD = 'STT'
                    , LST_CHG_PGM_ID = 'STT'
                    , LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND PROJECT_CD = :2
                    AND DOCUMENT_ID = :3
                    AND DOCUMENT_DT = TO_DATE(:4, 'YYYY-MM-DD HH24:MI:SS')
            """
            bind = (prgst_cd, project_cd, document_id, document_dt,)
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                logger.info('update_prgst_cd_to_call_meta is success')
                return True
            else:
                self.conn.rollback()
                exc_info = traceback.format_exc()
                logger.error('update_prgst_cd_to_call_meta is Fail')
                logger.error(exc_info)
                return False
        except Exception:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error('update_prgst_cd_to_call_meta is Fail')
            logger.error(exc_info)
            return False

    def update_stt_to_call_meta(self, logger, output_dict, pk):
        """
        Update progress code
        :param      logger:             Logger
        :param      output_dict:        Information dictionary of recording id
        :param      pk:                 Primary key
        :return                         Bool
        """
        document_id, document_dt, before_prgst_cd = pk.split('####')
        logger.info('Update stt result :  Recording ID [{0}]'.format(document_id))
        try:
            sql = """
                UPDATE
                    CALL_META
                SET
                    STT_REQ_DTM = :1
                    , STT_CM_DTM = :2
                    , STT_DURATION_HMS = :3
                    , LST_CHGP_CD = 'STT'
                    , LST_CHG_PGM_ID = 'STT'
                    , LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND PROJECT_CD in ('TM', 'CD')
                    AND DOCUMENT_ID = :4
                    AND DOCUMENT_DT = TO_DATE(:5, 'YYYY-MM-DD HH24:MI:SS')
            """
            bind = (
                output_dict['STT_REQ_DTM'],
                output_dict['STT_CM_DTM'],
                output_dict['STT_DURATION_HMS'],
                document_id,
                document_dt,
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                logger.info('update_stt_to_call_meta is success')
                return True
            else:
                self.conn.rollback()
                exc_info = traceback.format_exc()
                logger.error('update_stt_to_call_meta is Fail')
                logger.error(exc_info)
                return False
        except Exception:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error('update_stt_to_call_meta is Fail')
            logger.error(exc_info)
            return False

    def select_count_to_tb_tm_stt_rst(self, logger, info_dic):
        """
        Find stt result count of recording file using REC_ID , RFILE_NAME
        :param      logger:             Logger
        :param      info_dic:           Information dictionary
        :return:                        boolean
        """
        logger.debug('Find stt result count id information : target -> {0} & {1}'.format(info_dic['REC_ID'], info_dic['DOCUMENT_ID']))
        sql = """
            SELECT
                COUNT(*) cnt
            FROM
                TB_TM_STT_RST
            WHERE 1=1
                AND REC_ID = :1
                AND RFILE_NAME = :2
            """
        bind = (
            info_dic['REC_ID'],
            info_dic['DOCUMENT_ID'],
        )
        self.cursor.execute(sql, bind)
        row = self.cursor.fetchone()
        if row == 0:
            return False
        return True

    def delete_to_tb_tm_stt_rst(self, logger, info_dic):
        """
        delete information on TB_TM_STT_RST table
        :param      logger:             Logger
        :param      info_dic:           Information dictionary
        """
        logger.info('delete  TB_TM_STT_RST information of Recording ID [{0}] File name [{1}]'.format(
            info_dic['REC_ID'], info_dic['DOCUMENT_ID']))
        sql = ''
        try:
            sql = """
            DELETE FROM TB_TM_STT_RST
            WHERE 1=1
                AND REC_ID = :1
                AND RFILE_NAME = :2
            """
            bind = (
                info_dic['REC_ID'],
                info_dic['DOCUMENT_ID'],
            )
            self.cursor.execute(sql, bind)
            self.conn.commit()
            logger.info('delete is success')
            return True
        except Exception:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error('delete is Fail')
            logger.error('sql = {0}'.format(sql))
            logger.error(exc_info)
            raise Exception

    def insert_stt_result_to_tb_tm_stt_rst(self, logger, output_dict):
        """
        Insert information on TB_TM_STT_RST table
        :param      logger:             Logger
        :param      output_dict:        Output dictionary
        """
        logger.debug('Insert  TB_TM_STT_RST information of Recording ID [{0}] File name [{1}]'.format(
            output_dict['REC_ID'], output_dict['RFILE_NAME']))
        sql = ''
        try:
            sql = """
            INSERT INTO TB_TM_STT_RST
                    (
                        STT_SNTC_LIN_NO
                        , REC_ID
                        , RFILE_NAME
                        , STT_SNTC_CONT
                        , STT_SNTC_LEN
                        , STT_SNTC_STTM
                        , STT_SNTC_ENDTM
                        , STT_SNTC_SPKR_DCD
                        , MSK_DTC_YN
                        , STT_SNTC_SPCH_SPED
                        , STT_SILENCE
                        , STT_SNTC_SPCH_HMS
                        , REGP_CD
                        , RGST_PGM_ID
                        , RGST_DTM
                        , LST_CHGP_CD
                        , LST_CHG_PGM_ID
                        , LST_CHG_DTM
                    )
                VALUES
                    (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, 'STT', 'STT', CURRENT_DATE, 'STT', 'STT', CURRENT_DATE)
            """
            bind = (
                output_dict['STT_SNTC_LIN_NO'],
                output_dict['REC_ID'],
                output_dict['RFILE_NAME'],
                # output_dict['STT_SNTC_CONT'],
                output_dict['STT_SNTC_CONT_MSK'],
                output_dict['STT_SNTC_LEN'],
                output_dict['STT_SNTC_STTM'],
                output_dict['STT_SNTC_ENDTM'],
                output_dict['STT_SNTC_SPKR_DCD'],
                output_dict['MSK_DTC_YN'],
                output_dict['STT_SNTC_SPCH_SPED'],
                output_dict['STT_SILENCE'],
                output_dict['STT_SNTC_SPCH_HMS'],
            )
            logger.debug(bind)
            self.cursor.execute(sql, bind)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error('Insert is Fail')
            logger.error('sql = {0}'.format(sql))
            logger.error(exc_info)
            raise Exception

    def update_stt_spch_sped(self, project_cd, document_dt, document_id, stt_spch_sped_rx, stt_spch_sped_tx):
        try:
            query = """
                UPDATE
                    CALL_META
                SET
                    STT_SPCH_SPED_RX = :1,
                    STT_SPCH_SPED_TX = :2
                WHERE 1=1
                    AND PROJECT_CD = :3
                    AND DOCUMENT_DT = TO_DATE(:4, 'YYYY-MM-DD HH24:MI:SS')
                    AND DOCUMENT_ID = :5
            """
            bind = (
                stt_spch_sped_rx,
                stt_spch_sped_tx,
                project_cd,
                document_dt,
                document_id,
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

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


#######
# def #
#######
def calculate(seconds):
    if seconds is bool:
        return False
    hour = seconds / 3600
    seconds = seconds % 3600
    minute = seconds / 60
    seconds = seconds % 60
    times = '%02d%02d%02d' % (hour, minute, seconds)
    return times


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
        if (u'주민' in line and u'번호' in line) or (u'면허' in line and u'번호' in line) or (
                u'외국인' in line and u'등록' in line and u'번호' in line) or (u'여권' in line and u'번호' in line):
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


def extract_silence_line(total_duration, delimiter, input_line_split, next_input_line, silence_seconds,
                         before_input_line, biggest_before_endtime):
    """
    Extract silence section
    :param      total_duration:      Total record duration - seconds
    :param      delimiter:           Line delimiter
    :param      input_line_split:    Split input line
    :param      next_input_line:     Next input line
    :param      silence_seconds:     Target silence seconds
    :param      before_input_line:   Before input line
    :param      biggest_before_endtime:  biggest before_end_seconds
    :return                          Output dictionary
    """
    end_time = input_line_split[2]  # 현재라인 음성 끝나는 시간
    end_time_seconds = get_sec(end_time)  # 현재라인 음성 끝나는 시간
    # 마지막줄 이라면
    if next_input_line is None:
        next_start_time_seconds = total_duration
    else:
        next_line = next_input_line.strip()
        next_line_list = next_line.split(delimiter)
        next_start_time = next_line_list[1]
        next_start_time_seconds = get_sec(next_start_time)

    before_end_time_seconds = 0
    if before_input_line is not None:
        before_line = before_input_line.strip()
        before_line_list = before_line.split(delimiter)
        before_end_time = before_line_list[2]
        before_end_time_seconds = get_sec(before_end_time)

        if biggest_before_endtime > before_end_time_seconds:
            # 이전 라인중에 길었던 endTime
            before_end_time_seconds = biggest_before_endtime
        if end_time_seconds < before_end_time_seconds:
            # 이전라인 endTime이 현재라인endTime보다 크면 다음라인 시작시간.
            return (next_start_time_seconds - before_end_time_seconds), before_end_time_seconds

    # end_time_seconds  첫째줄 음성 끝나는 시간
    # start_time_seconds 두번째줄 음성 시작 시간
    duration = next_start_time_seconds - end_time_seconds
    return round(duration, 2), before_end_time_seconds


def oracle_connect():
    """
    Trying Connect to Oracle
    :return:    oracle
    """
    oracle = False
    for cnt in range(1, 4):
        try:
            oracle = Oracle()
            break
        except Exception as e:
            print e
            if cnt < 3:
                print "Fail connect Oracle, retrying count = {0}".format(cnt)
            continue
    if not oracle:
        raise Exception("Fail connect Oracle")
    return oracle


def check_file(name_form, file_name):
    """
    Check file name
    :param      name_form:      Check file name form
    :param      file_name:      Input file name
    :return:                    True or False
    """
    return file_name.endswith(name_form)


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for subprocess to finish
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


def error_process(logger, oracle, pk, prgst_cd):
    """
    Error process
    :param logger:
    :param oracle:
    :param pk:                              pk
    :param prgst_cd:                        Progress code
    :return:
    """
    global RCDG_ID_INFO_DICT
    document_id, document_dt, before_prgst_cd = pk.split('####')
    logger.error("Error process")
    logger.error("PK = {0}, change PRGST_CD = {1}, RCDG_ID_INFO_DICT = {2}, ".format(
        pk, prgst_cd, RCDG_ID_INFO_DICT))
    oracle.update_prgst_cd_to_call_meta(logger, pk, prgst_cd)
    del RCDG_ID_INFO_DICT[pk]
    rec_dir_path = '{0}/{1}'.format(CONFIG['rec_dir_path'], document_dt[:4] + document_dt[5:7] + document_dt[8:10])
    logger.debug(rec_dir_path)
    if not prgst_cd == '00':
        target_rec_path_list = glob.glob('{0}/{1}.*'.format(rec_dir_path, document_id))
        target_rec_path_list += glob.glob('{0}/{1}_*'.format(rec_dir_path, document_id))
        error_dir_path = '{0}/error_data/{1}/{2}/{3}'.format(CONFIG['stt_output_path'], DT[:4], DT[4:6], DT[6:8])
        if not os.path.exists(error_dir_path):
            os.makedirs(error_dir_path)
        logger.debug(target_rec_path_list)
        for target_path in target_rec_path_list:
            target_name = os.path.basename(target_path)
            move_path = '{0}/{1}'.format(error_dir_path, target_name)
            if os.path.exists(move_path):
                del_garbage(logger, move_path)
            logger.error('copy error file {0} -> {1}'.format(target_path, move_path))
            shutil.copy(target_path, move_path)


def modify_time_info(logger, speaker, file_name, output_dict):
    """
    Modify time info
    :param      logger:         Logger
    :param      speaker:        Speaker
    :param      file_name:      File name
    :param      output_dict:    Output dict
    :return:                    Output dict
    """
    for line in file_name:
        try:
            line_list = line.split(',')
            if len(line_list) != 3:
                continue
            st = line_list[0].strip()
            et = line_list[1].strip()
            start_time = str(timedelta(seconds=float(st.replace("ts=", "")) / 100))
            end_time = str(timedelta(seconds=float(et.replace("te=", "")) / 100))
            sent = line_list[2].strip()
            modified_st = st.replace("ts=", "").strip()
            if int(modified_st) not in output_dict:
                output_dict[int(modified_st)] = "{0}\t{1}\t{2}\t{3}".format(speaker, start_time, end_time, sent)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            logger.error("Error modify_time_info")
            logger.error(line)
            logger.error(exc_info)
            continue
    return output_dict


def none_check(argument):
    if 0 == len(argument) is bool:
        return 'None'
    return str(argument)


def get_sec(s):
    """
    Calculate time minus time
    :param      s   time
    :return:        Calculate sec
    """
    l = s.split(':')
    hour = float(l[0]) * 3600
    minute = float(l[1]) * 60
    seconds = float(l[2])
    return hour + minute + seconds


def make_stt_info_table_txt(logger, pk, oracle):
    """
    Make information text of CALL_META table
    :param      logger:                 Logger
    :param      pk:                     Primary key
#     :param      sftp_set_dir_path:      Directory of sftp set
    :param      oracle:                 Oracle
    """
    try:
        info_dic = RCDG_ID_INFO_DICT[pk]
        ts = time.time()
        stt_duration_hms = calculate(int(info_dic['DURATION']))
        # db upload information setting
        output_dict = collections.OrderedDict()
        output_dict['PROJECT_CD'] = info_dic['PROJECT_CD']
        output_dict['DOCUMENT_DT'] = str(info_dic['DOCUMENT_DT'])
        output_dict['DOCUMENT_ID'] = info_dic['DOCUMENT_ID']
        output_dict['CALL_TYPE'] = info_dic['CALL_TYPE']
        output_dict['CUSTOMER_ID'] = str(info_dic['CUSTOMER_ID'])
        #         output_dict['CALLER_NO'] = str(info_dic['CALLER_NO'])
        output_dict['AGENT_ID'] = str(info_dic['AGENT_ID'])
        output_dict['GROUP_ID'] = str(info_dic['GROUP_ID'])
        output_dict['CONTRACT_NO'] = str(info_dic['CONTRACT_NO'])
        output_dict['REC_ID'] = info_dic['REC_ID']
        output_dict['BRANCH_CD'] = str(info_dic['BRANCH_CD'])
        output_dict['CALL_DT'] = str(info_dic['CALL_DT'])
        output_dict['START_DTM'] = str(info_dic['START_DTM'])
        output_dict['END_DTM'] = str(info_dic['END_DTM'])
        output_dict['DURATION'] = str(info_dic['DURATION'])
        #         output_dict['CALLEE_NO'] = str(info_dic['CALLEE_NO'])
        output_dict['STATUS'] = info_dic['STATUS']
        output_dict['EXTENSION_PHONE_NO'] = info_dic['EXTENSION_PHONE_NO']
        output_dict['STT_PRGST_CD'] = info_dic['STT_PRGST_CD']
        output_dict['STT_DURATION_HMS'] = none_check(stt_duration_hms)
        output_dict['STT_REQ_DTM'] = '{0}-{1}-{2} {3}:{4}:{5}'.format(
            DT[:4], DT[4:6], DT[6:8], DT[8:10], DT[10:12], DT[12:14])
        output_dict['STT_CM_DTM'] = str(datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
        if not oracle.update_stt_to_call_meta(logger, output_dict, pk):
            raise Exception
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        raise Exception


def make_stt_result_table_txt(logger, pk, oracle):
    """
    Make information text of STT_RST table
    :param      logger:                 Logger
    :param      pk:                     Primary key
    :param      oracle:                 DB
    """
    info_dic = RCDG_ID_INFO_DICT[pk]
    # ready for db upload information setting
    document_id, document_dt, before_prgst_cd = pk.split('####')
    stt_sntc_lin_no = -1
    # detail file analysis
    detail_dir_path = '{0}/detail'.format(TARGET_DIR_PATH)
    chn_tp = info_dic['CHN_TP']
    if chn_tp == 'S':
        detail_file_path = '{0}/{1}_trx.detail'.format(detail_dir_path, document_id)
    else:
        detail_file_path = '{0}/{1}.detail'.format(detail_dir_path, document_id)

    if not os.path.exists(detail_dir_path):
        os.makedirs(detail_dir_path)
    detail_file = open(detail_file_path, 'r')
    lines = detail_file.readlines()
    masking_output_dict = masking(3, '\t', 'euc-kr', lines)

    if oracle.select_count_to_tb_tm_stt_rst(logger, info_dic):
        oracle.delete_to_tb_tm_stt_rst(logger, info_dic)

    idx = 0
    rx_sntc_len = 0
    tx_sntc_len = 0
    rx_during_time = 0
    tx_during_time = 0
    temp_biggest_before_endtime = 0
    for line in lines:
        # ready for db upload information setting
        output_dict = {}
        line = line.split('\t')
        start_time = line[1]
        end_time = line[2]
        sntc_sttm = start_time.split('.')[0].replace(':', '')
        sntc_endtm = end_time.split('.')[0].replace(':', '')
        during_time = get_sec(end_time) - get_sec(start_time)
        if during_time == 0:
            during_time = 0.01
        speaker = line[0][1]
        # db upload information setting
        stt_sntc_lin_no += 1
        sntc_cont = line[3].replace('\n', '')
        sntc_len = len(sntc_cont.replace(' ', '').decode('euc_kr'))
        sntc_sttm = sntc_sttm if len(sntc_sttm) == 6 else '0' + sntc_sttm
        sntc_endtm = sntc_endtm if len(sntc_endtm) == 6 else '0' + sntc_endtm
        sntc_spch_hms = str(round(during_time, 2))
        sntc_spch_sped = str(round(float(sntc_len) / during_time, 2))
        stt_sntc_spkr_dcd = 'S' if speaker == 'A' else speaker
        msk_dtd_yn = 'Y' if stt_sntc_lin_no in masking_output_dict else 'N'
        msk_info_lit = str(masking_output_dict[0][stt_sntc_lin_no].strip())
        # 무음 처리 시간 구하기
        if idx < 1:
            before_line = None
        else:
            before_line = lines[idx - 1]

        if idx + 1 == len(lines):
            next_line = None
        else:
            next_line = lines[idx + 1]
        # FIXME :: 카드사 경우 메타정보 duration 이 더 길어 마지막 라인이 묵음처리될 수 있음.
        # silence_seconds = extract_silence_line(info_dic['DURATION'], '\t', line, next_line, CONFIG['silence_seconds'])
        silence_seconds, temp_biggest_before_endtime = extract_silence_line(info_dic['DURATION'], '\t', line, next_line,
                                                                            CONFIG['silence_seconds'], before_line,
                                                                            temp_biggest_before_endtime)
        if speaker == 'A':
            tx_sntc_len += float(sntc_len)
            tx_during_time += during_time
        elif speaker == 'C':
            rx_sntc_len += float(sntc_len)
            rx_during_time += during_time
        else:
            tx_sntc_len += float(sntc_len)
            rx_sntc_len += float(sntc_len)
            tx_during_time += during_time
            rx_during_time += during_time
        # DB insert
        output_dict['DOCUMENT_ID'] = document_id
        output_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
        output_dict['REC_ID'] = info_dic['REC_ID']
        output_dict['RFILE_NAME'] = document_id
        output_dict['STT_SNTC_CONT'] = sntc_cont
        output_dict['STT_SNTC_CONT_MSK'] = msk_info_lit
        output_dict['STT_SNTC_LEN'] = sntc_len
        output_dict['STT_SNTC_STTM'] = sntc_sttm
        output_dict['STT_SNTC_ENDTM'] = sntc_endtm
        output_dict['STT_SNTC_SPKR_DCD'] = stt_sntc_spkr_dcd
        output_dict['MSK_DTC_YN'] = msk_dtd_yn
        output_dict['STT_SNTC_SPCH_SPED'] = sntc_spch_sped
        output_dict['STT_SNTC_SPCH_HMS'] = sntc_spch_hms
        output_dict['STT_SILENCE'] = silence_seconds

        oracle.insert_stt_result_to_tb_tm_stt_rst(logger, output_dict)
        if stt_sntc_lin_no % 100 == 0:
            if not oracle.commit(logger):
                logger.error('insert_stt_result_to_tb_tm_stt_rst is Fail : document_id -> {0}'.format(document_id))
                raise Exception
        idx += 1
    if not oracle.commit(logger):
        logger.error('insert_stt_result_to_tb_tm_stt_rst is Fail : document_id -> {0}'.format(document_id))
        raise Exception
    try:
        stt_spch_sped_rx = str(round(rx_sntc_len / rx_during_time, 2)) if rx_during_time != 0 else '0'
        stt_spch_sped_tx = str(round(tx_sntc_len / tx_during_time, 2)) if tx_during_time != 0 else '0'
        oracle.update_stt_spch_sped(RCDG_ID_INFO_DICT[pk]['PROJECT_CD'], document_dt, document_id, stt_spch_sped_rx, stt_spch_sped_tx)
    except Exception:
        logger.error(traceback.format_exc())
        raise Exception



def elapsed_time(sdate):
    """
    elapsed time
    :param      sdate:      date object
    :return:                Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]), int(sdate[8:10]), int(sdate[10:12]),
                          int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def statistical_data(logger):
    """
    Statistical data to print
    :param      logger:     Logger
    :return:    logger:     Logger
    """
    required_time = elapsed_time(DT)
    end_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M.%S')
    total_wav_duration = timedelta(seconds=TOTAL_PCM_TIME)
    if PCM_CNT == 0:
        division_pcm_cnt = 1
    else:
        division_pcm_cnt = PCM_CNT
    total_wav_average_duration = timedelta(seconds=TOTAL_PCM_TIME / float(division_pcm_cnt))
    xrt = (int(timedelta(seconds=TOTAL_PCM_TIME).total_seconds() / required_time.total_seconds()))
    logger.info('12. Statistical data print')
    logger.info('Start time                 = {0}'.format(ST))
    logger.info('End time                   = {0}'.format(end_time))
    logger.info('The time required          = {0}'.format(required_time))
    logger.info('WAV count                  = {0}'.format(PCM_CNT))
    logger.info('Result count               = {0}'.format(RESULT_CNT))
    logger.info('Total WAV duration         = {0}'.format(total_wav_duration))
    logger.info('Total WAV average duration = {0}'.format(total_wav_average_duration))
    logger.info('xRT                        = {0} xRT'.format(xrt))
    logger.info('Done STT')
    logger.info('Remove logger handler')
    logger.info('STT END.. Start time = {0}, The time required = {1}'.format(ST, elapsed_time(DT)))


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param      logger:             Logger
    :param      delete_file_path:   Input path
    """
    if os.path.exists(delete_file_path):
        # noinspection PyBroadException
        try:
            if os.path.isfile(delete_file_path):
                logger.debug('delete file -> {0}'.format(delete_file_path))
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                logger.debug('delete directory -> {0}'.format(delete_file_path))
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
    logger.info('11. Delete garbage file')
    for list_file in DELETE_FILE_LIST:
        try:
            logger.info('del_garbage : {0}'.format(list_file))
            del_garbage(logger, list_file)
        except Exception as e:
            logger.error(e)
            continue


def move_output(logger, oracle):
    """
    Move output to STT output path
    :param      logger:             Logger
    :param      oracle:             Oracle
    """
    logger.info("10. Move output to STT output path")

    # Create an output directory for each recording file and move the file.
    for pk in RCDG_ID_INFO_DICT.keys():
        # Create an output directory
        rec_stdt = str(RCDG_ID_INFO_DICT[pk]['CALL_DT'])
        # Make sure the wav directory exists.
        wav_output_path = '{0}/{1}/{2}/{3}'.format(
            CONFIG['wav_output_path'], rec_stdt[:4], rec_stdt[5:7], rec_stdt[8:10])
        if not os.path.exists(wav_output_path):
            os.makedirs(wav_output_path)
        output_dir_path = '{0}/{1}/{2}/{3}'.format(CONFIG['stt_output_path'], rec_stdt[:4], rec_stdt[5:7],
                                                   rec_stdt[8:10])
        output_list = ['mlf', 'unseg', 'do_space', 'txt', 'detail', 'result']
        # Move the file
        for target in output_list:
            output_target_path = '{0}/{1}'.format(output_dir_path, target)
            if not os.path.exists(output_target_path):
                os.makedirs(output_target_path)
            path_list = glob.glob('{0}/{1}/{2}*'.format(
                TARGET_DIR_PATH, target, RCDG_ID_INFO_DICT[pk]['DOCUMENT_ID']))
            for path in path_list:
                file_name = os.path.basename(path)
                enc_path = '{0}.enc'.format(path)
                if os.path.exists('{0}/{1}.enc'.format(output_target_path, file_name)):
                    del_garbage(logger, '{0}/{1}.enc'.format(output_target_path, file_name))
                logger.debug('path {0} -> enc_path {1}'.format(path, enc_path))
                if 0 != scp_enc_file(path, enc_path):
                    logger.error('scp_enc_file ERROR ==> '.format(path))
                    error_process(logger, oracle, pk, '03')
                    continue
                logger.debug('move file {0} -> {1}'.format(enc_path, output_target_path))
                shutil.move(enc_path, output_target_path)

        # Move the wav(gsm) file
        if os.path.exists('{0}/{1}.wav.enc'.format(wav_output_path, RCDG_ID_INFO_DICT[pk]['DOCUMENT_ID'])):
            del_garbage(logger, '{0}/{1}.wav.enc'.format(wav_output_path, RCDG_ID_INFO_DICT[pk]['DOCUMENT_ID']))
        gsm_wav_path = '{0}/wav'.format(TARGET_DIR_PATH)
        gsm_wav_file_path = '{0}/{1}.wav'.format(gsm_wav_path, RCDG_ID_INFO_DICT[pk]['DOCUMENT_ID'])
        if not os.path.exists(gsm_wav_file_path):
            logger.error('gsm_wav_file_path not os.path.exists ==> {0}'.format(gsm_wav_file_path))
            error_process(logger, oracle, pk, '03')
            continue
        else:
            gsm_wav_enc_file_path = '{0}.enc'.format(gsm_wav_file_path)
            if os.path.exists(gsm_wav_enc_file_path):
                del_garbage(logger, gsm_wav_enc_file_path)
            if 0 != scp_enc_file(gsm_wav_file_path, gsm_wav_enc_file_path):
                logger.error('gsm_wav_path not os.path.exists ==> ' + gsm_wav_path)
                error_process(logger, oracle, pk, '03')
                continue
            shutil.move(gsm_wav_enc_file_path, wav_output_path)


def make_db_upload_output(logger, oracle):
    """
    Make DB upload output
    :param      logger:                 Logger
    :param      oracle:                 Oracle
    """
    global RCDG_ID_INFO_DICT
    logger.info('9. Make DB upload output')
    # Create DB upload file.
    for pk in RCDG_ID_INFO_DICT.keys():
        try:
            RCDG_ID_INFO_DICT[pk]['STT_PRGST_CD'] = '05'
            # 8-1. Create db_upload txt of TB_QA_STT_TM_INFO table
            make_stt_info_table_txt(logger, pk, oracle)
            # 8-2. Create db upload txt of STT_RST table
            make_stt_result_table_txt(logger, pk, oracle)
        except Exception:
            RCDG_ID_INFO_DICT[pk]['STT_PRGST_CD'] = '03'
            error_process(logger, oracle, pk, '03')
            make_stt_info_table_txt(logger, pk, oracle)
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            delete_garbage_file(logger)
            raise Exception


def set_output(logger):
    """
    Set output directory
    :param      logger:     Logger
    """
    global RESULT_CNT
    logger.info("8. Set output directory")
    file_path_list = glob.glob('{0}/*'.format(TARGET_DIR_PATH))
    pcm_dir_path = '{0}/pcm'.format(TARGET_DIR_PATH)
    result_dir_path = '{0}/result'.format(TARGET_DIR_PATH)
    # Moving files
    if not os.path.exists(pcm_dir_path):
        os.makedirs(pcm_dir_path)
    if not os.path.exists(result_dir_path):
        os.makedirs(result_dir_path)
    for file_path in file_path_list:
        if check_file('.pcm', file_path):
            shutil.move(file_path, pcm_dir_path)
        if check_file('.result', file_path):
            shutil.move(file_path, result_dir_path)
    # Calculate result value
    RESULT_CNT = len(glob.glob('{0}/*.result'.format(result_dir_path)))


def make_output(logger, oracle, do_space_dir_path):
    """
    Make txt file and detail file
    :param      logger:                     Logger
    :param      oracle:                     DB
    :param      do_space_dir_path:          directory path of do space result
    """
    logger.info('7. Make output [txt file and detailed file]')
    # Create directory
    logger.info('Create directory')
    txt_dir_path = '{0}/txt'.format(TARGET_DIR_PATH)
    detail_dir_path = '{0}/detail'.format(TARGET_DIR_PATH)
    if not os.path.exists(txt_dir_path):
        os.makedirs(txt_dir_path)
    if not os.path.exists(detail_dir_path):
        os.makedirs(detail_dir_path)
    # Create txt & detail file
    logger.info('Create txt & detail file')
    for pk in RCDG_ID_INFO_DICT.keys():
        document_id, document_dt, before_prgst_cd = pk.split('####')
        output_dict = dict()
        chn_tp = RCDG_ID_INFO_DICT[pk]['CHN_TP']
        if chn_tp == 'S':
            # Check that both stt files exist.
            logger.info('Check that two stt files exist')
            rx_file_path = '{0}/{1}_rx.stt'.format(do_space_dir_path, document_id)
            tx_file_path = '{0}/{1}_tx.stt'.format(do_space_dir_path, document_id)
            if os.path.exists(rx_file_path) and os.path.exists(tx_file_path):
                logger.info('two stt files are exist')
                # Save the necessary information.
                rx_file = open(rx_file_path, 'r')
                tx_file = open(tx_file_path, 'r')
                output_dict = modify_time_info(logger, '[A]', tx_file, output_dict)
                output_dict = modify_time_info(logger, '[C]', rx_file, output_dict)
                tx_file.close()
                rx_file.close()
            else:
                logger.error("{0} don't have tx or rx file.".format(document_id))
                continue
            # Detailed txt & detail file creation.
            output_dict_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
            txt_output_file = open('{0}/{1}_trx.txt'.format(txt_dir_path, document_id), 'w')
            detail_output_file = open(
                '{0}/{1}_trx.detail'.format(detail_dir_path, document_id), 'w')
            for line_list in output_dict_list:
                detail_line = line_list[1]
                detail_line_list = detail_line.split('\t')
                trx_txt = "{0}{1}".format(detail_line_list[0], detail_line_list[3])
                print >> txt_output_file, trx_txt
                print >> detail_output_file, detail_line
            txt_output_file.close()
            detail_output_file.close()
        else:
            file_path = '{0}/{1}.stt'.format(do_space_dir_path, document_id)
            if os.path.exists(file_path):
                stt_file = open(file_path, 'r')
                txt_output_file = open('{0}/{1}.txt'.format(txt_dir_path, document_id), 'w')
                detail_output_file = open('{0}/{1}.detail'.format(detail_dir_path, document_id), 'w')
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
                logger.error("{0} don't have stt file.".format(document_id))
                error_process(logger, oracle, pk, '03')


def execute_unseg_and_do_space(logger, oracle):
    """
    Execute unseg.exe and do_space.exe
    :param      logger:                 Logger
    :param      oracle:                 Oracle
    :return:    Output                  directory path
    """
    logger.info("6. Execute unseg.exe and do_space.exe")
    mlf_file_path_list = glob.glob('{0}/*.mlf'.format(TARGET_DIR_PATH))
    for pk in RCDG_ID_INFO_DICT.keys():
        document_id, document_dt, before_prgst_cd = pk.split('####')
        chn_tp = RCDG_ID_INFO_DICT[pk]['CHN_TP']
        search_target_mlf = glob.glob('{0}/{1}*.mlf'.format(
            TARGET_DIR_PATH, document_id))
        if chn_tp == 'S':
            if len(search_target_mlf) == 2:
                continue
        else:
            if len(search_target_mlf) == 1:
                continue

        logger.info('document_id: {0}    document_dt : {1}'.format(document_id, document_dt))
        logger.info('create mlf file : {0}'.format(search_target_mlf))
        error_process(logger, oracle, pk, '03')
    mlf_dir_path = '{0}/mlf'.format(TARGET_DIR_PATH)
    unseg_dir_path = '{0}/unseg'.format(TARGET_DIR_PATH)
    do_space_dir_path = '{0}/do_space'.format(TARGET_DIR_PATH)
    os.chdir(CONFIG['stt_tool_path'])
    # Moving the mlf file
    logger.info('Moving the mlf file')
    if not os.path.exists(mlf_dir_path):
        os.makedirs(mlf_dir_path)
    for mlf_file_path in mlf_file_path_list:
        # noinspection PyBroadException
        try:
            shutil.move(mlf_file_path, mlf_dir_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't move mlf file {0} -> {1}".format(mlf_file_path, mlf_dir_path))
            logger.error(exc_info)
            continue
    # Run ./unseg.exe
    logger.info('Run ./unseg.exe')
    if not os.path.exists(unseg_dir_path):
        os.makedirs(unseg_dir_path)
    unseg_cmd = './unseg.exe -d {mp} {up} 300'.format(mp=mlf_dir_path, up=unseg_dir_path)
    sub_process(logger, unseg_cmd)
    # Run ./do_space.exe
    logger.info('Run ./do_space.exe')
    if not os.path.exists(do_space_dir_path):
        os.makedirs(do_space_dir_path)
    do_space_cmd = './do_space.exe {up} {dp}'.format(up=unseg_dir_path, dp=do_space_dir_path)
    sub_process(logger, do_space_cmd)
    return do_space_dir_path


def execute_dnn(logger, thread_cnt):
    """
    Execute DNN (mt_long_utt_dnn_support.gpu.exe)
    :param      logger:         Logger
    :param      thread_cnt:     Thread count
    """
    logger.info("5. Execute DNN (mt_long_utt_dnn_support.gpu.exe)")
    os.chdir(CONFIG['stt_path'])
    dnn_thread = thread_cnt if thread_cnt < CONFIG['thread'] else CONFIG['thread']
    cmd = "./mt_long_utt_dnn_support.gpu.exe {tn} {th} 1 1 {gpu} 128 0.8".format(
        tn=TARGET_DIR_NAME, th=dnn_thread, gpu=CONFIG['gpu'])
    sub_process(logger, cmd)


def load_pcm_file(logger, oracle, rcdg_id_info_dic):
    """
    Load pcm file
    :param      logger:             Logger
    :param      oracle:             Oracle
    :param      rcdg_id_info_dic:   Information dictionary of recording id
    :return:                        modified Information dictionary of recording id
    """
    logger.info("4. Load pcm file")
    for pk in rcdg_id_info_dic:
        document_id, document_dt, before_prgst_cd = pk.split('####')
        pcm_dir_path = '{0}/{1}'.format(TARGET_DIR_PATH, document_dt[:4] + document_dt[5:7] + document_dt[8:10])
        # pcm directory exist check
        logger.debug('Recording start date -> {0}'.format(document_dt))
        if not os.path.exists(pcm_dir_path):
            logger.error('pcm directory is not exist -> {0}'.format(pcm_dir_path))
            error_process(logger, oracle, pk, '03')
            continue
        # All pcm file exist check
        rx_incident_file_path = '{0}/{1}_rx.wav'.format(pcm_dir_path, rcdg_id_info_dic[pk]['DOCUMENT_ID'])
        tx_incident_file_path = '{0}/{1}_tx.wav'.format(pcm_dir_path, rcdg_id_info_dic[pk]['DOCUMENT_ID'])
        rename_rx_pcm_file_path = '{0}/{1}_rx.pcm'.format(TARGET_DIR_PATH, rcdg_id_info_dic[pk]['DOCUMENT_ID'])
        rename_tx_pcm_file_path = '{0}/{1}_tx.pcm'.format(TARGET_DIR_PATH, rcdg_id_info_dic[pk]['DOCUMENT_ID'])
        if os.path.exists(rx_incident_file_path) and os.path.exists(tx_incident_file_path):
            logger.debug('All pcm file is exists in incident_file')
            shutil.copy(rx_incident_file_path, rename_rx_pcm_file_path)
            shutil.copy(tx_incident_file_path, rename_tx_pcm_file_path)
        else:
            logger.debug('pcm file is not exists')
            logger.error(' {0} is not exist in rec_server'.format(document_id))
            error_process(logger, oracle, pk, '03')
            continue
    return rcdg_id_info_dic


def make_gsm(logger, rec_file_path, gsm_wav_file_path, sox_channel):
    """
    Make GSM wav
    :param          logger:                         Logger
    :param          rec_file_path:                  rec_file_path
    :param          gsm_wav_file_path:              gsm_wav_file_path
    :param          sox_channel:                    sox channel option
    """
    # test : sox -r 8000 -c 2 a.wav -r 8000 -c 2 -e gsm a_test.wav
    logger.info("4-1. Do make GSM wav file")
    if os.path.exists(gsm_wav_file_path):
        del_garbage(logger, gsm_wav_file_path)
    cmd = "{st}/sox -r 8000 -c {sox_channel} {rec} -r 8000 -c {sox_channel} -e gsm {gsm}".format(
        st=CONFIG['stt_tool_path'], rec=rec_file_path, gsm=gsm_wav_file_path, sox_channel=sox_channel)
    sub_process(logger, cmd)


def make_gsm_wav_file(logger):
    """
    Make GSM wav file
    :param              logger:             Logger
    :return                                 Thread count
    """
    logger.info("4. Do make GSM wav file")
    wav_dir_path = '{0}/wav'.format(TARGET_DIR_PATH)
    if not os.path.exists(wav_dir_path):
        os.makedirs(wav_dir_path)

    for pk in RCDG_ID_INFO_DICT.keys():
        document_id, document_dt, before_prgst_cd = pk.split('####')

        wav_file_name = document_id + ".wav"
        decrypt_wav_path = '{0}/decrypt_wav'.format(TARGET_DIR_PATH)
        decrypt_wav_file_path = "{0}/{1}".format(decrypt_wav_path, wav_file_name)

        gsm_wav_file_path = '{0}/{1}'.format(wav_dir_path, wav_file_name)
        chn_tp = RCDG_ID_INFO_DICT[pk]['CHN_TP']
        if chn_tp == 'S':
            make_gsm(logger, decrypt_wav_file_path, gsm_wav_file_path, 2)
        else:
            make_gsm(logger, decrypt_wav_file_path, gsm_wav_file_path, 1)


def make_pcm(logger, wav_file_path, pcm_file_path):
    """
    Make PCM file
    :param          logger:                     Logger
    :param          wav_file_path:              wav file path
    :param          pcm_file_path:              pcm file path
    """
    # If PCM file already existed remove file
    logger.info("3-1. Do make pcm file")
    if os.path.exists(pcm_file_path):
        del_garbage(logger, pcm_file_path)
    cmd = "{st}/sox -t wav {wav} -r 8000 -b 16 -t raw {pcm}".format(
        st=CONFIG['stt_tool_path'], wav=wav_file_path, pcm=pcm_file_path)
    sub_process(logger, cmd)


def make_pcm_file_and_list_file(logger):
    """
    MAke PCM and list file
    :param              logger:             Logger
    :return                                 Thread count
    """
    global PCM_CNT
    global TOTAL_PCM_TIME
    global DELETE_FILE_LIST
    logger.info("3. Do make PCM and list file")
    list_file_cnt = 0
    max_list_file_cnt = 0
    for pk in RCDG_ID_INFO_DICT.keys():
        document_id, document_dt, before_prgst_cd = pk.split('####')
        # Target directory path 하위에 있는 대상 파일만 실행.
        logger.info("3-1. Do make pcm file")
        # Enter the PCM file name in the List file.
        logger.debug('Enter the PCM file name in the List file')
        list_file_path = "{sp}/{tn}_n{cnt}.list".format(
            sp=CONFIG['stt_path'], tn=TARGET_DIR_NAME, cnt=list_file_cnt)
        curr_list_file_path = "{sp}/{tn}_n{cnt}_curr.list".format(
            sp=CONFIG['stt_path'], tn=TARGET_DIR_NAME, cnt=list_file_cnt)
        DELETE_FILE_LIST.append(list_file_path)
        DELETE_FILE_LIST.append(curr_list_file_path)
        output_file_div = open(list_file_path, 'a')
        chn_tp = RCDG_ID_INFO_DICT[pk]['CHN_TP']
        if chn_tp == 'S':
            rx_wav_file_path = "{fp}/{sn}_rx.wav".format(fp=TARGET_DIR_PATH, sn=document_id)
            rx_pcm_file_path = "{fp}/{sn}_rx.pcm".format(fp=TARGET_DIR_PATH, sn=document_id)
            make_pcm(logger, rx_wav_file_path, rx_pcm_file_path)

            tx_wav_file_path = "{fp}/{sn}_tx.wav".format(fp=TARGET_DIR_PATH, sn=document_id)
            tx_pcm_file_path = "{fp}/{sn}_tx.pcm".format(fp=TARGET_DIR_PATH, sn=document_id)
            make_pcm(logger, tx_wav_file_path, tx_pcm_file_path)

            print >> output_file_div, tx_pcm_file_path
            print >> output_file_div, rx_pcm_file_path

            TOTAL_PCM_TIME += os.stat(tx_pcm_file_path)[6] / 16000.0
            TOTAL_PCM_TIME += os.stat(rx_pcm_file_path)[6] / 16000.0
            PCM_CNT += 2
        else:
            wav_file_path = "{fp}/{sn}.wav".format(fp=TARGET_DIR_PATH, sn=document_id)
            pcm_file_path = "{fp}/{sn}.pcm".format(fp=TARGET_DIR_PATH, sn=document_id)
            make_pcm(logger, wav_file_path, pcm_file_path)
            print >> output_file_div, pcm_file_path
            TOTAL_PCM_TIME += os.stat(pcm_file_path)[6] / 16000.0
            PCM_CNT += 1
        output_file_div.close()

        # Calculate the result value.
        logger.debug('Calculate the result value')

        # Calculate the thread
        if list_file_cnt > max_list_file_cnt:
            max_list_file_cnt = list_file_cnt
        if list_file_cnt + 1 == CONFIG['thread']:
            list_file_cnt = 0
            continue
        list_file_cnt += 1

    # PCM error check
    w_ob = os.walk(TARGET_DIR_PATH)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file('.pcm.err', file_name):
                serial_number = os.path.splitext(file_name)[0]
                err_pcm = "{tn}/{sn}.pcm.err".format(tn=TARGET_DIR_PATH, sn=serial_number)
                if os.path.exists(err_pcm):
                    err_dir = "{0}/err_pcm".format(TARGET_DIR_PATH)
                    if not os.path.exists(err_dir):
                        os.makedirs(err_dir)
                    del_garbage(logger, err_pcm)
                    shutil.move(file_name, err_dir)
                    logger.error("Error pcm !! -> {0}".format(file_name))

    # Last Calculate the thread count
    logger.debug('Calculate the thread count')
    if max_list_file_cnt == 0:
        thread_cnt = 1
    else:
        thread_cnt = max_list_file_cnt + 1
    return thread_cnt


def separation_wav_file(logger):
    """
    Separation wav file
    :param              logger:                   Logger
    """
    logger.info("2. Do separation wav file")
    for pk in RCDG_ID_INFO_DICT.keys():
        document_id, document_dt, before_prgst_cd = pk.split('####')

        decrypt_wav_path = '{0}/decrypt_wav'.format(TARGET_DIR_PATH)
        rec_file_name = document_id + ".wav"
        rec_file_path = '{0}/{1}'.format(decrypt_wav_path, rec_file_name)

        wav_file_path = '{0}'.format(TARGET_DIR_PATH)
        if RCDG_ID_INFO_DICT[pk]['CHN_TP'] == 'S':
            if not os.path.exists(wav_file_path):
                os.makedirs(wav_file_path)

            rx_file_path = "{fp}/{sn}_rx.wav".format(fp=wav_file_path, sn=document_id)
            tx_file_path = "{fp}/{sn}_tx.wav".format(fp=wav_file_path, sn=document_id)
            # If rx.wav or tx.wav file is already existed remove file
            del_garbage(logger, rx_file_path)
            del_garbage(logger, tx_file_path)
            cmd = '{stt_tool}/ffmpeg'.format(stt_tool=CONFIG['stt_tool_path'])
            cmd += ' -i {wp}'.format(wp=rec_file_path)
            cmd += ' -filter_complex "[0:0]pan=1c|c0=c0[left];[0:0]pan=1c|c0=c1[right]"'
            cmd += ' -map "[right]" {rx}'.format(rx=rx_file_path)  # 고객
            cmd += ' -map "[left]" {tx}'.format(tx=tx_file_path)  # 상담원
            sub_process(logger, cmd)
        if RCDG_ID_INFO_DICT[pk]['CHN_TP'] == 'M':
            copy_wav_file_path = "{0}/{1}".format(wav_file_path, rec_file_name)
            shutil.copy(rec_file_path, copy_wav_file_path)
        # Delete stereo wav file
        # del_garbage(rec_file_path)


def decrypt_wav_file(logger, oracle):
    """
    Decrypt WAV file
    :param          logger:             Logger
    :param          oracle:             DB
    """
    logger.info("2. Do decrypt wav file")
    for pk in RCDG_ID_INFO_DICT.keys():
        document_id, document_dt, before_prgst_cd = pk.split('####')
        base_path = ''
        project_cd = RCDG_ID_INFO_DICT[pk]['PROJECT_CD']
        if project_cd == 'TM':
            base_path = '{0}/{1}'.format(
                CONFIG['rec_dir_path'], document_dt[:4] + document_dt[5:7] + document_dt[8:10])
        elif project_cd == 'CD':
            base_path = '{0}/{1}/{2}/{3}'.format(
                CONFIG['card_rec_dir_path'], document_dt[:4], document_dt[5:7], document_dt[8:10])

        rec_file_name = document_id + ".wav"
        rec_file_path = '{0}/{1}.enc'.format(base_path, rec_file_name)

        decrypt_wav_path = '{0}/decrypt_wav'.format(TARGET_DIR_PATH)
        decrypt_wav_file_path = "{0}/{1}".format(decrypt_wav_path, rec_file_name)
        if not os.path.exists(decrypt_wav_path):
            os.makedirs(decrypt_wav_path)

        if 0 != scp_dec_file(rec_file_path, decrypt_wav_file_path):
            logger.error('scp_dec_file ERROR ==> '.format(rec_file_path))
            error_process(logger, oracle, pk, '03')
            continue


def get_rcdg_info_dic(logger, oracle, pk_list):
    """
    Get recording information dictionary for recording id list
    :param      logger:                 Logger
    :param      oracle:                 Oracle
    :param      pk_list:                List of Primary key
    :return:                            Information dictionary of recording id
    """
    global RCDG_ID_INFO_DICT
    logger.info('1. Get recording information dictionary')
    logger.info(' load primary key list -> {0}'.format(pk_list))
    # Creating recording file dictionary
    for pk in pk_list:
        document_id, document_dt, before_prgst_cd = pk.split('####')
        # noinspection PyBroadException
        try:
            row = oracle.find_call_meta(logger, document_id, document_dt)
            logger.debug('document_id  [{0}] information'.format(document_id))
            RCDG_ID_INFO_DICT[pk] = {
                'PROJECT_CD': row.get('PROJECT_CD'),
                'DOCUMENT_DT': row.get('DOCUMENT_DT'),
                'DOCUMENT_ID': row.get('DOCUMENT_ID'),
                'CALL_TYPE': row.get('CALL_TYPE'),
                'CUSTOMER_ID': row.get('CUSTOMER_ID'),
                'AGENT_ID': row.get('AGENT_ID'),
                'GROUP_ID': row.get('GROUP_ID'),
                'CONTRACT_NO': row.get('CONTRACT_NO'),
                'REC_ID': row.get('REC_ID'),
                'BRANCH_CD': row.get('BRANCH_CD'),
                'CALL_DT': row.get('CALL_DT'),
                'START_DTM': row.get('START_DTM'),
                'END_DTM': row.get('END_DTM'),
                'DURATION': row.get('DURATION'),
                'CHN_TP': row.get('CHN_TP'),
                'STATUS': row.get('STATUS'),
                'EXTENSION_PHONE_NO': row.get('EXTENSION_PHONE_NO'),
                'STT_PRGST_CD': row.get('STT_PRGST_CD'),
                'STT_REQ_DTM': row.get('STT_REQ_DTM'),
                'STT_CM_DTM': row.get('LST_CHG_DTM')
            }
            if not oracle.update_prgst_cd_to_call_meta(logger, pk, '02'):
                raise Exception
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error(
                "Can't find '{0}(document_id)' & {1}(document_dt) & {2}(before_prgst_cd) in DB".format(document_id,
                                                                                                       document_dt,
                                                                                                       before_prgst_cd))
            RCDG_ID_INFO_DICT[pk]['STT_PRGST_CD'] = '03'
            # 8-1. Create db_upload txt of TB_QA_STT_TM_INFO table
            make_stt_info_table_txt(logger, pk, oracle)
            error_process(logger, oracle, pk, '03')


def processing(oracle, pk_list):
    """
    STT processing
    :param      oracle:         Oracle
    :param      pk_list:        Primary id list
    """
    global RCDG_ID_INFO_DICT
    global TARGET_DIR_NAME
    global TARGET_DIR_PATH
    global DELETE_FILE_LIST
    cnt = 0
    # Determine temp directory name to be used in script
    while True:
        TARGET_DIR_PATH = "{0}/temp_directory_{1}".format(CONFIG['stt_path'], cnt)
        if not os.path.exists(TARGET_DIR_PATH):
            os.makedirs(TARGET_DIR_PATH)
            TARGET_DIR_NAME = os.path.basename(TARGET_DIR_PATH)
            DELETE_FILE_LIST.append(TARGET_DIR_PATH)
            break
        cnt += 1
    # Determining log_name
    log_name = '{0}_{1}'.format(DT[:8], cnt)
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': log_name,
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start STT')

    # noinspection PyBroadException
    try:
        # 1. Get recording information dictionary for recording id list
        get_rcdg_info_dic(logger, oracle, pk_list)
        # 2. decrypt_wav_file
        decrypt_wav_file(logger, oracle)
        # 2. separation_wav_file
        separation_wav_file(logger)
        # 3. make_pcm_file
        thread_cnt = make_pcm_file_and_list_file(logger)
        # 4. Make GSM file : 청취용 음성 파일 생성
        make_gsm_wav_file(logger)
        # 5. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 6. Execute unseg.exe and do_space.exe
        do_space_dir = execute_unseg_and_do_space(logger, oracle)
        # 7. make output
        make_output(logger, oracle, do_space_dir)
        # 8. Set output
        set_output(logger)
        # 9. Make DB upload output
        make_db_upload_output(logger, oracle)
        # 10. Move output
        move_output(logger, oracle)
        # 11. Delete garbage list
        delete_garbage_file(logger)
        # 12. Print statistical data
        statistical_data(logger)
        # Update prgst code
        for pk in RCDG_ID_INFO_DICT.keys():
            if not oracle.update_prgst_cd_to_call_meta(logger, pk, '05'):
                raise Exception
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error('---------- ERROR ----------')
        for pk in RCDG_ID_INFO_DICT.keys():
            RCDG_ID_INFO_DICT[pk]['STT_PRGST_CD'] = '03'
            make_stt_info_table_txt(logger, pk, oracle)
            error_process(logger, oracle, pk, '03')
        delete_garbage_file(logger)
        sys.exit(1)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


def set_config(config_type):
    """
    active type setting
    :param config_type:
    :return:
    """
    global ORACLE_DB_CONFIG
    global CONFIG
    global MASKING_CONFIG

    ORACLE_DB_CONFIG = cfg.config.ORACLE_DB_CONFIG[config_type]
    CONFIG = cfg.config.CONFIG[config_type]
    MASKING_CONFIG = cfg.config.MASKING_CONFIG


########
# main #
########
def main(pk_list, config_type):
    """
    This is a program that execute STT
    :param      pk_list:            Primary key list
    :param      config_type:        Config Type
    """
    global DT
    global ST
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')

    set_config(config_type)

    oracle = oracle_connect()
    try:
        processing(oracle, pk_list)
        oracle.disconnect()
    except Exception:
        oracle.disconnect()
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)
