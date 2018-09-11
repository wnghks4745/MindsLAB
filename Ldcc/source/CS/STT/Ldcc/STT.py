#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-06, modification: 2018-08-06"

###########
# imports #
###########
import os
import re
import sys
import time
import glob
import shutil
import MySQLdb
import traceback
import subprocess
import collections
from datetime import datetime, timedelta
from operator import itemgetter
from lib.iLogger import set_logger
from lib.openssl import encrypt, encrypt_file
sys.path.append('/app/MindsVOC/CS')
from service.config import STT_CONFIG, MYSQL_DB_CONFIG, MASKING_CONFIG

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
STT_TEMP_DIR_PATH = ""
STT_TEMP_DIR_NAME = ""
MASKING_INFO_LIT = dict()
MLF_INFO_DICT = dict()
STT_KEYWORD_DTC_RST = dict()


#########
# class #
#########
class MySQL(object):
    def __init__(self, logger):
        self.logger = logger
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['passwd'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def insert_stt_rst(self, inset_set_dict):
        try:
            sql = """
                INSERT INTO
                    STT_RST
                    (
                        RECORDKEY,
                        RFILE_NAME,
                        STT_SNTC_LIN_NO,
                        STT_SNTC_CONT,
                        STT_SNTC_STTM,
                        STT_SNTC_ENDTM,
                        STT_SNTC_SPKR_DCD,
                        STT_SNTC_SPCH_TM,
                        STT_SNTC_SPCH_SPED,
                        MSK_DTC_YN,
                        MSK_INFO_LIT,
                        SILENCE_YN,
                        SILENCE_TIME,
                        CROSSTALK_YN,
                        CROSSTALK_TIME,
                        CREATOR_ID,
                        CREATED_DTM,
                        UPDATOR_ID,
                        UPDATED_DTM
                    )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    'STT', NOW(), 'STT',
                    NOW()
                )
            """
            values_list = list()
            for insert_dict in inset_set_dict.values():
                recordkey = insert_dict['RECORDKEY']
                rfile_name = insert_dict['RFILE_NAME']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                stt_sntc_cont = insert_dict['STT_SNTC_CONT']
                stt_sntc_sttm = insert_dict['STT_SNTC_STTM']
                stt_sntc_endtm = insert_dict['STT_SNTC_ENDTM']
                stt_sntc_spkr_dcd = insert_dict['STT_SNTC_SPKR_DCD']
                stt_sntc_spch_tm = insert_dict['STT_SNTC_SPCH_TM']
                stt_sntc_spch_sped = insert_dict['STT_SNTC_SPCH_SPED']
                msk_dtc_yn = insert_dict['MSK_DTC_YN']
                msk_info_lit = insert_dict['MSK_INFO_LIT']
                silence_yn = insert_dict['SILENCE_YN']
                silence_time = insert_dict['SILENCE_TIME']
                crosstalk_yn = insert_dict['CROSSTALK_YN']
                crosstalk_time = insert_dict['CROSSTALK_TIME']
                values_tuple = (
                    recordkey, rfile_name, stt_sntc_lin_no, stt_sntc_cont, stt_sntc_sttm, stt_sntc_endtm,
                    stt_sntc_spkr_dcd, stt_sntc_spch_tm, stt_sntc_spch_sped, msk_dtc_yn, msk_info_lit, silence_yn,
                    silence_time, crosstalk_yn, crosstalk_time
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

    def update_stt_spch_sped(self, recordkey, rfile_name, stt_spch_sped, issue_dtc_yn, prohibit_dtc_yn):
        try:
            query = """
                UPDATE
                    STT_RCDG_INFO
                SET
                    STT_SPCH_SPED = %s,
                    ISSUE_DTC_YN = %s,
                    PROHIBIT_DTC_YN = %s
                WHERE 1=1
                    AND RECORDKEY = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                stt_spch_sped,
                issue_dtc_yn,
                prohibit_dtc_yn,
                recordkey,
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
            raise Exception(traceback.format_exc())

    def insert_data_to_stt_rst_full(self, **kwargs):
        try:
            query = """
                INSERT INTO STT_RST_FULL
                (
                    RECORDKEY,
                    RFILE_NAME,
                    STT_CONT,
                    CREATOR_ID,
                    CREATED_DTM,
                    UPDATOR_ID,
                    UPDATED_DTM
                )
                VALUES (
                    %s, %s, %s, 'STT', NOW(),
                    'STT', NOW()
                )
            """
            bind = (
                kwargs.get('recordkey'),
                kwargs.get('rfile_name'),
                kwargs.get('stt_mlf'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            exc_info = traceback.format_exc()
            self.conn.rollback()
            raise Exception(exc_info)

    def update_stt_prgst_cd(self, stt_prgst_cd, recordkey, rfile_name):
        try:
            query = """
                UPDATE
                    STT_RCDG_INFO
                SET
                    STT_PRGST_CD = %s,
                    UPDATOR_ID = 'STT',
                    UPDATED_DTM = NOW()
                WHERE 1=1
                    AND RECORDKEY = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                stt_prgst_cd,
                recordkey,
                rfile_name
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

    def update_stt_cmdtm(self, recordkey, rfile_name):
        try:
            query = """
                UPDATE
                    STT_RCDG_INFO
                SET
                    STT_CMDTM = NOW(),
                    UPDATOR_ID = 'STT',
                    UPDATED_DTM = NOW()
                WHERE 1=1
                    AND RECORDKEY = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                recordkey,
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
            raise Exception(traceback.format_exc())

    def delete_stt_rst(self, recordkey, rfile_name):
        try:
            query = """
                DELETE FROM
                    STT_RST
                WHERE 1=1
                    AND RECORDKEY = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                recordkey,
                rfile_name
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

    def delete_data_to_stt_rst_full(self, recordkey, rfile_name):
        try:
            query = """
                DELETE FROM
                    STT_RST_FULL
                WHERE 1=1
                    AND RECORDKEY = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                recordkey,
                rfile_name
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

    def select_tb_qa_except(self, stt_dict_cd):
        query = """
            SELECT DISTINCT
                B.KEYWORD
            FROM
                TA_LOTTE.TB_QA_EXCEPT_DICT A,
                TA_LOTTE.TB_QA_EXCEPT_DT_INFO B
            WHERE 1=1
                AND A.DICT_ID = B.DICT_ID
                AND A.STT_DICT_CD = %s
                AND B.USE_YN = 'Y'
                AND B.DEL_F = 'N'
        """
        bind = (
            stt_dict_cd,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return list()
        return results

    def delete_data_to_stt_keyword_dtc_rst(self, recordkey, rfile_name):
        try:
            query = """
                DELETE FROM
                    STT_KEYWORD_DTC_RST
                WHERE 1=1
                    AND RECORDKEY = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                recordkey,
                rfile_name
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

    def insert_data_to_stt_keyword_dtc_rst(self, **kwargs):
        try:
            query = """
                INSERT INTO STT_KEYWORD_DTC_RST
                (
                    RECORDKEY,
                    RFILE_NAME,
                    STT_SNTC_LIN_NO,
                    DTC_CD,
                    DTC_KWD,
                    STT_SNTC_SPKR_DCD,
                    STT_SNTC_CONT,
                    STT_SNTC_STTM,
                    STT_SNTC_ENDTM,
                    CREATOR_ID,
                    CREATED_DTM,
                    UPDATOR_ID,
                    UPDATED_DTM
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    'STT', NOW(),
                    'STT', NOW()
                )
            """
            bind = (
                kwargs.get('recordkey'),
                kwargs.get('rfile_name'),
                kwargs.get('stt_sntc_lin_no'),
                kwargs.get('dtc_cd'),
                kwargs.get('dtc_kwd'),
                kwargs.get('stt_sntc_spkr_dcd'),
                kwargs.get('stt_sntc_cont'),
                kwargs.get('stt_sntc_sttm'),
                kwargs.get('stt_sntc_endtm'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            exc_info = traceback.format_exc()
            self.conn.rollback()
            raise Exception(exc_info)


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
            elif os.path.isdir(delete_file_path):
                shutil.rmtree(delete_file_path)
            else:
                logger.error('not exists path : {0}'.format(delete_file_path))
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:         Logger
    """
    logger.info("13. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception:
            continue


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:         Logger
    :param      cmd:            Command
    :return:                    Response out
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


def error_process(logger, mysql, recordkey, rfile_name, stt_prgst_cd, biz_cd):
    """
    Error process
    :param      logger:                 Logger
    :param      mysql:                  MySQL db
    :param      recordkey:              RECORDKEY(녹취키)
    :param      rfile_name:             RFILE_NAME(녹취파일명)
    :param      stt_prgst_cd:           STT_PRGST_CD(STT 진행상태코드)
    :param      biz_cd:                 BIZ_CD(업체구분코드)
    """
    logger.error("Error process")
    logger.error("RECORDKEY = {0}, RFILE_NAME = {1}, change STT_PRGST_CD = {2}".format(
        recordkey, rfile_name, stt_prgst_cd))
    mysql.update_stt_prgst_cd(stt_prgst_cd, recordkey, rfile_name)
    rec_path = '{0}/{1}'.format(STT_CONFIG['rec_dir_path'], biz_cd)
    if not stt_prgst_cd == '00':
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
    logger.info("14. Statistical data print")
    logger.info("\tStart time                   = {0}".format(ST))
    logger.info("\tEnd time                     = {0}".format(end_time))
    logger.info("\tThe time required            = {0}".format(required_time))
    logger.info("\tWAV count                    = {0}".format(WAV_CNT))
    logger.info("\tResult count                 = {0}".format(RESULT_CNT))
    logger.info("\tTotal WAV duration           = {0}".format(total_wav_duration))
    logger.info("\tTotal WAV average duration   = {0}".format(total_wav_average_duration))
    logger.info("\txRT                          = {0} xRT".format(xrt))
    logger.info("Done STT")
    logger.info("Remove logger handler")
    logger.info("STT END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))


def move_output(logger):
    """
    Move output to STT output path
    :param      logger:             Logger
    """
    global RCDG_INFO_DICT
    logger.info("12. Move output to STT output path")
    for info_dict in RCDG_INFO_DICT.values():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        chn_tp = info_dict['CHN_TP']
        call_start_time = str(info_dict['CALL_START_TIME']).strip()
        output_dir_path = '{0}/{1}/{2}/{3}/{4}-{5}'.format(
            STT_CONFIG['stt_output_path'], call_start_time[:4],
            call_start_time[5:7], call_start_time[8:10], recordkey, rfile_name)
        output_dict = {
            'mlf': {'ext': 'mlf', 'merge': 'N'},
            'unseg': {'ext': 'stt', 'merge': 'N'},
            'do_space': {'ext': 'stt', 'merge': 'N'},
            'txt': {'ext': 'txt', 'merge': 'Y'},
            'detail': {'ext': 'detail', 'merge': 'Y'},
            'result': {'ext': 'result', 'merge': 'N'},
            'masking': {'ext': 'detail', 'merge': 'Y'}
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
            encrypt(output_target_path)
            logger.info('encrypt {0}'.format(output_target_path))


def update_stt_keyword_dtc_rst(logger, mysql):
    """
    Update data to STT_KEYWORD_DTC_RST
    :param      logger:         Logger
    :param      mysql:          MySQL DB
    :return:
    """
    logger.info("11-4. DB upload STT_KEYWORD_DTC_RST")
    del_check_dict = dict()
    for info_dict in STT_KEYWORD_DTC_RST.values():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        stt_sntc_lin_no = info_dict['STT_SNTC_LIN_NO']
        dtc_cd = info_dict['DTC_CD']
        dtc_kwd = info_dict['DTC_KWD']
        stt_sntc_spkr_dcd = info_dict['STT_SNTC_SPKR_DCD']
        stt_sntc_cont = info_dict['STT_SNTC_CONT']
        stt_sntc_sttm = info_dict['STT_SNTC_STTM']
        stt_sntc_endtm = info_dict['STT_SNTC_ENDTM']
        key = '{0}_{1}'.format(recordkey, rfile_name)
        if key not in del_check_dict:
            del_check_dict[key] = 1
            mysql.delete_data_to_stt_keyword_dtc_rst(recordkey, rfile_name)
        mysql.insert_data_to_stt_keyword_dtc_rst(
            recordkey=recordkey,
            rfile_name=rfile_name,
            stt_sntc_lin_no=stt_sntc_lin_no,
            dtc_cd=dtc_cd,
            dtc_kwd=dtc_kwd,
            stt_sntc_spkr_dcd=stt_sntc_spkr_dcd,
            stt_sntc_cont=stt_sntc_cont,
            stt_sntc_sttm=stt_sntc_sttm,
            stt_sntc_endtm=stt_sntc_endtm
        )


def insert_stt_rst_full(logger, mysql):
    """
    Insert data to STT_RST_FULL
    :param      logger:     Logger
    :param      mysql:      MySQL DB
    """
    logger.info("11-3. DB upload STT_RST_FULL")
    for key, sent in MLF_INFO_DICT.items():
        recordkey = key.split("&")[0]
        rfile_name = key.split("&")[1]
        logger.debug("  RECORDKEY = {0}, RFILE_NAME = {1}".format(recordkey, rfile_name))
        mysql.delete_data_to_stt_rst_full(recordkey, rfile_name)
        mysql.insert_data_to_stt_rst_full(
            recordkey=recordkey,
            rfile_name=rfile_name,
            stt_mlf=sent
        )


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


def extract_silence(start_time_idx, end_time_idx, sentence_idx, total_duration, delimiter, input_line_list):
    """
    Extract silence section
    :param          start_time_idx:         Index start time of line split by delimiter
    :param          end_time_idx:           Index end time of line split by delimiter
    :param          sentence_idx:           Index sentence of line split by delimiter
    :param          total_duration:         Total record duration
    :param          delimiter:              Line delimiter
    :param          input_line_list:        Input line list
    :return:                                Output dictionary
    """
    silence_output_dict = collections.OrderedDict()
    crosstalk_output_dict = collections.OrderedDict()
    speaker_last_end_time_dict = {'A': False, 'C': False}
    speaker_last_start_time_dict = {'A': False, 'C': False}
    speaker_last_key_dict = {'A': False, 'C': False}
    speaker_last_sent_len_dict = {'A': False, 'C': False}
    for idx in range(0, len(input_line_list)):
        front_line = input_line_list[idx].strip()
        front_line_list = front_line.split(delimiter)
        if len(front_line_list) != 4:
            continue
        front_end_time = front_line_list[int(end_time_idx)]
        front_end_time_seconds = time_to_seconds(front_end_time)
        front_start_time = front_line_list[int(start_time_idx)]
        front_start_time_seconds = time_to_seconds(front_start_time)
        front_speaker = front_line_list[0].replace('[', '').replace(']', '').strip()
        front_sent = front_line_list[sentence_idx]
        speaker_last_sent_len_dict[front_speaker] = len(front_sent.decode('euc-kr'))
        speaker_last_end_time_dict[front_speaker] = front_end_time_seconds
        speaker_last_start_time_dict[front_speaker] = front_start_time_seconds
        compared_duration = 0
        crosstalk_duration = False
        if idx + 1 == len(input_line_list):
            hours_to_second = float(total_duration[:2]) * 3600
            minutes_to_second = float(total_duration[2:4]) * 60
            seconds = float(total_duration[4:6])
            back_start_time_seconds = hours_to_second + minutes_to_second + seconds
            back_sent = ''
            compared_speaker = front_speaker
        else:
            back_line = input_line_list[idx + 1].strip()
            back_line_list = back_line.split(delimiter)
            if len(back_line_list) != 4:
                continue
            back_sent = back_line_list[sentence_idx]
            back_start_time = back_line_list[int(start_time_idx)]
            back_end_time = back_line_list[int(end_time_idx)]
            back_start_time_seconds = time_to_seconds(back_start_time)
            back_end_time_seconds = time_to_seconds(back_end_time)
            back_line_speaker = back_line_list[0].replace('[', '').replace(']', '').strip()
            compared_speaker = 'A' if back_line_speaker == 'C' else 'C'
            compared_duration = back_start_time_seconds - speaker_last_end_time_dict[compared_speaker]
            if speaker_last_start_time_dict[compared_speaker] < back_start_time_seconds and back_end_time_seconds < speaker_last_end_time_dict[compared_speaker]:
                crosstalk_duration = back_start_time_seconds - back_end_time_seconds
        duration = back_start_time_seconds - front_end_time_seconds
        key = "{0}_{1}".format(idx, idx) if idx + 1 == len(input_line_list) else "{0}_{1}".format(idx, idx + 1)
        speaker_last_key_dict[front_speaker] = key
        silence_output_dict[key] = round(duration, 1) if duration < compared_duration else round(compared_duration, 1)
        if len(back_sent.decode('euc-kr')) > STT_CONFIG['crosstalk_ign_len'] and speaker_last_sent_len_dict[compared_speaker] > STT_CONFIG['crosstalk_ign_len']:
            temp = round(duration, 1) if duration > compared_duration else round(compared_duration, 1)
            temp = round(crosstalk_duration, 1) if crosstalk_duration else temp
            if temp > 0:
                temp = 0
            if speaker_last_key_dict[compared_speaker] not in crosstalk_output_dict:
                crosstalk_output_dict[speaker_last_key_dict[compared_speaker]] = temp
            else:
                crosstalk_output_dict[speaker_last_key_dict[compared_speaker]] += temp
    return silence_output_dict, crosstalk_output_dict


def set_stt_keyword_dtc_rst(word_list, info_dict, dtc_cd):
    """
    Set ADT DTC RST
    :param      word_list:      Word List
    :param      info_dict:      Information dictionary
    :param      dtc_cd:         Detect code
    """
    global STT_KEYWORD_DTC_RST
    key = False
    for item in word_list:
        keyword = item['KEYWORD']
        if keyword in info_dict['STT_SNTC_CONT']:
            stt_keyword_temp_dtc_rst = info_dict
            stt_keyword_temp_dtc_rst['DTC_CD'] = dtc_cd
            stt_keyword_temp_dtc_rst['DTC_KWD'] = keyword
            key = '{0}_{1}_{2}_{3}_{4}'.format(info_dict['RECORDKEY'], info_dict['RFILE_NAME'], info_dict['STT_SNTC_LIN_NO'], dtc_cd, keyword)
            if key not in STT_KEYWORD_DTC_RST:
                STT_KEYWORD_DTC_RST[key] = stt_keyword_temp_dtc_rst
    key = True if key else False
    return key


def update_stt_rst(logger, mysql):
    """
    UPDATE TB_TM_STT_RST
    :param      logger:             Logger
    :param      mysql:              MySQL db
    """
    global RCDG_INFO_DICT
    global MLF_INFO_DICT
    logger.info("11-2. DB update STT_RST")
    insert_set_dict = dict()
    issue_word_list = mysql.select_tb_qa_except('I')
    banned_word_list = mysql.select_tb_qa_except('B')
    for key, info_dict in RCDG_INFO_DICT.items():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        chn_tp = info_dict['CHN_TP']
        call_duration = info_dict['CALL_DURATION']
        biz_cd = info_dict['BIZ_CD']
        if chn_tp == 'M':
            detail_file_path = '{0}/masking/{1}.detail'.format(STT_TEMP_DIR_PATH, rfile_name)
        else:
            detail_file_path = '{0}/masking/{1}_trx.detail'.format(STT_TEMP_DIR_PATH, rfile_name)
        if not os.path.exists(detail_file_path):
            logger.error("Can't find detail file -> {0}".format(detail_file_path))
            error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
            del RCDG_INFO_DICT[key]
            continue
        detail_file = open(detail_file_path, 'r')
        detail_file_list = detail_file.readlines()
        detail_file.close()
        # 무음 처리 구간 조회
        silence_result, crosstalk_result = extract_silence(1, 2, 3, call_duration, "\t", detail_file_list)
        # DB insert 전 delete
        mysql.delete_stt_rst(recordkey, rfile_name)
        line_num = 0
        rx_sntc_len = 0
        tx_sntc_len = 0
        rx_during_time = 0
        tx_during_time = 0
        MLF_INFO_DICT[key] = ''
        issue_dtc_yn = 'N'
        prohibit_dtc_yn = 'N'
        for line in detail_file_list:
            insert_dict = dict()
            line_list = line.split('\t')
            speaker = line_list[0].replace('[', '').replace(']', '').strip()
            start_time = line_list[1].strip()
            end_time = line_list[2].strip()
            temp_start_time = start_time.replace(":", "").split(".")[0]
            temp_end_time = end_time.replace(":", "").split(".")[0]
            modified_start_time = temp_start_time if len(temp_start_time) == 6 else "0" + temp_start_time
            modified_end_time = temp_end_time if len(temp_end_time) == 6 else "0" + temp_end_time
            sent = line_list[3].strip()
            during_time = time_to_seconds(end_time) - time_to_seconds(start_time)
            stt_sntc_spch_tm = str(round(during_time, 1))
            sntc_len = len(sent.replace(' ', '').decode('euc_kr'))
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
            if during_time > 0:
                stt_sntc_spch_sped = str(round(float(sntc_len)/during_time, 1))
            else:
                stt_sntc_spch_sped = '0'
            silence_key = '{0}_{1}'.format(line_num, line_num+1) if line is not detail_file_list[-1] else '{0}_{0}'.format(line_num)
            silence_time = 0
            crosstalk_time = 0
            if silence_key in silence_result:
                silence_time = silence_result[silence_key]
            if silence_key in crosstalk_result:
                crosstalk_time = -crosstalk_result[silence_key]
            msk_info_lit = ''
            if line_num in MASKING_INFO_LIT[rfile_name]:
                msk_info_lit = ','.join(MASKING_INFO_LIT[rfile_name][line_num]) if len(MASKING_INFO_LIT[rfile_name][line_num]) > 0 else ''
            insert_dict['RECORDKEY'] = recordkey
            insert_dict['RFILE_NAME'] = rfile_name
            insert_dict['STT_SNTC_LIN_NO'] = line_num
            insert_dict['STT_SNTC_CONT'] = unicode(sent, 'euc-kr')
            insert_dict['STT_SNTC_STTM'] = modified_start_time
            insert_dict['STT_SNTC_ENDTM'] = modified_end_time
            insert_dict['STT_SNTC_SPKR_DCD'] = speaker
            insert_dict['STT_SNTC_SPCH_TM'] = stt_sntc_spch_tm
            insert_dict['STT_SNTC_SPCH_SPED'] = stt_sntc_spch_sped
            insert_dict['MSK_DTC_YN'] = 'Y' if '*' in sent else 'N'
            insert_dict['MSK_INFO_LIT'] = msk_info_lit
            insert_dict['SILENCE_YN'] = 'Y' if silence_time > 0 else 'N'
            insert_dict['SILENCE_TIME'] = str(silence_time) if insert_dict['SILENCE_YN'] == 'Y' else '0'
            insert_dict['CROSSTALK_YN'] = 'Y' if crosstalk_time > 0 else 'N'
            insert_dict['CROSSTALK_TIME'] = str(crosstalk_time) if insert_dict['CROSSTALK_YN'] == 'Y' else '0'
            MLF_INFO_DICT[key] += '{0}\n'.format(unicode(sent, 'euc-kr'))
            keyword = '{0}_{1}_{2}'.format(recordkey, rfile_name, line_num)
            if keyword not in insert_set_dict:
                insert_set_dict[keyword] = insert_dict
            if speaker == 'C':
                if set_stt_keyword_dtc_rst(issue_word_list, insert_dict, 'ISS'):
                    issue_dtc_yn = 'Y'
            if speaker == 'A':
                if set_stt_keyword_dtc_rst(banned_word_list, insert_dict, 'PRO'):
                    prohibit_dtc_yn = 'Y'
            line_num += 1
        stt_spch_sped = str(round((rx_sntc_len + tx_sntc_len)/(rx_during_time + tx_during_time), 1)) if rx_during_time + tx_during_time != 0 else '0'
        mysql.update_stt_spch_sped(recordkey, rfile_name, stt_spch_sped, issue_dtc_yn, prohibit_dtc_yn)
    mysql.insert_stt_rst(insert_set_dict)


def update_stt_rcdg_info(logger, mysql):
    """
    UPDATE STT_RCDG_INFO
    :param      logger:             Logger
    :param      mysql:              MySQL db
    """
    global RCDG_INFO_DICT
    logger.info("11-1. DB update STT_RCDG_INFO")
    for key, info_dict in RCDG_INFO_DICT.items():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        mysql.update_stt_prgst_cd('03', recordkey, rfile_name)
        mysql.update_stt_cmdtm(recordkey, rfile_name)


def set_output(logger):
    """
    Set output directory
    :param      logger:         Logger
    """
    global RESULT_CNT
    logger.info("9. Set output directory")
    result_dir_path = "{0}/result".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(result_dir_path):
        os.makedirs(result_dir_path)
    file_path_list = glob.glob("{0}/*".format(STT_TEMP_DIR_PATH))
    for file_path in file_path_list:
        if file_path.endswith(".result"):
            shutil.move(file_path, result_dir_path)
    RESULT_CNT = len(glob.glob("{0}/*.result".format(result_dir_path)))


def masking(str_idx, speaker_idx, delimiter, encoding, input_line_list):
    """
    Masking
    :param          str_idx:                Index sentence of line split by delimiter
    :param          speaker_idx:            Index speaker of line split by delimiter
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
    line_dict = collections.OrderedDict()
    speaker_dict = collections.OrderedDict()
    first_agent_line_num = False
    last_agent_line_num = False
    start_time_dict = dict()
    end_time_dict = dict()
    for line in input_line_list:
        line = line.strip()
        line_list = line.split(delimiter)
#        if str_idx >= len(line_list):
#            sent = ''
#        else :
#            sent = line_list[str_idx].strip()
        sent = line_list[str_idx].strip()
        speaker = line_list[speaker_idx].strip().replace("[", "").replace("]", "")
        start_time = time_to_seconds(line_list[1])
        end_time = time_to_seconds(line_list[2])
        if not first_agent_line_num and speaker == 'A':
            first_agent_line_num = line_cnt
        if speaker == 'A':
            last_agent_line_num = line_cnt
        start_time_dict[line_cnt] = start_time
        end_time_dict[line_cnt] = end_time
        try:
            line_dict[line_cnt] = sent.decode(encoding)
            speaker_dict[line_cnt] = speaker
        except Exception:
            if sent[-1] == '\xb1':
                line_dict[line_cnt] = sent[:-1].decode(encoding)
                speaker_dict[line_cnt] = speaker
        line_cnt += 1
    line_re_rule_dict = collections.OrderedDict()
    ans_yes_detect = dict()
    for line_num, line in line_dict.items():
        re_rule_dict = dict()
        detect_line = False
        if u'성함' in line or u'이름' in line:
            if u'확인' in line or u'어떻게' in line or u'여쭤' in line or u'맞으' in line or u'부탁' in line or u'말씀' in line:
                if 'name_rule' not in re_rule_dict:
                    re_rule_dict['name_rule'] = name_rule
        if u'아이디' in line:
            if u'맞으' in line or u'맞습' in line or u'말씀' in line:
                if 'id_rule' not in re_rule_dict:
                    re_rule_dict['id_rule'] = name_rule
                detect_line = True
                ans_yes_detect[line_num] = 1
        if (u'핸드폰' in line and u'번호' in line) or u'연락처' in line:
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
        if u'이메일' in line and u'주소' in line:
            if 'email_rule' not in re_rule_dict:
                re_rule_dict['email_rule'] = email_rule
        if u'주소' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'address_rule' not in re_rule_dict:
                    re_rule_dict['address_rule'] = address_rule
        # 면세점 특성상 면세점이 있는 지역명을 발화하는 경우가 많으므로 제외
        # if u'서울' in line or u'경기' in line or u'부산' in line or u'광주' in line or u'대구' in line or u'울산' in line or u'대전' in line or u'충청' in line or u'충북' in line or u'충남' in line or u'경상' in line or u'경북' in line or u'경남' in line or u'제주' in line:
        #     if 'address_rule' not in re_rule_dict:
        #         re_rule_dict['address_rule'] = address_rule
        if u'생년월일' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line or u'구요' in line:
                if 'birth_rule' not in re_rule_dict:
                    re_rule_dict['birth_rule'] = birth_rule
        if u'본인' in line:
            if u'가요' in line or u'맞으' in line or u'세요' in line or u'니까' in line:
                if 'me_name_rule' not in re_rule_dict:
                    re_rule_dict['me_name_rule'] = name_rule
                    detect_line = True
                    ans_yes_detect[line_num] = 1
        else:
            if 'etc_rule' not in re_rule_dict:
                re_rule_dict['etc_rule'] = etc_rule

        # 특이케이스 마스킹 탐지 발화 문장 포함
        if detect_line:
            if line_num not in line_re_rule_dict:
                line_re_rule_dict[line_num] = dict()
            line_re_rule_dict[line_num].update(re_rule_dict)

        next_line_cnt = int(MASKING_CONFIG['next_line_cnt'])
        for next_line_num in range(line_num + 1, len(line_dict)):
            if next_line_num in line_dict:
                for word in MASKING_CONFIG['precent_undetected']:
                    if word == line_dict[next_line_num].replace(' ', ''):
                        next_line_cnt += 1
                        break
                # 본인 확인 여부 이후 개인정보 발화 없음
                target = ['me_name_rule', 'id_rule']
                for rule_name in target:
                    if rule_name in re_rule_dict.keys() and line_num in ans_yes_detect:
                        if u'네' in line_dict[next_line_num] and speaker_dict[next_line_num] == 'C':
                            del re_rule_dict[rule_name]
                if next_line_num not in line_re_rule_dict:
                    line_re_rule_dict[next_line_num] = dict()
                line_re_rule_dict[next_line_num].update(re_rule_dict)
                next_line_cnt -= 1
                if next_line_cnt <= 0:
                    break
    # 상담사 처음, 끝 이름 룰 제외
    if first_agent_line_num:
        if first_agent_line_num in line_re_rule_dict:
            if 'name_rule' in line_re_rule_dict[first_agent_line_num]:
                del line_re_rule_dict[first_agent_line_num]['name_rule']
    if last_agent_line_num:
        if last_agent_line_num in line_re_rule_dict:
            if 'name_rule' in line_re_rule_dict[last_agent_line_num]:
                del line_re_rule_dict[last_agent_line_num]['name_rule']
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
            elif rule_name == 'id_rule':
                masking_code = "110"
                masking_cnt = 2
            elif rule_name == 'me_name_rule':
                masking_code = '120'
                masking_cnt = 2
            else:
                masking_code = "130"
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
                non_masking = False
                index_info.append({"start_idx": start, "end_idx": end, "masking_code": masking_code, "rule_name": rule_name})
                cnt = 0
                for word in MASKING_CONFIG['non_masking_word']:
                    temp_start = start-3 if start-3 > 0 else 0
                    if word in output_str[temp_start:end+3] or output_str in word:
                        non_masking = True
                        break
                for idx in output_str[start:end]:
                    if idx == " ":
                        masking_part += " "
                        continue
                    cnt += 1
                    if cnt % masking_cnt == 0:
                        masking_part += idx
                    else:
                        masking_part += "*"
                if not non_masking:
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
    global MASKING_INFO_LIT
    logger.info("10. Execute masking")
    target_file_list = glob.glob('{0}/detail/*'.format(STT_TEMP_DIR_PATH))
    masking_dir_path = '{0}/masking'.format(STT_TEMP_DIR_PATH)
    if not os.path.exists(masking_dir_path):
        os.makedirs(masking_dir_path)
    for target_file_path in target_file_list:
        try:
            target_file = open(target_file_path, 'r')
            file_name = os.path.splitext(os.path.basename(target_file_path))[0].replace('_trx', '')
            MASKING_INFO_LIT[file_name] = dict()
            line_list = target_file.readlines()
            sent_list = masking(3, 0, '\t', 'euc-kr', line_list)
            masking_file = open(os.path.join(masking_dir_path, os.path.basename(target_file_path)), 'w')
            line_num = 0
            for line in line_list:
                line_split = line.split('\t')
                new_line = line_split[:3]
                if line_num in sent_list[0]:
                    new_line.append(sent_list[0][line_num].strip())
                    rule_list = list()
                    if line_num in sent_list[1]:
                        for rule_name in sent_list[1][line_num]:
                            if rule_name["rule_name"] == 'etc_rule':
                                continue
                            rule_list.append(rule_name["rule_name"])
                    MASKING_INFO_LIT[file_name][line_num] = list(set(rule_list))
                else:
                    new_line.append(line_split[3].strip())
                print >> masking_file, '\t'.join(new_line)
                line_num += 1
            masking_file.close()
        except Exception:
            logger.error('masking Failed -> {0}'.format(target_file_path))
            raise Exception(traceback.format_exc())


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
        if int(ed_time) < int(mlf_st_time):
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
            start_time = str(timedelta(seconds=float(st.replace("ts=", "")) / 100))
            end_time = str(timedelta(seconds=float(et.replace("te=", "")) / 100))
            while True:
                if key not in output_dict:
                    output_dict[key] = "{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(speaker, modified_st, modified_et, sent, start_time, end_time)
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


def make_output(logger, mysql, target_dir_path):
    """
    Make txt file and detail file
    :param      logger:                 Logger
    :param      mysql:                  MySQL DB
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
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        chn_tp = info_dict['CHN_TP']
        biz_cd = info_dict['BIZ_CD']
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
                error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
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
                error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
                del RCDG_INFO_DICT[key]
                continue
            # Detailed txt & detail file creation.
            sorted_stt_info_output_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
            # Make mlf info
            rx_mlf_file_path = "{0}/mlf/{1}_rx.mlf".format(STT_TEMP_DIR_PATH, rfile_name)
            tx_mlf_file_path = "{0}/mlf/{1}_tx.mlf".format(STT_TEMP_DIR_PATH, rfile_name)
            if not os.path.exists(rx_mlf_file_path) or not os.path.exists(tx_mlf_file_path):
                logger.error("{0} don't have mlf file.".format(rfile_name))
                error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
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


def execute_unseg_and_do_space(logger, mysql):
    """
    Execute unseg.exe and do_space.exe
    :param      logger:             Logger
    :param      mysql:              MySQL DB
    :return:                        Output directory path
    """
    logger.info("7. Execute unseg.exe and do_space.exe")
    # Check the mlf file
    mlf_cnt = 0
    for info_dict in RCDG_INFO_DICT.values():
        chn_tp = info_dict['CHN_TP']
        if chn_tp == 'S':
            mlf_cnt += 2
            continue
        else:
            mlf_cnt += 1
            continue
    mlf_list = glob.glob("{0}/*.mlf".format(STT_TEMP_DIR_NAME))
    if len(mlf_list) != mlf_cnt:
        logger.error("mt_long Engine error occurred")
        logger.info("STT END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error("----------    STT ERROR   ----------")
        delete_garbage_file(logger)
        for key, info_dict in RCDG_INFO_DICT.items():
            recordkey = info_dict['RECORDKEY']
            rfile_name = info_dict['RFILE_NAME']
            biz_cd = info_dict['BIZ_CD']
            error_process(logger, mysql, recordkey, rfile_name, '00', biz_cd)
        mysql.disconnect()
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
    :param      logger:             Logger
    :param      thread_cnt:         Thread count
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
    :param      logger:         Logger
    :return:                    Thread count
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


def make_pcm_file(logger, mysql):
    """
    Make pcm file
    :param      logger:             Logger
    :param      mysql:              MySQL DB
    """
    global RCDG_INFO_DICT
    logger.info("4. Make pcm file")
    for key, info_dict in RCDG_INFO_DICT.items():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        biz_cd = info_dict['BIZ_CD']
        try:
            file_sprt = info_dict['FILE_SPRT']
            rec_ext = info_dict['REC_EXT']
            chn_tp = info_dict['CHN_TP']
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
                    sox_cmd = './sox -t raw -b 16 -e signed-integer -r 8000 -B -c 2 {0} {1}'.format(
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
            error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
            del RCDG_INFO_DICT[key]
            continue
    if len(RCDG_INFO_DICT.keys()) < 1:
        logger.info("STT END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error("----------    Job is ZERO(0)   ----------")
        delete_garbage_file(logger)
        mysql.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


def copy_data(logger, mysql):
    """
    Copy source data
    :param      logger:             Logger
    :param      mysql:              MySQL db
    """
    global RCDG_INFO_DICT
    logger.info("3. Copy data")
    for key, info_dict in RCDG_INFO_DICT.items():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        biz_cd = info_dict['BIZ_CD']
        chn_tp = info_dict['CHN_TP']
        rec_ext = info_dict['REC_EXT']
        file_sprt = info_dict['FILE_SPRT']
        try:
            target_dir = '{0}/{1}'.format(STT_CONFIG['rec_dir_path'], biz_cd)
            # Mono
            if chn_tp == 'M' or (chn_tp == 'S' and file_sprt == 'N'):
                file_path = '{0}/{1}.{2}'.format(target_dir, rfile_name, rec_ext)
                if not os.path.exists(file_path):
                    logger.error("{0} file is not exist -> {1}".format(rec_ext, file_path))
                    error_process(logger, mysql, recordkey, rfile_name, '90', biz_cd)
                    del RCDG_INFO_DICT[key]
                    continue
                logger.debug('\t{0} -> {1}'.format(file_path, STT_TEMP_DIR_PATH))
                shutil.copy(file_path, STT_TEMP_DIR_PATH)
            # Stereo
            elif chn_tp == 'S' and file_sprt == 'Y':
                rx_file_path = '{0}/{1}_rx.{2}'.format(target_dir, rfile_name, rec_ext)
                tx_file_path = '{0}/{1}_tx.{2}'.format(target_dir, rfile_name, rec_ext)
                if not os.path.exists(rx_file_path) or not os.path.exists(tx_file_path):
                    logger.error("{0} file is not exists -> {1} or {2}".format(rec_ext, rx_file_path, tx_file_path))
                    error_process(logger, mysql, recordkey, rfile_name, '90', biz_cd)
                    del RCDG_INFO_DICT[key]
                    continue
                logger.debug('\t{0} -> {1}'.format(rx_file_path, STT_TEMP_DIR_PATH))
                shutil.copy(rx_file_path, STT_TEMP_DIR_PATH)
                logger.debug('\t{0} -> {1}'.format(tx_file_path, STT_TEMP_DIR_PATH))
                shutil.copy(tx_file_path, STT_TEMP_DIR_PATH)
            else:
                logger.error("CHN_TP ERROR {0} : {1}".format(rfile_name, chn_tp))
                error_process(logger, mysql, recordkey, rfile_name, '90', biz_cd)
                del RCDG_INFO_DICT[key]
                continue
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("---------- copy data error ----------")
            error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
            del RCDG_INFO_DICT[key]
            continue
    if len(os.listdir(STT_TEMP_DIR_PATH)) < 1:
        logger.error("No such file -> {0}".format(RCDG_INFO_DICT.keys()))
        delete_garbage_file(logger)
        mysql.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


def update_status_and_select_rec_file(logger, mysql, job_list):
    """
    Update status and select rec file
    :param      logger:         Logger
    :param      mysql:          MySQL DB
    :param      job_list:       List of JOB
    """
    global RCDG_INFO_DICT
    logger.info("2. Get recording information dictionary")
    logger.info("\tload job list -> {0}".format(job_list))
    # Creating recording file dictionary
    for job in job_list:
        recordkey = job['RECORDKEY']
        rfile_name = job['RFILE_NAME']
        biz_cd = job['BIZ_CD']
        try:
            mysql.update_stt_prgst_cd('01', recordkey, rfile_name)
            key = '{0}&{1}'.format(recordkey, rfile_name)
            RCDG_INFO_DICT[key] = job
        except Exception as e:
            logger.error(e)
            error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
            continue


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.info('1. Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'MySQL':
                sql = MySQL(logger)
            else:
                raise Exception("Unknown DB [{0}]".format(db))
            logger.debug("Success connect {0} DB ...".format(db))
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
    STT processing
    :param      job_list:        Job
    """
    # 0. Setup data
    logger, cnt = setup_data()
    logger.info("-" * 100)
    logger.info('Start STT')
    # 1. Connect DB
    try:
        mysql = connect_db(logger, 'MySQL')
        if not mysql:
            logger.error("----------- Can't connect db -----------")
            delete_garbage_file(logger)
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("---------- Can't connect db -----------")
        delete_garbage_file(logger)
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        # 2. Update status and Get recording information dictionary using job list
        update_status_and_select_rec_file(logger, mysql, job_list)
        # 3. Copy data
        copy_data(logger, mysql)
        # 4. Make pcm file
        make_pcm_file(logger, mysql)
        # 5. Make list file
        thread_cnt = make_pcm_list_file(logger)
        # 6. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 7. Execute unseg.exe and do_space.exe
        do_space_dir_path = execute_unseg_and_do_space(logger, mysql)
        # 8. Make output
        make_output(logger, mysql, do_space_dir_path)
        # 9. Set output
        set_output(logger)
        # 10. Execute masking
        execute_masking(logger)
        # 11. DB upload
        try:
            update_stt_rcdg_info(logger, mysql)
            update_stt_rst(logger, mysql)
            insert_stt_rst_full(logger, mysql)
            update_stt_keyword_dtc_rst(logger, mysql)
        except Exception:
            logger.error('---------- DB RETRY ----------')
            try:
                mysql.disconnect()
            except Exception:
                logger.error('already mysql disconnect')
            try:
                mysql = connect_db(logger, 'MySQL')
                if not mysql:
                    logger.error("----------- Can't connect db -----------")
                    delete_garbage_file(logger)
                    for handler in logger.handlers:
                        handler.close()
                        logger.removeHandler(handler)
                    sys.exit(1)
            except Exception:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                logger.error("---------- Can't connect db -----------")
                delete_garbage_file(logger)
                for handler in logger.handlers:
                    handler.close()
                    logger.removeHandler(handler)
                sys.exit(1)
            update_stt_rcdg_info(logger, mysql)
            update_stt_rst(logger, mysql)
            insert_stt_rst_full(logger, mysql)
            update_stt_keyword_dtc_rst(logger, mysql)
        # 12. Move output
        move_output(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("STT END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    STT ERROR   ----------")
        for info_dict in RCDG_INFO_DICT.values():
            recordkey = info_dict['RECORDKEY']
            rfile_name = info_dict['RFILE_NAME']
            biz_cd = info_dict['BIZ_CD']
            error_process(logger, mysql, recordkey, rfile_name, '02', biz_cd)
        delete_garbage_file(logger)
        mysql.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # 13. Delete garbage list
    delete_garbage_file(logger)
    # 14. Print statistical data
    statistical_data(logger)
    # 15. Update status
    logger.info("15. Update status to STTA END(03)")
    for key, info_dict in RCDG_INFO_DICT.items():
        recordkey = info_dict['RECORDKEY']
        rfile_name = info_dict['RFILE_NAME']
        mysql.update_stt_prgst_cd('03', recordkey, rfile_name)
    for info_dict in RCDG_INFO_DICT.values():
        chn_tp = info_dict['CHN_TP']
        biz_cd = info_dict['BIZ_CD']
        file_sprt = info_dict['FILE_SPRT']
        rec_ext = info_dict['REC_EXT']
        rfile_name = info_dict['RFILE_NAME']
        if chn_tp == 'M' or (chn_tp == 'S' and file_sprt == 'N'):
            target_file_list = [
                '{0}/{1}/{2}.{3}'.format(STT_CONFIG['rec_dir_path'], biz_cd, rfile_name, rec_ext)
            ]
        else:
            target_file_list = [
                '{0}/{1}/{2}_rx.{3}'.format(STT_CONFIG['rec_dir_path'], biz_cd, rfile_name, rec_ext),
                '{0}/{1}/{2}_tx.{3}'.format(STT_CONFIG['rec_dir_path'], biz_cd, rfile_name, rec_ext)
            ]
        for target_file in target_file_list:
            logger.debug('delete {0}'.format(target_file))
            # del_garbage(logger, target_file)
    mysql.disconnect()
    logger.info("TOTAL END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job_list):
    """
    This is a program that execute STT
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
