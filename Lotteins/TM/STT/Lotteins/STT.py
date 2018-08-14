#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-21, modification: 2018-05-23"

###########
# imports #
###########
import os
import re
import sys
import time
import glob
import shutil
import cx_Oracle
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
STT_TEMP_DIR_PATH = ""
STT_TEMP_DIR_NAME = ""
TB_TM_STT_SLCD_DTC_RST_DICT = dict()


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

    def select_tb_tm_stt_rcdg_info(self, rec_id, rfile_name):
        query = """
            SELECT
                *
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND REC_ID = :1
                AND RFILE_NAME = :2
        """
        bind = (
            rec_id,
            rfile_name
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        info_dict = dict()
        for item in result:
            info_dict = dict(zip([d[0] for d in self.cursor.description], item))
            break
        return info_dict

    def insert_tb_tm_stt_rst(self, inset_set_dict):
        try:
            sql = """
                INSERT INTO
                    TB_TM_STT_RST
                    (
                        REC_ID,
                        RFILE_NAME,
                        STT_SNTC_LIN_NO,
                        STT_SNTC_CONT,
                        STT_SNTC_STTM,
                        STT_SNTC_ENDTM,
                        STT_SNTC_SPKR_DCD,
                        STT_SNTC_SPCH_TM,
                        STT_SNTC_SPCH_SPED,
                        MSK_DTC_YN,
                        SILENCE_YN,
                        SILENCE_TIME,
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
                    :11, :12,
                    'TM_STT', 'TM_STT', SYSDATE, 'TM_STT', 'TM_STT',
                    SYSDATE
                )
            """
            values_list = list()
            for insert_dict in inset_set_dict.values():
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                stt_sntc_cont = insert_dict['STT_SNTC_CONT']
                stt_sntc_sttm = insert_dict['STT_SNTC_STTM']
                stt_sntc_endtm = insert_dict['STT_SNTC_ENDTM']
                stt_sntc_spkr_dcd = insert_dict['STT_SNTC_SPKR_DCD']
                stt_sntc_spch_tm = insert_dict['STT_SNTC_SPCH_TM']
                stt_sntc_spch_sped = insert_dict['STT_SNTC_SPCH_SPED']
                msk_dtc_yn = insert_dict['MSK_DTC_YN']
                silence_yn = insert_dict['SILENCE_YN']
                silence_time = insert_dict['SILENCE_TIME']
                values_tuple = (
                    rec_id, rfile_name, stt_sntc_lin_no, stt_sntc_cont, stt_sntc_sttm, stt_sntc_endtm,
                    stt_sntc_spkr_dcd, stt_sntc_spch_tm, stt_sntc_spch_sped, msk_dtc_yn, silence_yn, silence_time,
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

    def insert_data_to_tb_tm_stt_slce_dtc_rst(self, **kwargs):
        try:
            query = """
                INSERT INTO TB_TM_STT_SLCE_DTC_RST
                (
                    REC_ID,
                    RFILE_NAME,
                    STT_SNTC_ST_NO,
                    STT_SNTC_END_NO,
                    STT_SNTC_SLCE_TM,
                    REGP_CD,
                    RGST_PGM_ID,
                    RGST_DTM,
                    LST_CHGP_CD,
                    LST_CHG_PGM_ID,
                    LST_CHG_DTM
                )
                VALUES (
                    :1, :2, :3, :4, :5, 'TM_STT', 'TM_STT',
                    SYSDATE, 'TM_STT', 'TM_STT', SYSDATE
                )
            """
            bind = (
                kwargs.get('rec_id'),
                kwargs.get('rfile_name'),
                kwargs.get('stt_sntc_st_no'),
                kwargs.get('stt_sntc_end_no'),
                kwargs.get('stt_sntc_slce_tm'),
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

    def update_stt_spch_sped(self, rec_id, rfile_name, stt_spch_sped_rx, stt_spch_sped_tx):
        try:
            query = """
                UPDATE
                    TB_TM_STT_RCDG_INFO
                SET
                    STT_SPCH_SPED_RX = :1,
                    STT_SPCH_SPED_TX = :2
                WHERE 1=1
                    AND REC_ID = :3
                    AND RFILE_NAME = :4
            """
            bind = (
                stt_spch_sped_rx,
                stt_spch_sped_tx,
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
            raise Exception(traceback.format_exc())

    def update_nqa_stta_prgst_cd(self, nqa_stta_prgst_cd, stt_prgst_cd, rec_id, rfile_name):
        try:
            query = """
                UPDATE
                    TB_TM_STT_RCDG_INFO
                SET
                    NQA_STTA_PRGST_CD = :1,
                    STT_PRGST_CD = :2,
                    LST_CHGP_CD = 'TM_STT',
                    LST_CHG_PGM_ID = 'TM_STT',
                    LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND REC_ID = :3
                    AND RFILE_NAME = :4
            """
            bind = (
                nqa_stta_prgst_cd,
                stt_prgst_cd,
                rec_id,
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

    def update_stt_cmdtm(self, rec_id, rfile_name):
        try:
            query = """
                UPDATE
                    TB_TM_STT_RCDG_INFO
                SET
                    STT_CMDTM = SYSDATE,
                    LST_CHGP_CD = 'TM_STT',
                    LST_CHG_PGM_ID = 'TM_STT',
                    LST_CHG_DTM = SYSDATE
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
            raise Exception(traceback.format_exc())

    def delete_tb_tm_stt_rst(self, rec_id, rfile_name):
        try:
            query = """
                DELETE FROM
                    TB_TM_STT_RST
                WHERE 1=1
                    AND REC_ID = :1
                    AND RFILE_NAME = :2
            """
            bind = (
                rec_id,
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

    def delete_data_to_tb_tm_stt_slce_dtc_rst(self, rec_id, rfile_name):
        try:
            query = """
                DELETE FROM
                    TB_TM_STT_SLCE_DTC_RST
                WHERE 1=1
                    AND REC_ID = :1
                    AND RFILE_NAME = :2
            """
            bind = (
                rec_id,
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
    :param      logger:     Logger
    """
    logger.info("17. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception:
            continue


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


def error_process(logger, oracle, rec_id, rfile_name, nqa_stta_prgst_cd, biz_cd):
    """
    Error process
    :param      logger:                 Logger
    :param      oracle:                 Oracle db
    :param      rec_id:                 REC_ID(녹취 ID)
    :param      rfile_name:             RFILE_NAME(녹취파일명)
    :param      nqa_stta_prgst_cd:      NQA_STTA_PRGST_CD(비체결_STTA 상태코드)
    :param      biz_cd:                 BIZ_CD(업체구분코드)
    """
    logger.error("Error process")
    logger.error("REC_ID = {0}, RFILE_NAME = {1}, change NQA_STTA_PRGST_CD = {2}".format(
        rec_id, rfile_name, nqa_stta_prgst_cd))
    oracle.update_nqa_stta_prgst_cd(nqa_stta_prgst_cd, '02', rec_id, rfile_name)
    rec_path = '{0}/TM/{1}'.format(STT_CONFIG['rec_dir_path'], biz_cd)
    if not nqa_stta_prgst_cd == '00':
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
    logger.info("18. Statistical data print")
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
    logger.info("15. Move output to STT output path")
    for info_dict in RCDG_INFO_DICT.values():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        chn_tp = info_dict['CHN_TP']
        call_start_time = str(info_dict['CALL_START_TIME']).strip()
        output_dir_path = '{0}/{1}/{2}/{3}/{4}-{5}'.format(
            STT_CONFIG['stt_output_path'], call_start_time[:4],
            call_start_time[5:7], call_start_time[8:10], rec_id, rfile_name)
        output_dict = {
            'mlf': {'ext': 'mlf', 'merge': 'N'},
            'unseg': {'ext': 'stt', 'merge': 'N'},
            'do_space': {'ext': 'stt', 'merge': 'N'},
            'txt': {'ext': 'txt', 'merge': 'Y'},
            'detail': {'ext': 'detail', 'merge': 'Y'},
            'result': {'ext': 'result', 'merge': 'N'},
#            'modified_nlp_line_number': {'ext': 'hmd.txt', 'merge': 'Y'},
#            'JSON': {'ext': 'json', 'merge': 'Y'},
#            'JSON2': {'ext': 'json2', 'merge': 'Y'},
#            'HMD': {'ext': 'hmd.txt', 'merge': 'Y'},
#            'MCNT': {'ext': 'morph.cnt', 'merge': 'Y'},
#            'NCNT': {'ext': 'ne.cnt', 'merge': 'Y'},
#            'IDX': {'ext': 'idx', 'merge': 'Y'},
#            'IDXVP': {'ext': 'idxvp', 'merge': 'Y'},
#            'W2V': {'ext': 'w2v.txt', 'merge': 'Y'},
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


def insert_tb_tm_stt_slce_dtc_rst(logger, oracle):
    """
    Insert data to TB_TM_STT_SLCE_DTC_RST
    :param      logger:     Logger
    :param      oracle:     Oracle DB
    """
    logger.info("14-3. DB upload TB_TM_STT_SLCE_DTC_RST")
    logger.debug("Extract silence list")
    for key, silence_result in TB_TM_STT_SLCD_DTC_RST_DICT.items():
        rec_id = key.split("-")[0]
        rfile_name = key.split("-")[1]
        logger.debug("  REC_ID = {0}, RFILE_NAME = {1}".format(rec_id, rfile_name))
        oracle.delete_data_to_tb_tm_stt_slce_dtc_rst(rec_id, rfile_name)
        for silence_key, duration in silence_result.items():
            stt_sntc_st_no = silence_key.split("_")[0]
            stt_sntc_end_no = silence_key.split("_")[1]
            oracle.insert_data_to_tb_tm_stt_slce_dtc_rst(
                rec_id=rec_id,
                rfile_name=rfile_name,
                stt_sntc_st_no=stt_sntc_st_no,
                stt_sntc_end_no=stt_sntc_end_no,
                stt_sntc_slce_tm=duration
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


def extract_silence_old(start_time_idx, end_time_idx, total_duration, delimiter, input_line_list, silence_seconds):
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
    speaker_last_end_time_dict = {'A': False, 'C': False}
    for idx in range(0, len(input_line_list)):
        front_line = input_line_list[idx].strip()
        front_line_list = front_line.split(delimiter)
        end_time = front_line_list[int(end_time_idx)]
        end_time_seconds = time_to_seconds(end_time)
        speaker = front_line_list[0].replace('[', '').replace(']', '').strip()
        speaker_last_end_time_dict[speaker] = end_time_seconds
        compared_duration = 0
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
            back_line_speaker = back_line_list[0].replace('[', '').replace(']', '').strip()
            compared_speaker = 'A' if back_line_speaker == 'C' else 'C'
            compared_duration = start_time_seconds - speaker_last_end_time_dict[compared_speaker]
            if compared_duration <= float(silence_seconds):
                continue
        duration = start_time_seconds - end_time_seconds
        if duration > float(silence_seconds) and compared_duration > float(silence_seconds):
            key = "{0}_{1}".format(idx, idx) if idx + 1 == len(input_line_list) else "{0}_{1}".format(idx, idx + 1)
            output_dict[key] = round(duration, 2) if duration < compared_duration else round(compared_duration, 2)
    return output_dict


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
    speaker_last_end_time_dict = {'A': False, 'C': False}
    for idx in range(0, len(input_line_list)):
        front_line = input_line_list[idx].strip()
        front_line_list = front_line.split(delimiter)
        end_time = front_line_list[int(end_time_idx)]
        end_time_seconds = time_to_seconds(end_time)
        speaker = front_line_list[0].replace('[', '').replace(']', '').strip()
        speaker_last_end_time_dict[speaker] = end_time_seconds
        compared_duration = 0
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
            back_line_speaker = back_line_list[0].replace('[', '').replace(']', '').strip()
            compared_speaker = 'A' if back_line_speaker == 'C' else 'C'
            compared_duration = start_time_seconds - speaker_last_end_time_dict[compared_speaker]
            if compared_duration <= float(silence_seconds):
                continue
        duration = start_time_seconds - end_time_seconds
        if duration > float(silence_seconds) and compared_duration > float(silence_seconds):
            if speaker_last_end_time_dict[back_line_speaker]:
                if speaker_last_end_time_dict[back_line_speaker] + float(silence_seconds) > start_time_seconds:
                        continue
            key = "{0}_{1}".format(idx, idx) if idx + 1 == len(input_line_list) else "{0}_{1}".format(idx, idx + 1)
            output_dict[key] = round(duration, 2) if duration < compared_duration else round(compared_duration, 2)
    return output_dict


def update_tb_tm_stt_rst(logger, oracle):
    """
    UPDATE TB_TM_STT_RST
    :param      logger:             Logger
    :param      oracle:             Oracle db
    """
    global RCDG_INFO_DICT
    global TB_TM_STT_SLCD_DTC_RST_DICT
    logger.info("14-2. DB update TB_TM_STT_RST")
    insert_set_dict = dict()
    for key, info_dict in RCDG_INFO_DICT.items():
        rec_id = info_dict['REC_ID']
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
            error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
            del RCDG_INFO_DICT[key]
            continue
        detail_file = open(detail_file_path, 'r')
        detail_file_list = detail_file.readlines()
        detail_file.close()
        # 무음 처리 구간 조회
        silence_result = extract_silence(1, 2, call_duration, "\t", detail_file_list, STT_CONFIG['silence_seconds'])
        if len(silence_result) > 0:
            slcd_key = '{0}-{1}'.format(rec_id, rfile_name)
            TB_TM_STT_SLCD_DTC_RST_DICT[slcd_key] = silence_result
        # DB insert 전 delete
        oracle.delete_tb_tm_stt_rst(rec_id, rfile_name)
        line_num = 0
        rx_sntc_len = 0
        tx_sntc_len = 0
        rx_during_time = 0
        tx_during_time = 0
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
            stt_sntc_spch_tm = str(round(during_time, 2))
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
                stt_sntc_spch_sped = str(round(float(sntc_len)/during_time, 2))
            else:
                stt_sntc_spch_sped = '0'
            silence_key = '{0}_{1}'.format(line_num, line_num+1) if line is not detail_file_list[-1] else '{0}_{0}'.format(line_num)
            insert_dict['REC_ID'] = rec_id
            insert_dict['RFILE_NAME'] = rfile_name
            insert_dict['STT_SNTC_LIN_NO'] = line_num
            insert_dict['STT_SNTC_CONT'] = unicode(sent, 'euc-kr')
            insert_dict['STT_SNTC_STTM'] = modified_start_time
            insert_dict['STT_SNTC_ENDTM'] = modified_end_time
            insert_dict['STT_SNTC_SPKR_DCD'] = speaker
            insert_dict['STT_SNTC_SPCH_TM'] = stt_sntc_spch_tm
            insert_dict['STT_SNTC_SPCH_SPED'] = stt_sntc_spch_sped
            insert_dict['MSK_DTC_YN'] = 'Y' if '*' in sent else 'N'
            insert_dict['SILENCE_YN'] = 'Y' if silence_key in silence_result else 'N'
            insert_dict['SILENCE_TIME'] = str(silence_result[silence_key]) if silence_key in silence_result else ''
            keyword = '{0}_{1}_{2}'.format(rec_id, rfile_name, line_num)
            if keyword not in insert_set_dict:
                insert_set_dict[keyword] = insert_dict
            line_num += 1
        stt_spch_sped_rx = str(round(rx_sntc_len/rx_during_time, 2)) if rx_during_time != 0 else '0'
        stt_spch_sped_tx = str(round(tx_sntc_len/tx_during_time, 2)) if tx_during_time != 0 else '0'
        oracle.update_stt_spch_sped(rec_id, rfile_name, stt_spch_sped_rx, stt_spch_sped_tx)
    oracle.insert_tb_tm_stt_rst(insert_set_dict)


def update_tb_tm_stt_rcdg_info(logger, oracle):
    """
    UPDATE TB_TM_STT_RCDG_INFO
    :param      logger:             Logger
    :param      oracle:             Oracle db
    """
    global RCDG_INFO_DICT
    logger.info("14-1. DB update TB_TM_STT_RCDG_INFO")
    for key, info_dict in RCDG_INFO_DICT.items():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        oracle.update_nqa_stta_prgst_cd('03', '03', rec_id, rfile_name)
        oracle.update_stt_cmdtm(rec_id, rfile_name)


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


def set_output(logger):
    """
    Set output directory
    :param      logger:     Logger
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
#        if str_idx >= len(line_list):
#            sent = ''
#        else :
#            sent = line_list[str_idx].strip()
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

        # if len(line_re_rule_dict[line_num]) == 0:
        #     etc_dict = {'etc_rule': etc_rule}
        #     line_re_rule_dict[line_num].update(etc_dict)
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


def make_output(logger, oracle, target_dir_path):
    """
    Make txt file and detail file
    :param      logger:                 Logger
    :param      oracle:                 Oracle DB
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
        rec_id = info_dict['REC_ID']
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
                error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
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
                error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
                del RCDG_INFO_DICT[key]
                continue
            # Detailed txt & detail file creation.
            sorted_stt_info_output_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
            # Make mlf info
            rx_mlf_file_path = "{0}/mlf/{1}_rx.mlf".format(STT_TEMP_DIR_PATH, rfile_name)
            tx_mlf_file_path = "{0}/mlf/{1}_tx.mlf".format(STT_TEMP_DIR_PATH, rfile_name)
            if not os.path.exists(rx_mlf_file_path) or not os.path.exists(tx_mlf_file_path):
                logger.error("{0} don't have mlf file.".format(rfile_name))
                error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
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
                start_time = detail_line_list[4]
                end_time = detail_line_list[5]
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


def execute_unseg_and_do_space(logger, oracle):
    """
    Execute unseg.exe and do_space.exe
    :param      logger:             Logger
    :param      oracle:             Oracle DB
    :return:                        Output directory path
    """
    global RCDG_INFO_DICT
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
            rec_id = info_dict['REC_ID']
            rfile_name = info_dict['RFILE_NAME']
            biz_cd = info_dict['BIZ_CD']
            error_process(logger, oracle, rec_id, rfile_name, '00', biz_cd)
        oracle.disconnect()
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


def make_pcm_file(logger, oracle):
    """
    Make pcm file
    :param      logger:             Logger
    :param      oracle:             Oracle DB
    """
    logger.info("4. Make pcm file")
    for key, info_dict in RCDG_INFO_DICT.items():
        rec_id = info_dict['REC_ID']
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
            error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
            del RCDG_INFO_DICT[key]
            continue
    if len(RCDG_INFO_DICT.keys()) < 1:
        logger.info("STT END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error("----------    Job is ZERO(0)   ----------")
        delete_garbage_file(logger)
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


def copy_data(logger, oracle):
    """
    Copy source data
    :param      logger:             Logger
    :param      oracle:             Oracle db
    """
    global RCDG_INFO_DICT
    logger.info("3. Copy data")
    for key, info_dict in RCDG_INFO_DICT.items():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        biz_cd = info_dict['BIZ_CD']
        chn_tp = info_dict['CHN_TP']
        rec_ext = info_dict['REC_EXT']
        project_cd = info_dict['PROJECT_CD']
        file_sprt = info_dict['FILE_SPRT']
        try:
            target_dir = '{0}/{1}/{2}'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd)
            # Mono
            if chn_tp == 'M' or (chn_tp == 'S' and file_sprt == 'N'):
                file_path = '{0}/{1}.{2}'.format(target_dir, rfile_name, rec_ext)
                if not os.path.exists(file_path):
                    logger.error("{0} file is not exist -> {1}".format(rec_ext, file_path))
                    error_process(logger, oracle, rec_id, rfile_name, '90', biz_cd)
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
                    error_process(logger, oracle, rec_id, rfile_name, '90', biz_cd)
                    del RCDG_INFO_DICT[key]
                    continue
                logger.debug('\t{0} -> {1}'.format(rx_file_path, STT_TEMP_DIR_PATH))
                shutil.copy(rx_file_path, STT_TEMP_DIR_PATH)
                logger.debug('\t{0} -> {1}'.format(tx_file_path, STT_TEMP_DIR_PATH))
                shutil.copy(tx_file_path, STT_TEMP_DIR_PATH)
            else:
                logger.error("CHN_TP ERROR {0} : {1}".format(rec_id, chn_tp))
                error_process(logger, oracle, rec_id, rfile_name, '90', biz_cd)
                del RCDG_INFO_DICT[key]
                continue
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("---------- copy data error ----------")
            error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
            del RCDG_INFO_DICT[key]
            continue
    if len(os.listdir(STT_TEMP_DIR_PATH)) < 1:
        logger.error("No such file -> {0}".format(RCDG_INFO_DICT.keys()))
        delete_garbage_file(logger)
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)


def update_status_and_select_rec_file(logger, oracle, job_list):
    """
    Update status and select rec file
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      job_list:       List of JOB
    """
    global RCDG_INFO_DICT
    logger.info("2. Get recording information dictionary")
    logger.info("\tload job list -> {0}".format(job_list))
    # Creating recording file dictionary
    for job in job_list:
        rec_id = job['REC_ID']
        rfile_name = job['RFILE_NAME']
        biz_cd = job['BIZ_CD']
        try:
            oracle.update_nqa_stta_prgst_cd('01', '01', rec_id, rfile_name)
            result_dict = oracle.select_tb_tm_stt_rcdg_info(rec_id, rfile_name)
            if not result_dict:
                raise Exception("No data TM rcdg information to TB_QA_STT_TM_RCDG_INFO,"
                                " REC_ID = {0}, RFILE_NAME = {1}".format(rec_id, rfile_name))
            key = '{0}&{1}'.format(rec_id, rfile_name)
            RCDG_INFO_DICT[key] = result_dict
        except Exception as e:
            logger.error(e)
            error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
            continue


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.info('1. Connect {0} ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
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
    :param      job_list:        Job(REC_ID, RFILE_NAME, NQA_STTA_PRGST_CD)
    """
    # 0. Setup data
    logger, cnt = setup_data()
    logger.info("-" * 100)
    logger.info('Start STT')
    # 1. Connect DB
    try:
        oracle = connect_db(logger, 'Oracle')
        if not oracle:
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
        update_status_and_select_rec_file(logger, oracle, job_list)
        # 3. Copy data
        copy_data(logger, oracle)
        # 4. Make pcm file
        make_pcm_file(logger, oracle)
        # 5. Make list file
        thread_cnt = make_pcm_list_file(logger)
        # 6. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 7. Execute unseg.exe and do_space.exe
        do_space_dir_path = execute_unseg_and_do_space(logger, oracle)
        # 8. Make output
        make_output(logger, oracle, do_space_dir_path)
        # 9. Set output
        set_output(logger)
        # 10. Execute masking
        execute_masking(logger)
        # 11. Execute TA
        #execute_new_lang(logger)
        # 12. Make statistics file
        #make_statistics_file(logger)
        # 13. Modify nlp output
        #modify_nlp_output_line_number(logger)
        # 14. DB upload
        try:
            update_tb_tm_stt_rcdg_info(logger, oracle)
            update_tb_tm_stt_rst(logger, oracle)
            insert_tb_tm_stt_slce_dtc_rst(logger, oracle)
        except Exception:
            logger.error('---------- DB RETRY ----------')
            try:
                oracle.disconnect()
            except Exception:
                logger.error('already oracle disconnect')
            try:
                oracle = connect_db(logger, 'Oracle')
                if not oracle:
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
            update_tb_tm_stt_rcdg_info(logger, oracle)
            update_tb_tm_stt_rst(logger, oracle)
            insert_tb_tm_stt_slce_dtc_rst(logger, oracle)
        # 15. Move output
        move_output(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("STT END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    STT ERROR   ----------")
        for info_dict in RCDG_INFO_DICT.values():
            rec_id = info_dict['REC_ID']
            rfile_name = info_dict['RFILE_NAME']
            biz_cd = info_dict['BIZ_CD']
            error_process(logger, oracle, rec_id, rfile_name, '02', biz_cd)
        delete_garbage_file(logger)
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    if len(RCDG_INFO_DICT.keys()) > 0:
        try:
            # 16. Execute TA
            sys.path.append(STT_CONFIG['ta_script_path'])
            import IE_TA
            reload(IE_TA)
            args = Namespace(
                logger=logger,
                stt_tm_info_dict=RCDG_INFO_DICT,
                cnt=cnt
            )
            #IE_TA.main(args)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            for key, info_dict in RCDG_INFO_DICT.items():
                rec_id = info_dict['REC_ID']
                rfile_name = info_dict['RFILE_NAME']
                biz_cd = info_dict['BIZ_CD']
                error_process(logger, oracle, rec_id, rfile_name, '12', biz_cd)
            delete_garbage_file(logger)
            oracle.disconnect()
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
        # 17. Delete garbage list
        delete_garbage_file(logger)
        # 18. Print statistical data
        statistical_data(logger)
        # 19. Update status
        logger.info("19. Update status to STTA END(13)")
        for key, info_dict in RCDG_INFO_DICT.items():
            rec_id = info_dict['REC_ID']
            rfile_name = info_dict['RFILE_NAME']
            oracle.update_nqa_stta_prgst_cd('13', '03', rec_id, rfile_name)
    for info_dict in RCDG_INFO_DICT.values():
        chn_tp = info_dict['CHN_TP']
        biz_cd = info_dict['BIZ_CD']
        file_sprt = info_dict['FILE_SPRT']
        project_cd = info_dict['PROJECT_CD']
        rec_ext = info_dict['REC_EXT']
        rfile_name = info_dict['RFILE_NAME']
        if chn_tp == 'M' or (chn_tp == 'S' and file_sprt == 'N'):
            target_file_list = [
                '{0}/{1}/{2}/{3}.{4}'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name, rec_ext)
            ]
        else:
            target_file_list = [
                '{0}/{1}/{2}/{3}_rx.{4}'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name, rec_ext),
                '{0}/{1}/{2}/{3}_tx.{4}'.format(STT_CONFIG['rec_dir_path'], project_cd, biz_cd, rfile_name, rec_ext)
            ]
        for target_file in target_file_list:
            logger.debug('delete {0}'.format(target_file))
            del_garbage(logger, target_file)
    oracle.disconnect()
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
