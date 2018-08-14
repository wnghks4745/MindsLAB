#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-11-30, modification: 2017-00-00"

###########
# imports #
###########
import re
import os
import sys
import time
import glob
import shutil
import MySQLdb
import pymssql
import traceback
import subprocess
import collections
from argparse import Namespace
from operator import itemgetter
from datetime import datetime
from datetime import timedelta
from lib.openssl import decrypt
from lib.iLogger import set_logger
from cfg.config import STT_CONFIG
from cfg.config import MYSQL_DB_CONFIG
from cfg.config import MSSQL_DB_CONFIG
from cfg.config import MASKING_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')

#############
# constants #
#############
DT = ''
ST = ''
WAV_CNT = ''
RESULT_CNT = ''
TOTAL_WAV_TIME = 0
OUTPUT_DIR_NAME = ''
STT_TEMP_DIR_PATH = ''
STT_OUTPUT_DIR_PATH = ''
DELETE_FILE_LIST = list()

#########
# class #
#########
class MSSQL(object):
    def __init__(self, logger):
        self.logger = logger
        self.conn = pymssql.connect(
            host=MSSQL_DB_CONFIG['host'],
            user=MSSQL_DB_CONFIG['user'],
            password=MSSQL_DB_CONFIG['password'],
            database=MSSQL_DB_CONFIG['database'],
            port=MSSQL_DB_CONFIG['port'],
            charset=MSSQL_DB_CONFIG['charset'],
            login_timeout=MSSQL_DB_CONFIG['login_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

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
        self.cursor = self.conn.cursor()

    def update_stt_prgst_cd(self, **kwargs):
        try:
            """
            TM_STT계약정보 테이블에서
            조건에 맞는 증서번호와 계약일자를 갖고있는 데이터의
            상태코드와 STT요청일시를 변경한다.
            """
            query = """
                UPDATE
                    TB_QA_STT_TM_CNTR_INFO
                SET
                    STT_PRGST_CD = %s
                    STT_REQ_DTM = %s
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('stt_prgst_cd'),
                kwargs.get('stt_req_dtm'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute("set names utf8")
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

    def select_rcdg_id(self, poli_no, ctrdt):
        """
        TM_STT정보 테이블에서
        증서번호와 계약일자를 가지고
        STT처리상태코드가 '00'인
        녹취ID를 조회한다.
        """
        query = """
            SELECT
                RCDG_ID
            FROM
                TB_QA_STT_TM_INFO
            WHERE 1=1
                AND POLI_NO = %s
                AND CTRDT = %s
                AND STT_PRC_SCD = '00'
        """
        bind = (
            poli_no,
            ctrdt
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def update_stt_prc_scd(self, **kwargs):
        try:
            """
            TM_STT정보 테이블에서
            증서번호와 계약일자를 가지는
            CS 처리상태 코드를 업데이트한다.
            """
            query = """
                UPDATE
                    TB_QA_STT_TM_INFO
                SET
                    STT_PRC_SCD = %s
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('stt_prc_scd'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute('set names utf8')
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

    def insert_data_to_tm_rcdg_info(self, **kwargs):
        try:
            query = """
                INSERT INTO TB_QA_STT_TM_RCDG_INFO
                (
                    POLI_NO,
                    CTRDT,
                    RCDG_ID,
                    RCDG_FILE_NM,
                    RCDG_FILE_PATH_NM
                )
                VALUES
                (
                    %s, %s, %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                POLI_NO = %s,
                CTRDT = %s,
                RCDG_ID = %s,
                RCDG_FILE_NM = %s,
                RCDG_FILE_PATH_NM = %s
            """
            bind = (
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
                kwargs.get('rcdg_id'),
                kwargs.get('rcdg_file_nm'),
                kwargs.get('rcdg_file_path_nm'),
            ) * 2
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
            if os.path.isdir(delete_file_path):
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def statistical_data(logger):
    """
    Statistical data print
    :param      logger:     Logger
    """
    global RESULT_CNT
    required_time = elapsed_time(DT)
    end_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M.%S')
    total_wav_duration = timedelta(seconds=TOTAL_WAV_TIME)
    total_wav_average_duration = timedelta(seconds=TOTAL_WAV_TIME / float(WAV_CNT))
    xrt = (int(timedelta(seconds=TOTAL_WAV_TIME).total_seconds() / required_time.total_seconds()))
    logger.info("13. Statistical data print")
    logger.info("   Start time                  = {0}".format(ST))
    logger.info("   End time                    = {0}".format(end_time))
    logger.info("   The time required           = {0}".format(required_time))
    logger.info("   WAV count                   = {0}".format(WAV_CNT))
    logger.info("   Result count                = {0}".format(RESULT_CNT))
    logger.info("   Total WAV duration          = {0}".format(total_wav_duration))
    logger.info("   Total WAV average duration  = {0}".format(total_wav_average_duration))
    logger.info("   xRT                         = {0} xRT".format(xrt))
    logger.info("Done CS")
    logger.info("Remove logger handler")
    logger.info("CS END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info("12. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception:
            continue


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


def move_output(logger, ctrdt):
    """
    Move output to CS output path
    :param      logger:     Logger
    :param      ctrdt:      CTRDT(계약 일자)
    """
    global STT_OUTPUT_DIR_PATH
    logger.info("11. Move output to CS output path")
    output_dir_path = "{0}/{1}/{2}/{3}".format(STT_CONFIG['stt_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8])
    STT_OUTPUT_DIR_PATH = "{0}/{1}".format(output_dir_path, OUTPUT_DIR_NAME)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    if os.path.exists(STT_OUTPUT_DIR_PATH):
        logger.info("Output file already existed. -> {0}".format(STT_OUTPUT_DIR_PATH))
        del_garbage(logger, STT_OUTPUT_DIR_PATH)
    os.rename(STT_TEMP_DIR_PATH, STT_OUTPUT_DIR_PATH)


def make_file_for_tb_qa_stt_tm_rst(logger, poli_no, ctrdt, chn_tp, rec_file_dict):
    """
    Make file for TB_QA_STT_TM_RST
    :param      logger:             Logger
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      chn_tp:             CHN_TP(채널 타입)
    :param      rec_file_dict:      REC file dictionary
    """
    logger.info("10-2. Make db upload file for TB_QA_STT_TM_RST")
    detail_output_dir_path = "{0}/detail".format(STT_TEMP_DIR_PATH)
    output_dir_path = "{0}/TB_QA_STT_TM_RST".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    tm_rst_output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_rst.txt".format(output_dir_path, poli_no, ctrdt)
    tm_rst_output_file = open(tm_rst_output_file_path, 'w')
    w_ob = os.walk(detail_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if chn_tp == "M":
                ori_file_name = file_name.replace(".detail", "").replace("__", ".")
            else:
                ori_file_name = file_name.replace("_trx.detail", "").replace("__", ".")
            if ori_file_name not in rec_file_dict:
                raise Exception("Can't find RCDG_ID -> {0}({1})".format(file_name, ori_file_name))
            tm_info = rec_file_dict[ori_file_name]
            rcdg_id = tm_info['rcdg_id']
            detail_file = open(os.path.join(dir_path, file_name), 'r')
            detail_file_list = detail_file.readlines()
            detail_file.close()
            masking_output_dict = masking(detail_file_list)
            line_num = 0
            for line in detail_file_list:
                line_list = line.split("\t")
                speaker = line_list[0].replace("[", "").replace("]", "")
                start_time = line_list[1].strip()
                end_time = line_list[2].strip()
                temp_start_time = start_time.replace(":", "")
                temp_end_time = end_time.replace(":", "")
                modified_start_time = temp_start_time if len(temp_start_time) == 6 else "0" + temp_start_time
                modified_end_time = temp_end_time if len(temp_end_time) == 6 else "0" + temp_end_time
                sent = line_list[3].strip()
                msk_dtc_yn = "Y" if line_num in masking_output_dict else "N"
                msk_info_lit = str(masking_output_dict[line_num]) if mak_dtc_yn == "Y" else "None"
                output_dict = collections.OrderedDict()
                output_dict['poli_no'] = poli_no
                output_dict['ctrdt'] = ctrdt
                output_dict['rcdg_id'] = rcdg_id
                output_dict['rcdg_file_nm'] = ori_file_name
                output_dict['stt_sntc_lin_no'] = str(line_num)
                output_dict['stt_sntc_cont'] = sent
                output_dict['stt_sntc_sttm'] = modified_start_time
                output_dict['stt_sntc_endtm'] = modified_end_time
                output_dict['stt_sntc_spkr_dcd'] = speaker
                output_dict['msk_dtc_yn'] = msk_dtc_yn
                output_dict['msk_info_lit'] = msk_info_lit.replace("'", '"')
                output_list = list(output_dict.values())
                line_num += 1
                print >> tm_rst_output_file, '\t'.join(output_list)
    tm_rst_output_file.close()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_RST".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    shutil.copy(tm_rst_output_file_path, db_upload_dir_path)


def make_file_for_tb_qa_stt_tm_rcdg_info(logger, mysql, poli_no, ctrdt, chn_tp, rec_file_dict):
    """
    Make file for TB_QA_STT_TM_INFO
    :param      logger:             Logger
    :param      mysql:              MySQL
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      chn_tp:             CHN_TP(채널 타입)
    :param      rec_file_dict:      REC file dictionary
    :return:                        CS TM information dictionary
    """
    stt_tm_info_dict = dict()
    logger.info("10-2. Make db upload file for TB_QA_STT_TM_RCDG_INFO")
    output_dir_path = "{0}/TB_QA_STT_TM_RCDG_INFO".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    tm_info_output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_rcdg_info.txt".format(output_dir_path, poli_no, ctrdt)
    tm_info_output_file = open(tm_info_output_file_path, 'w')
    for rcdf_file_nm, tm_info in rec_file_dict.items():
        rec_file_path = "{0}/{1}/{2}/{3}.wav".format(ctrdt[:4], ctrdt[4:6], ctrdt[6:8], rcdf_file_nm.replace(".", "__"))
        rcdg_id = tm_info['rcdg_id']
        rcdf_file_nm = tm_info['rcdf_file_nm']
        rcdg_crnc_hms = tm_info['rcdg_crnc_hms']
        try:
            int_rcdg_crnc_hms = int(rcdg_crnc_hms)
        except Exception:
            int_rcdg_crnc_hms = '0'
        modified_rcdg_crnc_hms = str(timedelta(seconds=int_rcdg_crnc_hms)).replace(":", "")
        if len(modified_rcdg_crnc_hms) == 5:
            modified_rcdg_crnc_hms = "0" + modified_rcdg_crnc_hms
        rcdg_stdtm = tm_info['rcdg_stdtm']
        rcdg_edtm = tm_info['rcdg_edtm']
        cnsr_id = tm_info['cnsr_id']
        start_tm = "{0}{1}".format(rcdf_file_nm[:8], rcdg_stdtm)
        end_tm = "{0}{1}".format(rcdf_file_nm[:8], rcdg_edtm)
        output_dict = collections.OrderedDict()
        output_dict['poli_no'] = poli_no
        output_dict['ctrdt'] = ctrdt
        output_dict['rcdg_id'] = rcdg_id
        output_dict['rcdg_file_nm'] = rcdf_file_nm
        output_dict['rcdg_file_path_nm'] = rec_file_path
        output_dict['chn_tp_cd'] = chn_tp
        output_dict['rcdg_dt'] = rcdf_file_nm[:8]
        output_dict['rcdg_stdtm'] = str(datetime.strptime(start_tm, "%Y%m%d%H%M%S"))
        output_dict['rcdg_edtm'] = str(datetime.strptime(end_tm, "%Y%m%d%H%M%S"))
        output_dict['rcdg_crnc_hms'] = modified_rcdg_crnc_hms
        output_dict['cnsr_id'] = cnsr_id
        output_list = list(output_dict.values())
        print >> tm_info_output_file, '\t'.join(output_list)
        stt_tm_info_dict[rcdf_file_nm] = {'rcdg_id': rcdg_id, 'rcdg_file_path_nm': rec_file_path, 'chn_tp': chn_tp}
        #   check_data=mysql.select_data_to_tm_rcdg_info(
        #       poli_no=poli_no,
        #       ctrdt=ctrdt
        #       rcdg_id=rcdg_id,
        #       rcdg_file_nm=rcdf_file_nm
        #   )
        #   if check_data:
        #       mysql.delete_data_to_tm_rcdg_info(
        #           poli_no=poli_no,
        #           ctrdt=ctrdt,
        #           rcdg_id=rcdg_id,
        #           rcdg_file_nm=rcdf_file_nm
        #       )
        mysql.insert_data_to_tm_rcdg_info(
            poli_no=poli_no,
            ctrdt=ctrdt,
            rcdg_id=rcdg_id,
            rcdg_file_nm=rcdg_file_nm,
            rcdg_file_path_nm=rec_file_path
        )
    tm_info_output_file.close()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_RCDG_INFO".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    shutil.copy(tm_info_output_file_path, db_upload_dir_path)
    return stt_tm_info_dict


def make_file_for_tb_qa_stt_tm_info(logger, poli_no, ctrdt, stt_req_dtm, rec_file_dict):
    """
    Make file for TB_QA_STT_TM_INFO
    :param      logger:             Logger
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      stt_req_dtm:        STT_REQ_DTM(CS 요청 일시)
    :param      rec_file_dict:      REC file dictionary
    :return:                        CS TM information dictionary
    """
    logger.info("10-1. Make db upload file for TB_QA_STT_TM_INFO")
    output_dir_path = "{0}/TB_QA_STT_TM_INFO".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    tm_info_output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_info.txt".format(output_dir_path, poli_no, ctrdt)
    tm_info_output_file = open(tm_info_output_file_path, 'w')
    for rcdf_file_nm, tm_info in rec_file_dict.items():
        rcdg_id = tm_info['rcdg_id']
        cnsr_id = tm_info['cnsr_id']
        output_dict = collections.OrderedDict()
        output_dict['poli_no'] = poli_no
        output_dict['ctrdt'] = ctrdt
        output_dict['rcdg_id'] = rcdg_id
        output_dict['stt_prc_scd'] = '01'
        output_dict['stt_req_dtm'] = stt_req_dtm
        output_dict['stt_cmdtm'] = str(datetime.now())
        output_dict['cnsr_id'] = cnsr_id
        output_list = list(output_dict.values())
        print >> tm_info_output_file, "\t".join(output_list)
    tm_info_output_file.close()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_INFO".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    shutil.copy(tm_info_output_file_path, db_upload_dir_path)


def make_wav_file(logger, chn_tp, ctrdt):
    """
    Make wav file
    :param      logger:     Logger
    :param      chn_tp:     CHN_TP(채널 타입)
    :param      ctrdt:      CTRDT(계약 일자)
    :return:
    """
    temp_file_list = list()
    logger.info("9. Make wav file")
    w_ob = os.walk("{0}/pcm".format(STT_TEMP_DIR_PATH))
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            pcm_file = os.path.join(dir_path, file_name)
            s16_file = "{0}/wav/{1}.s16".format(STT_TEMP_DIR_PATH, file_name.replace(".pcm", ""))
            shutil.copy(pcm_file, s16_file)
            temp_file_list.append(s16_file)
    if chn_tp == 'M':
        w_ob = os.walk("{0}/wav".format(STT_TEMP_DIR_PATH))
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                input_file = os.path.join(dir_path, file_name)
                output_file = os.path.join(dir_path, file_name.replace(".s16", ".wav"))
                cmd = 'sox -r 8000 -c 1 {0} -r 8000 -c 1 -e gsm {1}'.format(input_file, output_file)
                sub_process(logger, cmd)
                wav_output_path = "{0}/{1}/{2}/{3}".format(
                    STT_CONFIG['wav_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8])
                if not os.path.exists(wav_output_path):
                    os.makedirs(wav_output_path)
                shutil.copy(output_file, wav_output_path)
    else:
        w_ob = os.walk("{0}/wav".format(STT_TEMP_DIR_PATH))
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                if file_name.endswith("_tx.s16"):
                    tx_file_path = os.path.join(dir_path, file_name)
                    rx_file_path = os.path.join(dir_path, file_name.replace("_tx.s16", "_rx.s16"))
                    merge_file_path = os.path.join(dir_path, file_name.replace("_tx.s16", "_trx.s16"))
                    temp_file_list.append(merge_file_path)
                    wav_file_path = os.path.join(dir_path, file_name.replace("_tx.s16", ".wav"))
                    wav_output_path = "{0}/{1}/{2}/{3}".format(
                        STT_CONFIG['wav_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8])
                    if os.path.exists(tx_file_path) and os.path.exists(rx_file_path):
                        merge_cmd = 'sox -M -r 8000 -c 1 {0} -r 8000 -c 1 {1} -r 8000 -c 1 {2}'.format(
                            tx_file_path, rx_file_path, merge_file_path)
                        sub_process(logger, merge_cmd)
                        cmd = 'sox -r 8000 -c 1 {0} -r 8000 -c 1 -e gsm {1}'.format(merge_file_path, wav_file_path)
                        sub_process(logger, cmd)
                        if not os.path.exists(wav_output_path):
                            os.makedirs(wav_output_path)
                        shutil.copy(wav_file_path, wav_output_path)
                    else:
                        logger.error("Not pair file")
                        if not os.path.exists(tx_file_path):
                            logger.error("Not existed -> {0}".format(tx_file_path))
                        if not os.path.exists(rx_file_path):
                            logger.error("Not existed -> {0}".format(rx_file_path))
                        continue
    for temp_file in temp_file_list:
        del_garbage(logger, temp_file)


def set_output(logger):
    """
    Set output directory
    :param      logger:     Logger
    """
    global RESULT_CNT
    logger.info("8. Set output directory")
    pcm_dir_path = "{0}/pcm".format(STT_TEMP_DIR_PATH)
    wav_dir_path = "{0}/wav".format(STT_TEMP_DIR_PATH)
    result_dir_path = "{0}/result".format(STT_TEMP_DIR_PATH)
    if not os.path.exists(wav_dir_path):
        os.makedirs(wav_dir_path)
    if not os.path.exists(result_dir_path):
        os.makedirs(result_dir_path)
    if not os.path.exists(pcm_dir_path):
        os.makedirs(pcm_dir_path)
    file_path_list = glob.glob("{0}/*".format(STT_TEMP_DIR_PATH))
    for file_path in file_path_list:
        if file_path.endswith(".wav") or file_path.endswith(".WAV"):
            shutil.move(file_path, wav_dir_path)
        elif file_path.endswith(".pcm"):
            shutil.move(file_path, pcm_dir_path)
        elif file_path.endswith(".result"):
            shutil.move(file_path, result_dir_path)
    RESULT_CNT = len(glob.glob("{0}/*.result".format(result_dir_path)))


def modify_time_info(logger, speaker, file_name, output_dict):
    """
    Modify time info
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
            start_time = str(timedelta(seconds=int(st.replace("ts=", "")) / 100))
            end_time = str(timedelta(seconds=int(et.replace("te=", "")) / 100))
            sent = line_list[2].strip()
            modified_st = st.replace("ts=", "").strip()
            if int(modified_st) not in output_dict:
                output_dict[int(modified_st)] = "{0}\t{1}\t{2}\t{3}".format(speaker, start_time, end_time, sent)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Error modify_time_info")
            logger.error(line)
            logger.error(exc_info)
            continue
    return output_dict


def make_output(logger, chn_tp, do_space_dir_path):
    """
    Make txt file and detail file
    :param      logger:                 Logger
    :param      chn_tp:                 CHN_TP(채널 타입)
    :param      do_space_dir_path:      Output directory path
    """
    logger.info("7. Make output [txt file and detailed file]")
    txt_dir_path = "{0}/txt".format(STT_TEMP_DIR_PATH)
    detail_dir_path = "{0}/detail".format(STT_TEMP_DIR_PATH)
    output_dir_list = [txt_dir_path, detail_dir_path]
    for output_dir_path in output_dir_list:
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    w_ob = os.walk(do_space_dir_path)
    # Mono
    if chn_tp == 'M':
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                if file_name.endswith(".stt"):
                    serial_number = file_name.replace(".stt", "")
                    stt_file = open(os.path.join(dir_path, file_name), 'r')
                    txt_output_file = open(txt_dir_path + "/" + serial_number + '.txt', 'w')
                    detail_output_file = open(detail_dir_path + "/" + serial_number + '.detail', 'w')
                    for line in stt_file:
                        line_list = line.split(",")
                        if len(line_list) != 3:
                            continue
                        st = line_list[0].strip()
                        et = line_list[1].strip()
                        start_time = str(timedelta(seconds=int(st.replace("ts=", "")) / 100))
                        end_time = str(timedelta(seconds=int(et.replace("te=", "")) / 100))
                        sent = line_list[2].strip()
                        speaker = '[M]'
                        print >> txt_output_file, "{0}{1}".format(speaker, sent)
                        print >> detail_output_file, "{0}\t{1}\t{2}\t{3}".format(speaker, start_time, end_time, sent)
                    stt_file.close()
                    txt_output_file.close()
                    detail_output_file.close()
    # Stereo
    else:
        # target_dict 에 각 serial_number 에 tx, rx 파일 갯수를 저장
        target_dict = dict()
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                if file_name.endswith("_tx.stt"):
                    serial_number = file_name.replace("_tx.stt", "")
                    if serial_number not in target_dict:
                        target_dict[serial_number] = 1
                    else:
                        target_dict[serial_number] += 1
                elif file_name.endswith("_rx.stt"):
                    serial_number = file_name.replace("_rx.stt", "")
                    if serial_number not in target_dict:
                        target_dict[serial_number] = 1
                    else:
                        target_dict[serial_number] += 1
        # *_tx.stt 와 *_rx.stt 파일을 *_trx.stt 파일로 merge. 각각의 serial_number 에 tx, rx 파일이 2개인 경우만 merge
        for serial_number, cnt in target_dict.items():
            output_dict = dict()
            if cnt == 2:
                tx_file = open(do_space_dir_path + "/" + serial_number + '_tx.stt', 'r')
                rx_file = open(do_space_dir_path + "/" + serial_number + '_rx.stt', 'r')
                output_dict = modify_time_info(logger, "agent", tx_file, output_dict)
                output_dict = modify_time_info(logger, "client", rx_file, output_dict)
                tx_file.close()
                rx_file.close()
            else:
                logger.error("{0} don't have tx or rx file.".format(serial_number))
                continue
            output_dict_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
            txt_output_file = open(txt_dir_path + "/" + serial_number + '_trx.txt', 'w')
            detail_output_file = open(detail_dir_path + "/" + serial_number + '_trx.detail', 'w')
            for line_list in output_dict_list:
                detail_line = line_list[1]
                detail_line_list = detail_line.split('\t')
                speaker = '[S]' if detail_line_list[0] == 'agent' else '[C]'
                start_time = detail_line_list[1]
                end_time = detail_line_list[2]
                sent = detail_line_list[3]
                print >> txt_output_file, "{0}{1}".format(speaker, sent)
                print >> detail_output_file, "{0}\t{1}\t{2}\t{3}".format(speaker, start_time, end_time, sent)
            txt_output_file.close()
            detail_output_file.close()


def execute_unseg_and_do_space(logger, poli_no, ctrdt):
    """
    Execute unseg.exe and do_space.exe
    :param      logger:         Logger
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(계약 일자)
    :return:                    Output directory path
    """
    logger.info("6. Execute unseg.exe and do_space.exe")
    # MLF 복사
    mlf_dir_path = "{0}/mlf".format(STT_TEMP_DIR_PATH)
    unseg_dir_path = "{0}/unseg".format(STT_TEMP_DIR_PATH)
    do_space_dir_path = "{0}/do_space".format(STT_TEMP_DIR_PATH)
    output_dir_list = [mlf_dir_path, unseg_dir_path, do_space_dir_path]
    for output_dir_path in output_dir_list:
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    mlf_list = glob.glob("{0}/*.mlf".format(STT_TEMP_DIR_PATH))
    if len(mlf_list) < 1:
        err_str = "Mlf file is not created -> POLI_NO = {0} , CTRDT = {1}".format(poli_no, ctrdt)
        raise Exception(err_str)
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
    # Execute do_space.exe
    do_space_cmd = './do_space.exe {up} {db}'.format(up=unseg_dir_path, db=do_space_dir_path)
    sub_process(logger, do_space_cmd)
    return do_space_dir_path


def execute_dnn(logger, thread_cnt):
    """
    Execute DNN (mt_long_utt_dnn_support.gpu.exe)
    :param      logger:         Logger
    :param      thread_cnt:     Thread count
    """
    logger.info("5. Execute DNN (mt_long_utt_dnn_support.gpu.exe)")
    os.chdir(STT_CONFIG['stt_path'])
    dnn_thread = thread_cnt if thread_cnt < STT_CONFIG['thread'] else STT_CONFIG['thread']
    cmd = "./mt_long_utt_dnn_support.gpu.exe {tn} {th} 1 1 {gpu} 128 0.8".format(
        tn=OUTPUT_DIR_NAME, th=dnn_thread, gpu=STT_CONFIG['gpu'])
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
    logger.info("4. Do make list file")
    list_file_cnt = 0
    max_list_file_cnt = 0
    # Decrypt REC file
    decrypt(STT_TEMP_DIR_PATH)
    w_ob = os.walk(STT_TEMP_DIR_PATH)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if file_name.endswith(".pcm"):
                # List 파일에 PCM 파일명을 입력한다.
                list_file_path = "{sp}/{tn}_n{cnt}.list".format(
                    sp=STT_CONFIG['stt_path'], tn=OUTPUT_DIR_NAME, cnt=list_file_cnt)
                curr_list_file_path = "{sp}/{tn}_n{cnt}_curr.list".format(
                    sp=STT_CONFIG['stt_path'], tn=OUTPUT_DIR_NAME, cnt=list_file_cnt)
                DELETE_FILE_LIST.append(list_file_path)
                DELETE_FILE_LIST.append(curr_list_file_path)
                output_file_div = open(list_file_path, 'a')
                print >> output_file_div, "{tn}/{sn}".format(tn=OUTPUT_DIR_NAME, sn=file_name)
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


def wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm):
    """
    Wav file not found process
    :param      logger:             Logger
    :param      mysql:              MySQL
    :param      stt_prgst_cd:       STT_PRGST_CD(CS 진행상태코드)
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      stt_req_dtm:        CS
    :return:
    """
    logger.info("Wav file not found. [POLI_NO = {0}, CTRDT = {1}]".format(poli_no, ctrdt))
    mysql.update_stt_prgst_cd(
        stt_prgst_cd='90',
        stt_req_dtm=stt_req_dtm,
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    if stt_prgst_cd == '06':
        output_dir_name = "{0}_{1}_supplementation".format(poli_no, ctrdt)
    else:
        output_dir_name = "{0}_{1}".format(poli_no, ctrdt)
    output_dir_path = "{0}/{1}/{2}/{3}/{4}/TB_QA_STT_TM_CNTR_INFO".format(
        STT_CONFIG['stt_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8], output_dir_name)
    if os.path.exists(output_dir_path):
        del_garbage(logger, output_dir_path)
    os.makedirs(output_dir_path)
    tm_cntr_info_output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_cntr_info.txt".format(output_dir_path, poli_no, ctrdt)
    tm_cntr_info_output_file = open(tm_cntr_info_output_file_path, 'a')
    output_dict = collection.OrderDict()
    output_dict['poli_no'] = poli_no
    output_dict['ctrdt'] = ctrdt
    output_dict['stt_prgst_cd'] = '90'
    output_dict['stt_req_dtm'] = stt_req_dtm
    output_dict['ta_cmdtm'] = str(datetime.now())
    output_list = list(output_dict.values())
    print >> tm_cntr_info_output_file, "\t".join(output_list)
    tm_cntr_info_output_file.close()
    temp_db_upload_dir_path = "{0}/{1}.tmp".format(STT_CONFIG['db_upload_path'], output_dir_name)
    tm_cntr_info_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_CNTR_INFO".format(STT_CONFIG['db_upload_path'], output_dir_name)
    db_upload_dir_path = "{0}/{1}".format(STT_CONFIG['db_upload_path'], output_dir_name)
    if os.path.exists(temp_db_upload_dir_path):
        del_garbage(logger, temp_db_upload_dir_path)
    os.makedirs(tm_cntr_info_dir_path)
    shutil.copy(tm_cntr_info_output_file_path, tm_cntr_info_dir_path)
    if os.path.exists(db_upload_dir_path):
        logger.info("{0} is already exists.".format(output_dir_name))
        del_garbage(logger, db_upload_dir_path)
    os.rename(temp_db_upload_dir_path, db_upload_dir_path)


def copy_data(logger, mysql, mssql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm, rec_file_dict):
    """
    Copy source data
    :param      logger:             Logger
    :param      mysql:              MySQL
    :param      mssql:              MsSQL
    :param      stt_prgst_cd:       STT_PRGST_CD(CS 진행상태코드)
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      stt_req_dtm:        STT_REQ_DTM(CS 요청 일시)
    :param      rec_file_dict:      REC file dictionary
    :return:                        CHN_TP(채널 타입)
    """
    logger.info("3. Copy data")
    chn_tp = ''
    for rcdf_file_nm in rec_file_dict.keys():
        rcdf_file_date = rcdf_file_nm[:8]
        target_mono_file_path = "{0}/{1}/{2}.enc".format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        target_rx_file_path = "{0}/{1}/{2}.rx.enc".format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        target_tx_file_path = "{0}/{1}/{2}.tx.enc".format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        incident_target_mono_file_path = "{0}/{1}.enc".format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        incident_target_rx_file_path = "{0}/{1}.rx.enc".format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        incident_target_tx_file_path = "{0}/{1}.tx.enc".format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        if os.path.exists(target_mono_file_path):
            if len(chn_tp) < 1:
                chn_tp = 'M'
            rename_target_mono_file_path = "{0}/{1}.pcm.enc".format(STT_TEMP_DIR_PATH, rcdf_file_nm.replace(".", "__"))
            shutil.copy(target_mono_file_path, rename_target_mono_file_path)
            logger.debug("CHN_TP = M, {0} -> {1}".format(target_mono_file_path, STT_TEMP_DIR_PATH))
        elif os.path.exists(target_rx_file_path) and os.path.exists(target_tx_file_path):
            if len(chn_tp) < 1:
                chn_tp = 'S'
            rename_target_rx_file_path = "{0}/{1}_rx.pcm.enc".format(STT_TEMP_DIR_PATH, rcdf_file_nm.replace(".", "__"))
            rename_target_tx_file_path = "{0}/{1}_tx.pcm.enc".format(STT_TEMP_DIR_PATH, rcdf_file_nm.replace(".", "__"))
            shutil.copy(target_rx_file_path, rename_target_rx_file_path)
            shutil.copy(target_tx_file_path, rename_target_tx_file_path)
            logger.debug("CHN_TP = S, {0} -> {1}".format(target_rx_file_path, STT_TEMP_DIR_PATH))
            logger.debug("CHN_TP = S, {0} -> {1}".format(target_tx_file_path, STT_TEMP_DIR_PATH))
        elif os.path.exists(incident_target_mono_file_path):
            if len(chn_tp) < 1:
                chn_tp = 'M'
            rename_incident_target_mono_file_path = "{0}/{1}.pcm.enc".format(
                STT_TEMP_DIR_PATH, rcdf_file_nm.replace(".", "__"))
            shutil.copy(incident_target_mono_file_path, rename_incident_target_mono_file_path)
            logger.debug("CHN_TP = M, {0} -> {1}".format(incident_target_mono_file_path, STT_TEMP_DIR_PATH))
        elif os.path.exists(incident_target_rx_file_path) and os.path.exists(incident_target_tx_file_path)
            if len(chn_tp) < 1:
                chn_tp = 'S'
            rename_incident_target_rx_file_path = "{0}/{1}_rx.pcm.enc".format(
                STT_TEMP_DIR_PATH, rcdf_file_nm.replace(".", "__"))
            rename_incident_target_tx_file_path = "{0}/{1}_tx.pcm.enc".format(
                STT_TEMP_DIR_PATH, rcdf_file_nm.replace(".", "__"))
            shutil.copy(incident_target_rx_file_path, rename_incident_target_rx_file_path)
            shutil.copy(incident_target_tx_file_path, rename_incident_target_tx_file_path)
            logger.debug("CHN_TP = S, {0} -> {1}".format(incident_target_rx_file_path, STT_TEMP_DIR_PATH))
            logger.debug("CHN_TP = S, {0} -> {1}".format(incident_target_tx_file_path, STT_TEMP_DIR_PATH))
        else:
            delete_garbage_file(logger)
            logger.error("No such file -> POLI_NO = {0}, CTRDT = {1}, RCDF_FILE_NM = {2}".format(
                poli_no, ctrdt, rcdf_file_nm))
            wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm)
            mysql.disconnect()
            mssql.disconnect()
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    if len(os.listdir(STT_TEMP_DIR_PATH)) < 1 or len(chn_tp) < 1:
        delete_garbage_file(logger)
        logger.error("Target directory is empty -> POLI_NO = {0} , CTRDT = {1}".format(poli_no, ctrdt))
        wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm)
        mysql.disconnect()
        mssql.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    return chn_tp


def update_status_and_select_rec_file(logger, mysql, mssql, poli_no, ctrdt, stt_req_dtm):
    """
    Update status and select rec file
    :param      logger:             Logger
    :param      mysql:              MySQL
    :param      mssql:              MsSQL
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      stt_req_dtm:        STT_REQ_DTM(CS 요청 일시)
    :return:                        REC file dictionary
    """
    logger.info("2. Update status and select REC file")
    # Update STT_PRGST_CD status
    mysql.update_stt_prgst_cd(
        stt_prgst_cd='02',
        stt_req_dtm=stt_req_dtm,
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    # Select REC file name and REC information
    rec_file_dict = dict()
    rcdg_id_list = mysql.select_rcdg_id(poli_no, ctrdt)
    # Update STT_PRC_SCD status
    mysql.update_stt_prc_scd(
        stt_prc_scd='01',
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    if not rcdg_id_list:
        raise Exception("No data RCDG_ID [POLI_NO : {0}, CTRDT : {1}]".format(poli_no, ctrdt))
    for tm_info in rcdg_id_list:
        rcdg_id = str(tm_info[0]).strip() if tm_info[0] else "None"
        results = mssql.select_tm_info(rcdg_id)
        if not results:
            raise Exception("No data TM information to DBO.VREC_STT_INFO, R_KEY_CODE = {0}".format(rcdg_id))
        for result in results:
            rcdf_file_nm = str(result[0]).strip() if result[0] else "None"
            rcdg_crnc_hms = str(result[1]).strip() if result[1] else "None"
            rcdg_stdtm = str(result[2]).strip() if result[2] else "None"
            rcdg_edtm = str(result[3]).strip() if result[3] else "None"
            cnsr_id = str(result[4]).strip() if result[4] else "None"
            result_dict = {
                'rcdg_id': rcdg_id,
                'rcdf_file_nm': rcdf_file_nm,
                'rcdg_crnc_hms': rcdg_crnc_hms,
                'rcdg_stdtm': rcdg_stdtm,
                'rcdg_edtm': rcdg_edtm,
                'cnsr_id': cnsr_id
            }
            rec_file_dict[rcdf_file_nm] = result_dict
    return rec_file_dict


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
            if db == 'MsSQL':
                os.environ['NLS_LANG'] = ".AL32UTF8"
                sql = MSSQL(logger)
            elif db == 'MySQL':
                sql = MySQL(logger)
            else:
                raise Exception("Unknown database [{0}], MsSQL or MySQL".format(db))
            logger.info('Success connect '.format(db))
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


def setup_data(job):
    """
    Setup data and target directory
    :param      job:           Job
    :return:                   Logger, POLI_NO(증서 번호), CTRDT(계약 일자),
                                STT_PRGST_CD(상태 코드), STT_REQ_DTM(CS 요청일시)
    """
    global OUTPUT_DIR_NAME
    global STT_TEMP_DIR_PATH
    global DELETE_FILE_LIST
    poli_no = str(job[0]).strip()
    ctrdt = str(job[1]).strip()
    stt_prgst_cd = str(job[2]).strip()
    stt_req_dtm = str(job[3]).strip()
    # Make Target directory name
    if stt_prgst_cd == '06':
        cnt = 0
        while True:
            OUTPUT_DIR_NAME = "{0}_{1}_supplementation_{2}".format(poli_no, ctrdt, cnt)
            output_dir_path = "{0}/{1}/{2}/{3}/{4}".format(
                STT_CONFIG['stt_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8], OUTPUT_DIR_NAME)
            if not os.path.exists(output_dir_path):
                break
            cnt += 1
    else:
        OUTPUT_DIR_NAME = "{0}_{1}".format(poli_no, ctrdt)
    STT_TEMP_DIR_PATH = "{0}/{1}".format(STT_CONFIG['stt_path'], OUTPUT_DIR_NAME)
    DELETE_FILE_LIST.append(STT_TEMP_DIR_PATH)
    if os.path.exists(STT_TEMP_DIR_PATH):
        shutil.rmtree(STT_TEMP_DIR_PATH)
    os.makedirs(STT_TEMP_DIR_PATH)
    # Add logging
    logger_args = {
        'base_path': STT_CONFIG['log_dir_path'],
        'log_file_name': '{0}.log'.format(OUTPUT_DIR_NAME),
        'log_level': STT_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    if stt_prgst_cd == '06':
        output_dir_path = '{0}/DELETE_TARGET_TM'.format(STT_TEMP_DIR_PATH)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
        delete_target_file_path = "{0}/{1}_{2}_delete_target.txt".format(output_dir_path, poli_no, ctrdt)
        delete_target_file = open(delete_target_file_path, 'w')
        print >> delete_target_file, "{0}\t{1}".format(poli_no, ctrdt)
        delete_target_file.close()
        db_upload_dir_path = "{0}/{1}.tmp/DELETE_TARGET_TM".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
        if not os.path.exists(db_upload_dir_path):
            os.makedirs(db_upload_dir_path)
        shutil.copy(delete_target_file_path, db_upload_dir_path)
    return logger, poli_no, ctrdt, stt_prgst_cd, stt_req_dtm


def processing(job):
    """
    CS processing
    :param      job:        Job(POLI_NO, CTRDT, STT_PRGST_CD)
    """
    # 0. Setup data
    logger, poli_no, ctrdt, stt_prgst_cd, stt_req_dtm = setup_data(job)
    logger.info("-" * 100)
    logger.info('Start CS')
    # 1. Connect DB
    mysql = connect_db(logger, 'MySQL')
    mssql = connect_db(logger, 'MsSQL')
    try:
        # 2. Update status and select REC file
        rec_file_dict = update_status_and_select_rec_file(logger, mysql, mssql, poli_no, ctrdt, stt_req_dtm)
        # 3. Copy data
        chn_tp = copy_data(logger, mysql, mssql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm, rec_file_dict)
        # 4. Make list file
        thread_cnt = make_pcm_list_file(logger)
        # 5. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 6. Execute unseg.exe and do_space.exe
        do_space_dir_path = execute_unseg_and_do_space(logger, poli_no, ctrdt)
        # 7. Make output
        make_output(logger, chn_tp, do_space_dir_path)
        # 8. Set output
        set_output(logger)
        # 9. Make wav file
        make_wav_file(logger, chn_tp, ctrdt)
        # 10. Make DB upload file
        make_file_for_tb_qa_stt_tm_info(logger, poli_no, ctrdt, stt_req_dtm, rec_file_dict)
        stt_tm_info_dict = make_file_for_tb_qa_stt_tm_rcdg_info(logger, mysql, poli_no, ctrdt, chn_tp, rec_file_dict)
        make_file_for_tb_qa_stt_tm_rst(logger, poli_no, ctrdt, chn_tp, rec_file_dict)
        # 11. Move output
        move_output(logger, ctrdt)
        # 12. Delete garbage list
        delete_garbage_file(logger)
        # 13. Print statistical data
        statistical_data(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("CS END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    CS ERROR   ----------")
        error_process(logger, mysql, poli_no, ctrdt, stt_req_dtm, '03')
        delete_garbage_file(logger)
        mssql.disconnect()
        mysql.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removehandler(handler)
        sys.exit(1)
    try:
        # 14. Execute TA
        sys.path.append(STT_CONFIG['ta_script_path'])
        import TA
        reload(TA)
        args = Namespace(
            logger=logger,
            dir_path=STT_OUTPUT_DIR_PATH,
            poli_no=poli_no,
            ctrdt=ctrdt,
            file_extension='txt',
            stt_tm_info_dict=stt_tm_info_dict
        )
        TA.main(args)


########
# main #
########
def main(job):
    """
    This is a program that execute CS
    :param      job:        JOB
    """
    global DT
    global ST
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        if len(job) > 0:
            processing(job)