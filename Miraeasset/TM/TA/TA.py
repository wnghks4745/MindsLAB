#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-11-30, modification: 2017-00-00"

###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import MySQLdb
import traceback
import subprocess
import workerpool
import collections
from datetime import datetime
from operator import itemgetter
from cfg.config import STT_CONFIG, TA_CONFIG, WORD2VEC_CONFIG, MYSQL_DB_CONFIG

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
RESULT_DIR_PATH = ""
OUTPUT_DIR_NAME = ""
TA_TEMP_DIR_PATH = ""
STT_OUTPUT_DIR_PATH = ""
NLP_INFO_DICT = dict()
DELETE_FILE_LIST = list()
PI_TIME_CHECK_DICT = dict()
START_SENTENCE_LIST = list()
DETECT_CATEGORY_DICT = dict()
PLICD_DICT = dict()
REP_PI_YN_LIST = list()
TA_FBWD_DTC_RST_LIST = list()
WAV_FILE_INFO_DICT = dict()
SCRIPT_SECTION_SAVE_DICT = dict()
PRODUCT_SECTION_SAVE_DICT = dict()
TARGET_CATEGORY_DICT = collections.OrderedDict()

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
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_data_to_scrt_sntc_dtc_info(self, sntc_no):
        query = """
            SELECT
                DTC_CONT
            FROM
                TB_QA_STT_SCRT_SNTC_DTC_INFO
            WHERE 1=1
                 AND SNTC_NO = %s
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

    def select_data_to_tm_cntr_scrt_info(self, poli_no, ctrdt, ref_pi_yn):
        query = """
            SELECT
                QA_SCRT_LCCD,
                QA_SCRT_MCCD,
                PI_NO
            FROM
                TB_QA_STT_TM_CNTR_SCRT_INFO
            WHERE 1=1
                 AND POLI_NO = %s
                 AND CTRDT = %s
                 AND REF_PI_YN = %s
        """
        bind = (
            poli_no,
            ctrdt,
            ref_pi_yn
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_scrt_sntc_info(self, qa_scrt_lccd, qa_scrt_mccd, pi_no):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_QA_STT_SCRT_SNTC_INFO
            WHERE 1=1
                 AND QA_SCRT_LCCD = %s
                 AND QA_SCRT_MCCD = %s
                 AND PI_NO = %s
                 AND USE_YN = 'Y'
        """
        bind = (
            qa_scrt_lccd,
            qa_scrt_mccd,
            pi_no
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def check_start_sntc(self, qa_scrt_lccd, qa_scrt_mccd, pi_no, sntc_no):
        query = """
            SELECT
                *
            FROM
                TB_QA_STT_SCRT_SNTC_INFO
            WHERE 1=1
                AND QA_SCRT_LCCD = %s
                AND QA_SCRT_MCCD = %s
                AND PI_NO = %s
                AND SNTC_NO = %s
                AND STRT_SNTC_YN = 'Y'
        """
        bind = (
            qa_scrt_lccd,
            qa_scrt_mccd,
            pi_no,
            sntc_no,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result:
            return True
        return False

    def check_prod_start_sntc(self, plicd, sntc_no):
        query = """
            SELECT
                *
            FROM
                TB_QA_STT_SCRT_PROD_SNTC_INFO
            WHERE 1=1
                AND PLICD = %s
                AND SNTC_NO = %s
                AND STRT_SNTC_YN = 'Y'
        """
        bind = (
            plicd,
            sntc_no,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result:
            return True
        return False

    def select_sntc_cd(self, sntc_cd):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_QA_STT_SCRT_SNTC_MST_INFO
            WHERE 1=1
                 AND SNTC_CD = %s
        """
        bind = (
            sntc_cd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_cust_asw_yn(self):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_QA_STT_SCRT_SNTC_MST_INFO
            WHERE 1=1
                 AND CUST_ASW_YN = 'Y'
        """
        self.cursor.execute(query, )
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_kywd_lit(self, sntc_no):
        query = """
            SELECT
                KYWD_LIT
            FROM
                TB_QA_STT_SCRT_SNTC_MST_INFO
            WHERE 1=1
                 AND SNTC_NO = %s
        """
        bind = (
            sntc_no,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_fbwd_impft_sntc_no(self, sntc_cd):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_QA_STT_SCRT_SNTC_MST_INFO
            WHERE 1=1
                 AND SNTC_CD = %s
        """
        bind = (
            sntc_cd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_dtc_sntc_cd_and_nm(self, sntc_no, sntc_cd):
        query = """
            SELECT
                DTC_SNTC_CD,
                DTC_SNTC_NM
            FROM
                TB_QA_STT_SCRT_SNTC_MST_INFO
            WHERE 1=1
                 AND SNTC_NO = %s
                 AND SNTC_CD = %s
        """
        bind = (
            sntc_no,
            sntc_cd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_tm_cntr_scrt_prod_info(self, poli_no, ctrdt, qa_scrt_lccd, qa_scrt_mccd, pi_no):
        query = """
             SELECT
                PLICD
             FROM
                TB_QA_STT_TM_CNTR_SCRT_PROD_INFO
             WHERE 1=1
                AND POLI_NO = %s
                AND CTRDT = %s
                AND QA_SCRT_LCCD = %s
                AND QA_SCRT_MCCD = %s
                AND PI_NO = %s
         """
        bind = (
            poli_no,
            ctrdt,
            qa_scrt_lccd,
            qa_scrt_mccd,
            pi_no
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_data_to_scrt_prod_sntc_info(self, plicd):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_QA_STT_SCRT_PROD_SNTC_INFO
            WHERE 1=1
                 AND PLICD = %s
                 AND USE_YN = 'Y'
        """
        bind = (
            plicd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_scrt_pi_info(self, qa_scrt_lccd, qa_scrt_mccd, pi_no):
        query = """
            SELECT
                QA_SCRT_LCCD,
                QA_SCRT_MCCD,
                PI_NO
            FROM
                TB_QA_STT_SCRT_PI_INFO
            WHERE 1=1
                AND QA_SCRT_LCCD = %s
                AND QA_SCRT_MCCD = %s
                AND PI_NO = %s
                AND SCTN_SAVE_YN = 'Y'
        """
        bind = (
            qa_scrt_lccd,
            qa_scrt_mccd,
            pi_no,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_pnt_prod_info(self, qa_scrt_lccd, qa_scrt_mccd, pi_no, poli_no, ctrdt, plicd):
        query = """
            SELECT
                B.PLICD
            FROM
                TB_QA_STT_TM_CNTR_SCRT_PROD_INFO AS A,
                TB_QA_STT_PNT_PROD_INFO AS B
            WHERE 1=1
                AND A.QA_SCRT_LCCD = %s
                AND A.QA_SCRT_MCCD = %s
                AND A.PI_NO = %s
                AND A.POLI_NO = %s
                AND A.CTRDT = %s
                AND A.PLICD = %s
                AND B.SCTN_SAVE_YN = 'Y'
                AND A.PLICD = B.PLICD
        """
        bind = (
            qa_scrt_lccd,
            qa_scrt_mccd,
            pi_no,
            poli_no,
            ctrdt,
            plicd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return True


#######
# def #
#######


def elapsed_time(sdate):
    """
    elapsed time
    :param          sdate:          date object
    :return                         Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def check_file(name_form, file_name):
    """
    Check file name
    :param          name_form:          Check file name form
    :param          file_name:          Input file name
    :return:                            True or False
    """
    return file_name.endswith(name_form)


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param          logger:         Logger
    :param          delete_file_path:           Input path
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
    :param          logger:         Logger
    :param          cmd:            Command
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)


def pool_sub_process(cmd):
    """
    Execute subprocess
    :param      cmd:        Command
    """
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    sub_pro.communicate()


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param          logger:         Logger
    """
    logger.info("15. Delete garbage file")
    for list_file in DELETE_FILE_LIST:
        try:
            del_garbage(logger, list_file)
        except Exception as e:
            print e
            continue


def vec_word_combine(tmp_result, output, strs_list, ws, level):
    """
    Vec word combine
    :param      tmp_result:         Temp result
    :param      output:             Output
    :param      strs_list:          Strs list
    :param      ws:                 Ws
    :param      level:              Level
    :return:                        Temp result
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


def connect_db(logger, db):
    """
    Connect database
    :param          logger:         Logger
    :param          db:             Database
    :return:                        SQL Object
    """
    # Connect DB
    logger.info('Connect {0} ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            sql = MySQL(logger)
            logger.info("Success connect ".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                print "Fail connect {0}, retrying count = {1}".format(db, cnt)
                logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        err_str = "Fail connect {0}".format(db)
        raise Exception(err_str)
    return sql


def move_output(logger, args):
    """
    Move output
    :param          logger:         Logger
    :param          args:           Arguments
    """
    logger.info("14. Move output")
    output_dir_path = "{0}/{1}/{2}/{3}".format(
        TA_CONFIG['ta_output_path'], args.ctrdt[:4], args.ctrdt[4:6], args.ctrdt[6:8])
    output_abs_dir_path = "{0}/{1}".format(output_dir_path, OUTPUT_DIR_NAME)
    if os.path.exists(output_abs_dir_path):
        del_garbage(logger, output_abs_dir_path)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    shutil.move(TA_TEMP_DIR_PATH, output_dir_path)


def make_file_for_tb_qa_stt_tm_sctn_rcdg_save(logger, args):
    """
    Make file for TB_QA_STT_TM_SCTN_RCDG_SAVE
    :param          logger:             Logger
    :param          args:               Argument
    """
    logger.info("13. Make file for TB_QA_STT_TM_SCTN_RCDG_SAVE")
    mysql = connect_db(logger, 'MySQL')
    output_dir_path = "{0}/TB_QA_STT_TM_SCTN_RCDG_SAVE".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_sctn_rcdg_save.txt".format(output_dir_path, args.poli_no, args.ctrdt)
    output_file = open(output_file_path, 'w')
    # SCRIPT_SECTION_SAVE_DICT
    script_cnt = 0
    for tm_info in WAV_FILE_INFO_DICT.values():
        qa_scrt_lccd = tm_info.get('qa_scrt_lccd')
        qa_scrt_mccd = tm_info.get('qa_scrt_mccd')
        pi_no = tm_info.get('pi_no')
        rcdg_file_path_nm = tm_info.get('rcdg_file_path_nm')
        rcdg_file_nm = tm_info.get('rcdg_file_nm')
        rcdg_id = tm_info.get('rcdg_id')
        save_list_result = mysql.select_data_to_scrt_pi_info(qa_scrt_lccd, qa_scrt_mccd, pi_no)
        if not save_list_result:
            continue
        for result in save_list_result:
            script_section_key = "{0}_{1}_{2}".format(result[0], result[1], result[2])
            if script_section_key in SCRIPT_SECTION_SAVE_DICT:
                pi_sttm = SCRIPT_SECTION_SAVE_DICT[script_section_key][0]
                pi_endtm = SCRIPT_SECTION_SAVE_DICT[script_section_key][1]
                sctn_save_file_nm = "{0}_{1}_{2}.wav".format(rcdg_file_nm.replace(".", "__"), pi_sttm, pi_endtm)
                sctn_save_file_path = "{0}/{1}".format(os.path.dirname(rcdg_file_path_nm), sctn_save_file_nm)
                duration = int(pi_endtm) - int(pi_sttm)
                if not os.path.exists("{0}/{1}".format(STT_CONFIG['wav_output_path'], sctn_save_file_path)):
                    cmd = "sox {vp}/{rc} {vp}/{sa} trim {st} {du}".format(
                        vp=STT_CONFIG['wav_output_path'], rc=rcdg_file_path_nm,
                        sa=sctn_save_file_path, st=pi_sttm, du=duration)
                    sub_process(logger, cmd)
            else:
                continue
            output_dict = collections.OrderedDict()
            output_dict['sctn_save_dcd'] = '40'
            output_dict['poli_no'] = args.poli_no
            output_dict['ctrdt'] = args.ctrdt
            output_dict['sctn_save_seq'] = str(script_cnt)
            output_dict['rcdg_id'] = rcdg_id
            output_dict['rcdg_file_path_nm'] = rcdg_file_path_nm
            output_dict['rcdg_file_nm'] = rcdg_file_nm
            output_dict['sctn_save_file_nm'] = sctn_save_file_nm
            output_dict['sctn_rcdg_crt_dtm'] = str(datetime.now())
            output_dict['sctn_sttm'] = pi_sttm
            output_dict['sctn_endtm'] = pi_endtm
            output_list = list(output_dict.values())
            print >> output_file, "\t".join(output_list)
            script_cnt += 1
    # PRODUCT_SECTION_SAVE_DICT
    product_cnt = 0
    for prod_tm_info in PRODUCT_SECTION_SAVE_DICT.values():
        prod_qa_scrt_lccd = prod_tm_info.get('qa_scrt_lccd')
        prod_qa_scrt_mccd = prod_tm_info.get('qa_scrt_mccd')
        prod_pi_no = prod_tm_info.get('pi_no')
        prod_plicd = prod_tm_info.get('plicd')
        prod_rcdg_file_nm = prod_tm_info.get('rcdg_file_nm')
        prod_rcdg_file_path_nm = prod_tm_info.get('rcdg_file_path_nm')
        prod_pi_sttm = prod_tm_info.get('pi_sttm')
        modified_prod_pi_sttm = "{0}:{1}:{2}".format(prod_pi_sttm[:2], prod_pi_sttm[2:4], prod_pi_sttm[4:6])
        prod_pi_endtm = prod_tm_info.get('pi_endtm')
#        modified_prod_pi_endtm = "{0}:{1}:{2}".format(prod_pi_endtm[:2], prod_pi_endtm[2:4], prod_pi_endtm[4:6])
        prod_rcdg_id = prod_tm_info.get('rcdg_id')
        prod_result = mysql.select_data_to_pnt_prod_info(
            prod_qa_scrt_lccd, prod_qa_scrt_mccd, prod_pi_no, args.poli_no, args.ctrdt, prod_plicd)
        if prod_result:
            sctn_save_file_nm = "{0}_{1}_{2}.wav".format(
                prod_rcdg_file_nm.replace(".", "__"), prod_pi_sttm, prod_pi_endtm)
            sctn_save_file_path = "{0}/{1}".format(os.path.dirname(prod_rcdg_file_path_nm), sctn_save_file_nm)
            prod_duration = int(prod_pi_endtm) - int(prod_pi_sttm)
            if not os.path.exists("{0}/{1}".format(STT_CONFIG['wav_output_path'], sctn_save_file_path)):
                cmd = "sox {vp}/{rc} {vp}/{sa} trim {st} {du}".format(
                    vp=STT_CONFIG['wav_output_path'], rc=prod_rcdg_file_path_nm,
                    sa=sctn_save_file_path, st=modified_prod_pi_sttm, du=prod_duration)
                sub_process(logger, cmd)
            output_dict = collections.OrderedDict()
            output_dict['sctn_save_dcd'] = '40'
            output_dict['poli_no'] = args.poli_no
            output_dict['ctrdt'] = args.ctrdt
            output_dict['sctn_save_seq'] = str(product_cnt)
            output_dict['rcdg_id'] = prod_rcdg_id
            output_dict['rcdg_file_path_nm'] = prod_rcdg_file_path_nm
            output_dict['rcdg_file_nm'] = prod_rcdg_file_nm
            output_dict['sctn_save_file_nm'] = sctn_save_file_nm
            output_dict['sctn_rcdg_crt_dtm'] = str(datetime.now())
            output_dict['sctn_sttm'] = prod_pi_sttm
            output_dict['sctn_endtm'] = prod_pi_endtm
            output_list = list(output_dict.values())
            print >> output_file, "\t".join(output_list)
            product_cnt += 1
    output_file.close()
    mysql.disconnect()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_SCTN_RCDG_SAVE".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    shutil.copy(output_file_path, db_upload_dir_path)


def make_file_for_tb_qa_stt_tm_fbwd_dtc_rst(logger, args):
    """
    Make db upload file for TB_QA_STT_TM_FBWD_DTC_RST
    :param          logger:             Logger
    :param          args:               Arguments
    """
    logger.info("12. Make db upload file for TB_QA_STT_TM_FBWD_DTC_RST")
    output_dir_path = "{0}/TB_QA_STT_TM_FBWD_DTC_RST".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    utf8_output_file_path = "{0}/utf8_{1}_{2}_tb_qa_stt_tm_fbwd_dtc_rst.txt".format(
        output_dir_path, args.poli_no, args.ctrdt)
    output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_fbwd_dtc_rst.txt".format(output_dir_path, args.poli_no, args.ctrdt)
    output_file = open(utf8_output_file_path, 'w')
    for line_list in TA_FBWD_DTC_RST_LIST:
        print >> output_file, "\t".join(line_list)
    output_file.close()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_FBWD_DTC_RST".format(
        STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    cmd = "iconv -f utf-8 -t euc-kr {0} > {1}".format(utf8_output_file_path, output_file_path)
    sub_process(logger, cmd)
    shutil.copy(output_file_path, db_upload_dir_path)


def make_file_for_tb_qa_stt_tm_cntr_scrt_info(logger, args):
    """
    Make db upload file for TB_QA_STT_TM_CNTR_SCRT_INFO
    :param          logger:         Logger
    :param          args:           Arguments
    """
    global SCRIPT_SECTION_SAVE_DICT
    logger.info("11. Make db upload file for TB_QA_STT_TM_CNTR_SCRT_INFO")
    output_dir_path = "{0}/TB_QA_STT_TM_CNTR_SCRT_INFO".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_cntr_scrt_info.txt".format(output_dir_path, args.poli_no, args.ctrdt)
    output_file = open(output_file_path, 'w')
    for key, value in TARGET_CATEGORY_DICT.items():
        key_list = key.split("|")
        qa_scrt_lccd = key_list[0]
        qa_scrt_mccd = key_list[1]
        pi_no = key_list[2]
        pi_dtc_yn = 'N'
        pi_sttm = "None"
        pi_endtm = "None"
        rcdg_id = "None"
        rcdg_file_nm = "None"
        if key in DETECT_CATEGORY_DICT:
            detect_sntc_no_list = DETECT_CATEGORY_DICT[key]
            target_sntc_no_list = value
            if set(target_sntc_no_list) == set(detect_sntc_no_list) & set(target_sntc_no_list):
                pi_dtc_yn = 'Y'
            else:
                pi_dtc_yn = 'P'
        if key in PI_TIME_CHECK_DICT:
            pi_sttm = PI_TIME_CHECK_DICT[key][0]
            pi_endtm = PI_TIME_CHECK_DICT[key][1]
            rcdg_id = PI_TIME_CHECK_DICT[key][2]
            rcdg_file_nm = PI_TIME_CHECK_DICT[key][3]
        output_dict = collections.OrderedDict()
        output_dict['qa_scrt_lccd'] = qa_scrt_lccd
        output_dict['qa_scrt_mccd'] = qa_scrt_mccd
        output_dict['pi_no'] = pi_no
        output_dict['poli_no'] = args.poli_no
        output_dict['ctrdt'] = args.ctrdt
        output_dict['pi_dtc_yn'] = pi_dtc_yn
        output_dict['pi_sttm'] = pi_sttm
        output_dict['pi_endtm'] = pi_endtm
        output_dict['rcdg_id'] = rcdg_id
        output_dict['rcdg_file_nm'] = rcdg_file_nm
        output_list = list(output_dict.values())
        print >> output_file, "\t".join(output_list)
        if pi_dtc_yn == 'Y' or pi_dtc_yn == 'P':
            script_section_key = "{0}_{1}_{2}".format(qa_scrt_lccd, qa_scrt_mccd, pi_no)
            SCRIPT_SECTION_SAVE_DICT[script_section_key] = [pi_sttm, pi_endtm]
    output_file.close()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_CNTR_SCRT_INFO".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    shutil.copy(output_file_path, db_upload_dir_path)


def make_cust_agrm_sntc_cont_dict(dir_path, file_name, rcdg_file_nm):
    """
    Make customer agreement sentence cont dictionary
    :param          dir_path:           Directory path
    :param          file_name:          File name
    :param          rcdg_file_nm:       Record file name
    :return:                            Customer agreement sentence cont dictionary
    """
    cust_agrm_sntc_cont_dict = dict()
    final_output_file = open(os.path.join(dir_path, file_name), 'r')
    for line in final_output_file:
        line = line.strip()
        line_list = line.split("\t")
        rcdg_file_name = line_list[1].replace("_trx", "").replace("__", ".")
        line_number = line_list[2]
        sent = line_list[6]
        key = "{0}_{1}".format(rcdg_file_name, line_number)
        if sent.startswith("[C]") and rcdg_file_name == rcdg_file_nm:
            cust_agrm_sntc_cont_dict[key] = sent.replace("[C]", "").strip()
    final_output_file.close()
    return cust_agrm_sntc_cont_dict


def make_cust_agrm_sntc_cont(cust_agyn, dir_path, file_name, stt_sntc_lin_no, rcdg_file_nm):
    """
    Make customer agreement sentence cont
    :param          cust_agyn:              Customer agreement Y/N
    :param          dir_path:               Directory path
    :param          file_name:              File name
    :param          stt_sntc_lin_no:        CS sentence line number
    :param          rcdg_file_nm:           Record file name
    :return:                                Customer agreement sentence cont
    """
    cust_agrm_sntc_cont_dict = make_cust_agrm_sntc_cont_dict(dir_path, file_name, rcdg_file_nm)
    if cust_agyn == 'Y':
        cust_agrm_sntc_cont = ""
        for cnt in range(1, 4):
            key = "{0}_{1}".format(rcdg_file_nm, str(cnt + int(stt_sntc_lin_no)))
            if key in cust_agrm_sntc_cont_dict:
                cust_agrm_sntc_cont += cust_agrm_sntc_cont_dict[key] + " "
        cust_agrm_sntc_cont = cust_agrm_sntc_cont.strip()
        if len(cust_agrm_sntc_cont) < 1:
            cust_agrm_sntc_cont = "None"
    else:
        cust_agrm_sntc_cont = "None"
    return cust_agrm_sntc_cont


def set_data_for_tb_qa_stt_tm_ta_dtc_rst(logger, args, dir_path, file_name):
    """
    Set data for TB_QA_STT_TM_TA_DTC_RST
    :param          logger:                 Logger
    :param          args:                   Arguments
    :param          dir_path:               Directory path
    :param          file_name:              File name
    :return:                                Upload data list
    """
    global WAV_FILE_INFO_DICT
    global PI_TIME_CHECK_DICT
    global DETECT_CATEGORY_DICT
    global TA_FBWD_DTC_RST_LIST
    global PRODUCT_SECTION_SAVE_DICT
    mysql = connect_db(logger, 'MySQL')
    # 금칙어 리스트
    fbwd_list = list()
    fbwd_result = mysql.select_sntc_cd("FBWD")
    for item in fbwd_result:
        fbwd_list.append(item[0])
    # 불완전 판매 리스트
    impft_list = list()
    impft_result = mysql.select_sntc_cd("IMPFT")
    for item in impft_result:
        impft_list.append(item[0])
    # 고객 답변 여부 리스트
    cust_asw_list = list()
    cust_asw_yn_result = mysql.select_cust_asw_yn()
    for item in cust_asw_yn_result:
        cust_asw_list.append(item[0])
    sntc_seq_cnt = 0
    upload_data_list = list()
    overlap_check_dict = dict()
    final_output_file = open(os.path.join(dir_path, file_name), 'r')
    for line in final_output_file:
        line = line.strip()
        line_list = line.split("\t")
        if line_list[4] == 'none' or line_list[4] == 'miss':
            continue
        category = line_list[4].split("_")[0]
        category_list = category.split("|")
        if len(category_list) < 3:
            qa_scrt_lccd = category_list[0]
            qa_scrt_mccd = category_list[0]
            pi_no = category_list[0]
        else:
            qa_scrt_lccd = category_list[0]
            qa_scrt_mccd = category_list[1]
            pi_no = category_list[2]
        sntc_no = line_list[4].split("_")[1]
        overlap_check_key = "{0}_{1}_{2}_{3}_{4}_{5}".format(
            args.poli_no, args.ctrdt, qa_scrt_lccd, qa_scrt_mccd, pi_no, sntc_no)
        if overlap_check_key not in overlap_check_dict:
            sntc_seq = str(sntc_seq_cnt)
        else:
            sntc_seq_cnt += 1
            sntc_seq = str(sntc_seq_cnt)
        overlap_check_dict[overlap_check_key] = 1
        rcdg_file_nm = os.path.basename(line_list[1]).replace("_trx", "").replace("__", ".")
        rcdg_id = args.stt_tm_info_dict[rcdg_file_nm]["rcdg_id"]
        rcdg_file_path_nm = args.stt_tm_info_dict[rcdg_file_nm]["rcdg_file_path_nm"]
        chn_tp = args.stt_tm_info_dict[rcdg_file_nm]["chn_tp"]
        sntc_cont = line_list[6].replace("[C]", "").replace("[S]", "").replace("[M]", "").strip()
        stt_sntc_lin_no = line_list[2].strip()
        scrt_sntc_sttm = line_list[8]
        scrt_sntc_endtm = line_list[9]
        temp_sntc_sttm = scrt_sntc_sttm.replace(":", "")
        temp_sntc_endtm = scrt_sntc_endtm.replace(":", "")
        modified_sntc_sttm = temp_sntc_sttm if len(temp_sntc_sttm) == 6 else "0" + temp_sntc_sttm
        modified_sntc_endt = temp_sntc_endtm if len(temp_sntc_endtm) == 6 else "0" + temp_sntc_endtm
        cust_agyn = 'Y' if sntc_no in cust_asw_list and chn_tp == 'S' else 'N'
        cust_agrm_sntc_cont = make_cust_agrm_sntc_cont(cust_agyn, dir_path, file_name, stt_sntc_lin_no, rcdg_file_nm)
        keyword_result = mysql.select_kywd_lit(sntc_no)
        if not keyword_result:
            dtc_kywd_lit = "None"
            nudtc_kywd_lit = "None"
            kywd_dtc_rate = "0.00"
        elif not keyword_result[0]:
            dtc_kywd_lit = "None"
            nudtc_kywd_lit = "None"
            kywd_dtc_rate = "0.00"
        else:
            keyword = keyword_result[0].encode('euc-kr').strip()
            if keyword.endswith(";"):
                keyword = keyword[:-1]
            elif keyword.startswith(";"):
                keyword = keyword[1:]
            keyword_list = keyword.split(";")
            nlp_sent = line_list[7].replace("[ C ]", "").replace("[ S ]", "").replace("[ M ]", "").strip()
            nlp_sent_list = nlp_sent.split()
            dtc_kywd_lit = ";".join(list(set(keyword_list) & set(nlp_sent_list)))
            cnt_dtc_kywd_lit = len(list(set(keyword_list) & set(nlp_sent_list)))
            nudtc_kywd_lit = ";".join(list(set(keyword_list) - set(nlp_sent_list)))
            dtc_rate = (float(cnt_dtc_kywd_lit) / float(len(keyword_list))) * 100
            kywd_dtc_rate = '%.2f' % dtc_rate
            if len(dtc_kywd_lit) < 1:
                dtc_kywd_lit = "None"
            if len(nudtc_kywd_lit) < 1:
                nudtc_kywd_lit = "None"
        plicd_key = "{0}|{1}|{2}_{3}".format(qa_scrt_lccd, qa_scrt_mccd, pi_no, sntc_no)
        plicd = PLICD_DICT[plicd_key] if plicd_key in PLICD_DICT else "None"
        ta_fbwd_dtc_rst_dict = collections.OrderedDict()
        if sntc_no in fbwd_list or sntc_no in impft_list:
            if sntc_no in fbwd_list:
                dtc_dcd = '00'
                sntc_dtc_lst_result = mysql.select_dtc_sntc_cd_and_nm(sntc_no, "FBWD")
            else:
                dtc_dcd = '01'
                sntc_dtc_lst_result = mysql.select_dtc_sntc_cd_and_nm(sntc_no, "IMPFT")
            if not sntc_dtc_lst_result:
                logger.error("Can't select DTC_SNTC_CD and DTC_SNTC_NM, SNTC_NO = {0}".format(sntc_no))
                continue
            dtc_snct_cd = sntc_dtc_lst_result[0]
            dtc_sntc_nm = sntc_dtc_lst_result[1]
            ta_fbwd_dtc_rst_dict['poli_no'] = args.poli_no
            ta_fbwd_dtc_rst_dict['ctrdt'] = args.ctrdt
            ta_fbwd_dtc_rst_dict['sntc_no'] = sntc_no
            ta_fbwd_dtc_rst_dict['sntc_seq'] = sntc_seq
            ta_fbwd_dtc_rst_dict['dtc_dcd'] = dtc_dcd
            ta_fbwd_dtc_rst_dict['dtc_sntc_cd'] = dtc_snct_cd
            ta_fbwd_dtc_rst_dict['dtc_sntc_nm'] = dtc_sntc_nm
            ta_fbwd_dtc_rst_dict['rcdg_id'] = rcdg_id
            ta_fbwd_dtc_rst_dict['rcdg_file_path_nm'] = rcdg_file_path_nm
            ta_fbwd_dtc_rst_dict['rcdg_file_nm'] = rcdg_file_nm
            ta_fbwd_dtc_rst_dict['sntc_cont'] = sntc_cont.decode('euc-kr')
            ta_fbwd_dtc_rst_dict['stt_sntc_lin_no'] = stt_sntc_lin_no
            ta_fbwd_dtc_rst_dict['scrt_sntc_sttm'] = modified_sntc_sttm
            ta_fbwd_dtc_rst_dict['scrt_sntc_endtm'] = modified_sntc_endt
            ta_fbwd_dtc_rst_dict['dtc_info_crt_dtm'] = str(datetime.now())
            TA_FBWD_DTC_RST_LIST.append(ta_fbwd_dtc_rst_dict.values())
            continue
        output_str = "{0}\t".format(args.poli_no)
        output_str += "{0}\t".format(args.ctrdt)
        output_str += "{0}\t".format(qa_scrt_lccd)
        output_str += "{0}\t".format(qa_scrt_mccd)
        output_str += "{0}\t".format(pi_no)
        output_str += "{0}\t".format(sntc_no)
        output_str += "{0}\t".format(sntc_seq)
        output_str += "{0}\t".format(plicd)
        output_str += "{0}\t".format(rcdg_id)
        output_str += "{0}\t".format(rcdg_file_path_nm)
        output_str += "{0}\t".format(rcdg_file_nm)
        output_str += "{0}\t".format(sntc_cont)
        output_str += "{0}\t".format(stt_sntc_lin_no)
        output_str += "{0}\t".format(modified_sntc_sttm)
        output_str += "{0}\t".format(modified_sntc_endt)
        output_str += "{0}\t".format('Y')
        output_str += "{0}\t".format(cust_agyn)
        output_str += "{0}\t".format(cust_agrm_sntc_cont)
        output_str += "{0}\t".format(dtc_kywd_lit)
        output_str += "{0}\t".format(nudtc_kywd_lit)
        output_str += "{0}\t".format("None")
        output_str += "{0}\t".format(kywd_dtc_rate)
        output_str += "{0}\t".format("None")
        output_str += "{0}\t".format("None")
        output_str += "{0}".format(str(datetime.now()))
        upload_data_list.append(output_str)
        category_key = "{0}|{1}|{2}".format(qa_scrt_lccd, qa_scrt_mccd, pi_no)
        if category_key not in DETECT_CATEGORY_DICT:
            DETECT_CATEGORY_DICT[category_key] = [sntc_no]
        else:
            DETECT_CATEGORY_DICT[category_key].append(sntc_no)
        if category_key in PI_TIME_CHECK_DICT:
            if PI_TIME_CHECK_DICT[category_key][3] == rcdg_file_nm:
                PI_TIME_CHECK_DICT[category_key][1] = modified_sntc_endt
        else:
            PI_TIME_CHECK_DICT[category_key] = [modified_sntc_sttm, modified_sntc_endt, rcdg_id, rcdg_file_nm]
        WAV_FILE_INFO_DICT[category_key] = {
            'qa_scrt_lccd': qa_scrt_lccd,
            'qa_scrt_mccd': qa_scrt_mccd,
            'rcdg_id': rcdg_id,
            'rcdg_file_path_nm': rcdg_file_path_nm,
            'pi_no': pi_no,
            'rcdg_file_nm': rcdg_file_nm
        }
        if plicd != 'None':
            PRODUCT_SECTION_SAVE_DICT[category_key] = {
                'qa_scrt_lccd': qa_scrt_lccd,
                'qa_scrt_mccd': qa_scrt_mccd,
                'rcdg_id': rcdg_id,
                'rcdg_file_path_nm': rcdg_file_path_nm,
                'pi_no': pi_no,
                'plicd': plicd,
                'pi_sttm': modified_sntc_sttm,
                'pi_endtm': modified_sntc_endt,
                'rcdg_file_nm': rcdg_file_nm
            }
    final_output_file.close()
    mysql.disconnect()
    return upload_data_list


def make_file_for_tb_qa_stt_tm_ta_dtc_rst(logger, args, final_output_dir_path):
    """
    Make db upload file for TB_QA_STT_TM_TA_DTC_RST
    :param          logger:                     Logger
    :param          args:                       Arguments
    :param          final_output_dir_path:      Final output directory path
    """
    logger.info("10. Make db upload file for TB_QA_STT_TM_TA_DTC_RST")
    output_dir_path = "{0}/TB_QA_STT_TM_TA_DTC_RST".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_ta_dtc_rst.txt".format(output_dir_path, args.poli_no, args.ctrdt)
    output_file = open(output_file_path, 'w')
    w_ob = os.walk(final_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            upload_data_list = set_data_for_tb_qa_stt_tm_ta_dtc_rst(logger, args, dir_path, file_name)
            for line in upload_data_list:
                line = line.strip()
                print >> output_file, line
    output_file.close()
    db_upload_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_TA_DTC_RST".format(STT_CONFIG['db_upload_path'], OUTPUT_DIR_NAME)
    if not os.path.exists(db_upload_dir_path):
        os.makedirs(db_upload_dir_path)
    shutil.copy(output_file_path, db_upload_dir_path)


def modify_hmd_output(logger, sorted_hmd_output_dir_path):
    """
    Modify HMD output
    :param      logger:                             Logger
    :param      sorted_hmd_output_dir_path:         Sorted HMD output directory path
    """
    logger.info("9. Modify HMD output")
    final_output_dir_path = "{0}/final_output".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(final_output_dir_path):
        os.makedirs(final_output_dir_path)
    output_list = list()
    w_ob = os.walk(sorted_hmd_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        files.sort()
        for file_name in files:
            temp_section_cd = ""
            overlap_check_dict = dict()
            hmd_output_file = open(os.path.join(dir_path, file_name), 'r')
            for line in hmd_output_file:
                line = line.strip()
                line_list = line.split("\t")
                line_num = line_list[2].strip()
                category = line_list[4].strip()
                sent = line_list[6].strip()
                overlap_check_key = "{0}_{1}_{2}".format(line_num, category, sent)
                if category == 'none':
                    if overlap_check_key not in overlap_check_dict:
                        output_list.append(line_list)
                        overlap_check_dict[overlap_check_key] = 1
                    continue
                section_cd = category.split("_")[0]
                if section_cd == 'FBWD' or section_cd == 'IMPFT':
                    if overlap_check_key not in overlap_check_dict:
                        output_list.append(line_list)
                        overlap_check_dict[overlap_check_key] = 1
                    continue
                if category in START_SENTENCE_LIST:
                    temp_section_cd = section_cd
                    if overlap_check_key not in overlap_check_dict:
                        output_list.append(line_list)
                        overlap_check_dict[overlap_check_key] = 1
                    continue
                if section_cd == temp_section_cd:
                    if overlap_check_key not in overlap_check_dict:
                        output_list.append(line_list)
                        overlap_check_dict[overlap_check_key] = 1
                    continue
                if section_cd != temp_section_cd:
                    line_list[4] = "miss"
                    if overlap_check_key not in overlap_check_dict:
                        output_list.append(line_list)
                        overlap_check_dict[overlap_check_key] = 1
                    continue
            hmd_output_file.close()
    # Make time information dictionary
    detail_dir_path = "{0}/detail".format(STT_OUTPUT_DIR_PATH)
    w_ob = os.walk(detail_dir_path)
    time_info_dict = dict()
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            detail_file = open(os.path.join(dir_path, file_name), 'r')
            cnt = 0
            for line in detail_file:
                line = line.strip()
                line_list = line.split("\t")
                wav_file_name = os.path.splitext(file_name)[0]
                start_time = line_list[1]
                end_time = line_list[2]
                if str(cnt) not in time_info_dict:
                    time_info_dict[str(cnt)] = {wav_file_name: [start_time, end_time]}
                else:
                    time_info_dict[str(cnt)].update({wav_file_name: [start_time, end_time]})
                cnt += 1
            detail_file.close()
    # Add NLP and time information
    for item_list in output_list:
        rcdg_file_nm = item_list[1]
        stt_sntc_lin_no = item_list[2]
        ctrdt = item_list[3]
        nlp_info_key = "{0}|{1}|{2}".format(rcdg_file_nm, stt_sntc_lin_no, ctrdt)
        nlp_sent = NLP_INFO_DICT[nlp_info_key]
        item_list.append(nlp_sent)
        time_info = time_info_dict[stt_sntc_lin_no][rcdg_file_nm]
        scrt_sntc_sttm = time_info[0]
        scrt_sntc_endtm = time_info[1]
        item_list.append(scrt_sntc_sttm)
        item_list.append(scrt_sntc_endtm)
    final_output_file = open("{0}/{1}.txt".format(final_output_dir_path, OUTPUT_DIR_NAME), 'w')
    for item_list in output_list:
        print >> final_output_file, "\t".join(item_list)
    final_output_file.close()
    return final_output_dir_path


def sort_hmd_output(logger, hmd_output_dir_path):
    """
    Sort HMD output
    :param          logger:                     Logger
    :param          hmd_output_dir_path:        HMD output directory path
    :return:                                    Sorted HMD output directory path
    """
    logger.info("8. Modify HMD output")
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


def execute_hmd(logger, matrix_file_path, modified_line_number_dir_path):
    """
    Execute HMD
    :param          logger:                                 Logger
    :param          matrix_file_path:                       Matrix file path
    :param          modified_line_number_dir_path:          Modified line number file directory
    :return                                                 HMD output directory path
    """
    global DELETE_FILE_LIST
    logger.info("7. Execute HMD")
    os.chdir(TA_CONFIG['hmd_script_path'])
    hmd_file_list = glob.glob("{0}/*".format(modified_line_number_dir_path))
    hmd_output_dir_path = "{0}/HMD_result".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(hmd_output_dir_path):
        os.makedirs(hmd_output_dir_path)
    start = 0
    end = 0
    cmd_list = list()
    thread = len(hmd_file_list) if len(hmd_file_list) < int(TA_CONFIG['hmd_thread']) else int(TA_CONFIG['hmd_thread'])
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


def modify_nlp_output_line_number(logger):
    """
    Modify NLP output file line number
    :param          logger:                         Logger
    :return:                                        Modified NLP file directory path
    """
    global NLP_INFO_DICT
    logger.info("6. Modify NLP output file line number")
    hmd_result_dir_path = "{0}/HMD".format(TA_TEMP_DIR_PATH)
    modified_nlp_line_number_dir_path = "{0}/modified_nlp_line_number".format(TA_TEMP_DIR_PATH)
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
                merge_nlp_sent = line_list[4].strip()
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
                    merge_temp_list.append(line_list)
                    merge_temp_sent = merge_sent
                    merge_temp_nlp_sent = merge_nlp_sent
                    merge_temp_num += 1
                    if idx == len(hmd_result_file_list):
                        line_list[1] = str(merge_temp_num).strip()
                        line_list[3] = merge_temp_sent.strip()
                        line_list[4] = merge_temp_nlp_sent.strip()
                        merge_temp_list.append(line_list)
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
                    key = "{0}|{1}|{2}".format(merged_line_list[0], merged_line_list[1], merged_line_list[2])
                    NLP_INFO_DICT[key] = merged_line_list[4]
                    print >> modified_line_number_file, "\t".join(merged_line_list)
                    line_number += 1
            hmd_result_file.close()
            modified_line_number_file.close()
    return modified_nlp_line_number_dir_path


def make_matrix_file(logger, args):
    """
    Make matrix file
    :param          logger:             Logger
    :param          args:               Arguments
    :return:                            Matrix file path
    """
    global PLICD_DICT
    global REP_PI_YN_LIST
    global START_SENTENCE_LIST
    global TARGET_CATEGORY_DICT
    logger.info("5. Make matrix file")
    mysql = connect_db(logger, 'MySQL')
    # Select SNTC_NO and DTC_CONT (문장 번호, 탐지 사전 내용)
    scrt_sntc_dtc_info_result_list = list()
    # 금칙어, 불완전판매 matrix
    fbwd_result = mysql.select_fbwd_impft_sntc_no("FBWD")
    for fbwd_item in fbwd_result:
        fbwd_sntc_no = fbwd_item[0]
        fbwd_scrt_sntc_dtc_info_result = mysql.select_data_to_scrt_sntc_dtc_info(fbwd_sntc_no)
        if not fbwd_scrt_sntc_dtc_info_result:
            continue
        for fbwd_dtc_cont_result in fbwd_scrt_sntc_dtc_info_result:
            fbwd_dtc_cont = fbwd_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(["FBWD", fbwd_sntc_no, fbwd_dtc_cont])
    impft_result = mysql.select_fbwd_impft_sntc_no("IMPFT")
    for impft_item in impft_result:
        impft_sntc_no = impft_item[0]
        impft_scrt_sntc_dtc_info_result = mysql.select_data_to_scrt_sntc_dtc_info(impft_sntc_no)
        if not impft_scrt_sntc_dtc_info_result:
            continue
        for impft_dtc_cont_result in impft_scrt_sntc_dtc_info_result:
            impft_dtc_cont = impft_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(["IMPFT", impft_sntc_no, impft_dtc_cont])
    # 스크립트 문항 matrix
    tm_cntr_scrt_info_result_n = mysql.select_data_to_tm_cntr_scrt_info(args.poli_no, args.ctrdt, 'N')
    if not tm_cntr_scrt_info_result_n:
        tm_cntr_scrt_info_result_n = list()
    for tm_cntr_scrt_info_item_n in tm_cntr_scrt_info_result_n:
        qa_scrt_lccd_n = tm_cntr_scrt_info_item_n[0]
        qa_scrt_mccd_n = tm_cntr_scrt_info_item_n[1]
        pi_no_n = tm_cntr_scrt_info_item_n[2]
        scrt_sntc_info_result = mysql.select_data_to_scrt_sntc_info(qa_scrt_lccd_n, qa_scrt_mccd_n, pi_no_n)
        if not scrt_sntc_info_result:
            continue
        for scrt_sntc_info_item in scrt_sntc_info_result:
            sntc_no_n = scrt_sntc_info_item[0]
            category_key_n = "{0}|{1}|{2}".format(qa_scrt_lccd_n, qa_scrt_mccd_n, pi_no_n)
            if category_key_n not in TARGET_CATEGORY_DICT:
                TARGET_CATEGORY_DICT[category_key_n] = [sntc_no_n]
            else:
                TARGET_CATEGORY_DICT[category_key_n].append(sntc_no_n)
            scrt_sntc_dtc_info_result_n = mysql.select_data_to_scrt_sntc_dtc_info(sntc_no_n)
            if not scrt_sntc_dtc_info_result_n:
                continue
            start_sntc_n = mysql.check_start_sntc(qa_scrt_lccd_n, qa_scrt_mccd_n, pi_no_n, sntc_no_n)
            if start_sntc_n:
                START_SENTENCE_LIST.append("{0}|{1}|{2}_{3}".format(qa_scrt_lccd_n, qa_scrt_mccd_n, pi_no_n, sntc_no_n))
            for scrt_sntc_dtc_info_item_n in scrt_sntc_dtc_info_result_n:
                dtc_cont_n = scrt_sntc_dtc_info_item_n[0]
                scrt_sntc_dtc_info_result_list.append([qa_scrt_lccd_n, qa_scrt_mccd_n, pi_no_n, sntc_no_n, dtc_cont_n])
    # 상품 문항 matrix
    tm_cntr_scrt_info_result_y = mysql.select_data_to_tm_cntr_scrt_info(args.poli_no, args.ctrdt, 'Y')
    if not tm_cntr_scrt_info_result_y:
        tm_cntr_scrt_info_result_y = list()
    for tm_cntr_scrt_info_item_y in tm_cntr_scrt_info_result_y:
        qa_scrt_lccd_y = tm_cntr_scrt_info_item_y[0]
        qa_scrt_mccd_y = tm_cntr_scrt_info_item_y[1]
        pi_no_y = tm_cntr_scrt_info_item_y[2]
        tm_cntr_scrt_prod_info_result = mysql.select_data_to_tm_cntr_scrt_prod_info(
            args.poli_no, args.ctrdt, qa_scrt_lccd_y, qa_scrt_mccd_y, pi_no_y)
        for tm_cntr_scrt_prod_info_item in tm_cntr_scrt_prod_info_result:
            plicd = tm_cntr_scrt_prod_info_item[0]
            scrt_prod_sntc_info_result = mysql.select_data_to_scrt_prod_sntc_info(plicd)
            if not scrt_prod_sntc_info_result:
                continue
            for scrt_prod_sntc_info_item in scrt_prod_sntc_info_result:
                sntc_no_y = scrt_prod_sntc_info_item[0]
                category_key_y = "{0}|{1}|{2}".format(qa_scrt_lccd_y, qa_scrt_mccd_y, pi_no_y)
                category_and_sntc_no = "{0}|{1}|{2}_{3}".format(qa_scrt_lccd_y, qa_scrt_mccd_y, pi_no_y, sntc_no_y)
                if category_key_y not in REP_PI_YN_LIST:
                    REP_PI_YN_LIST.append(category_key_y)
                if category_and_sntc_no not in PLICD_DICT:
                    PLICD_DICT[category_and_sntc_no] = plicd
                if category_key_y not in TARGET_CATEGORY_DICT:
                    TARGET_CATEGORY_DICT[category_key_y] = [sntc_no_y]
                else:
                    TARGET_CATEGORY_DICT[category_key_y].append(sntc_no_y)
                scrt_sntc_dtc_info_result_y = mysql.select_data_to_scrt_sntc_dtc_info(sntc_no_y)
                if not scrt_sntc_dtc_info_result_y:
                    continue
                start_sntc_y = mysql.check_prod_start_sntc(plicd, sntc_no_y)
                if start_sntc_y:
                    START_SENTENCE_LIST.append("{0}|{1}|{2}_{3}".format(
                        qa_scrt_lccd_y, qa_scrt_mccd_y, pi_no_y, sntc_no_y))
                for scrt_sntc_dtc_info_item_y in scrt_sntc_dtc_info_result_y:
                    dtc_cont_y = scrt_sntc_dtc_info_item_y[0]
                    scrt_sntc_dtc_info_result_list.append(
                        [qa_scrt_lccd_y, qa_scrt_mccd_y, pi_no_y, sntc_no_y, dtc_cont_y])
    mysql.disconnect()
    # Make matrix file
    matrix_dir_path = '{0}/HMD_matrix'.format(TA_TEMP_DIR_PATH)
    if not os.path.exists(matrix_dir_path):
        os.makedirs(matrix_dir_path)
    matrix_file_path = '{0}/{1}_{2}.matrix'.format(matrix_dir_path, args.poli_no, args.ctrdt)
    output_list = list()
    for item in scrt_sntc_dtc_info_result_list:
        strs_list = list()
        if len(item) == 3:
            sntc_no = str(item[1]).strip()
            category = "{0}_{1}".format(item[0], sntc_no)
            dtc_cont = str(item[2].encode("euc-kr")).strip()
        else:
            qa_scrt_lccd = str(item[0]).strip()
            qa_scrt_mccd = str(item[1]).strip()
            pi_no = str(item[2]).strip()
            sntc_no = str(item[3]).strip()
            category = "{0}|{1}|{2}_{3}".format(qa_scrt_lccd, qa_scrt_mccd, pi_no, sntc_no)
            dtc_cont = str(item[4].encode("euc-kr")).strip()
        detect_keyword_list = split_input(dtc_cont)
        for idx in range(len(detect_keyword_list)):
            detect_keyword = detect_keyword_list[idx].split("|")
            strs_list.append(detect_keyword)
        ws = ''
        output = ''
        tmp_result = []
        output += "{0}\t".format(category)
        output_list += vec_word_combine(tmp_result, output, strs_list, ws, 0)
    matrix_file = open(matrix_file_path, 'w')
    for item in output_list:
        print >> matrix_file, item
    matrix_file.close()
    return matrix_file_path


def execute_word2vec(logger):
    """
    Execute word2vec
    :param          logger:         Logger
    """
    global DELETE_FILE_LIST
    print "4. Execute word2vec"
    logger.info("4. Execute word2vec")
    w2v_file_list = glob.glob("{0}/W2V/*.w2v.txt".format(TA_TEMP_DIR_PATH))
    total_w2v_file_path = "{0}/{1}.w2v".format(TA_TEMP_DIR_PATH, OUTPUT_DIR_NAME)
    utf8_total_w2v_file_path = "{0}/utf8_{1}.w2v".format(TA_TEMP_DIR_PATH, OUTPUT_DIR_NAME)
    DELETE_FILE_LIST.append(total_w2v_file_path)
    total_w2v_file = open(total_w2v_file_path, 'w')
    for w2v_file_path in w2v_file_list:
        w2v_file = open(w2v_file_path, 'r')
        for line in w2v_file:
            line = line.strip()
            print >> total_w2v_file, line
        w2v_file.close()
    total_w2v_file.close()
    iconv_cmd = "iconv -c -f euc-kr -t utf-8 {0} > {1}".format(total_w2v_file_path, utf8_total_w2v_file_path)
    sub_process(logger, iconv_cmd)
    os.chdir(TA_CONFIG['ta_bin_path'])
    cbow = WORD2VEC_CONFIG['cbow']
    dim = WORD2VEC_CONFIG['dim']
    win_size = WORD2VEC_CONFIG['win_size']
    negative = WORD2VEC_CONFIG['negative']
    hs = WORD2VEC_CONFIG['hs']
    sample = WORD2VEC_CONFIG['sample']
    thread = WORD2VEC_CONFIG['thread']
    binary = WORD2VEC_CONFIG['binary']
    iteration = WORD2VEC_CONFIG['iteration']
    cmd = "./word2vec -train {0} -output {0}.bin".format(utf8_total_w2v_file_path)
    cmd += " -cbow {cbow} -size {size} -window {window}".format(cbow=cbow, size=dim, window=win_size)
    cmd += " -negative {negative} -hs {hs} -sample {sample}".format(negative=negative, hs=hs, sample=sample)
    cmd += " -thread {thread} -binary {binary} -iter {iter}".format(thread=thread, binary=binary, iter=iteration)
    sub_process(logger, cmd)


def make_morph_cnt_file(logger):
    """
    Make morph.cnt file
    :param          logger:         Logger
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


def make_statistics_file(logger):
    """
    Make statistics file
    :param          logger:         Logger
    """
    logger.info("3. Make statistics file")
    logger.info("3-1. Make morph.cnt file")
    make_morph_cnt_file(logger)
    logger.info("3-2. Make ne.cnt file")
    make_ne_cnt_file()


def execute_new_lang(logger, args):
    """
    Execute new_lang.exe [ make nlp result file ]
    :param          logger:         Logger
    :param          args:           Arguments
    """
    global DELETE_FILE_LIST
    logger.info("2. Execute new_lang.exe")
    start = 0
    end = 0
    cmd_list = list()
    os.chdir(TA_CONFIG['ta_bin_path'])
    target_list = glob.glob("{0}/*".format(RESULT_DIR_PATH))
    thread = len(target_list) if len(target_list) < int(TA_CONFIG['nl_thread']) else int(TA_CONFIG['nl_thread'])
    output_dir_list = ['JSON', 'JSON2', 'HMD', 'MCNT', 'NCNT', 'IDX', 'IDXVP', 'W2V']
    for dir_name in output_dir_list:
        output_dir_path = "{0}/{1}".format(TA_TEMP_DIR_PATH, dir_name)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    temp_new_lang_dir_path = "{0}/{1}".format(TA_CONFIG['ta_bin_path'], OUTPUT_DIR_NAME)
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
        cmd = "./new_lang.exe -DJ {0} {1} {2}".format(list_file_path, args.file_extension, args.ctrdt)
        cmd_list.append(cmd)
    pool = workerpool.WorkerPool(thread)
    pool.map(pool_sub_process, cmd_list)
    pool.shutdown()
    pool.wait()


def setup_temp_data(logger, args):
    """
    Copy source directory
    :param          logger:         Logger
    :param          args:           Arguments
    """
    global OUTPUT_DIR_NAME
    global RESULT_DIR_PATH
    global TA_TEMP_DIR_PATH
    global DELETE_FILE_LIST
    global STT_OUTPUT_DIR_PATH
    logger.info("1. Setup TEMP data")
    STT_OUTPUT_DIR_PATH = args.dir_path[:-1] if args.dir_path.endswith("/") else args.dir_path
    OUTPUT_DIR_NAME = os.path.basename(STT_OUTPUT_DIR_PATH)
    TA_TEMP_DIR_PATH = "{0}/{1}".format(TA_CONFIG['ta_data_path'], OUTPUT_DIR_NAME)
    RESULT_DIR_PATH = "{0}/results".format(TA_TEMP_DIR_PATH)
    if os.path.exists(TA_TEMP_DIR_PATH):
        del_garbage(logger, TA_TEMP_DIR_PATH)
    os.makedirs(RESULT_DIR_PATH)
    DELETE_FILE_LIST.append(TA_TEMP_DIR_PATH)
    target_file_list = glob.glob("{0}/txt/*{1}".format(STT_OUTPUT_DIR_PATH, args.file_extension))
    for target_file in target_file_list:
        shutil.copy(target_file, RESULT_DIR_PATH)


def processing(args):
    """
    TA processing
    :param          args:            Arguments
    """
    logger = args.logger
    logger.info('Start TA')
    try:
        # 1. Setup data
        setup_temp_data(logger, args)
        # 2. Execute TA
        execute_new_lang(logger, args)
        # 3. Make statistics file
        make_statistics_file(logger)
        # 4. Execute word2vec
#        execute_word2vec(logger)
        # 5. Make matrix file
        matrix_file_path = make_matrix_file(logger, args)
        # 6. Modify nlp output
        modified_nlp_line_number_dir_path = modify_nlp_output_line_number(logger)
        # 7. Execute HMD
        hmd_output_dir_path = execute_hmd(logger, matrix_file_path, modified_nlp_line_number_dir_path)
        # 8. Sorted HMD output
        sorted_hmd_output_dir_path = sort_hmd_output(logger, hmd_output_dir_path)
        # 9. Modify HMD output
        final_output_dir_path = modify_hmd_output(logger, sorted_hmd_output_dir_path)
        # 10. Make DB upload file TB_QA_STT_TM_TA_DTC_RST
        make_file_for_tb_qa_stt_tm_ta_dtc_rst(logger, args, final_output_dir_path)
        # 11. Make DB upload file TB_QA_STT_TM_CNTR_SCRT_INFO
        make_file_for_tb_qa_stt_tm_cntr_scrt_info(logger, args)
        # 12. Make DB upload file for TB_QA_STT_TM_FBWD_DTC_RST
        make_file_for_tb_qa_stt_tm_fbwd_dtc_rst(logger, args)
        # 13. Make DB upload file for TB_QA_STT_TM_SCTN_RCDG_SAVE
        make_file_for_tb_qa_stt_tm_sctn_rcdg_save(logger, args)
        # 14. Move output
        move_output(logger, args)
        # 15. Delete garbage file
        delete_garbage_file(logger)
        logger.info("Remove logger handler")
        logger.info("TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.info("---------      TA END      ---------")
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("---------      TA ERROR      ---------")
        delete_garbage_file(logger)
        raise Exception(exc_info)

########
# main #
########


def main(args):
    """
    This is a program that execute TA
    :param          args:            Arguments
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
