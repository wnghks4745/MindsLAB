#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-03-10, modification: 2017-04-20"

###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import socket
import requests
import pymssql
import traceback
import ConfigParser
from collections import defaultdict
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
sys.path.append("/data1/MindsVOC/TA/LA/bin")
from lib.iLogger import set_logger
from lib.meritz_enc import encrypt
import TA_process

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DT = ""
PD_CD = ""
LOGGER = ""
TA_PATH = ""
ISP_TMS = ""
DB_CONFIG = ""
STT_PATH = ""
STT_OUT_PATH = ""
HMD_PATH = ""
TA_THREAD = ""
LOG_LEVEL = ""
HOST_NAME = ""
TRANS_NO = ""
DOC_DATE = ""
STT_DIR_PATH = ""
TA_FILE_PATH = ""
TA_OUT_FILE_PATH = ""
STT_MERGE_FILE_PATH = ""
QUESTION_OF_ASSENT_24 = 0
QUESTION_OF_ASSENT_25 = 0


#########
# class #
#########


class MSSQL(object):
    def __init__(self):
        global DB_CONFIG
        self.conn = pymssql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            charset=DB_CONFIG['charset'],
            login_timeout=5
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_ta_targets(self):
        query = """
            SELECT
                a.TRANSFER_NUMBER,
                a.RECORD_FILE_NAME,
                a.ISP_TMS
                (
                    SELECT DISTINCT
                        PD_CD
                    FROM
                        STTACRREQ WITH(NOLOCK)
                    WHERE
                        TRANSFER_NUMBER = a.TRANSFER_NUMBER
                )
            FROM
                STTACLLREQ a WITH(NOLOCK)
            INNER JOIN
                (
                    SELECT TOP(1)
                        TRANSFER_NUMBER,
                        max(isp_tms) AS isptms,
                        PROG_STAT_CD
                    FROM
                        STTACLLREQ WITH(NOLOCK)
                    WHERE
                        PROG_STAT_CD = '31'
                        AND ISP_TMS = '1'
                    GROUP BY
                        TRANSFER_NUMBER,
                        ISP_TMS,
                        PROG_STAT_CD
                ) b
            ON
                a.TRANSGER_NUMBER = b.TRANSGER_NUMBER
                AND a.ISP_TMS = B.isptms
                AND a.PROG_STAT_CD = b.PROG_STAT_CD
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if result:
            return result
        return False

    def select_agree_points(self):
        query = """
            SELECT DISTINCT
                SCRP_CD
            FROM
                STTASCRPKWDINF WITH(NOLOCK)
            WHERE
                STTA_CUS_REPLY_CD IN ('02', '03')
        """
        self.cursor.execute(query,)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if result:
            return result
        return False

    def select_cot_ins_date(self, trans_no):
        query = """
            SELECT
                PRV_COT_INS_DATE
            FROM
                STTACTRREQ WITH(NOLOCK)
            WHERE
                TRANSFER_NUMBER = %s
                AND ISP_TMS = '1'
        """
        bind = (
            trans_no
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if result:
            return result
        return False

    def select_start_point(self, pd_cd, trans_no):
        query = """
            WITH TEMP_INFO as
            (
                SELECT
                    max(Info.HIS_ST_DT) AS INFO_DATE
                FROM
                    STTACTRREQ AS Request WITH(NOLOCK),
                    STTAPDSECTINF as Info WITH(NOLOCK)
                WHERE
                    Request.PD_CD = Info.PD_CD
                    AND Info.PD_CD = %s
                    AND Request.TRANSFER_NUMBER = %s
                    AND Info.HIS_ST_DT <= Request.PRV_COT_INS_DATE
            )
            SELECT
                Info.PD_CD,
                Info.SCRP_SECT_CD,
                Info.SCRP_CD
            FROM
                STTAPDSECTINF as Info WITH(NOLOCK),
                TEMP_INFO as LatestDate
            WHERE
                Info.HIS_ST_DT = LatestDate.INFO_DATE
                AND Info.PD_CD = %s
                AND Info.SCRP_SECT_ST_YN = 'Y'
        """
        bind = (pd_cd, trans_no, pd_cd)
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if result:
            return result
        return False

    def select_time_info(self, call_id, stmt_no):
        query = """
            SELECT
                STMT_ST,
                STMT_END
            FROM
                STTARSL WITH(NOLOCK)
            WHERE
                DCM_NO = %s
                AND STMT_NO = %s
        """
        bind = (call_id, stmt_no)
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if result:
            return result
        return False

    def select_recent_record_date(self, trans_no):
        query = """
            SELECT
                RECORD_FILE_NAME,
                RECORD_START_DATE
            FROM
                STTACLLREQ WITH(NOLOCK)
            WHERE
                TRANSFER_NUMBER = %s
            ORDER BY
                RECORD_START_DATE desc,
                RECORD_START_TIME desc
        """
        bind = (trans_no, )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if result:
            return result
        return False

    def select_verification_keyword(self):
        query = """
            SELECT
                SCRP_CD,
                SCRP_KWD_NM
            FROM
                STTASCRPKWDINF WITH(NOROCK)
            WHERE
                SCRP_CD != 00000
                AND LEN(SCRP_KWD_NM) > 0
            ORDER BY
                SCRP_CD
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if result:
            return result
        return False

    def insert_data_to_mastr(self, **kwargs):
        global LOGGER
        try:
            query = """
                INSERT INTO STTARSLSCRPMSTR
                (
                    TRANSFER_NUMBER,
                    DCM_NO,
                    STMT_NO,
                    STMT_SEQ,
                    CHN_ID,
                    PD_CD,
                    SCRP_SECT_CD,
                    SCRP_STMT_CD,
                    STMT_ST,
                    STMT_END,
                    SCRP_STMT_YN,
                    PRHW_YN,
                    STMT,
                    CUS_YN,
                    CUS_TN_STMT,
                    CUS_CHN_ID,
                    DCM_DT,
                    DTCT_KWD,
                    CTGR,
                    KWD_DTCT_RTO,
                    KWD_TT_CNT,
                    KWD_DTCT_CNT,
                    SYS_DTM
                )
                VALUES
                (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, CONVERT(char(10), GetDate(), 126)
                )
            """
            bind = (
                kwargs.get('transfer_number'),
                kwargs.get('dcm_no'),
                kwargs.get('stmt_no'),
                kwargs.get('stmt_seq'),
                kwargs.get('chn_id'),
                kwargs.get('pd_cd'),
                kwargs.get('scrp_sect_cd'),
                kwargs.get('scrp_stmt_cd'),
                kwargs.get('stmt_st'),
                kwargs.get('stmt_end'),
                kwargs.get('scrp_stmt_yn'),
                kwargs.get('prhw_yn'),
                kwargs.get('stmt'),
                kwargs.get('cus_yn'),
                kwargs.get('cus_yn_stmt'),
                kwargs.get('cus_chn_id'),
                kwargs.get('dcm_dt'),
                kwargs.get('dtct_kwd'),
                kwargs.get('ctgr'),
                kwargs.get('kwd_dtct_rto'),
                kwargs.get('kwd_tt_cnt'),
                kwargs.get('kwd_dtct_cnt'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                # self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            self.conn.rollback()

    def insert_data_to_dtl(self, **kwargs):
        global LOGGER
        try:
            query = """
                INSERT INTO STTARSLSCRPDTL
                (
                    TRANSFER_NUMBER,
                    DCM_NO,
                    STMT_NO,
                    KWD_ID,
                    CHN_ID,
                    DCM_DTM,
                    KWD.
                    KWD_DTCT_YN,
                    PRHW_YN,
                    KWD_DTCT_CNT,
                    SCRP_CD,
                    SCRP_SEQ
                )
                VALUES
                (
                    %s, %s, %s, %s, %s,
                    CONVERT(char(10), GetDate(), 126),
                    %s, %s, %s, %s, %s,
                    %s
                )
            """
            bind = (
                kwargs.get('transfer_number'),
                kwargs.get('dcm_no'),
                kwargs.get('stmt_no'),
                kwargs.get('kwd_id'),
                kwargs.get('chn_id'),
                kwargs.get('kwd'),
                kwargs.get('kwd_dict_yn'),
                kwargs.get('prhw_yn'),
                kwargs.get('kwd_dtct_cnt'),
                kwargs.get('scrp_cd'),
                kwargs.get('scrp_seq'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                # self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            self.conn.rollback()

    def insert_data_to_arsl(self, **kwargs):
        global LOGGER
        try:
            query = """
                INSERT INTO STTARSL
                (
                    TRANSFER_NUMBER,
                    DCM_NO,
                    STMT_NO,
                    CHN_ID,
                    STMT_ST,
                    STMT_END,
                    STMT
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """
            bind = (
                kwargs.get('trans_no'),
                kwargs.get('dcm_no'),
                kwargs.get('stmt_no'),
                kwargs.get('chn_id'),
                kwargs.get('stmt_st'),
                kwargs.get('stmt_end'),
                kwargs.get('stmt'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                # self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            self.conn.rollback()

    def insert_data_to_sect(self, pd_cd, trans_no):
        global LOGGER
        try:
            query = """
                Declare @LatestDate datetime
                SET NOCOUNT ON
                SELECT
                    @LatestDate = max(Info.HIS_ST_DT)
                FROM
                    STTACTRREQ AS Request WITH(NOLOCK),
                    STTAPDSECTINF AS Info WITH(NOLOCK)
                WHERE 1=1
                    AND Request.PD_CD = Info.PD_CD
                    AND Info.PD_CD = %s
                    AND Request.TRANSFER_NUMBER = %s
                    AND Info.HIS_ST_DT <= Request.PRV_COT_INS_DATE
                SET NOCOUNT OFF

                Declare @TEMP_RESULT Table(
                    TRANSFER_NUMBER char(18),
                    DCM_NO char(25),
                    SCRP_SECT_CD char(6),
                    SEQ int,
                    STMT_ST time(7),
                    KEYWORD_RANK int
                    )
                INSERT INTO @TEMP_RESULT
                    SELECT
                        %s,
                        Mstr.DCM_NO,
                        Mstr.SCRP_SECT_CD,
                        1 as SEQ,
                        Arsl.STMT_ST,
                        ROW_NUMBER() over (partition by Mstr.SCRP_SECT_CD order by Mstr.KWD_DTCT_RTD desc) as kwd_rank
                    FROM
                        STTARSLSCRPMSTR Mstr WITH(NOLOCK),
                        STTARSL Arsl WITH(NOLOCK),
                        STTAPDSECTINF Info WITH(NOLOCK)
                    WHERE 1=1
                        AND Mstr.TRANSFER_NUMBER = Arsl.TRANSFER_NUMBER
                        AND Mstr.STMT_NO = Arsl.STMT_NO
                        AND Mstr.DDCM_NO = Arsl.DCM_NO
                        AND Info.SCRP_SECT_ST_YN = 'Y'
                        AND Mstr.PD_CD = Info.PD_CD
                        AND Mstr.SCRP_SECT_CD = Info.SCRP_SECT_CD
                        AND Mstr.SCRP_STMT_CD = Info.SCRP_CD
                        AND Mstr.TRANSFER_NUMBER = %s
                        AND Mstr.PD_CD = %s
                        AND Info.HIS_ST_DT = @LatestDate

                INSERT INTO STTASCRPSECT(
                    TRANSFER_NUMBER,
                    DCM_NO,
                    SCRP_SECT_CD,
                    SCRP_SECT_SEQ,
                    SCRP_ST,
                    SCRP_END,
                    RECORD_START_DATE
                    )
                    SELECT
                        Result.TRANSFER_NUMBER,
                        Result.DCM_NO,
                        Result.SCRP_SECT_CD,
                        Result.SEQ,
                        Result.STMT_ST,
                        (
                            SELECT
                                max(Arsl.stmt_end)
                            FROM
                                STTARSLSCRPMSTR Mstr WITH(NOLOCK)
                            INNER JOIN
                                STTARSL Arsl WITH(NOLOCK)
                            ON
                                Mstr.DCM_NO = Arsl.DCM_NO
                            WHERE 1=1
                                AND Mstr.TRANSFER_NUMBER = %s
                                AND Mstr.DCM_NO = Result.DCM_NO
                            GROUP BY
                                Mstr.TRANSFER_NUMBER,
                                Mstr.DCM_NO
                        ) AS tEND,
                        Record.RECORD_START_DATE AS tREC_ST
                    FROM
                        @TEMP_RESULT Result,
                        STTACLLREQ Record WITH(NOLOCK)
                    WHERE 1=1
                        AND Result.TRANSFER_NUMBER = Record.TRANSFER_NUMBER
                        AND Result.DCM_NO = REPLACE(Record.RECORD_FILE_NAME, '.wav', '')
                        AND Result.KEYWORD_RANK = '1'
            """
            bind = (
                pd_cd,
                trans_no,
                trans_no,
                trans_no,
                pd_cd,
                trans_no
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                # self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            self.conn.rollback()

    def insert_data_to_transitsect(self, **kwargs):
        global LOGGER
        try:
            query = """
                INSERT INTO STTATRANSITSECT
                (
                    TRANSFER_NUMBER,
                    RECORD_STATUS,
                    DCM_NO,
                    SCRP_ST,
                    SCRP_END,
                    RECORD_START_DATE,
                    CUS_YN,
                    STMT
                )
                VALUES
                (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """
            bind = (
                kwargs.get('trans_no'),
                kwargs.get('status'),
                kwargs.get('dcm_no'),
                kwargs.get('scrp_st'),
                kwargs.get('scrp_end'),
                kwargs.get('start_date'),
                kwargs.get('cus_yn'),
                kwargs.get('stmt')
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            self.conn.rollback()

    def update_file_status_ll(self, trans_no, basename, isp, new_state):
        try:
            query = """
                UPDATE
                    STTACLLREQ
                SET
                    PROG_STAT_CD = %s
                WHERE 1=1
                    AND TRANSFER_NUMBER = %s
                    AND RECORD_FILE_NAME = %s
                    AND ISP_TMS = %s
            """
            bind = (
                new_state,
                trans_no,
                basename,
                isp
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            self.conn.rollback()
            self.disconnect()
            raise Exception(exc_info)

    def update_file_status_rr(self, trans_no, host_name, isp, new_state):
        try:
            query = """
                UPDATE
                    STTACTRREQ
                SET
                    PROG_STAT_CD = %s,
                    STTA_ED_DTM = CONVERT(varchar, GetDate(), 120),
                    TA_HOST_NM = %s
                WHERE 1=1
                    AND TRANSFER_NUMBER = %s
                    AND ISP_TMS = %s
            """
            bind = (
                new_state,
                host_name,
                trans_no,
                isp
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            self.conn.rollback()
            self.disconnect()
            raise Exception(exc_info)

    def update_ta_end_time(self, trans_no, host_name, isp):
        try:
            query = """
                UPDATE
                    STTACTRREQ
                SET
                    STTA_ED_DTM = CONVERT(varchar, GetDate(), 120),
                    TA_HOST_NM = %s
                WHERE 1=1
                    AND TRANSFER_NUMBER = %s
                    AND ISP_TMS = %s
            """
            bind = (
                host_name,
                trans_no,
                isp
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            self.conn.rollback()
            self.disconnect()
            raise Exception(exc_info)


#######
# def #
#######
def elapsed_time(sdate):
    """
    elapsed time
    @param      sdate          date object
    @return                    days, hour, minute, sec
    """
    e = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    s = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]), int(sdate[8:10]), int(sdate[10:12]),
                 int(sdate[12:14]))
    days = (e - s).days
    sec = (e - s).seconds
    hour, sec = divmod(sec, 3600)
    minute, sec = divmod(sec, 60)
    return days, hour, minute, sec


def load_conf(state, cfg_name):
    """
    Load config file
    :param      state:          state name
    :param      cfg_name:       config name
    :return:                    tate info type is dict
    """
    config = ConfigParser.RawConfigParser()
    script_path = os.path.abspath(os.path.dirname(__file__))
    conf_path = script_path.replace("/bin", "/cfg/{0}.cfg".format(cfg_name))
    config.read(conf_path)
    result = dict(config.items(state))
    return result


def check_file(name_form, file_name):
    """
    Extract need CS file
    :param          name_form:      Check file name form
    :param          file_name:      Input file name
    :return:                        True or False
    """
    return file_name.endswith(name_form)


def update_targets_status_to_start(mssql, ta_targets_list):
    """
    Update TA target status
    :param      mssql:                 MsSQL
    :param      ta_targets_list:       TA targets [(,,), (,,)]
    """
    global LOGGER
    global LOG_LEVEL
    global TA_PATH

    is_first = True
    for ta_target in ta_targets_list:
        basename = ta_target[1].strip()
        isp = str(ta_target[2]).strip()
        if is_first:
            # TA 대상 중 처음 한번만 LOGGER 와 STTACTRREQ table 에 status update
            rr_true_or_false = mssql.update_file_status_rr(TRANS_NO, HOST_NAME, isp, 40)
            # Add logging
            args = {
                'base_path': TA_PATH['path'] + "/Meritz",
                'log_file_name': TRANS_NO,
                'log_level': LOG_LEVEL['level']
            }
            LOGGER = set_logger(args)
            LOGGER.info("Set logger TRANS_NO = {tr}".format(tr=TRANS_NO))
            if not rr_true_or_false:
                LOGGER.error("Failed STTACTRREQ update status --> {0}/{1}".format(TRANS_NO, isp))
            is_first = False
        LOGGER.info("{0} / {1} = Update status 31 to 40".format(TRANS_NO, basename))
        ll_true_or_false = mssql.update_file_status_ll(TRANS_NO, basename, isp, 30)
        if not ll_true_or_false:
            LOGGER.error("Failed STTACLLREQ update status --> {0}/{1}".format(TRANS_NO, basename))
            continue
    mssql.conn.commit()
    LOGGER.info("Success update file state to start 31-40")


def update_file_status_to_error(mssql, ta_targets_list):
    """
    Update TA targets status
    :param          mssql:                   MsSQL
    :param          ta_targets_list:        TA targets
    """
    global LOGGER

    is_first = True
    for ta_target in ta_targets_list:
        basename = ta_target[1].strip()
        isp = str(ta_target[2]).strip()
        LOGGER.info("{0}/{1} = Update state 42".format(TRANS_NO, basename))
        if is_first:
            rr_true_or_false = mssql.update_file_status_rr(TRANS_NO, HOST_NAME, isp, 42)
            if not rr_true_or_false:
                LOGGER.error("Failed STTACTRREQ update state --> {0}/{1}".format(TRANS_NO, isp))
            is_first = False
        ll_true_or_false = mssql.update_file_status_ll(TRANS_NO, basename, isp, 42)
        if not ll_true_or_false:
            LOGGER.info("Failed update state --> {0}/{1}".format(TRANS_NO, basename))
            continue
    mssql.conn.commit()

    if socket.gethostname() == 'vrstt1v':
        requests.get(
            "http://tmdev.meritzfire.com:29600/tmsys/70000/70000_STT_RESULT.jsp?TransferNumber={tr}&SttaRslCd=03".format(
                tr=TRANS_NO))
    elif socket.gethostname() == 'vrta1p' or socket.gethostname() == 'vrta2p':
        requests.get(
            "https://tm.meritzfire.com/tmsys/70000/70000_STT_RESULT.jsp?TransferNumber={tr}&SttaRslCd=03".format(
                tr=TRANS_NO))


def find_matrix_file(mssql):
    """
    Find matrix file
    :param          mssql:          MsSQL
    :return:                        Matrix file name
    """
    global LOGGER
    global HMD_PATH

    # 가장 최신의 matrix 을 가져온다
    hmd_time_tuple = mssql.select_cot_ins_date(TRANS_NO)
    if hmd_time_tuple:
        prv_date = hmd_time_tuple[0]
        base_date = datetime.strptime("2016-12-31", "%Y-%M-%d")
        if prv_date < base_date:
            prv_date = datetime.now()
        str_prv_date = prv_date.strftime("%Y%m%d").strip()
        LOGGER.info("str_prv_date = {0}".format(str_prv_date))
        LOGGER.info("PD_CD = {0}".format(PD_CD))
    else:
        raise Exception("Can't select HMD PRV_COT_INS_DATE")

    os.chdir("/data1/MindsVOC/TA/Meritz/cfg/HMD_txt")
    file_list = glob.glob("HMD_{pd_cd}_*.matrix".format(pd_cd=PD_CD))
    file_list.sort(reverse=True)
    expected_file_name = "HMD_{pd_cd}_{std}.matrix".format(pd_cd=PD_CD, std=str_prv_date)
    for file_name in file_list:
        if file_name <= expected_file_name:
            return file_name

    raise Exception("Can't find matrix file")


def make_ta_dir():
    """
    Make TA directory
    """
    global TA_PATH

    file_dir_path = TA_PATH['path'] + "/data/" + TRANS_NO + "/results"
    if not os.path.exists(file_dir_path):
        os.makedirs(file_dir_path)


def copy_stt_file():
    """
    Copy CS file
    """
    global TA_PATH

    w_ob = os.walk(STT_MERGE_FILE_PATH)
    for dir_name in w_ob:
        for file_name in dir_name[2]:
            if check_file("_trx_updated.txt", file_name):
                shutil.copy(os.path.join(dir_name[0], file_name), "{ta}/data/{tr}/results".format(ta=TA_PATH['path'], tr=TRANS_NO))


def execute_ta_process(mssql):
    """
    Do TA process
    :param          mssql:          MsSQL
    """
    global LOGGER
    global HMD_PATH
    global TA_THREAD

    LOGGER.info("DO TA_process")
    # HMD matrix file 중 TA target 에 적용될 수 있는 가장 최신 날짜의 matrix 를 가져온다
    matrix_file_name = find_matrix_file(mssql)
    hmd_matrix_path = "{hp}/{mfn}".format(hp=HMD_PATH['path'], mfn=matrix_file_name)
    # Make TA output dir
    make_ta_dir()
    # Copy CS output file(*_trx_updated.txt) for TA_process
    copy_stt_file()
    TA_process.MakeNLPResultFile(TRANS_NO, ".txt", DOC_DATE, int(TA_THREAD['thread']))
    TA_process.func_FindHMD(TRANS_NO, hmd_matrix_path, int(TA_THREAD['thread']))
    LOGGER.info("Success do TA_process")


def modify_dup_out_line_num(tsv_file):
    """
    First modify line number.
    :param              tsv_file:           tsv file
    :return:                                modified dup output list
    """
    global LOGGER

    cnt = 0
    pre_sent = ''
    pre_call_id = ''
    modify_dup_out_list = list()
    overlap_check_dict = dict()
    # TA output 중 잘 못 분리된 문장을 기준으로 line number 를 수정한다
    for line in tsv_file:
        try:
            line = line.strip()
            line_list = line.split('\t')
            call_id = line_list[1]
            sent_num = line_list[2]
            hmd_cat = line_list[4]
            sent = line_list[6]
            check_key = "{0}_{1}_{2}_{3}".format(call_id, sent_num, hmd_cat, sent)

            if check_key not in overlap_check_dict:
                overlap_check_dict[check_key] = 1
            else:
                continue

            if sent.startswith('[A]') or sent.startswith('[C]'):
                pre_sent = ''
                if pre_call_id != call_id:
                    cnt = 0
                new_sent_num = int(sent_num) - int(cnt)
                line_list[2] = str(new_sent_num)
                modify_dup_out_list.append('\t'.join(line_list))
            else:
                cnt += 1
                if pre_sent == sent and hmd_cat != 'none':
                    cnt -= 1
                new_sent_num = int(sent_num) - int(cnt)
                pre_sent = sent
                pre_call_id = call_id
                line_list[2] = str(new_sent_num)
                modify_dup_out_list.append("\t".join(line_list))
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            continue
    return modify_dup_out_list


def merge_dup_out_line(modify_dup_out_list):
    """
    Second merge line use call id and HMD category
    :param          modify_dup_out_list:             Modified line num file
    :return:                                         Merged dup output line
    """
    sent_list = list()
    non_sent_list = list()

    # TA 과정 중 분리되어진 기준 문장과 통합 되어야 할 대상 문장을 각각 다른 리스트에 추가한다
    for ori_line in modify_dup_out_list:
        ori_line_list = ori_line.split('\t')
        ori_sent = ori_line_list[6].strip()
        if ori_sent.startswith("[C]") or ori_sent.startswith('[A]'):
            sent_list.append(ori_line_list)
        else:
            non_sent_list.append(ori_line_list)

    # 통합 되어야 할 문장들 중 call_id 와 line_number 가 같으면 문장들을 합치고 merge_sent_dict dictionary 에 추가 한다
    merge_sent_dict = dict()
    base_non_sent = ""
    for non_line_list in non_sent_list:
        non_call_id = non_line_list[1].strip()
        non_line_no = non_line_list[2].strip()
        non_sent = non_line_list[6].strip()
        non_key = non_call_id + "_" + non_line_no
        if non_key not in merge_sent_dict:
            merge_sent_dict[non_key] = non_sent
            base_non_sent = non_sent
        else:
            if non_sent == base_non_sent:
                continue
            merge_sent_dict[non_key] += " " + non_sent
            base_non_sent = non_sent

    # 통합 되어야 할 문장들을 모두 합쳐준다
    for modify_non_line_list in non_sent_list:
        modify_non_call_id = modify_non_line_list[1].strip()
        modify_non_line_no = modify_non_line_list[2].strip()
        modify_key = modify_non_call_id + "_" + modify_non_line_no
        if modify_key in merge_sent_dict:
            modify_non_line_list[6] = merge_sent_dict[modify_key].strip()

    # 통합 되어야 할 문장들 중  call_id 와 line number 를  key 로 중복 제거를 한다
    modified_non_sent_dict = dict()
    for check_non_line_list in non_sent_list:
        check_non_call_id = check_non_line_list[1].strip()
        check_non_line_no = check_non_line_list[2].strip()
        check_non_sent = check_non_line_list[6].strip()
        key = "{0}_{1}".format(check_non_call_id, check_non_line_no)
        if key not in modified_non_sent_dict:
            modified_non_sent_dict[key] = check_non_sent

    # 기준 문장을 통합 되어야 할 문장들로 수정한다
    for output_line_list in sent_list:
        call_id = output_line_list[1].strip()
        line_no = output_line_list[2].strip()
        output_check_key = "{0}_{1}".format(call_id, line_no)
        if output_check_key in modified_non_sent_dict:
            output_line_list[6] = output_line_list[6].strip()
            output_line_list[6] += " " + modified_non_sent_dict[output_check_key]

    # 기준 문장을  call_id 와 line_number 를 기준으로 dictionary 에 추가한다
    output_check_dict = dict()
    for output_line_list in sent_list:
        modified_call_id = output_line_list[1].strip()
        modified_line_no = output_line_list[2].strip()
        modified_sent = output_line_list[6].strip()
        output_key = "{0}_{1}".format(modified_call_id.strip(), modified_line_no.strip())
        output_check_dict[output_key] = modified_sent

    # 최종적으로 모든 line 을 통합 되어진 문장으로 교체한다
    final_output_list = list()
    for output_modify_line in modify_dup_out_list:
        output_modify_line_list = output_modify_line.split('\t')
        output_call_id = output_modify_line_list[1].strip()
        output_line_no = output_modify_line_list[2].strip()
        output_hmd_cat = output_modify_line_list[4].strip()
        output_sent = output_modify_line_list[6].strip()
        check_key = "{0}_{1}".format(output_call_id, output_line_no)
        if output_hmd_cat == 'none':
            if output_sent.startswith('[C]') or output_sent.startswith('[A]'):
                if check_key in output_check_dict:
                    output_modify_line_list[6] = output_check_dict[check_key]
                final_output_list.append('\t'.join(output_modify_line_list))
        elif check_key in output_check_dict:
            output_modify_line_list[6] = output_check_dict[check_key]
            final_output_list.append('\t'.join(output_modify_line_list))
        else:
            final_output_list.append('\t'.join(output_modify_line_list))

    return final_output_list


def make_new_none_and_overlap_check(mssql, merged_dup_out_list):
    """
    Third modify HMD category column
    :param          mssql:                      MsSQL
    :param          merged_dup_out_list:        Merged dup output line
    :return:                                    Modified dup output list
    """
    global LOGGER

    start_point_list = list()
    start_point_tuple = mssql.select_start_point(PD_CD, TRANS_NO)
    is_first = True
    for items in start_point_tuple:
        start_point = "{0}_{1}_{2}".format(items[0].strip(), items[1].strip(), items[2].strip())
        start_point_list.append(start_point)
        if is_first:
            start_point = "{0}_00001_00000".format(items[0].strip())
            start_point_list.append(start_point)
            is_first = False
    start_point_dict = dict()
    identified_start_point_output_list = list()
    base_hmd_sect_cd = ""

    # 탐지 된 문장들 중 시작 부분  call id 와 line number 기준으로 dictionary 에 추가한다
    for line in merged_dup_out_list:
        line_list = line.split('\t')
        call_id = line_list[1].strip()
        line_no = line_list[2].strip()
        hmd_cat = line_list[4].strip()
        if hmd_cat != 'none':
            hmd_cat_list = hmd_cat.split('_')
            hmd_sect_cd = hmd_cat_list[1].strip()
            std_sent = line_list[6].strip()
            if std_sent.startswith('[C]'):
                continue
            if hmd_cat in start_point_list:
                if call_id not in start_point_dict:
                    start_point_dict[call_id] = {line_no: [hmd_sect_cd]}
                else:
                    if line_no not in start_point_dict[call_id]:
                        start_point_dict[call_id][line_no] = [hmd_sect_cd]
                    else:
                        start_point_dict[call_id][line_no].append(hmd_sect_cd)

    # 통합 되어진 list 중 시작 부분 영역에 포함 되어지는 문장을 제외 한 나머지를 new none 처리한다
    for curr_line in merged_dup_out_list:
        try:
            curr_line = curr_line.strip()
            curr_line_list = curr_line.split('\t')
            curr_hmd_cat = curr_line_list[4].strip()
            curr_line_no = curr_line_list[2].strip()
            curr_call_id = curr_line_list[1].strip()

            if curr_line_no == '0':
                base_hmd_sect_cd = ""

            if curr_hmd_cat != 'none':
                curr_hmd_cat_list = curr_hmd_cat.split("_")
                curr_hmd_sect_cd = curr_hmd_cat_list[1].strip()
                curr_std_sent = curr_line_list[1].strip()
                if curr_std_sent.startswith('[C]'):
                    none_line = curr_line.replace(curr_hmd_cat, 'none')
                    identified_start_point_output_list.append(none_line)
                    continue
                if curr_hmd_cat in start_point_list:
                    identified_start_point_output_list.append(curr_line)
                    base_hmd_sect_cd = curr_hmd_sect_cd
                elif curr_call_id in start_point_dict:
                    subtraction_line_no = str(int(curr_line_no) - 1)
                    if curr_line_no in start_point_dict[curr_call_id]:
                        if curr_hmd_sect_cd in start_point_dict[curr_call_id][curr_line_no]:
                            base_hmd_sect_cd = curr_hmd_sect_cd
                            identified_start_point_output_list.append(curr_line)
                        else:
                            if base_hmd_sect_cd == curr_hmd_sect_cd:
                                identified_start_point_output_list.append(curr_line)
                            else:
                                if subtraction_line_no in start_point_dict[curr_call_id]:
                                    if curr_hmd_sect_cd in start_point_dict[curr_call_id][subtraction_line_no]:
                                        base_hmd_sect_cd = curr_hmd_sect_cd
                                        identified_start_point_output_list.append(curr_line)
                                else:
                                    new_none_line = curr_line.replace(curr_hmd_cat, "new none")
                                    identified_start_point_output_list.append(new_none_line)
                    else:
                        if base_hmd_sect_cd == curr_hmd_sect_cd:
                            identified_start_point_output_list.append(curr_line)
                        else:
                            if subtraction_line_no in start_point_dict[curr_call_id]:
                                if curr_hmd_sect_cd in start_point_dict[curr_call_id][subtraction_line_no]:
                                    base_hmd_sect_cd = curr_hmd_sect_cd
                                    identified_start_point_output_list.append(curr_line)
                            else:
                                new_none_line = curr_hmd_sect_cd.replace(curr_hmd_cat, 'new none')
                                identified_start_point_output_list.append(new_none_line)
                else:
                    new_none_line = curr_line.replace(curr_hmd_cat, 'new none')
                    identified_start_point_output_list.append(new_none_line)
            else:
                identified_start_point_output_list.append(curr_line)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            identified_start_point_output_list.append(curr_line)
            continue

    return identified_start_point_output_list


def question_of_assent(mssql, identified_start_point_output_list):
    """
    Firth add question of assent column
    :param          mssql:                                          MsSQL
    :param          identified_start_point_output_list:             dup output list
    :return:                                                        Add column dup output list
    """
    output_list = list()
    assent_dict = dict()
    agree_point_list = list()
    agree_point_tuple = mssql.select_agree_points()
    for agree_point in agree_point_tuple:
        agree_point_list.append(str(agree_point[0]))

    for assent_line in identified_start_point_output_list:
        line_list = assent_line.split('\t')
        std_call_id = line_list[1].strip()
        std_line_no = line_list[2].strip()
        std_sent = line_list[6].strip()
        key = '{0}_{1}'.format(std_call_id, std_line_no)
        if key not in assent_dict and std_sent.startswith('[C]'):
            assent_dict[key] = std_sent

    # 고객의 동의 구간이 필요한 line 의 다음 다음 line  중 답변에 해당하는 문장을 가져온다
    for idx, line in enumerate(identified_start_point_output_list):
        line_list = line.split('\t')
        temp_list = line_list
        std_call_id = line_list[1].strip()
        std_line_no = line_list[2].strip()
        std_hmd = line_list[4].strip()
        if std_hmd.count('_') > 1:
            std_hmd_list = std_hmd.split('\t')
            point = std_hmd_list[2].strip()
            if point in agree_point_list:
                value = ""
                for cnt in range(1, 3):
                    modified_std_line_no = str(int(std_line_no) + cnt)
                    key = "{0}_{1}".format(std_call_id, modified_std_line_no)
                    if key in assent_dict:
                        if len(value) < 1:
                            value = assent_dict[key]
                        else:
                            value += assent_dict[key].replace("[C]", " ")
                if len(value) > 1:
                    temp_list.append(value)
                    identified_start_point_output_list[idx] = '\t'.join(temp_list)

    # 수정되어진  output line 의 index 를 같게 하기 위하여 'none' 을 추가한다
    for check_line in identified_start_point_output_list:
        line_list = check_line.split("\t")
        if len(line_list) != 8:
            line_list.append("none")
        output_list.append("\t".join(line_list))

    return output_list


def modify_ta_result(mssql):
    """
    Modify TA result
    :param              mssql:              MsSQL
    :return:                                Dup output list
    """
    global TA_PATH
    global LOGGER

    LOGGER.info("Modify TA result")
    tsv_file_path = "{ta}/data/{tr}/{tr}_hmd_search.tsv".format(ta=TA_PATH['path'], tr=TRANS_NO)
    tsv_file = open(tsv_file_path, 'r')

    modify_dup_out_list = modify_dup_out_line_num(tsv_file)
    modify_dup_out_file_path = TA_PATH['path'] + '/data/' + TRANS_NO + "/modify_dup_out.txt"
    modify_dup_out_file = open(modify_dup_out_file_path, 'wt')
    for modify_dup_out_line in modify_dup_out_list:
        print >> modify_dup_out_file, modify_dup_out_line
    modify_dup_out_file.close()

    merged_dup_out_list = merge_dup_out_line(modify_dup_out_list)
    merged_dup_out_file_path = TA_PATH['path'] + '/data/' + TRANS_NO + "/merged_dup_out.txt"
    merged_dup_out_file = open(merged_dup_out_file_path, 'wt')
    for merged_dup_out_line in merged_dup_out_list:
        print >> merged_dup_out_file, merged_dup_out_line
    merged_dup_out_file.close()

    identified_start_point_output_list = make_new_none_and_overlap_check(mssql, merged_dup_out_list)
    start_point_output_file_path = TA_PATH['path'] + '/data/' + TRANS_NO + '/start_point_dup_out.txt'
    start_point_output_file = open(start_point_output_file_path, 'wt')
    for output_line in identified_start_point_output_list:
        print >> start_point_output_file, output_line
    start_point_output_file.close()

    dup_output_list = question_of_assent(mssql, identified_start_point_output_list)
    agree_output_file_path = TA_PATH['path'] + '/data/' + TRANS_NO + "/agree_dup_out.txt"
    agree_output_file = open(agree_output_file_path, 'wt')
    for agree_output_line in dup_output_list:
        print >> agree_output_file, agree_output_line
    agree_output_file.close()

    tsv_file.close()
    LOGGER.info("Success modified TA result")
    return dup_output_list


def load_verification_keyword(mssql):
    """
    Load verification keyword
    :param          mssql:          MsSQL
    :return:                        Verification keyword dict type
    """
    global LOGGER

    LOGGER.info("Load verification keyword")
    verification_keyword_list = mssql.select_verification_keyword()
    verification_keyword_dict = dict()
    for item in verification_keyword_list:
        scrp_cd = item[0].strip()
        scrp_kwd = item[0].strip()
        if scrp_cd not in verification_keyword_dict:
            verification_keyword_dict[scrp_cd] = [scrp_kwd]
        else:
            verification_keyword_dict[scrp_cd].append(scrp_kwd)
    LOGGER.info("Success load verification keyword")
    return verification_keyword_dict


def modify_nlp_line_num(nlp_file):
    """
    Modify NLP output line number
    :param          nlp_file:           NLP output
    :return:                            Modified NLP output
    """
    cnt = 0
    pre_call_id = ''
    modify_nlp_dup_out = list()
    for line in nlp_file:
        line = line.strip()
        line_list = line.split('\t')
        call_id = line_list[0].strip()
        sent_num = line_list[1].strip()
        sent = line_list[3].strip()
        if sent.startswith('[A]') or sent.startswith('[C]'):
            if pre_call_id != call_id:
                cnt = 0
            new_sent_num = int(sent_num) - cnt
            line_list[1] = str(new_sent_num)
            modify_nlp_dup_out.append('\t'.join(line_list))
        else:
            cnt += 1
            new_sent_num = int(sent_num) - cnt
            pre_call_id = call_id
            line_list[1] = str(new_sent_num)
            modify_nlp_dup_out.append('\t'.join(line_list))
    return modify_nlp_dup_out


def merge_nlp_dup_out(modify_nlp_dup_out):
    """
    Merge NLP dup output
    :param          modify_nlp_dup_out:         Modified NLP dup output
    :return:                                    Merged NLP dup output
    """
    merged_nlp_dup_out_list = list()
    pre_line_list = list()
    for idx, line in enumerate(modify_nlp_dup_out):
        line = line.strip()
        line_list = line.split('\t')
        sent = line_list[3].strip()
        nlp_sent = line_list[4].strip()
        if idx + 1 == len(merged_nlp_dup_out_list):
            if sent.startswith('[A]') or sent.startswith('[C]'):
                merged_nlp_dup_out_list.append('\t'.join(pre_line_list))
            else:
                pre_line_list[3] += " " + sent
                pre_line_list[4] += " " + nlp_sent
                merged_nlp_dup_out_list.append('\t'.join(pre_line_list))
        if sent.startswith('[A]') or sent.startswith('[C]'):
            if len(pre_line_list) > 0:
                merged_nlp_dup_out_list.append('\t'.join(pre_line_list))
            pre_line_list = line_list
        else:
            if len(pre_line_list) > 1:
                pre_line_list[3] += " " + sent
                pre_line_list[4] += " " + nlp_sent
            else:
                pre_line_list = line_list
    return merged_nlp_dup_out_list


def load_nlp_info():
    """
    Load NLP info
    :return:            NLP line dict
    """
    global TA_PATH
    global LOGGER
    # *_trx_updated.hmd.txt 에서 NLP 결과를 dictionary 에 추가한다
    nlp_line_dict = dict()
    w_ob = os.walk("{ta}/data/{tr}/HMD/".format(ta=TA_PATH['path'], doc=DOC_DATE, tr=TRANS_NO))
    for dir_name in w_ob:
        for file_name in dir_name[2]:
            if check_file("_trx_updated.hmd.txt", file_name):
                nlp_file = open("{ta}/data/{tr}/HMD/{fn}".format(ta=TA_PATH['path'], doc=DOC_DATE, tr=TRANS_NO, fn=file_name), 'r')
                modify_nlp_dup_out = modify_nlp_line_num(nlp_file)
                merged_nlp_dup_out_list = merge_nlp_dup_out(modify_nlp_dup_out)
                for nlp_line in merged_nlp_dup_out_list:
                    try:
                        nlp_line = nlp_line.strip()
                        nlp_line_list = nlp_line.split('\t')
                        if len(nlp_line_list) > 4:
                            nlp_key = nlp_line_list[0].strip() + "_" + nlp_line_list[3].strip()
                            nlp_key = nlp_key.replace(" ", "")
                            nlp_value = nlp_line_list[4]
                            if nlp_key not in nlp_line_dict:
                                nlp_line_dict[nlp_key] = nlp_value
                    except Exception as e:
                        print e
                        exc_info = traceback.format_exc()
                        LOGGER.error(exc_info)
                nlp_file.close()
    return nlp_line_dict


def add_nlp_info(dup_output_list, nlp_line_dict):
    """
    Add NLP info
    :param          dup_output_list:         doc date
    :param          nlp_line_dict:           trans number
    :return:                                 Merge dup nlp list
    """
    global LOGGER
    merge_dup_nlp_list = list()
    # Add NLP info
    for dup_line in dup_output_list:
        dup_line_list = dup_line.split('\t')
        try:
            dup_key = dup_line_list[1].strip() + "_" + dup_line_list[6].strip()
            dup_key = dup_key.replace(" ", "")
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error("Add NLP info ERROR")
            LOGGER.error(dup_line)
            LOGGER.error(exc_info)
            continue
        if dup_key in nlp_line_dict:
            dup_line_list.append(nlp_line_dict[dup_key])
        else:
            dup_line_list.append('none')
        merge_dup_nlp_list.append(dup_line_list)
    return merge_dup_nlp_list


def add_verification_key_and_accuracy(merge_dup_nlp_list, verification_keyword_dict):
    """
    Add verification key and accuracy
    :param          merge_dup_nlp_list:                 doc date
    :param          verification_keyword_dict:          verification keyword
    :return:                                            Merge dup nlp list
    """
    global LOGGER
    add_accuracyy_info_list = list()
    for idx, line_list in enumerate(merge_dup_nlp_list):
        try:
            hmd_cat = line_list[4]
            if hmd_cat != 'none' and hmd_cat != 'new none':
                hmd_cat_list = hmd_cat.split('_')
                if len(hmd_cat_list) != 3:
                    continue
                scrp_cd = hmd_cat_list[2].strip()
                nlp_sent = line_list[8]
                nlp_sent_list = nlp_sent.replace("[ A ]", "").replace('[ C ]', '').split()
                if scrp_cd in verification_keyword_dict:
                    verification_keyword_list = list()
                    for uni_key in verification_keyword_dict[scrp_cd]:
                        verification_keyword_list.append(uni_key.encode('euc-kr'))
                    appear_list = list(set(verification_keyword_list) & set(nlp_sent_list))
                    appear_cnt = len(appear_list)
                    accuracy = float(appear_cnt) / float(len(verification_keyword_dict[scrp_cd])) * 100
                    line_list.append(str(appear_cnt))
                    line_list.append(str(len(verification_keyword_dict[scrp_cd])))
                    line_list.append(str(round(accuracy, 1)))
                    line_list.append(",".join(appear_list))
                    add_accuracyy_info_list.append(line_list)
                else:
                    line_list.append('none')
                    line_list.append('none')
                    line_list.append('none')
                    line_list.append('none')
                    add_accuracyy_info_list.append(line_list)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
    return add_accuracyy_info_list


def extract_time_from_stt():
    """
    Extract time from CS
    :return:            Time info dict
    """
    w_ob = os.walk(STT_MERGE_FILE_PATH)
    time_info_dict = dict()
    # *_trx.txt 에서 time 정보를 dictionary 에 추가한다
    for dir_name in w_ob:
        for file_name in dir_name[2]:
            if check_file("_trx.txt", file_name):
                time_file = open("{mp}/{fn}".format(mp=STT_MERGE_FILE_PATH, doc=DOC_DATE, tr=TRANS_NO, fn=file_name), 'r')
                for idx, line in enumerate(time_file):
                    line = line.strip()
                    line_list = line.split('\t')
                    if len(line_list) < 3:
                        continue
                    speaker = line_list[0].replace('client', '[C]').replace('agent', '[A]')
                    start_time = str(timedelta(seconds=int(line_list[1].replace('ts=', '')) / 100))
                    end_time = str(timedelta(seconds=int(line_list[2].replace('te=', '')) / 100))
                    sent = line_list[3][:8000]
                    key = "{fn}|{idx}|{speaker}{sent}".format(fn=file_name.replace('.txt', '_updated'), idx=idx, speaker=speaker, sent=sent)
                    value = [start_time, end_time]
                    time_info_dict[key] = value
                time_file.close()
    return time_info_dict


def add_time_info(time_info_dict, add_accuracy_info_list):
    """
    Add time info
    :param          time_info_dict:                     Time info dict
    :param          add_accuracy_info_list:             dup output
    :return:                                            Add time info
    """
    global LOGGER
    add_time_info_list = list()
    for line_list in add_accuracy_info_list:
        try:
            call_id = line_list[1]
            line_num = line_list[2]
            sent = line_list[6]
            check_key = "{id}|{num}|{sent}".format(id=call_id, num=line_num, sent=sent)
            if check_key in time_info_dict:
                line_list.append(time_info_dict[check_key][0])
                line_list.append(time_info_dict[check_key][1])
            else:
                line_list.append('0:00:00')
                line_list.append('0:00:00')
            add_time_info_list.append(line_list)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
    return add_time_info_list


def add_info_to_dup_out(mssql, dup_output_list):
    """
    Add info to dup output
    :param          mssql:                  MSSQL
    :param          dup_output_list:        dup output list
    :return:                                output list
    """
    global LOGGER

    LOGGER.info("Add info to dup output")
    verification_keyword_dict = load_verification_keyword(mssql)
    nlp_line_dict = load_nlp_info()
    merge_dup_nlp_list = add_nlp_info(dup_output_list, nlp_line_dict)
    add_accuracy_info_list = add_verification_key_and_accuracy(merge_dup_nlp_list, verification_keyword_dict)
    time_info_dict = extract_time_from_stt()
    final_dup_output = add_time_info(time_info_dict, add_accuracy_info_list)
    LOGGER.info("Success add info to dup output")
    return final_dup_output


def insert_data_to_master_and_dtl_table(mssql, final_dup_output):
    """
    Insert data to MsSQL master and dtl table
    :param          mssql:                  MsSQL
    :param          final_dup_output:       Dup output
    :return:
    """
    global LOGGER
    kwd_idx = 0
    for idx, line_list in enumerate(final_dup_output):
        kwd_idx += 1
        if len(line_list) != 15:
            LOGGER.error('Out put format length is not 15')
            LOGGER.error("\t".join(line_list))
        else:
            try:
                transfer_number = line_list[0]
                dcm_no = line_list[1].replace('_trx_updated', '')
                stmt_no = line_list[2]
                stmt_seq = idx
                chn_id = 'C' if line_list[6].startswith('[C]') else 'A'
                pd_cd = line_list[4].split("_")[0]
                scrp_sect_cd = line_list[4].split("_")[1]
                scrp_stmt_cd = line_list[4].split("_")[2]
                stmt_st = line_list[13][:8000]
                stmt_end = line_list[14]
                scrp_stmt_yn = 'Y'
                prhw_yn = 'Y' if scrp_stmt_cd == '00000' else 'N'
                stmt = line_list[6][:8000]
                cus_yn = 'Y' if line_list[7] != 'none' else 'N'
                cus_yn_stmt = line_list[7][:8000] if line_list[7] != 'none' else ''
                cus_chn_id = chn_id
                dcm_dt = line_list[3]
                dtct_kwd = line_list[5]
                ctgr = line_list[4]
                kwd_dtct_rto = float(line_list[11]) if line_list[11] != 'none' else 0
                kwd_tt_cnt = int(line_list[10]) if line_list[10] != 'none' else 0
                kwd_dtct_cnt = int(line_list[9]) if line_list[9] != 'none' else 0
                kwd_dtct_list = line_list[12].split(',')
            except Exception as e:
                print e
                exc_info = traceback.format_exc()
                LOGGER.error('Out put format is wrong')
                LOGGER.error(exc_info)
                LOGGER.error('\t'.join(line_list))
                continue

            mssql.insert_data_to_mastr(
                transfer_number=transfer_number,
                dcm_no=dcm_no,
                stmt_no=stmt_no,
                stmt_seq=stmt_seq,
                chn_id=chn_id,
                pd_cd=pd_cd,
                scrp_sect_cd=scrp_sect_cd,
                scrp_stmt_cd=scrp_stmt_cd,
                stmt_st=stmt_st,
                stmt_end=stmt_end,
                scrp_stmt_yn=scrp_stmt_yn,
                prhw_yn=prhw_yn,
                stmt=stmt,
                cus_yn=cus_yn,
                cus_yn_stmt=cus_yn_stmt,
                cus_chn_id=cus_chn_id,
                dcm_dt=dcm_dt,
                dtct_kwd=dtct_kwd,
                ctgr=ctgr,
                kwd_dtct_rto=kwd_dtct_rto,
                kwd_tt_cnt=kwd_tt_cnt,
                kwd_dtct_cnt=kwd_dtct_cnt
            )

            if len(kwd_dtct_list) > 0:
                for keyword in kwd_dtct_list:
                    if len(keyword.strip()) < 1:
                        continue
                    mssql.insert_data_to_dtl(
                        transfer_number=transfer_number,
                        dcm_no=dcm_no,
                        stmt_no=stmt_no,
                        kwd_id=str(kwd_idx + 1),
                        chn_id=chn_id,
                        kwd=keyword,
                        kwd_dict_yn=prhw_yn,
                        scrp_cd=scrp_stmt_cd,
                        scrp_seq=kwd_idx + 1
                    )
                    kwd_idx += 1

    mssql.conn.commit()


def insert_data_to_sect_table(mssql):
    """
    Insert data to MsSQL master and dtl table
    :param          mssql:           MsSQL
    """
    mssql.insert_data_to_sect(PD_CD, TRANS_NO)
    mssql.conn.commit()


def insert_data_to_mssql(mssql, final_dup_output):
    """
    Insert data to MsSQL master, dtl and sect table
    :param          mssql:                  MsSQL
    :param          final_dup_output:       Dup output
    :return:
    """
    global LOGGER
    global TA_PATH
    LOGGER.info('Insert data to table')
    final_dup_output_path = TA_PATH['path'] + '/data/' + TRANS_NO + '/final_dup_out.txt'
    final_dup_output_file = open(final_dup_output_path, 'wt')
    for final_dup_out_line in final_dup_output:
        print >> final_dup_output_file, '\t'.join(final_dup_out_line)
    final_dup_output_file.close()
    insert_data_to_master_and_dtl_table(mssql, final_dup_output)
    insert_data_to_sect_table(mssql)
    LOGGER.info("Success insert data to table")


def execute_ta_process_check_matrix():
    """
    Do TA process use hmd_check matrix
    """
    global LOGGER
    global HMD_PATH
    global TA_THREAD

    LOGGER.info("Do TA_process use check matrix")
    hmd_matrix_path = "{hp}/hmd_check.matrix".format(hp=HMD_PATH['path'])
    TA_process.func_FindHMD(TRANS_NO, hmd_matrix_path, int(TA_THREAD['thread']))
    LOGGER.info("Success do TA_process use check matrix")


def merge_check_matrix_output_line(modify_dup_out_list):
    """
    Merge check matrix output line
    :param          modify_dup_out_list:        Modified line num file
    :return:                                    Merged dup output line
    """
    base_hmd = ''
    base_line = ''
    base_call_id = ''
    base_line_list = list()
    merged_dup_out_list = list()
    # 잘 못 분리 되어진 문장을 하나의 문장으로 수정 call id, line number, HMD 가 같을 경우 출력 아닐 경우 merge
    for idx, line in enumerate(modify_dup_out_list):
        curr_line_list = line.split('\t')
        curr_call_id = curr_line_list[1]
        curr_line_no = curr_line_list[2]
        curr_hmd = curr_line_list[4]
        curr_sent = curr_line_list[6]
        if idx + 1 == len(modify_dup_out_list):
            if base_call_id != curr_call_id or base_line != curr_line_no:
                merged_dup_out_list.append('\t'.join(base_line_list))
                merged_dup_out_list.append(line)
            else:
                base_line_list[6] += " " + curr_sent
                merged_dup_out_list.append('\t'.join(base_line_list))
            continue
        if base_call_id != curr_call_id or base_line != curr_line_no or base_hmd != curr_hmd:
            if idx == 0:
                base_line_list = curr_line_list
                base_call_id = curr_call_id
                base_line = curr_line_no
                base_hmd = curr_hmd
                continue
            merged_dup_out_list.append('\t'.join(base_line_list))
            base_line_list = curr_line_list
            base_call_id = curr_call_id
            base_line = curr_line_no
        else:
            base_line_list[6] += " " + curr_sent
    return merged_dup_out_list


def make_array_check_matrix_output_line(modify_dup_out_list):
    """
    Make array check matrix output line
    :param          modify_dup_out_list:        Modified line num file
    :return:                                    Output array dict
    """
    global LOGGER
    dup_out_array_dict = defaultdict(list)
    for line in modify_dup_out_list:
        line = line.strip()
        line_list = line.split('\t')
        if len(line_list) != 7:
            LOGGER.error("Error check matrix out put line field is %d = %s" % (len(line_list), line))
            continue
        call_id = line_list[1].replace("_trx_updated", "").strip()
        if call_id not in dup_out_array_dict:
            dup_out_array_dict[call_id] = [line]
        else:
            dup_out_array_dict[call_id] += [line]
    return dup_out_array_dict


def modify_check_matrix_output_line(mssql, merged_dup_out_list):
    """
    Modify dup out for STTATRANSITSECT table
    :param      mssql:                      MsSQL
    :param      merged_dup_out_list:        Merged dup out list
    :return:                                Modified dup out list
    """
    modified_dup_out_list = list()
    # 녹취 파일의 end time 은 시작 시간 +5초로 한다
    for line in merged_dup_out_list:
        line_list = line.split('\t')
        call_id = line_list[1].replace("_trx_updated", "")
        hmd_cat = line_list[4]
        stmt_no = line_list[2]
        uni_hmd_cat = line_list[4].decode('euc-kr')
        if hmd_cat != 'none':
            if u'계속보험료시작' in uni_hmd_cat:
                time_info = mssql.select_time_info(call_id, stmt_no)
                uni_start_time = time_info[0].strip()
                uni_end_time = time_info[1].strip()
                line_list.append(uni_start_time.encode('euc-kr'))
                line_list.append(uni_end_time.endoce('euc-kr'))
                modified_dup_out_list.append(line_list)
            else:
                time_info = mssql.select_time_info(call_id, stmt_no)
                uni_start_time = time_info[0].strip()
                uni_end_time = time_info[1].strip()
                line_list.append(uni_start_time.encode('euc-kr'))
                end_date_time = datetime.strptime(uni_end_time.endoce('euc-kr')[:15], "%H:%M:%S.%f")
                end_date_time += timedelta(seconds=5)
                str_end_date_time = datetime.strftime(end_date_time, "%H:%M:%S.%f")
                line_list.append(str_end_date_time)
                modified_dup_out_list.append(line_list)
        else:
            modified_dup_out_list.append(line_list)
    return modified_dup_out_list


def insert_data_for_transitsect_table(mssql, modified_dup_out_list, recent_call_date):
    """
    Insert data to STTATRANSITSECT table
    :param          mssql:                      MsSQL
    :param          modified_dup_out_list:      Modified dup out list
    :param          recent_call_date:           Recent call date
    """
    global TA_PATH
    global QUESTION_OF_ASSENT_25
    global QUESTION_OF_ASSENT_24
    output_call_id = ''
    output_st_24 = ''
    output_et_24 = ''
    output_st_25 = ''
    output_et_25 = ''
    output_25_sent = ''
    output_24_sent = ''
    start_time = ''
    end_time = ''
    next_sent_24 = ''
    next_sent_25 = ''
    sent_end_check_point = True

    sttatransitsect_dup_out_file_path = TA_PATH['path'] + '/data/' + TRANS_NO + '/sttatransitsect_dup_out.txt'
    sttatransitsect_dup_out_file = open(sttatransitsect_dup_out_file_path, 'a+')
    for modify_dup_out_line in modified_dup_out_list:
        print >> sttatransitsect_dup_out_file, "\t".join(modify_dup_out_line)
    sttatransitsect_dup_out_file.close()

    # Set output format
    # 응답 구간은 탐지된 구간의 최대 +2 line 까지 검색한다
    # 마지막 계속보험료 시작 다음에 오는 척번째 종료를 한 쌍으로 한다
    # 초회동일은 가장 마지막 구간으로 한다
    # 가장 최근 날짜의 call id 를 기준으로 DB insert 는 QUESTION_OF_ASSENT 를 바꾸어 한번만 insert 하도록 한다
    for idx, modified_line_list in enumerate(modified_dup_out_list):
        call_id = modified_line_list[1].replace("_trx_updated", "")
        output_call_id = call_id
        uni_hmd_cat = modified_line_list[4].decode('euc-kr')
        sent = modified_line_list[6].strip()
        if len(modified_line_list) > 7:
            start_time = modified_line_list[7]
            end_time = modified_line_list[8]
        if u"계속보험료시작" in uni_hmd_cat:
            output_st_25 = start_time
            output_25_sent = sent
            output_et_25 = ''
            sent_end_check_point = True
        elif u"계속보험료종료" in uni_hmd_cat:
            if len(output_et_25) < 1 < len(output_st_25):
                sent_end_check_point = False
                output_et_25 = end_time
                output_25_sent += "\n" + sent
                for cnt in range(1, 3):
                    total_idx = idx + cnt
                    if total_idx >= len(modified_dup_out_list):
                        continue
                    next_sent_25 = modified_dup_out_list[total_idx][6].strip()
                    if next_sent_25.startswith('[C]'):
                        output_25_sent += '\n' + next_sent_25
        elif len(output_25_sent) > 1 and sent_end_check_point:
            output_25_sent += '\n' + sent
        elif u'초회' in uni_hmd_cat:
            output_st_24 = start_time
            output_et_24 = end_time
            output_24_sent = sent
            for cnt in range(1, 3):
                total_idx = idx + cnt
                if total_idx > len(modified_dup_out_list):
                    continue
                next_sent_24 = modified_dup_out_list[total_idx][6].strip()
                if next_sent_24.startswith('[C]'):
                    output_24_sent += '\n' + next_sent_24

    # Update status 24
    if len(output_st_24) > 1 and len(output_et_24) > 1 and QUESTION_OF_ASSENT_24 == 0:
        QUESTION_OF_ASSENT_24 = 1
        mssql.insert_data_to_transitsect(
            trans_no=TRANS_NO,
            status='24',
            dcm_no=output_call_id,
            scrp_st=output_st_24,
            scrp_end=output_et_24,
            start_date=recent_call_date,
            cus_yn='Y' if len(next_sent_24) > 0 else 'N',
            stmt=output_24_sent[:8000]
        )

    # Update status 25
    if len(output_st_25) > 1 and len(output_et_25) > 1 and QUESTION_OF_ASSENT_25 == 0:
        QUESTION_OF_ASSENT_25 = 1
        mssql.insert_data_to_transitsect(
            trans_no=TRANS_NO,
            status='25',
            dcm_no=output_call_id,
            scrp_st=output_st_25,
            scrp_end=output_et_25,
            start_date=recent_call_date,
            cus_yn='Y' if len(next_sent_25) > 0 else 'N',
            stmt=output_25_sent[:8000]
        )


def set_data_for_transitsect_table(mssql):
    """
    Set data for STTATRANSITSECT
    :param          mssql:          MsSQL
    """
    global TA_PATH
    global LOGGER
    LOGGER.info("Set data for STTATRANSITSECT")
    tsv_file_path = "{ta}/data/{tr}/{tr}_hmd_search.tsv".format(ta=TA_PATH['path'], tr=TRANS_NO)
    tsv_file = open(tsv_file_path, 'r')
    modify_dup_out_list = modify_dup_out_line_num(tsv_file)
    merged_dup_out_list = merge_check_matrix_output_line(modify_dup_out_list)
    dup_out_array_dict = make_array_check_matrix_output_line(merged_dup_out_list)
    sorted_call_id_and_date = mssql.select_recent_record_date(TRANS_NO)
    # 전송 번호 기준으로 가장 최근 일자의  call id 를 순차적으로 검색한다
    for item in sorted_call_id_and_date:
        recent_call_id = item[0].replace('.wav', '').strip()
        recent_call_date = item[1].strip()
        if recent_call_id in dup_out_array_dict:
            if QUESTION_OF_ASSENT_24 == 0 or QUESTION_OF_ASSENT_25 == 0:
                line_list = dup_out_array_dict[recent_call_id]
                modified_dup_out_list = modify_check_matrix_output_line(mssql, line_list)
                insert_data_for_transitsect_table(mssql, modified_dup_out_list, recent_call_date)
    tsv_file.close()
    LOGGER.info("Success set data for STTATRANSITSECT")


def update_stta_ed_dtm(mssql):
    """
    Update TA targets state
    :param          mssql:          MsSQL
    """
    global LOGGER
    LOGGER.info("Update STTA end time and host name TR={tr}, HN={hn}".format(tr=TRANS_NO, hn=HOST_NAME))
    mssql.update_ta_end_time(TRANS_NO, HOST_NAME, ISP_TMS)


def update_file_status_to_end(mssql, ta_targets_list):
    """
    Update TA targets state
    :param          mssql:                  MsSQL
    :param          ta_targets_list:        TA targets
    """
    global LOGGER
    LOGGER.info("Update file state to end 40 -41")
    is_first = True
    for ta_target in ta_targets_list:
        basename = ta_target[1].strip()
        isp = str(ta_target[2]).strip()
        LOGGER.info("{0}/{1} = Update state 40 to 41".format(TRANS_NO, basename))
        if is_first:
            rr_true_or_false = mssql.update_file_status_rr(TRANS_NO, HOST_NAME, isp, 41)
            if not rr_true_or_false:
                LOGGER.error("Failed STTACTRREQ update status --> {0}/{1}".format(TRANS_NO, isp))
            is_first = False
        ll_true_or_false = mssql.update_file_status_ll(TRANS_NO, basename, isp, 41)
        if not ll_true_or_false:
            LOGGER.error("Failed STTACLLREQ update state --> {0}/{1}".format(TRANS_NO, basename))
            continue
    update_stta_ed_dtm(mssql)
    mssql.conn.commit()
    if socket.gethostname() == 'vrstt1v':
        requests.get(
            "http://tmdev.meritzfire.com:29600/tmsys/70000/70000_STT_RESULT.jsp?TransferNumber={tr}&SttaRslCd=02".format(
                tr=TRANS_NO))
    elif socket.gethostname() == 'vrta1p' or socket.gethostname() == 'vrta2p':
        requests.get(
            "https://tm.meritzfire.com/tmsys/70000/70000_STT_RESULT.jsp?TransferNumber={tr}&SttaRslCd=02".format(
                tr=TRANS_NO))
    LOGGER.info("Success update file state to end 40 - 41")


def execute_enc():
    """
    Execute enc, copy output file and remove garbage file
    """
    global LOGGER
    LOGGER.info("Execute enc")

    # CS ENC
    w_ob = os.walk(STT_DIR_PATH)
    # Delete files without *_trx_updated.txt and *_trx.txt file
    for dir_name in w_ob:
        for file_name in dir_name[2]:
            if check_file("_trx_updated.txt", file_name) or check_file("_trx.txt", file_name):
                continue
            else:
                try:
                    os.remove(os.path.join(dir_name[0], file_name))
                except Exception as e:
                    print e
                    continue
    # Execute encrypt
    encrypt(STT_DIR_PATH)

    # TA ENC
    w_ob = os.walk(TA_FILE_PATH)
    # Delete files without *_trx_updated.hmd.txt, *_dup_out.txt ad *_hmd_search.tsv file
    for dir_name in w_ob:
        for file_name in dir_name[2]:
            if check_file("_trx_updated.hmd.txt", file_name) or check_file("_dup_out.txt", file_name) or check_file("_hmd_search.tsv", file_name):
                continue
            else:
                try:
                    os.remove(os.path.join(dir_name[0], file_name))
                except Exception as e:
                    print e
                    continue
    # Execute encrypt
    encrypt(TA_FILE_PATH)

    # Copy and delete TA output file
    if os.path.exists(TA_OUT_FILE_PATH):
        shutil.rmtree(TA_OUT_FILE_PATH)
    if not os.path.exists(TA_OUT_FILE_PATH):
        os.makedirs(TA_OUT_FILE_PATH)

    try:
        shutil.move(TA_FILE_PATH, TA_OUT_FILE_PATH)
    except Exception as e:
        print e
        exc_info = traceback.format_exc()
        LOGGER.error("Can't move file")
        LOGGER.error(exc_info)
        pass

    if os.path.exists(TA_FILE_PATH):
        shutil.rmtree(TA_FILE_PATH)
    LOGGER.info("Success execute enc")


def processing(mssql, ta_targets_list):
    """
    This is function that TA process
    @param      mssql                       MsSQL
    @param      ta_targets_list            TA target list
    """
    global DT
    global PD_CD
    global ISP_TMS
    global TA_PATH
    global STT_PATH
    global HMD_PATH
    global TA_THREAD
    global LOGGER
    global LOG_LEVEL
    global STT_OUT_PATH
    global STT_MERGE_FILE_PATH
    global HOST_NAME
    global TRANS_NO
    global DOC_DATE
    global STT_DIR_PATH
    global TA_FILE_PATH
    global TA_OUT_FILE_PATH
    global QUESTION_OF_ASSENT_24
    global QUESTION_OF_ASSENT_25

    try:
        # Load config info
        TA_PATH = load_conf('TA_PATH', 'TA')
        STT_PATH = load_conf('STT_PATH', 'TA')
        STT_OUT_PATH = load_conf('STT_OUT_PATH', 'TA')
        HMD_PATH = load_conf('HMD_PATH', 'TA')
        TA_THREAD = load_conf('TA_THREAD', 'TA')
        LOG_LEVEL = load_conf('LOG_LEVEL', 'TA')
        TRANS_NO = ta_targets_list[0][0]
        ISP_TMS = str(ta_targets_list[0][2]).strip()
        PD_CD = ta_targets_list[0][3]
        HOST_NAME = socket.gethostname()
        now_tmp = datetime.now()
        DOC_DATE = now_tmp.strftime("%y%m%d")
        TA_FILE_PATH = "{ta}/data/{tr}".format(ta=TA_PATH['path'], tr=TRANS_NO)
        STT_DIR_PATH = "{stt}/{y}/{m}/{d}/{tr}_{isp}".format(stt=STT_OUT_PATH['path'], y=TRANS_NO[:4], m=TRANS_NO[4:6], d=TRANS_NO[6:8], tr=TRANS_NO, isp=ISP_TMS)
        STT_MERGE_FILE_PATH = "{stt}/{y}/{m}/{d}/{tr}_{isp}/STT_merge".format(stt=STT_OUT_PATH['path'], y=TRANS_NO[:4], m=TRANS_NO[4:6], d=TRANS_NO[6:8], tr=TRANS_NO, isp=ISP_TMS)
        TA_OUT_FILE_PATH = "{std}/TA_out".format(std=STT_DIR_PATH)
        # Update targets status to start
        update_targets_status_to_start(mssql, ta_targets_list)
    except Exception as e:
        exc_info = traceback.format_exc()
        print e, exc_info
        update_file_status_to_error(mssql, ta_targets_list)
        if mssql:
            mssql.disconnect()
        sys.exit(1)

    try:
        # Execute TA_process
        execute_ta_process(mssql)
        # Modify TA result
        dup_output_list = modify_ta_result(mssql)
        # Add info to dup output
        final_dup_output = add_info_to_dup_out(mssql, dup_output_list)
        # Insert data to MsSQL
        insert_data_to_mssql(mssql, final_dup_output)
        # Do TA_process use check matrix for STTATRANSITSECT
        execute_ta_process_check_matrix()
        # Set data for STTATRANSITSECT
        set_data_for_transitsect_table(mssql)
        # Update file state to end
        update_file_status_to_end(mssql, ta_targets_list)
        # Execute enc
        execute_enc()
        # MsSQL disconnect and LOGGER close
        LOGGER.info("MySQL disconnect")
        mssql.disconnect()
        LOGGER.info("Remove logger handler")
        LOGGER.info("END.. The time required = {0}".format(elapsed_time(DT)))
        for handler in LOGGER.handlers:
            handler.close()
            LOGGER.removeHandler(handler)
    except Exception as e:
        print e
        exc_info = traceback.format_exc()
        LOGGER.error(exc_info)
        update_file_status_to_error(mssql, ta_targets_list)
        if mssql:
            mssql.disconnect()
            LOGGER.error("MySQL disconnect")
        LOGGER.error("Remove logger handler")
        for handler in LOGGER.handlers:
            handler.close()
            LOGGER.removeHandler(handler)
        sys.exit(1)


########
# main #
########


def execute_ta():
    """
    This is a program that execute TA
    """
    global DB_CONFIG
    global DT

    ts = time.time()
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')

    try:
        # Load config info
        DB_CONFIG = load_conf('DB_CONFIG', 'TA')
        # Load MsSQL
        mssql = MSSQL()
        # Select TA targets
        ta_targets_list = mssql.select_ta_targets()
        if ta_targets_list == bool:
            print "TA target is None"
            if mssql:
                mssql.disconnect()
        if not ta_targets_list:
            print "TA target is None"
            if mssql:
                mssql.disconnect()
        else:
            processing(mssql, ta_targets_list)
    except Exception as e:
        exc_info = traceback.format_exc()
        print e, exc_info
        sys.exit(1)


if __name__ == "__main__":
    execute_ta()

    # vim: set expandtab:
