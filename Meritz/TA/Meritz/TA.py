#!/usr/bin/python
#-*- coding: euc-kr -*-

"""program"""
__author__ = "MINDs LAB"
__date__ = "creation: 2017-11-30, modification: 2017-11-30"

###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import socket
import pymssql
import requests
import traceback
import ConfigParser
from datetime import datetime, timedelta
from cfg.config import DB_CONFIG
from collections import defaultdict
from lib.iLogger import set_logger
from lib.meritz_enc import encrypt
sys.path.append('/data1/MindsVOC/TA/LA/bin')
import TA_process

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')

#############
# constants #
#############
DT = ''

#########
# class #
#########
class MSSQL():
    def __init__(self):
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
        self.cursor = self.conn.cursor(as_dict=True)

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
                a.ISP_TMS,
                (
                    SELECT DISTINCT
                        PD_CD
                    FROM
                        STTACTRREQ WITH(NOLOCK)
                    WHERE
                        TRANSFER_NUMBER = a.TRANSFER_NUMBER
                ) AS PD_CD
            FROM
                STTACLLREQ a WITH(NOLOCK)
            INNER JOIN
                (
                    SELECT TOP(1)
                        TRANSFER_NUMBER,
                        MAX(ISP_TMS) AS isptms,
                        PROG_STAT_CD
                    FROM
                        STTACLLREQ WITH(NOLOCK)
                    WHERE 1=1
                        AND PROG_STAT_CD = '31'
                        AND ISP_TMS = '1'
                    GROUP BY
                        TRANSFER_NUMBER,
                        ISP_TMS,
                        PROG_STAT_CD
                ) b
            ON 1=1
                AND a.TRANSFER_NUMBER = b.TRANSFER_NUMBER
                AND a.ISP_TMS = b.isptms
                AND a.PROG_STAT_CD = b.PROG_STAT_CD
        """
        self.cursor.execute(query)
        result_dict = self.cursor.fetchall()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
        return False

    def select_agree_points(self):
        query = """
            SELECT DISTINCT
                SCRP_CD
            FROM
                STTACTRREQ WITH(NOLOCK)
            WHERE
                STTA_CUS_REPLY_CD IN ('02', '03')
        """
        self.cursor.execute(query)
        result_dict = self.cursor.fetchall()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
        return False

    def select_cot_ins_date(self, trans_no):
        query = """
            SELECT
                PRY_COT_INS_DATE
            FROM
                STTACTRREQ WITH(NOLOCK)
            WHERE
                TRANSFER_NUMBER = %s
                AND ISP_TMS = '1'
        """
        bind = (trans_no, )
        self.cursor.execute(query, bind)
        result_dict = self.cursor.fetchone()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
        return False

    def select_start_point(self, pd_cd, trans_no):
        query = """
            WITH TEMP_INFO as
            (
                SELECT
                    max(Info.HIS_ST_DT) AS INFO_DATE
                FROM
                    STTACTRREQ AS Request WITH(NOLOCK),
                    STTAPDSECTINF AS Info WITH(NOLOCK)
                WHERE 1=1
                    AND Request.PD_CD = Info.PD_CD
                    AND Info.PD_CD = %s
                    AND Request.TRANSFER_NUMBER = %s
                    AND Info.HIS_ST_DT <= Request.PRV_COT_INS_DATE
            )
            SELECT
                Info.PD_CD,
                Info.SCRP_SECT_CD,
                Info.SCRP_CD
            FROM
                STTAPDSECTINF AS Info WITH(NOLOCK)
                TEMP_INFO AS LatestDate
            WHERE 1=1
                AND Info.HIS_ST_DT = LatestDate.INFO_DATE
                AND Info.PD_CD = %s
                AND Info.SCRP_SECT_ST_YN = 'Y'
        """
        bind = (pd_cd, trans_no, pd_cd, )
        self.cursor.execute(query, bind)
        result_dict = self.cursor.fetchall()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
        return False

    def select_time_info(self, call_id, stmt_no):
        query = """
            SELECT
                STMT_ST,
                STMT_END
            FROM
                STTARSL WITH(NOLOCK)
            WHERE 1=1
                AND DCM_NO = %s
                AND STMT_NO = %s
        """
        bind = (call_id, stmt_no, )
        self.cursor.execute(query, bind)
        result_dict = self.cursor.fetchone()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
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
        result_dict = self.cursor.fetchall()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
        return False

    def select_verification_keyword(self):
        query = """
            SELECT
                SCRP_CD,
                SCRP_KWD_NM
            FROM
                STTASCRPKWDINF WITH(NOLOCK)
            WHERE 1=1
                AND SCRP_CD != 00000
                AND LEN(SCRP_KWD_NM) > 0
            ORDER BY
                SCRP_CD
        """
        self.cursor.execute(query)
        result_dict = self.cursor.fetchall()
        if result_dict is bool:
            return False
        if result_dict:
            return result_dict
        return False

    def insert_data_to_mastr(self, logger, **kwargs):
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
                    CUS_YN_STMT,
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
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            self.conn.rollback()

    def insert_data_to_dtl(self, logger, **kwargs):
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
                    KWD,
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
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            self.conn.rollback()

    def insert_data_to_arsl(self, logger, **kwargs):
        try:
            query = """
                INSERT INTO
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
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            self.conn.rollback()

    def insert_data_to_sect(self, pd_cd, trans_no):
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
                    DCM_NO char(25)
                    
                )
            """
        except Exception:
            print 'error'