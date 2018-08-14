#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-09, modification: 2018-05-04"

###########
# imports #
###########
import os
import re
import sys
import glob
import time
import shutil
import requests
import cx_Oracle
import traceback
import subprocess
import workerpool
import collections
from datetime import datetime
from operator import itemgetter
from cfg.config import DB_CONFIG, QA_TA_CONFIG, MASKING_CONFIG
from lib.iLogger import set_logger
from lib.openssl import decrypt, encrypt

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
BWD_RESULT = list()
GRT_CD_DICT = dict()
NLP_DIR_PATH = ""
NLP_INFO_DICT = dict()
INT_SEC_RESULT = list()
DETAIL_DIR_PATH = ""
OUTPUT_DIR_NAME = ""
RFILE_INFO_DICT = dict()
DELETE_FILE_LIST = list()
INCOMSALE_RESULT = list()
IP_SEC_SAVE_DICT = dict()
TA_TEMP_DIR_PATH = ""
OVERLAP_CHECK_DICT = dict()
OVERLAP_CHECK_DICT_VER_TWO = dict()
MOTHER_SNTC_NO_LIST = list()
SEC_TIME_CHECK_DICT = dict()
DETECT_CATEGORY_DICT = dict()
TARGET_CATEGORY_DICT = collections.OrderedDict()
TB_TM_QA_TA_DTC_RST_DICT = dict()
TB_TM_QA_TA_ADT_DTC_RST_LIST = list()


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

    def select_pk(self, poli_no, ctrdt):
        query = """
            SELECT DISTINCT
                REC_ID,
                RFILE_NAME
            FROM
                TB_TM_CNTR_RCDG_INFO
            WHERE 1=1
                AND POLI_NO = :1
                AND CTRDT = :2
                AND USE_YN = 'Y'
        """
        bind = (
            poli_no,
            ctrdt,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_stt_server_id(self, rec_id, rfile_name):
        query = """
            SELECT
                STT_SERVER_ID,
                CALL_START_TIME,
                CHN_TP
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND REC_ID = :1
                AND RFILE_NAME = :2
        """
        bind = (
            rec_id,
            rfile_name,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_scrt_sntc_dtc_info(self, sntc_no):
        query = """
            SELECT
                DTC_CONT
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

    def select_data_to_scrt_sec_sntc_info(self, **kwargs):
        query = """
            SELECT
                SNTC_NO,
                STRT_SNTC_YN
            FROM
                TB_SCRT_SEC_SNTC_INFO
            WHERE 1=1
                AND IP_CD = :1
                AND QA_SCRT_LCCD = :2
                AND QA_SCRT_MCCD = :3
                AND QA_SCRT_SCCD = :4
                AND SEC_NO = :5
                AND USE_YN = 'Y'
        """
        bind = (
            kwargs.get('ip_cd'),
            kwargs.get('qa_scrt_lccd'),
            kwargs.get('qa_scrt_mccd'),
            kwargs.get('qa_scrt_sccd'),
            kwargs.get('sec_no'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_grt_cd(self, poli_no, ctrdt, ip_cd):
        query = """
            SELECT
                GRT_CD
            FROM
                TB_TM_QA_TA_GRT_INFO
            WHERE 1=1
                AND POLI_NO = :1
                AND CTRDT = :2
                AND IP_CD = :3
        """
        bind = (
            poli_no,
            ctrdt,
            ip_cd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_sntc_no_list(self, grt_cd):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_GRT_SNTC_INFO
            WHERE 1=1
                AND GRT_CD = :1
                AND USE_YN = 'Y'
        """
        bind = (
            grt_cd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
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

    def select_data_to_tm_qa_ta_sec_info(self, **kwargs):
        query = """
            SELECT
                QA_SCRT_LCCD,
                QA_SCRT_MCCD,
                QA_SCRT_SCCD,
                SEC_NO
            FROM
                TB_TM_QA_TA_SEC_INFO
            WHERE 1=1
                AND POLI_NO = :1
                AND CTRDT = :2
                AND IP_CD = :3
                AND GRT_SEC_YN = :4
        """
        bind = (
            kwargs.get('poli_no'),
            kwargs.get('ctrdt'),
            kwargs.get('ip_cd'),
            kwargs.get('grt_sec_yn'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_cust_anyn(self):
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_SNTC_MST_INFO
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

    def select_ruser_nor_add_que_use_yn(self):
        query = """
            SELECT
                SNTC_NO,
                RUSER_NOR_ADD_QUE_KYWD_LIT
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND RUSER_NOR_ADD_QUE_USE_YN = 'Y'
        """
        self.cursor.execute(query, )
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_cu_nor_anyn_use_yn(self):
        query = """
            SELECT
                SNTC_NO,
                CU_NOR_ANYN_KYWD_LIT
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND CU_NOR_ANYN_USE_YN = 'Y'
        """
        self.cursor.execute(query, )
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_ruser_ab_add_que_use_yn(self):
        query = """
            SELECT
                SNTC_NO,
                RUSER_AB_ADD_QUE_KYWD_LIT
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND RUSER_AB_ADD_QUE_USE_YN = 'Y'
        """
        self.cursor.execute(query, )
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_cu_ab_anyn_use_yn(self):
        query = """
            SELECT
                SNTC_NO,
                CU_AB_ANYN_KYWD_LIT
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND CU_AB_ANYN_USE_YN = 'Y'
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
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND SNTC_NO = :1
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

    def select_dtc_cd_and_nm(self, sntc_no):
        query = """
            SELECT
                SNTC_DCD,
                DTC_CD,
                DTC_CD_NM
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND SNTC_NO = :1
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

    def select_data_to_scrt_sec_info(self, ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no):
        query = """
            SELECT
                QA_SCRT_LCCD,
                QA_SCRT_MCCD,
                QA_SCRT_SCCD,
                SEC_NO
            FROM
                TB_SCRT_SEC_INFO
            WHERE 1=1
                AND IP_CD = :1
                AND QA_SCRT_LCCD = :2
                AND QA_SCRT_MCCD = :3
                AND QA_SCRT_SCCD = :4
                AND SEC_NO = :5
                AND SCTN_SAVE_YN = 'Y'
        """
        bind = (
            ip_cd,
            qa_scrt_lccd,
            qa_scrt_mccd,
            qa_scrt_sccd,
            sec_no,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def update_qa_stta_prgst_cd(self, **kwargs):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    QA_STTA_PRGST_CD = :1
                WHERE 1=1
                    AND POLI_NO = :2
                    AND CTRDT = :3
                    AND CNTR_COUNT = :4
            """
            bind = (
                kwargs.get('qa_stta_prgst_cd'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
                kwargs.get('cntr_count'),
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

    def update_ta_cmdtm(self, poli_no, ctrdt, cntr_count):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    TA_CMDTM = SYSDATE
                WHERE 1=1
                    AND POLI_NO = :1
                    AND CTRDT = :2
                    AND CNTR_COUNT = :3
            """
            bind = (
                poli_no,
                ctrdt,
                cntr_count
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

    def update_tb_tm_qa_ta_sec_info(self, insert_set_dict):
        try:
            sql = """
                UPDATE
                    TB_TM_QA_TA_SEC_INFO
                SET
                    GRT_SEC_YN = :1,
                    SEC_DTC_YN = :2,
                    SEC_STTM = :3,
                    SEC_ENDTM = :4,
                    REC_ID = :5,
                    RFILE_NAME = :6,
                    REGP_CD = 'TM_QA_TA',
                    RGST_PGM_ID = 'TM_QA_TA',
                    RGST_DTM = SYSDATE,
                    LST_CHGP_CD = 'TM_QA_TA',
                    LST_CHG_PGM_ID = 'TM_QA_TA',
                    LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND POLI_NO = :7
                    AND CTRDT = :8
                    AND IP_CD = :9
                    AND CNTR_COUNT = :10
                    AND QA_SCRT_LCCD = :11
                    AND QA_SCRT_MCCD = :12
                    AND QA_SCRT_SCCD = :13
                    AND SEC_NO = :14
            """
            values_list = list()
            for insert_dict in insert_set_dict.values():
                poli_no = insert_dict['POLI_NO']
                ctrdt = insert_dict['CTRDT']
                ip_cd = insert_dict['IP_CD']
                cntr_count = insert_dict['CNTR_COUNT']
                qa_scrt_lccd = insert_dict['QA_SCRT_LCCD']
                qa_scrt_mccd = insert_dict['QA_SCRT_MCCD']
                qa_scrt_sccd = insert_dict['QA_SCRT_SCCD']
                sec_no = insert_dict['SEC_NO']
                grt_sec_yn = insert_dict['GRT_SEC_YN']
                sec_dtc_yn = insert_dict['SEC_DTC_YN']
                sec_sttm = insert_dict['SEC_STTM']
                sec_endtm = insert_dict['SEC_ENDTM']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                values_tuple = (
                    grt_sec_yn, sec_dtc_yn, sec_sttm, sec_endtm, rec_id, rfile_name, poli_no, ctrdt, ip_cd, cntr_count,
                    qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no,
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

    def update_http_status(self, **kwargs):
        try:
            sql = """
                   UPDATE
                       TB_TM_CNTR_INFO
                   SET
                       HTTP_TRANS_CD = :1,
                       LST_CHGP_CD = 'TM_QA_TA',
                       LST_CHG_PGM_ID = 'TM_QA_TA',
                       LST_CHG_DTM = SYSDATE
                   WHERE 1=1
                       AND POLI_NO = :2
                       AND CTRDT = :3
                       AND CNTR_COUNT = :4
               """
            bind = (
                kwargs.get('http_trans_cd'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
                kwargs.get('cntr_count')
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def insert_tb_tm_qa_ta_dtc_rst(self, upload_data_dict):
        try:
            sql = """
                INSERT INTO
                    TB_TM_QA_TA_DTC_RST
                    (
                        POLI_NO,
                        CTRDT,
                        QA_SCRT_LCCD,
                        QA_SCRT_MCCD,
                        QA_SCRT_SCCD,
                        SEC_NO,
                        SNTC_NO,
                        SNTC_SEQ,
                        IP_CD,
                        GRT_CD,
                        STT_SNTC_LIN_NO,
                        SNTC_DTC_YN,
                        SNTC_CONT,
                        SNTC_STTM,
                        SNTC_ENDTM,
                        CUST_ANYN,
                        CUST_NOR_ANYN,
                        CUST_AB_ANYN,
                        CUST_AGRM_SNTC_CONT,
                        DTC_KYWD_LIT,
                        NUDTC_KYWD_LIT,
                        KYWD_CTGRY_NM,
                        KYWD_DTC_RATE,
                        VRFC_KYWD_QTTY,
                        VRFC_KYWD_DTC_QTTY,
                        DTC_INFO_CRT_DTM,
                        CNTR_COUNT,
                        REC_ID,
                        RFILE_NAME,
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
                    :11, :12, :13, :14, :15,
                    :16, :17, :18, :19, :20,
                    :21, :22, :23, :24, :25,
                    :26, :27, :28, :29,
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE, 'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in upload_data_dict.values():
                poli_no = insert_dict['POLI_NO']
                ctrdt = insert_dict['CTRDT']
                qa_scrt_lccd = insert_dict['QA_SCRT_LCCD']
                qa_scrt_mccd = insert_dict['QA_SCRT_MCCD']
                qa_scrt_sccd = insert_dict['QA_SCRT_SCCD']
                sec_no = insert_dict['SEC_NO']
                sntc_no = insert_dict['SNTC_NO']
                sntc_seq = insert_dict['SNTC_SEQ']
                ip_cd = insert_dict['IP_CD']
                grt_cd = insert_dict['GRT_CD']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                sntc_dtc_yn = insert_dict['SNTC_DTC_YN']
                sntc_cont = insert_dict['SNTC_CONT']
                sntc_sttm = insert_dict['SNTC_STTM']
                sntc_endtm = insert_dict['SNTC_ENDTM']
                cust_anyn = insert_dict['CUST_ANYN']
                cust_nor_anyn = insert_dict['CUST_NOR_ANYN']
                cust_ab_anyn = insert_dict['CUST_AB_ANYN']
                cust_agrm_sntc_cont = insert_dict['CUST_AGRM_SNTC_CONT']
                dtc_kywd_lit = insert_dict['DTC_KYWD_LIT']
                nudtc_kywd_lit = insert_dict['NUDTC_KYWD_LIT']
                kywd_ctgry_nm = insert_dict['KYWD_CTGRY_NM']
                kywd_dtc_rate = insert_dict['KYWD_DTC_RATE']
                vrfc_kywd_qtty = insert_dict['VRFC_KYWD_QTTY']
                vrfc_kywd_dtc_qtty = insert_dict['VRFC_KYWD_DTC_QTTY']
                dtc_info_crt_dtm = insert_dict['DTC_INFO_CRT_DTM']
                cntr_count = insert_dict['CNTR_COUNT']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                values_tuple = (poli_no, ctrdt, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, sntc_no, sntc_seq,
                                ip_cd, grt_cd, stt_sntc_lin_no, sntc_dtc_yn, sntc_cont, sntc_sttm, sntc_endtm,
                                cust_anyn, cust_nor_anyn, cust_ab_anyn, cust_agrm_sntc_cont, dtc_kywd_lit,
                                nudtc_kywd_lit, kywd_ctgry_nm, kywd_dtc_rate, vrfc_kywd_qtty, vrfc_kywd_dtc_qtty,
                                dtc_info_crt_dtm, cntr_count, rec_id, rfile_name)
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

    def insert_tb_tm_qa_ta_adt_dtc_rst(self, upload_data_list):
        try:
            sql = """
                INSERT INTO
                    TB_TM_QA_TA_ADT_DTC_RST(
                        POLI_NO,
                        CTRDT,
                        SNTC_NO,
                        SNTC_SEQ,
                        SNTC_DCD,
                        STT_SNTC_LIN_NO,
                        DTC_CD,
                        DTC_CD_NM,
                        REC_ID,
                        RFILE_NAME,
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
                    :11, :12, :13, 'TM_QA_TA', 'TM_QA_TA', 
                    SYSDATE, 'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in upload_data_list:
                poli_no = insert_dict['POLI_NO']
                ctrdt = insert_dict['CTRDT']
                sntc_no = insert_dict['SNTC_NO']
                sntc_seq = insert_dict['SNTC_SEQ']
                sntc_dcd = insert_dict['SNTC_DCD']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                dtc_cd = insert_dict['DTC_CD']
                dtc_cd_nm = insert_dict['DTC_CD_NM']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                sntc_cont = insert_dict['SNTC_CONT']
                sntc_sttm = insert_dict['SNTC_STTM']
                sntc_endtm = insert_dict['SNTC_ENDTM']
                values_tuple = (
                    poli_no, ctrdt, sntc_no, sntc_seq, sntc_dcd, stt_sntc_lin_no, dtc_cd, dtc_cd_nm, rec_id, rfile_name,
                    sntc_cont, sntc_sttm, sntc_endtm
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

    def insert_tb_tm_qa_int_sec_save(self, upload_data_dict):
        try:
            sql = """
                INSERT INTO
                    TB_TM_QA_INT_SEC_SAVE
                    (
                        POLI_NO,
                        CTRDT,
                        SNTC_DCD,
                        DTC_CD,
                        SCTN_SAVE_SEQ,
                        REC_ID,
                        RFILE_NAME,
                        INT_SCTN_STTM,
                        INT_SCTN_ENDTM,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                VALUES (
                    :1, :2, :3, :4, :5,
                    :6, :7, :8, :9, 'TM_QA_TA',
                    'TM_QA_TA', SYSDATE, 'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in upload_data_dict.values():
                poli_no = insert_dict['POLI_NO']
                ctrdt = insert_dict['CTRDT']
                sntc_dcd = insert_dict['SNTC_DCD']
                dtc_cd = insert_dict['DTC_CD']
                sctn_save_seq = insert_dict['SCTN_SAVE_SEQ']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                int_sctn_sttm = insert_dict['INT_SCTN_STTM']
                int_sctn_endtm = insert_dict['INT_SCTN_ENDTM']
                values_tuple = (poli_no, ctrdt, sntc_dcd, dtc_cd,
                                sctn_save_seq, rec_id, rfile_name, int_sctn_sttm, int_sctn_endtm)
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

    def delete_tb_tm_qa_int_sec_save(self, poli_no, ctrdt):
        try:
            query = """
                DELETE FROM
                    TB_TM_QA_INT_SEC_SAVE
                WHERE 1=1
                    AND POLI_NO = :1
                    AND CTRDT = :2
            """
            bind = (
                poli_no,
                ctrdt,
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

    def delete_tb_tm_qa_ta_dtc_rst(self, poli_no, ctrdt, cntr_count):
        try:
            query = """
                DELETE FROM
                    TB_TM_QA_TA_DTC_RST
                WHERE 1=1
                    AND POLI_NO = :1
                    AND CTRDT = :2
                    AND CNTR_COUNT = :3
            """
            bind = (
                poli_no,
                ctrdt,
                cntr_count
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

    def delete_tb_tm_qa_ta_adt_dtc_rst(self, poli_no, ctrdt):
        try:
            query = """
                DELETE FROM
                    TB_TM_QA_TA_ADT_DTC_RST
                WHERE 1=1
                    AND POLI_NO = :1
                    AND CTRDT = :2
            """
            bind = (
                poli_no,
                ctrdt,
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
def get_cntr_info(logger, oracle, poli_no, status, ip_dcd, ctrdt, cntr_count):
    """
    http requests get cntr info
    :param      logger:         Logger
    :param      oracle:         Oracle
    :param      poli_no:        POLI_NO(증서번호)
    :param      status:
    :param      ip_dcd:         IP_DCD(보험상품구분코드)
    :param      ctrdt:          CTRDT(청약일자)
    :param      cntr_count:     CNTR_COUNT(심하회차)
    :return:
    """
    bjgb = ''
    if ip_dcd == '01' or ip_dcd == '04':
        bjgb = 'L'
    elif ip_dcd == '02':
        bjgb = 'O'
    elif ip_dcd == '03':
        bjgb = 'A'
    url = QA_TA_CONFIG['http_url']
    params = {
        'bjgb': bjgb,
        'polno': poli_no,
        'sttstatus': status,
        'cntr_count': cntr_count
    }
    try:
        res = requests.get(url, params=params, timeout=QA_TA_CONFIG['requests_timeout'])
        oracle.update_http_status(
            http_trans_cd='01',
            poli_no=poli_no,
            ctrdt=ctrdt,
            cntr_count=cntr_count
        )
        logger.info('\thttp status send -> {0}'.format(res.url))
    except Exception:
        logger.error('\tFail http status send -> {0}'.format(poli_no))
        oracle.update_http_status(
            http_trans_cd='02',
            poli_no=poli_no,
            ctrdt=ctrdt,
            cntr_count=cntr_count
        )


def error_process(logger, oracle, poli_no, ctrdt, status, cntr_count, ip_dcd):
    """
    Error process
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(계약 일자)
    :param      status:         Status
    :param      cntr_count:     CNTR_COUNT(심사 회차)
    :param      ip_dcd:         IP_DCD(보험상품구분코드)
    """
    logger.info("Error process POLI_NO = {0}, CTRDT = {1}".format(poli_no, ctrdt))
    oracle.conn.commit()
    result = oracle.update_qa_stta_prgst_cd(
        qa_stta_prgst_cd=status,
        poli_no=poli_no,
        ctrdt=ctrdt,
        cntr_count=cntr_count
    )
    if result:
        oracle.update_ta_cmdtm(poli_no, ctrdt, cntr_count)
        get_cntr_info(logger, oracle, poli_no, status, ip_dcd, ctrdt, cntr_count)


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


def move_output(logger, ctrdt):
    """
    Move output
    :param      logger:     Logger
    :param      ctrdt:      CTRDT(계약 일자)
    """
    logger.info("14. Move output")
    output_dir_path = "{0}/{1}/{2}/{3}".format(QA_TA_CONFIG['ta_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8])
    output_abs_dir_path = "{0}/{1}".format(output_dir_path, OUTPUT_DIR_NAME)
    if os.path.exists(output_abs_dir_path):
        del_garbage(logger, output_abs_dir_path)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    shutil.move(TA_TEMP_DIR_PATH, output_dir_path)
    logger.info('\tencrypt {0}'.format(output_abs_dir_path))
    encrypt(output_abs_dir_path)


def db_upload_tb_tm_qa_int_sec_save(logger, oracle, poli_no, ctrdt):
    """
    DB upload TB_TM_QA_INT_SEC_SAVE
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(계약 일자)
    """
    logger.info("14. DB upload TB_TM_QA_INT_SEC_SAVE")
    # IP_SEC_SAVE_DICT
    ip_cnt = 0
    insert_dict = dict()
    for tm_info in RFILE_INFO_DICT.values():
        ip_cd = tm_info.get('IP_CD')
        qa_scrt_lccd = tm_info.get('QA_SCRT_LCCD')
        qa_scrt_mccd = tm_info.get('QA_SCRT_MCCD')
        qa_scrt_sccd = tm_info.get('QA_SCRT_SCCD')
        sec_no = tm_info.get('SEC_NO')
        rec_id = tm_info.get('REC_ID')
        rfile_name = tm_info.get('RFILE_NAME')
        sntc_dcd = tm_info.get('SNTC_DCD')
        dtc_cd = tm_info.get('DTC_CD')
        save_list_result = oracle.select_data_to_scrt_sec_info(ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
        if not save_list_result:
            continue
        for result in save_list_result:
            ip_sec_key = '{0}_{1}_{2}_{3}'.format(result[0], result[1], result[2], result[3])
            if ip_sec_key in IP_SEC_SAVE_DICT:
                int_sctn_sttm = IP_SEC_SAVE_DICT[ip_sec_key][0]
                int_sctn_endtm = IP_SEC_SAVE_DICT[ip_sec_key][1]
            else:
                continue
            tb_tm_qa_int_sec_save_dict = dict()
            tb_tm_qa_int_sec_save_dict['POLI_NO'] = poli_no
            tb_tm_qa_int_sec_save_dict['CTRDT'] = ctrdt
            tb_tm_qa_int_sec_save_dict['SNTC_DCD'] = sntc_dcd
            tb_tm_qa_int_sec_save_dict['DTC_CD'] = dtc_cd
            tb_tm_qa_int_sec_save_dict['SCTN_SAVE_SEQ'] = str(ip_cnt)
            tb_tm_qa_int_sec_save_dict['REC_ID'] = rec_id
            tb_tm_qa_int_sec_save_dict['RFILE_NAME'] = rfile_name
            tb_tm_qa_int_sec_save_dict['INT_SCTN_STTM'] = int_sctn_sttm
            tb_tm_qa_int_sec_save_dict['INT_SCTN_ENDTM'] = int_sctn_endtm
            key = '{0}_{1}_{2}_{3}_{4}'.format(poli_no, ctrdt, sntc_dcd, dtc_cd, str(ip_cnt))
            insert_dict[key] = tb_tm_qa_int_sec_save_dict
            ip_cnt += 1
    oracle.delete_tb_tm_qa_int_sec_save(poli_no, ctrdt)
    oracle.insert_tb_tm_qa_int_sec_save(insert_dict)


def db_upload_tb_tm_qa_ta_adt_dtc_rst(logger, oracle, poli_no, ctrdt):
    """
    DB upload TB_TM_QA_TA_ADT_DTC_RST
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(계약 일자)
    """
    logger.info("13. DB upload TB_TM_QA_TA_ADT_DTC_RST")
    oracle.delete_tb_tm_qa_ta_adt_dtc_rst(poli_no, ctrdt)
    oracle.insert_tb_tm_qa_ta_adt_dtc_rst(TB_TM_QA_TA_ADT_DTC_RST_LIST)


def db_upload_tb_tm_qa_ta_dtc_rst(logger, oracle, poli_no, ctrdt, cntr_count):
    """
    DB upload to TB_QA_STT_TM_TA_DTC_RST
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      poli_no:                    POLI_NO(증서 번호)
    :param      ctrdt:                      CTRDT(청약일자)
    :param      cntr_count:                 CNTR_COUNT(심사회차)
    """
    logger.info("12. DB upload to TB_QA_STT_TM_TA_DTC_RST")
    oracle.delete_tb_tm_qa_ta_dtc_rst(poli_no, ctrdt, cntr_count)
    oracle.insert_tb_tm_qa_ta_dtc_rst(TB_TM_QA_TA_DTC_RST_DICT)


def make_cust_agrm_sntc_cont_dict(dir_path, file_name, rfile_name):
    """
    Make customer agreement sentence cont dictionary
    :param      dir_path:       Directory path
    :param      file_name:      File name
    :param      rfile_name:     RFILE_NAME(녹취파일명)
    :return:                    Customer agreement sentence cont dictionary
    """
    cust_agrm_sntc_cont_dict = dict()
    final_output_file = open(os.path.join(dir_path, file_name), 'r')
    for line in final_output_file:
        line = line.strip()
        line_list = line.split("\t")
        rec_file_name = line_list[1].replace("_trx", "")
        line_number = line_list[2]
        sent = line_list[6]
        key = "{0}_{1}".format(rec_file_name, line_number)
        if sent.startswith("[C]") and rec_file_name == rfile_name:
            cust_agrm_sntc_cont_dict[key] = sent.replace("[C]", "").strip()
    final_output_file.close()
    return cust_agrm_sntc_cont_dict


def make_cust_agrm_sntc_cont(cust_anyn, dir_path, file_name, stt_sntc_lin_no, rfile_name):
    """
    Make customer agreement sentence cont
    :param      cust_anyn:              CUST_ANYN(고객응답여부)
    :param      dir_path:               Directory path
    :param      file_name:              File name
    :param      stt_sntc_lin_no:        STT_SNTC_LIN_NO(STT 문장 라인 번호)
    :param      rfile_name:             RFILE_NAME(녹취파일명)
    :return:                            CUST_AGRM_SNTC_CONT(고객동의문장내용)
    """
    cust_agrm_sntc_cont_dict = make_cust_agrm_sntc_cont_dict(dir_path, file_name, rfile_name)
    cust_agrm_sntc_cont = ""
    if cust_anyn == 'Y':
        for cnt in range(1, QA_TA_CONFIG['kywd_detect_range'] + 1):
            key = "{0}_{1}".format(rfile_name, str(cnt + int(stt_sntc_lin_no)))
            if key in cust_agrm_sntc_cont_dict:
                cust_agrm_sntc_cont += cust_agrm_sntc_cont_dict[key] + " "
        cust_agrm_sntc_cont = cust_agrm_sntc_cont.strip()
    return cust_agrm_sntc_cont


def check_keyword_from_results_of_morpheme_analysis(keyword, nlp_result):
    """
    Check keyword from results of morpheme analysis
    :param          keyword:            Target keyword
    :param          nlp_result:         Results of morpheme analysis
    :return:                            True or False
    """
    keyword_list = keyword.split(",")
    nlp_result_list = nlp_result.split()
    for item in keyword_list:
        if '|' in item:
            item_list = item.split('|')
            difference_of_sets = set(item_list).difference(nlp_result_list)
            if len(difference_of_sets) < 1:
                return True
        else:
            if item in nlp_result_list:
                return True
    return False


def set_data_for_tb_tm_qa_ta_dtc_rst(logger, oracle, dir_path, file_name, rec_info_dict, poli_no, ctrdt, cntr_count):
    """
    Set data for TB_TM_QA_TA_DTC_RST
    :param      logger:             Logger
    :param      oracle:             Oracle DB
    :param      dir_path:           Directory path
    :param      file_name:          File name
    :param      rec_info_dict:      REC information dictionary
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(청약 일자)
    :param      cntr_count:         CNTR_COUNT(심사 회차)
    """
    global RFILE_INFO_DICT
    global SEC_TIME_CHECK_DICT
    global DETECT_CATEGORY_DICT
    global TB_TM_QA_TA_DTC_RST_DICT
    global TB_TM_QA_TA_ADT_DTC_RST_LIST
    global OVERLAP_CHECK_DICT
    global OVERLAP_CHECK_DICT_VER_TWO
    # 관심구간 리스트
    int_sec_list = list()
    for item in INT_SEC_RESULT:
        int_sec_list.append(item[0])
    # 금칙어 리스트
    bwd_list = list()
    for item in BWD_RESULT:
        bwd_list.append(item[0])
    # 불완전 판매 리스트
    incomsale_list = list()
    for item in INCOMSALE_RESULT:
        incomsale_list.append(item[0])
    # 고객 답변 여부 리스트
    cust_anyn_list = list()
    cust_anyn_result = oracle.select_cust_anyn()
    for item in cust_anyn_result:
        cust_anyn_list.append(item[0])
    # 고객 정상 응답 키워드 사전
    cu_nor_anyn_dict = dict()
    cu_nor_anyn_result = oracle.select_cu_nor_anyn_use_yn()
    for item in cu_nor_anyn_result:
        cu_nor_anyn_dict[item[0]] = item[1]
    # 고객 이상 응답 키워드 사전
    cu_ab_anyn_dict = dict()
    cu_ab_anyn_result = oracle.select_cu_ab_anyn_use_yn()
    for item in cu_ab_anyn_result:
        cu_ab_anyn_dict[item[0]] = item[1]
    # 상담사 정상 추가 담화 키워드 사전
    ruser_nor_add_que_dict = dict()
    ruser_nor_add_que_result = oracle.select_ruser_nor_add_que_use_yn()
    for item in ruser_nor_add_que_result:
        ruser_nor_add_que_dict[item[0]] = item[1]
    # 상담사 이상 추가 담화 키워드 사전
    ruser_ab_add_que_kywd_dict = dict()
    ruser_ab_add_que_kywd_result = oracle.select_ruser_ab_add_que_use_yn()
    for item in ruser_ab_add_que_kywd_result:
        ruser_ab_add_que_kywd_dict[item[0]] = item[1]
    # output 결과 파일 정리
    temp_tb_tm_qa_ta_dtc_rst_dict = collections.OrderedDict()
    sntc_seq_cnt = 0
    final_output_file = open(os.path.join(dir_path, file_name), 'r')
    for line in final_output_file:
        line = line.strip()
        line_list = line.split('\t')
        if line_list[4] == 'none' or line_list[4] == 'new_none':
            continue
        category_list = line_list[4].split("_")
        if len(category_list) < 3:
            ip_cd = category_list[0]
            qa_scrt_lccd = category_list[0]
            qa_scrt_mccd = category_list[0]
            qa_scrt_sccd = category_list[0]
            sec_no = ""
            sntc_no = category_list[1]
        else:
            ip_cd = category_list[0]
            qa_scrt_list = category_list[1].split('|')
            qa_scrt_lccd = qa_scrt_list[0]
            qa_scrt_mccd = qa_scrt_list[1]
            qa_scrt_sccd = qa_scrt_list[2]
            sec_no = qa_scrt_list[3]
            sntc_no = category_list[2]
        overlap_check_key = "{0}_{1}|{2}|{3}|{4}_{5}".format(
            ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, sntc_no)
        if overlap_check_key not in OVERLAP_CHECK_DICT:
            sntc_seq = str(sntc_seq_cnt)
        else:
            sntc_seq_cnt += 1
            sntc_seq = str(sntc_seq_cnt)
        OVERLAP_CHECK_DICT[overlap_check_key] = 1
        rfile_name = os.path.basename(line_list[1]).replace("_trx", "")
        rec_id = rec_info_dict[rfile_name]['REC_ID']
        chn_tp = rec_info_dict[rfile_name]['CHN_TP']
        sntc_cont = line_list[6].replace("[C]", "").replace("[A]", "").strip()
        stt_sntc_lin_no = line_list[2].strip()
        scrt_sntc_sttm = line_list[7]
        scrt_sntc_endtm = line_list[8]
        temp_sntc_sttm = scrt_sntc_sttm.replace(":", "").split('.')[0]
        temp_sntc_endtm = scrt_sntc_endtm.replace(":", "").split('.')[0]
        modified_sntc_sttm = temp_sntc_sttm if len(temp_sntc_sttm) == 6 else "0" + temp_sntc_sttm
        modified_sntc_endtm = temp_sntc_endtm if len(temp_sntc_endtm) == 6 else "0" + temp_sntc_endtm
        # 고객응답여부 판단
        cust_anyn = 'N'
        if sntc_no in cust_anyn_list and chn_tp == 'S':
            for cnt in range(1, QA_TA_CONFIG['kywd_detect_range'] + 1):
                line_num = int(stt_sntc_lin_no) + cnt
                nlp_key = '{0}|{1}|{2}'.format(line_list[1], line_num, ctrdt)
                if not nlp_key in NLP_INFO_DICT:
                    continue
                sent = NLP_INFO_DICT[nlp_key][0].strip()
                if sent.startswith('[C]'):
                    cust_anyn = 'Y'
        # 정상 및 비정상 응답 여부 판단
        cust_nor_anyn = 'N'
        cust_ab_anyn = 'N'
        if cust_anyn == 'Y':
            for cnt in range(1, QA_TA_CONFIG['kywd_detect_range'] + 1):
                line_num = int(stt_sntc_lin_no) + cnt
                nlp_key = '{0}|{1}|{2}'.format(line_list[1], line_num, ctrdt)
                if not nlp_key in NLP_INFO_DICT:
                    continue
                sent = NLP_INFO_DICT[nlp_key][0].strip()
                nlp_list = NLP_INFO_DICT[nlp_key][1]
                if sent.startswith('[C]'):
                    if sntc_no in cu_nor_anyn_dict:
                        cu_nor_anyn_kywd_lit = cu_nor_anyn_dict[sntc_no]
                        cu_nor_anyn_key_result = check_keyword_from_results_of_morpheme_analysis(
                            cu_nor_anyn_kywd_lit, nlp_list)
                        if cu_nor_anyn_key_result:
                            cust_nor_anyn = 'Y'
                    if sntc_no in cu_ab_anyn_dict:
                        cu_ab_anyn_kywd_lit = cu_ab_anyn_dict[sntc_no]
                        cu_ab_anyn_key_result = check_keyword_from_results_of_morpheme_analysis(
                            cu_ab_anyn_kywd_lit, nlp_list)
                        if cu_ab_anyn_key_result:
                            cust_ab_anyn = 'Y'
                if sent.startswith('[A]'):
                    if sntc_no in ruser_nor_add_que_dict:
                        ruser_nor_add_que_kywd_lit = ruser_nor_add_que_dict[sntc_no]
                        ruser_nor_add_que_key_result = check_keyword_from_results_of_morpheme_analysis(
                            ruser_nor_add_que_kywd_lit, nlp_list)
                        if ruser_nor_add_que_key_result:
                            cust_nor_anyn = 'Y'
                    if sntc_no in ruser_ab_add_que_kywd_dict:
                        ruser_ab_add_que_kywd_lit = ruser_ab_add_que_kywd_dict[sntc_no]
                        ruser_ab_add_que_key_result = check_keyword_from_results_of_morpheme_analysis(
                            ruser_ab_add_que_kywd_lit, nlp_list)
                        if ruser_ab_add_que_key_result:
                            cust_ab_anyn = 'Y'
        cust_agrm_sntc_cont = make_cust_agrm_sntc_cont(cust_anyn, dir_path, file_name, stt_sntc_lin_no, rfile_name)
        keyword_result = oracle.select_kywd_lit(sntc_no)
        if not keyword_result:
            dtc_kywd_lit = "None"
            nudtc_kywd_lit = "None"
            kywd_dtc_rate = "0.00"
        elif not keyword_result[0]:
            dtc_kywd_lit = "None"
            nudtc_kywd_lit = "None"
            kywd_dtc_rate = "0.00"
        else:
            keyword = keyword_result[0].encode('euc_kr').strip()
            if keyword.endswith(";"):
                keyword = keyword[:-1]
            elif keyword.startswith(";"):
                keyword = keyword[1:]
            keyword_list = keyword.split(";")
            nlp_sent = line_list[7].replace("[ C ]", "").replace("[ A ]", "").replace("[ M ]", "").strip()
            nlp_sent_list = nlp_sent.split()
            dtc_kywd_lit = ";".join(list(set(keyword_list) & set(nlp_sent_list)))
            cnt_dtc_kywd_lit = len(list(set(keyword_list) & set(nlp_sent_list)))
            nudtc_kywd_lit = ";".join(list(set(keyword_list) - set(nlp_sent_list)))
            dtc_rate = (float(cnt_dtc_kywd_lit) / float(len(keyword_list))) * 100
            kywd_dtc_rate = '%.2f' % dtc_rate
        grt_cd_key = "{0}_{1}_{2}_{3}_{4}_{5}_{6}".format(
            poli_no, ctrdt, ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
        grt_cd = GRT_CD_DICT[grt_cd_key] if grt_cd_key in GRT_CD_DICT else ""
        tb_tm_qa_ta_adt_dtc_rst_dict = dict()
        sntc_dtc_lst_result = oracle.select_dtc_cd_and_nm(sntc_no)
        if not sntc_dtc_lst_result:
            if not sntc_no.startswith('F'):
                logger.error(
                    "Can't select SNTC_DCD and DTC_CD and DTC_CD_NM, SNTC_NO = {0}".format(sntc_no))
            sntc_dcd = ''
            dtc_cd = ''
            dtc_cd_nm = ''
        else:
            sntc_dcd = sntc_dtc_lst_result[0]
            dtc_cd = sntc_dtc_lst_result[1]
            dtc_cd_nm = sntc_dtc_lst_result[2]
        if sntc_no in int_sec_list or sntc_no in bwd_list or sntc_no in incomsale_list:
            sntc_seq = 0
            while True:
                overlap_check_key_ver_two = '{0}_{1}_{2}_{3}'.format(poli_no, ctrdt, sntc_no, sntc_seq)
                if overlap_check_key_ver_two not in OVERLAP_CHECK_DICT_VER_TWO:
                    OVERLAP_CHECK_DICT_VER_TWO[overlap_check_key_ver_two] = 1
                    break
                sntc_seq += 1
            tb_tm_qa_ta_adt_dtc_rst_dict['POLI_NO'] = poli_no
            tb_tm_qa_ta_adt_dtc_rst_dict['CTRDT'] = ctrdt
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_NO'] = sntc_no
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_SEQ'] = sntc_seq
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_DCD'] = sntc_dcd
            tb_tm_qa_ta_adt_dtc_rst_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
            tb_tm_qa_ta_adt_dtc_rst_dict['DTC_CD'] = dtc_cd
            tb_tm_qa_ta_adt_dtc_rst_dict['DTC_CD_NM'] = dtc_cd_nm
            tb_tm_qa_ta_adt_dtc_rst_dict['REC_ID'] = rec_id
            tb_tm_qa_ta_adt_dtc_rst_dict['RFILE_NAME'] = rfile_name
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_CONT'] = sntc_cont
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_STTM'] = modified_sntc_sttm
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_ENDTM'] = modified_sntc_endtm
            TB_TM_QA_TA_ADT_DTC_RST_LIST.append(tb_tm_qa_ta_adt_dtc_rst_dict)
            continue
        tb_tm_qa_ta_dtc_rst_dict = dict()
        tb_tm_qa_ta_dtc_rst_dict['POLI_NO'] = poli_no
        tb_tm_qa_ta_dtc_rst_dict['CTRDT'] = ctrdt
        tb_tm_qa_ta_dtc_rst_dict['CNTR_COUNT'] = cntr_count
        tb_tm_qa_ta_dtc_rst_dict['QA_SCRT_LCCD'] = qa_scrt_lccd
        tb_tm_qa_ta_dtc_rst_dict['QA_SCRT_MCCD'] = qa_scrt_mccd
        tb_tm_qa_ta_dtc_rst_dict['QA_SCRT_SCCD'] = qa_scrt_sccd
        tb_tm_qa_ta_dtc_rst_dict['SEC_NO'] = sec_no
        tb_tm_qa_ta_dtc_rst_dict['SNTC_NO'] = sntc_no
        tb_tm_qa_ta_dtc_rst_dict['SNTC_SEQ'] = sntc_seq
        tb_tm_qa_ta_dtc_rst_dict['IP_CD'] = ip_cd
        tb_tm_qa_ta_dtc_rst_dict['GRT_CD'] = grt_cd
        tb_tm_qa_ta_dtc_rst_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
        tb_tm_qa_ta_dtc_rst_dict['SNTC_DTC_YN'] = 'Y'
        tb_tm_qa_ta_dtc_rst_dict['SNTC_CONT'] = sntc_cont
        tb_tm_qa_ta_dtc_rst_dict['SNTC_STTM'] = modified_sntc_sttm
        tb_tm_qa_ta_dtc_rst_dict['SNTC_ENDTM'] = modified_sntc_endtm
        tb_tm_qa_ta_dtc_rst_dict['CUST_ANYN'] = cust_anyn
        tb_tm_qa_ta_dtc_rst_dict['CUST_NOR_ANYN'] = cust_nor_anyn
        tb_tm_qa_ta_dtc_rst_dict['CUST_AB_ANYN'] = cust_ab_anyn
        tb_tm_qa_ta_dtc_rst_dict['CUST_AGRM_SNTC_CONT'] = cust_agrm_sntc_cont
        tb_tm_qa_ta_dtc_rst_dict['DTC_KYWD_LIT'] = dtc_kywd_lit
        tb_tm_qa_ta_dtc_rst_dict['NUDTC_KYWD_LIT'] = nudtc_kywd_lit
        tb_tm_qa_ta_dtc_rst_dict['KYWD_CTGRY_NM'] = ""
        tb_tm_qa_ta_dtc_rst_dict['KYWD_DTC_RATE'] = kywd_dtc_rate
        tb_tm_qa_ta_dtc_rst_dict['VRFC_KYWD_QTTY'] = ""
        tb_tm_qa_ta_dtc_rst_dict['VRFC_KYWD_DTC_QTTY'] = ""
        tb_tm_qa_ta_dtc_rst_dict['DTC_INFO_CRT_DTM'] = str(datetime.now())[:10]
        tb_tm_qa_ta_dtc_rst_dict['REC_ID'] = rec_id
        tb_tm_qa_ta_dtc_rst_dict['RFILE_NAME'] = rfile_name
        key = "{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}".format(
            poli_no, ctrdt, cntr_count, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, sntc_no, sntc_seq, ip_cd)
        if key not in temp_tb_tm_qa_ta_dtc_rst_dict:
            temp_tb_tm_qa_ta_dtc_rst_dict[key] = tb_tm_qa_ta_dtc_rst_dict
        category_key = "{0}|{1}|{2}|{3}|{4}".format(ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
        if category_key not in DETECT_CATEGORY_DICT:
            DETECT_CATEGORY_DICT[category_key] = [sntc_no]
        else:
            DETECT_CATEGORY_DICT[category_key].append(sntc_no)
        if category_key in SEC_TIME_CHECK_DICT:
            if SEC_TIME_CHECK_DICT[category_key][3] == rfile_name:
                SEC_TIME_CHECK_DICT[category_key][1] = modified_sntc_endtm
            else:
                try:
                    org_rfile_name = SEC_TIME_CHECK_DICT[category_key][3]
                    org_date = datetime.strptime(org_rfile_name[:14], '%Y%m%d%H%M%S')
                    date = datetime.strptime(rfile_name[:14], '%Y%m%d%H%M%S')
                    if org_date < date:
                        SEC_TIME_CHECK_DICT[category_key] = [modified_sntc_sttm, modified_sntc_endtm, rec_id, rfile_name]
                except Exception:
                    logger.error('Failed compared rfile_name')
        else:
            SEC_TIME_CHECK_DICT[category_key] = [modified_sntc_sttm, modified_sntc_endtm, rec_id, rfile_name]
        RFILE_INFO_DICT[category_key] = {
            'IP_CD': ip_cd,
            'QA_SCRT_LCCD': qa_scrt_lccd,
            'QA_SCRT_MCCD': qa_scrt_mccd,
            'QA_SCRT_SCCD': qa_scrt_sccd,
            'SEC_NO': sec_no,
            'REC_ID': rec_id,
            'RFILE_NAME': rfile_name,
            'SNTC_DCD': sntc_dcd,
            'DTC_CD': dtc_cd
        }
    final_output_file.close()
    dedup_dict = dict()
    for ori_key, info_dict in temp_tb_tm_qa_ta_dtc_rst_dict.items():
        ip_cd = info_dict['IP_CD']
        qa_scrt_lccd = info_dict['QA_SCRT_LCCD']
        qa_scrt_mccd = info_dict['QA_SCRT_MCCD']
        qa_scrt_sccd = info_dict['QA_SCRT_SCCD']
        sec_no = info_dict['SEC_NO']
        stt_sntc_lin_no = info_dict['STT_SNTC_LIN_NO']
        sntc_cont = info_dict['SNTC_CONT']
        cust_anyn = info_dict['CUST_ANYN']
        key = '{0}_{1}_{2}_{3}_{4}_{5}'.format(
            ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, stt_sntc_lin_no, sntc_cont)
        if key in dedup_dict and cust_anyn == 'Y':
            dedup_dict[key] = [ori_key, info_dict]
        elif key not in dedup_dict:
            dedup_dict[key] = [ori_key, info_dict]
    for items in dedup_dict.values():
        key = items[0]
        info_dict = items[1]
        TB_TM_QA_TA_DTC_RST_DICT[key] = info_dict


def db_upload_tb_tm_qa_ta_sec_info(logger, oracle, final_output_dir_path, rec_info_dict, poli_no, ctrdt, cntr_count):
    """
    DB upload to TB_TM_QA_TA_SEC_INFO
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      final_output_dir_path:      Final output directory path
    :param      rec_info_dict:              Rec information dictionary
    :param      poli_no:                    POLI_NO(증서 번호)
    :param      ctrdt:                      CTRDT(청약일자)
    :param      cntr_count:                 CNTR_COUNT(심사 회차)
    """
    global IP_SEC_SAVE_DICT
    logger.info("11. DB upload to TB_TM_QA_TA_SEC_INFO")
    w_ob = os.walk(final_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            set_data_for_tb_tm_qa_ta_dtc_rst(
                logger, oracle, dir_path, file_name, rec_info_dict, poli_no, ctrdt, cntr_count)
    insert_dict = dict()
    for key, value in TARGET_CATEGORY_DICT.items():
        key_list = key.split("|")
        ip_cd = key_list[0]
        qa_scrt_lccd = key_list[1]
        qa_scrt_mccd = key_list[2]
        qa_scrt_sccd = key_list[3]
        sec_no = key_list[4]
        grt_sec_yn = key_list[5]
        sec_dtc_yn = 'N'
        sec_sttm = ''
        sec_endtm = ''
        rec_id = ''
        rfile_name = ''
        detect_check_key = '|'.join(key_list[:5])
        if detect_check_key in DETECT_CATEGORY_DICT:
            detect_sntc_no_list = DETECT_CATEGORY_DICT[detect_check_key]
            target_sntc_no_list = value
            if set(target_sntc_no_list) == set(detect_sntc_no_list) & set(target_sntc_no_list):
                sec_dtc_yn = 'Y'
            else:
                sec_dtc_yn = 'P'
        if detect_check_key in SEC_TIME_CHECK_DICT:
            sec_sttm = SEC_TIME_CHECK_DICT[detect_check_key][0]
            sec_endtm = SEC_TIME_CHECK_DICT[detect_check_key][1]
            rec_id = SEC_TIME_CHECK_DICT[detect_check_key][2]
            rfile_name = SEC_TIME_CHECK_DICT[detect_check_key][3]
        tb_tm_qa_ta_sec_info_dict = dict()
        tb_tm_qa_ta_sec_info_dict['POLI_NO'] = poli_no
        tb_tm_qa_ta_sec_info_dict['CTRDT'] = ctrdt
        tb_tm_qa_ta_sec_info_dict['IP_CD'] = ip_cd
        tb_tm_qa_ta_sec_info_dict['CNTR_COUNT'] = cntr_count
        tb_tm_qa_ta_sec_info_dict['QA_SCRT_LCCD'] = qa_scrt_lccd
        tb_tm_qa_ta_sec_info_dict['QA_SCRT_MCCD'] = qa_scrt_mccd
        tb_tm_qa_ta_sec_info_dict['QA_SCRT_SCCD'] = qa_scrt_sccd
        tb_tm_qa_ta_sec_info_dict['SEC_NO'] = sec_no
        tb_tm_qa_ta_sec_info_dict['GRT_SEC_YN'] = grt_sec_yn
        tb_tm_qa_ta_sec_info_dict['SEC_DTC_YN'] = sec_dtc_yn
        tb_tm_qa_ta_sec_info_dict['SEC_STTM'] = sec_sttm
        tb_tm_qa_ta_sec_info_dict['SEC_ENDTM'] = sec_endtm
        tb_tm_qa_ta_sec_info_dict['REC_ID'] = rec_id
        tb_tm_qa_ta_sec_info_dict['RFILE_NAME'] = rfile_name
        insert_key = '{0}_{1}_{2}_{3}_{4}_{5}_{6}_{7}'.format(
            poli_no, ctrdt, ip_cd, cntr_count, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
        if insert_key not in insert_dict:
            insert_dict[insert_key] = tb_tm_qa_ta_sec_info_dict
        if sec_dtc_yn == 'Y' or sec_dtc_yn == 'P':
            ip_sec_key = "{0}_{1}_{2}_{3}".format(qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
            IP_SEC_SAVE_DICT[ip_sec_key] = [sec_sttm, sec_endtm]
    oracle.update_tb_tm_qa_ta_sec_info(insert_dict)


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
    logger.info("10. Execute masking")
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
    logger.info("9. Modify HMD output")
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


def detect_sect_hmd_output(logger, dedup_hmd_output_dir_path):
    """
    Modify HMD
    :param      logger:                         Logger
    :param      dedup_hmd_output_dir_path:      Dedup hmd output directory path
    :return:                                    Detect section hmd output directory path
    """
    logger.info("8. Detect section HMD output")
    detect_sect_output_dir_path = "{0}/detect_sect_output".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(detect_sect_output_dir_path):
        os.makedirs(detect_sect_output_dir_path)
    w_ob = os.walk(dedup_hmd_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            hmd_output_file = open(os.path.join(dir_path, file_name), 'r')
            detect_sect_hmd_output_file = open(os.path.join(detect_sect_output_dir_path, file_name), 'w')
            current_category = 'False'
            current_category_dict = dict()
            temp_dict = collections.OrderedDict()
            for line in hmd_output_file:
                line = line.strip()
                line_list = line.split('\t')
                line_num = line_list[2].strip()
                category = line_list[4].strip()
                category_list = category.split('_')
                # check the line num dictionary
                if line_num not in temp_dict:
                    temp_dict[line_num] = dict()
                    temp_dict[line_num]['mother'] = list()
                    temp_dict[line_num]['baby'] = list()
                # make the mother check
                if len(category_list) < 3:
                    mother_check = category
                else:
                    mother_check = category_list[2]
                # mother category check
                if mother_check in MOTHER_SNTC_NO_LIST:
                    temp_dict[line_num]['mother'].append({'category': category, 'line': line})
                    # if 'mother' not in temp_dict[line_num]:
                    #     temp_dict[line_num]['mother'] = {'category': category, 'line': line}
                    # else:
                    #     temp_dict[line_num]['baby'].append({'category': category, 'line': line})
                else:
                    temp_dict[line_num]['baby'].append({'category': category, 'line': line})
            # make the detect section output
            flag = False
            current_category_list = list()
            current_sec_no_list = list()
            for detect_line_num in temp_dict.keys():
                # check the mother sentence
                if len(temp_dict[detect_line_num]['mother']) > 0:
                    current_category_list = list()
                    current_sec_no_list = list()
                    for value in temp_dict[detect_line_num]['mother']:
                        category = value['category']
                        category_list = category.split('_')
                        current_category_list.append("_".join(category_list[:2]))
                        print >> detect_sect_hmd_output_file, value['line']
                    for current_category in current_category_list:
                        current_sec_no_list.append(current_category.split('|')[-1])
                        if int(current_category.split('|')[-1]) >= 17:
                            flag = True
                #if 'mother' in temp_dict[detect_line_num]:
                #    category = temp_dict[detect_line_num]['mother']['category']
                #    category_list = category.split('_')
                #    current_category = "_".join(category_list[:2])
                #    current_sec_no = current_category.split('|')[-1]
                #    if int(current_sec_no) >= 17:
                #        flag = True
                #    print >> detect_sect_hmd_output_file, temp_dict[detect_line_num]['mother']['line']
                # save the category
                current_category_dict[detect_line_num] = current_category_list
                last_detect_line_num = str(int(detect_line_num)-1) if int(detect_line_num) > 0 else '0'
                for category_dict in temp_dict[detect_line_num]['baby']:
                    category = category_dict['category']
                    category_list = category.split('_')
                    sntc_no = category_list[-1]
                    # print baby category
                    for current_category in current_category_list:
                        if category.startswith(current_category):
                            print >> detect_sect_hmd_output_file, category_dict['line']
                            continue
                    # print last baby category
                    for last_category in current_category_dict[last_detect_line_num]:
                        if category.startswith(last_category):
                            print >> detect_sect_hmd_output_file, category_dict['line']
                            continue
                    # print 부가 탐색 and 담보 category
                    if sntc_no.startswith('I') or sntc_no.startswith('P') or sntc_no.startswith('Y'):
                        print >> detect_sect_hmd_output_file, category_dict['line']
                    elif sntc_no.startswith('F') and flag:
                        print >> detect_sect_hmd_output_file, category_dict['line']
                    # print none category
                    elif len(category_list) < 2:
                        print >> detect_sect_hmd_output_file, category_dict['line']
                    # print new_none category
                    else:
                        line_list = category_dict['line'].split('\t')
                        line_list[4] = 'new_none'
                        new_line = '\t'.join(line_list)
                        print >> detect_sect_hmd_output_file, new_line
            hmd_output_file.close()
            detect_sect_hmd_output_file.close()
    return detect_sect_output_dir_path


def dedup_hmd_output(logger, hmd_output_dir_path):
    """
    Deduplication HMD output
    :param      logger:                     Logger
    :param      hmd_output_dir_path:        HMD output directory path
    :return:                                Deduplication HMD output directory path
    """
    logger.info("7. Deduplication HMD output")
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
    logger.info("6. Sorted HMD output")
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


def execute_hmd(logger, matrix_file_path):
    """
    Execute HMD
    :param      logger:                             Logger
    :param      matrix_file_path:                   Matrix file path
    :return:                                        HMD output directory path
    """
    global DELETE_FILE_LIST
    logger.info("5. Execute HMD")
    os.chdir(QA_TA_CONFIG['hmd_script_path'])
    hmd_file_list = glob.glob("{0}/*".format(NLP_DIR_PATH))
    hmd_output_dir_path = "{0}/HMD_result".format(TA_TEMP_DIR_PATH)
    if not os.path.exists(hmd_output_dir_path):
        os.makedirs(hmd_output_dir_path)
    start = 0
    end = 0
    cmd_list = list()
    thread = len(hmd_file_list) if len(hmd_file_list) < int(QA_TA_CONFIG['hmd_thread']) else int(
        QA_TA_CONFIG['hmd_thread'])
    # Make list file
    for cnt in range(thread):
        end += len(hmd_file_list) / thread
        if (len(hmd_file_list) % thread) > cnt:
            end += 1
        list_file_path = "{0}/{1}_{2}.list".format(QA_TA_CONFIG['hmd_script_path'], OUTPUT_DIR_NAME, cnt)
        DELETE_FILE_LIST.append(list_file_path)
        list_file = open(list_file_path, 'w')
        for idx in range(start, end):
            print >> list_file, hmd_file_list[idx]
        list_file.close()
        start = end
        cmd = "python {0}/hmd.py {1} {2} {3} {4}".format(
            QA_TA_CONFIG['hmd_script_path'], OUTPUT_DIR_NAME, list_file_path, matrix_file_path, hmd_output_dir_path)
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


def make_matrix_file(logger, oracle, poli_no, ctrdt, ip_cd):
    """
    Make matrix file
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      poli_no:        POLI NO(증서 번호)
    :param      ctrdt:          CTRDT(청약 일자)
    :param      ip_cd:          IP_CD(보종 코드)
    :return:                    Matrix file path
    """
    global GRT_CD_DICT
    global MOTHER_SNTC_NO_LIST
    global TARGET_CATEGORY_DICT
    global INT_SEC_RESULT
    global BWD_RESULT
    global INCOMSALE_RESULT
    logger.info("4. Make matrix file")
    # Select SNTC_NO and DTC_CONT (문장 번호, 탐지 사전 내용)
    scrt_sntc_dtc_info_result_list = list()
    # 관심구간 matrix 조회
    INT_SEC_RESULT = oracle.select_sntc_no('03')
    for int_sec_item in INT_SEC_RESULT:
        sntc_no = int_sec_item[0]
        int_sec_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no)
        if not int_sec_scrt_sntc_dtc_info_result:
            continue
        for int_sec_dtc_cont_result in int_sec_scrt_sntc_dtc_info_result:
            dtc_cont = int_sec_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(['INTSEN', sntc_no, dtc_cont])
    # 금칙어 matrix 조회
    BWD_RESULT = oracle.select_sntc_no('04')
    for bwd_item in BWD_RESULT:
        sntc_no = bwd_item[0]
        bwd_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no)
        if not bwd_scrt_sntc_dtc_info_result:
            continue
        for bwd_dtc_cont_result in bwd_scrt_sntc_dtc_info_result:
            dtc_cont = bwd_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(['BWD', sntc_no, dtc_cont])
    # 불완전판매 matrix 조회
    INCOMSALE_RESULT = oracle.select_sntc_no('05')
    for incomsale_item in INCOMSALE_RESULT:
        sntc_no = incomsale_item[0]
        incomsale_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no=sntc_no)
        if not incomsale_scrt_sntc_dtc_info_result:
            continue
        for incomsale_dtc_cont_result in incomsale_scrt_sntc_dtc_info_result:
            dtc_cont = incomsale_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(['INCOMSALE', sntc_no, dtc_cont])
    # 상품 문항 matrix
    tb_tm_qa_ta_sec_info_result = oracle.select_data_to_tm_qa_ta_sec_info(
        poli_no=poli_no,
        ctrdt=ctrdt,
        ip_cd=ip_cd,
        grt_sec_yn='N'
    )
    if not tb_tm_qa_ta_sec_info_result:
        tb_tm_qa_ta_sec_info_result = list()
    for tb_tm_qa_ta_sec_info in tb_tm_qa_ta_sec_info_result:
        qa_scrt_lccd = tb_tm_qa_ta_sec_info[0]
        qa_scrt_mccd = tb_tm_qa_ta_sec_info[1]
        qa_scrt_sccd = tb_tm_qa_ta_sec_info[2]
        sec_no = tb_tm_qa_ta_sec_info[3]
        scrt_sec_sntc_info_result = oracle.select_data_to_scrt_sec_sntc_info(
            ip_cd=ip_cd,
            qa_scrt_lccd=qa_scrt_lccd,
            qa_scrt_mccd=qa_scrt_mccd,
            qa_scrt_sccd=qa_scrt_sccd,
            sec_no=sec_no
        )
        if not scrt_sec_sntc_info_result:
            continue
        for scrt_sec_sntc_info_item in scrt_sec_sntc_info_result:
            sntc_no = scrt_sec_sntc_info_item[0]
            strt_sntc_yn = scrt_sec_sntc_info_item[1]
            category_key = "{0}|{1}|{2}|{3}|{4}|N".format(
                ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
            if category_key not in TARGET_CATEGORY_DICT:
                TARGET_CATEGORY_DICT[category_key] = [sntc_no]
            else:
                TARGET_CATEGORY_DICT[category_key].append(sntc_no)
            if strt_sntc_yn == 'Y':
                MOTHER_SNTC_NO_LIST.append(sntc_no)
            ip_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no)
            if not ip_scrt_sntc_dtc_info_result:
                continue
            for ip_dtc_cont_result in ip_scrt_sntc_dtc_info_result:
                dtc_cont = ip_dtc_cont_result[0]
                scrt_sntc_dtc_info_result_list.append(
                    [ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, sntc_no, dtc_cont]
                )
    # 담보 문항 matrix
    tb_tm_qa_ta_sec_info_result_grt = oracle.select_data_to_tm_qa_ta_sec_info(
        poli_no=poli_no,
        ctrdt=ctrdt,
        ip_cd=ip_cd,
        grt_sec_yn='Y'
    )
    if tb_tm_qa_ta_sec_info_result_grt:
        tb_tm_qa_ta_sec_info = tb_tm_qa_ta_sec_info_result_grt[0]
        qa_scrt_lccd = tb_tm_qa_ta_sec_info[0]
        qa_scrt_mccd = tb_tm_qa_ta_sec_info[1]
        qa_scrt_sccd = tb_tm_qa_ta_sec_info[2]
        sec_no = tb_tm_qa_ta_sec_info[3]
        grt_cd_list = oracle.select_grt_cd(poli_no, ctrdt, ip_cd)
        if grt_cd_list:
            for grt_cd_item in grt_cd_list:
                grt_cd = grt_cd_item[0]
                category_key_sntc_no = "{0}_{1}_{2}_{3}_{4}_{5}_{6}".format(
                    poli_no, ctrdt, ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
                if category_key_sntc_no not in GRT_CD_DICT:
                    GRT_CD_DICT[category_key_sntc_no] = grt_cd
                sntc_no_list = oracle.select_sntc_no_list(grt_cd)
                if not sntc_no_list:
                    continue
                for sntc_no_item in sntc_no_list:
                    sntc_no = sntc_no_item[0]
                    category_key = "{0}|{1}|{2}|{3}|{4}|Y".format(
                        ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no)
                    if category_key not in TARGET_CATEGORY_DICT:
                        TARGET_CATEGORY_DICT[category_key] = [sntc_no]
                    else:
                        TARGET_CATEGORY_DICT[category_key].append(sntc_no)
                    grt_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no)
                    if not grt_scrt_sntc_dtc_info_result:
                        continue
                    for grt_dtc_cont_result in grt_scrt_sntc_dtc_info_result:
                        dtc_cont = grt_dtc_cont_result[0]
                        scrt_sntc_dtc_info_result_list.append(
                            [ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, sntc_no, dtc_cont]
                        )
    # Make matrix file
    matrix_dir_path = '{0}/HMD_matrix'.format(TA_TEMP_DIR_PATH)
    if not os.path.exists(matrix_dir_path):
        os.makedirs(matrix_dir_path)
    matrix_file_path = '{0}/{1}_{2}.matrix'.format(matrix_dir_path, poli_no, ctrdt)
    output_list = list()
    for item in scrt_sntc_dtc_info_result_list:
        strs_list = list()
        if len(item) == 3:
            sntc_no = str(item[1]).strip()
            category = '{0}_{1}'.format(item[0], sntc_no)
            dtc_cont = str(item[2]).strip()
        else:
            ip_cd = str(item[0]).strip()
            qa_scrt_lccd = str(item[1]).strip()
            qa_scrt_mccd = str(item[2]).strip()
            qa_scrt_sccd = str(item[3]).strip()
            sec_no = str(item[4]).strip()
            sntc_no = str(item[5]).strip()
            category = '{0}_{1}|{2}|{3}|{4}_{5}'.format(
                ip_cd, qa_scrt_lccd, qa_scrt_mccd, qa_scrt_sccd, sec_no, sntc_no)
            dtc_cont = str(item[6]).strip()
        detect_keyword_list = split_input(dtc_cont)
        for idx in range(len(detect_keyword_list)):
            detect_keyword = detect_keyword_list[idx].split("|")
            strs_list.append(detect_keyword)
        ws = ''
        output = ''
        tmp_result = []
        output += '{0}\t'.format(category)
        output_list += vec_word_combine(tmp_result, output, strs_list, ws, 0)
    matrix_file = open(matrix_file_path, 'w')
    for item in output_list:
        print >> matrix_file, item
    matrix_file.close()
    return matrix_file_path


def copy_stt_and_nlp_output(logger, rec_info_dict, ctrdt):
    """
    Copy STT output
    :param      logger:                 Logger
    :param      rec_info_dict:          Recording information dictionary
    :param      ctrdt:                  CTRDT(청약일자)
    """
    global NLP_INFO_DICT
    logger.info("3. Copy STT output and NLP output")
    for rfile_name, info_dict in rec_info_dict.items():
        rec_id = info_dict['REC_ID']
        stt_server_id = info_dict['STT_SERVER_ID']
        call_start_time = info_dict['CALL_START_TIME']
        chn_tp = info_dict['CHN_TP']
        stt_output_dir_path = '{0}/{1}/STTA_output/{2}/{3}/{4}/{5}-{6}'.format(
            QA_TA_CONFIG['stt_output_path'], stt_server_id, call_start_time[:4], call_start_time[5:7],
            call_start_time[8:10], rec_id, rfile_name)
        if chn_tp == 'S':
            target_file = '{0}/modified_nlp_line_number/{1}_trx.hmd.txt'.format(stt_output_dir_path, rfile_name)
        else:
            target_file = '{0}/modified_nlp_line_number/{1}.hmd.txt'.format(stt_output_dir_path, rfile_name)
        detail_file = '{0}/detail/{1}_trx.detail'.format(stt_output_dir_path, rfile_name)
        target_file += '.enc'
        detail_file += '.enc'
        if not os.path.exists(target_file):
            logger.error('{0} is not exists'.format(target_file))
        if not os.path.exists(detail_file):
            logger.error('{0} is not exists'.format(detail_file))
        logger.info('\tcopy {0} -> {1}'.format(target_file, NLP_DIR_PATH))
        shutil.copy(target_file, NLP_DIR_PATH)
        logger.info('\tcopy {0} -> {1}'.format(detail_file, DETAIL_DIR_PATH))
        shutil.copy(detail_file, DETAIL_DIR_PATH)
    logger.info('\tdecrypt {0}'.format(NLP_DIR_PATH))
    decrypt(NLP_DIR_PATH)
    decrypt(DETAIL_DIR_PATH)
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
                key = '{0}|{1}|{2}'.format(file_nm, line_num, ctrdt)
                NLP_INFO_DICT[key] = [sent, nlp_sent]


def update_status_and_select_rec_file(logger, oracle, poli_no, ctrdt, cntr_count, ip_dcd):
    """
    Update status and select rec file
    :param      logger:         Logger
    :param      oracle:         Oracle db
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(청약 일자)
    :param      cntr_count:     CNTR_COUNT(심사 회차)
    :param      ip_dcd:         IP_DCD(보험상품구분코드)
    :return:                    STT output dictionary
    """
    logger.info("2. Update status and select REC file")
    # Update TA_PRGST_CD status
    result = oracle.update_qa_stta_prgst_cd(
        qa_stta_prgst_cd='11',
        poli_no=poli_no,
        ctrdt=ctrdt,
        cntr_count=cntr_count
    )
    if result:
        get_cntr_info(logger, oracle, poli_no, '11', ip_dcd, ctrdt, cntr_count)
    # 이미 실행 중인 경우 프로그램 종료.
    else:
        logger.error("POLI_NO = {0} CTRDT = {1} is already process in another server".format(poli_no, ctrdt))
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # Select REC file name and STT output information
    rec_info_dict = dict()
    pk_list = oracle.select_pk(poli_no, ctrdt)
    if not pk_list:
        raise Exception("No data RCDG_ID [POLI_NO : {0}, CTRDT : {1}]".format(poli_no, ctrdt))
    for pk in pk_list:
        rec_id = str(pk[0]).strip()
        rfile_name = str(pk[1]).strip()
        result = oracle.select_stt_server_id(rec_id, rfile_name)
        if not result:
            raise Exception("No data STT_SERVER_ID information to TB_TM_STT_RCDG_INFO, "
                            "RCDG_ID = {0} RCDG_FILE_NM = {1}".format(rec_id, rfile_name))
        info_dict = dict()
        info_dict['REC_ID'] = rec_id
        info_dict['STT_SERVER_ID'] = str(result[0]).strip()
        info_dict['CALL_START_TIME'] = str(result[1]).strip()
        info_dict['CHN_TP'] = str(result[2]).strip()
        rec_info_dict[rfile_name] = info_dict
    return rec_info_dict


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
                os.environ['NLS_LANG'] = ".KO16MSWIN949"
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


def setup_data(job):
    """
    Setup data and target directory
    :param      job:        Job
    :return:                Logger, POLI_NO(증서 번호), CTRDT(청약 일자), IP_CD(보종 코드), EXP_WP_CD(만기종 코드)
                            QA_STTA_PRGST_CD(TA 상태 코드), TA_REQ_DTM(TA 요청 일시), CNTR_COUNT(심사 회차),
                            IP_DCD(보험상품구분코드)
    """
    global NLP_DIR_PATH
    global DETAIL_DIR_PATH
    global OUTPUT_DIR_NAME
    global TA_TEMP_DIR_PATH
    global DELETE_FILE_LIST
    poli_no = str(job[0]).strip()
    ctrdt = str(job[1]).strip()
    ip_cd = str(job[2]).strip()
    cntr_count = str(job[3]).strip()
    ta_req_dtm = str(job[4]).strip()
    ip_dcd = str(job[5]).strip()
    # Make Target directory name
    OUTPUT_DIR_NAME = '{0}_{1}_{2}'.format(poli_no, ctrdt, cntr_count)
    TA_TEMP_DIR_PATH = "{0}/{1}".format(QA_TA_CONFIG['ta_data_path'], OUTPUT_DIR_NAME)
    DELETE_FILE_LIST.append(TA_TEMP_DIR_PATH)
    if os.path.exists(TA_TEMP_DIR_PATH):
        shutil.rmtree(TA_TEMP_DIR_PATH)
    NLP_DIR_PATH = '{0}/modify_nlp_line_number'.format(TA_TEMP_DIR_PATH)
    DETAIL_DIR_PATH = '{0}/detail'.format(TA_TEMP_DIR_PATH)
    os.makedirs(NLP_DIR_PATH)
    os.makedirs(DETAIL_DIR_PATH)
    # Add logging
    logger_args = {
        'base_path': QA_TA_CONFIG['log_dir_path'],
        'log_file_name': "{0}.log".format(OUTPUT_DIR_NAME),
        'log_level': QA_TA_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    return logger, poli_no, ctrdt, ip_cd, ta_req_dtm, cntr_count, ip_dcd


def processing(job):
    """
    TA processing
    :param      job:        Job( POLI_NO(증서 번호), CTRDT(청약 일자), IP_CD(보종코드), CNTR_COUNT(심사회차),
                            QA_STTA_PRGST_CD(QA_STTA 상태코드), TA_REQ_DTM(TA 요청일시), IP_DCD(보험상품구분코드) )
    """
    # 0. Setup data
    logger, poli_no, ctrdt, ip_cd, ta_req_dtm, cntr_count, ip_dcd = setup_data(job)
    logger.info("-" * 100)
    logger.info('Start QA TA')
    # 1. Connect DB
    oracle = connect_db(logger, 'Oracle')
    try:
        # 2. Update status and select REC file
        rec_info_dict = update_status_and_select_rec_file(logger, oracle, poli_no, ctrdt, cntr_count, ip_dcd)
        # 3. Copy STT and NLP output
        copy_stt_and_nlp_output(logger, rec_info_dict, ctrdt)
        # 4. Make matrix file
        matrix_file_path = make_matrix_file(logger, oracle, poli_no, ctrdt, ip_cd)
        # 5. Execute HMD
        hmd_output_dir_path = execute_hmd(logger, matrix_file_path)
        # 6. Sorted HMD output
        sorted_hmd_output_dir_path = sort_hmd_output(logger, hmd_output_dir_path)
        # 7. Deduplication HMD output
        dedup_hmd_output_dir_path = dedup_hmd_output(logger, sorted_hmd_output_dir_path)
        # 8. Detect section HMD output
        detect_hmd_output_dir_path = detect_sect_hmd_output(logger, dedup_hmd_output_dir_path)
        # 9. Modify HMD output
        final_output_dir_path = modify_hmd_output(logger, detect_hmd_output_dir_path)
        # 10. Execute masking
        masking_dir_path = execute_masking(logger, final_output_dir_path)
        # 11. DB upload TB_QA_STT_TM_TA_DTC_RST
        db_upload_tb_tm_qa_ta_sec_info(logger, oracle, masking_dir_path, rec_info_dict, poli_no, ctrdt, cntr_count)
        # 12. DB upload TB_TM_QA_TA_DTC_RST
        db_upload_tb_tm_qa_ta_dtc_rst(logger, oracle, poli_no, ctrdt, cntr_count)
        # 13. DB upload TB_TM_QA_TA_ADT_DTC_RST
        db_upload_tb_tm_qa_ta_adt_dtc_rst(logger, oracle, poli_no, ctrdt)
        # 14. DB upload TB_TM_QA_INT_SEC_SAVE
        db_upload_tb_tm_qa_int_sec_save(logger, oracle, poli_no, ctrdt)
        # 15. Move output
        move_output(logger, ctrdt)
        # 16. Delete garbage file
        delete_garbage_file(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("QA TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    QA TA ERROR   ----------")
        error_process(logger, oracle, poli_no, ctrdt, '12', cntr_count, ip_dcd)
        delete_garbage_file(logger)
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # Update status
    logger.info("16. Update status to QA TA END(04)")
    result = oracle.update_qa_stta_prgst_cd(
        qa_stta_prgst_cd='13',
        poli_no=poli_no,
        ctrdt=ctrdt,
        cntr_count=cntr_count
    )
    if result:
        oracle.update_ta_cmdtm(poli_no, ctrdt, cntr_count)
        get_cntr_info(logger, oracle, poli_no, '13', ip_dcd, ctrdt, cntr_count)
    oracle.disconnect()
    logger.info("QA TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    logger.info("Remove logger handler")
    logger.info("----------     QA TA END      ----------")
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job):
    """
    This is a program that execute QA TA
    :param      job:       JOB ( POLI_NO(증서 번호), CTRDT(청약 일자), IP_CD(보종코드), CNTR_COUNT(심사회차),
                                QA_STTA_PRGST_CD(QA_STTA 상태코드), TA_REQ_DTM(TA 요청일시) )
    """
    global DT
    global ST
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        if len(job) > 0:
            processing(job)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)
