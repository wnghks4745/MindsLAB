#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-09, modification: 2018-03-28"

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
from lib.openssl import decrypt, encrypt, decrypt_string
from lib.damo import scp_dec_file, dir_scp_enc_file

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DB_CONFIG = {}
QA_TA_CONFIG = {}
MASKING_CONFIG = {}
DT = ""
ST = ""
FBWD_RESULT = list()
# GRT_CD_DICT = dict()
NLP_DIR_PATH = ""
NLP_INFO_DICT = dict()
# INT_SEC_RESULT = list()
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
# SEC_TIME_CHECK_DICT = dict()
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

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_pk(self, org_fof_c, epl_id, prps_date):
        """
        SELECT recording call meta of cntr
        :param      org_fof_c:      ORG_FOF_C(점포코드)
        :param      epl_id:         EPL_ID(가입설계ID)
        :param      prps_date:      PRPS_DATE(청약일)
        :return:                    CALL_META_LIST
        """
        query = """
            SELECT DISTINCT
                A.REC_NO
                , B.DOCUMENT_ID AS RFILE_NAME
                , B.DOCUMENT_DT
                , B.STT_PRGST_CD
                , B.CHN_TP
            FROM
                TB_TM_CNTR_RCDE_INFO A, CALL_META B
            WHERE 1=1
                AND A.ORG_FOF_C = :1
                AND A.EPL_ID = :2
                AND A.PRPS_DATE = :3
                AND B.PROJECT_CD in ('TM', 'CD')
                AND A.REC_NO = B.REC_ID
        """
        bind = (
            org_fof_c,
            epl_id,
            prps_date
        )
        self.cursor.execute(query, bind)
        result = self.rows_to_dict_list()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_scrt_sntc_dtc_info(self, sntc_no):
        """
        문장번호로 사전 내용 조회
        :param sntc_no:                 문장번호  ex) 'qa001'
        :return: 사전 ['(%부업)(%겸업)', ...]
        """
        query = """
            SELECT
                DTC_CONT
            FROM
                TB_SCRT_SNTC_DTC_INFO
            WHERE 1=1
                AND SNTC_NO = :1
                AND USE_YN = 'Y'
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

    def select_tb_scrt_condition_mapping(self, ip_cd, sntc_no_list):
        """
        문장별 발화 조건 조회
        :param iitem:                 상품코드
        :param sp_mp_gb:               상품매핑구분
        :param sp_mp_gb:               상품매핑구분
        :return:
        """
        query = """
            SELECT
                IP_CD
                , SNTC_NO
                , FETUS_YN
                , AGE_YN
                , SEX_TC_YN
                , SEX_TC
                , AGE_OVER
                , AGE_UNDER
            FROM
                TB_SCRT_CONDITION_MAPPING
            WHERE 1=1
                AND IP_CD = :1
        """
        bind = (
            ip_cd,
        )
        if sntc_no_list:
            insert_list = [value['SNTC_NO'] for value in sntc_no_list]
            query += " AND SNTC_NO in ('{0}')".format("','".join(insert_list))
        self.cursor.execute(query, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_tb_scrt_sec_info(self, ip_cd):
        """
         구간(스크립트) 대/중 카테고리 및 구간번호 조회
         :param ip_cd:
         :param qa_scrt_tp_list:
         :return: [[QA스크립트 대분류코드, QA스크립트 중분류코드,구간번호], ...]
         """
        query = """
             SELECT
                 QA_SCRT_LCCD,
                 QA_SCRT_MCCD,
                 SEC_NO
             FROM
                 TB_SCRT_SEC_INFO
             WHERE 1=1
                 AND IP_CD = :1
                 AND DEFAULT_YN = 'Y'
         """
        bind = (
            ip_cd,
        )
        self.cursor.execute(query, bind)
        result = self.rows_to_dict_list()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_tb_scrt_sp_add_opt_mapping(self, ip_cd, iitem_list):
        """
        상품별 추가 발화해야할 스크립트 조회
        :param ip_cd:
        :param iitem_list:
        """
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_SP_ADD_OPT_MAPPING
            WHERE 1=1
                AND IP_CD = :1
        """
        bind = (
            ip_cd,
        )
        if iitem_list:
            insert_list = [value['IITEM'] for value in iitem_list]
            query += " AND IITEM in ('{0}')".format("','".join(insert_list))
        self.cursor.execute(query, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_tb_scrt_adt_dtc_mapping(self, sp_tp):
        """
        금칙어 문장번호 조회
        :param sp_tp:
        :return:
        """
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_ADT_DTC_MAPPING
            WHERE 1=1
                AND SNTC_CD = :1
        """
        bind = (
            sp_tp,
        )
        self.cursor.execute(query, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_data_to_tb_scrt_sntc_by_scrt_sec_info(self, ip_cd, scrt_sec_info_dict_list):
        query = """
            SELECT
                A.QA_SCRT_LCCD
                , A.QA_SCRT_MCCD
                , A.SEC_NO
                , B.SNTC_NO
                , B.STRT_SNTC_YN
            FROM
                TB_SCRT_SEC_SNTC_INFO A , TB_SCRT_SNTC_MST_INFO B
            WHERE 1=1
                AND A.IP_CD = :1
                AND A.USE_YN = 'Y'
                AND A.IP_CD = B.SNTC_CD
                AND A.SNTC_NO = B.SNTC_NO
        """
        bind = (
            ip_cd,
        )
        if scrt_sec_info_dict_list:
            insert_list = [(value['QA_SCRT_LCCD'], value['QA_SCRT_MCCD'], value['SEC_NO']) for value in scrt_sec_info_dict_list]
            query += " AND (A.QA_SCRT_LCCD, A.QA_SCRT_MCCD, A.SEC_NO) in ({0})".format(",".join(map(str, insert_list)))

        self.cursor.execute(query, bind)
        result = self.rows_to_dict_list()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_data_to_tb_scrt_sntc_by_sntc_no(self, ip_cd, sntc_no_list):
        query = """
            SELECT
                A.QA_SCRT_LCCD
                , A.QA_SCRT_MCCD
                , A.SEC_NO
                , B.SNTC_NO
                , B.STRT_SNTC_YN
            FROM
                TB_SCRT_SEC_SNTC_INFO A , TB_SCRT_SNTC_MST_INFO B
            WHERE 1=1
                AND A.IP_CD = :1
                AND A.USE_YN = 'Y'
                AND A.IP_CD = B.SNTC_CD
                AND A.SNTC_NO = B.SNTC_NO
        """
        bind = (
            ip_cd,
        )

        if sntc_no_list:
            insert_list = [value['SNTC_NO'] for value in sntc_no_list]
            query += " AND A.SNTC_NO in ('{0}')".format("','".join(insert_list))
        # self.logger.debug(query)
        # self.logger.debug(bind)
        self.cursor.execute(query, bind)
        result = self.rows_to_dict_list()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_fbwd_sntc_no(self, logger, adt_dtc_sntc_no_list):
        """
        사전마스터에서 문장코드(SNTC_CD)로 금칙어 문장번호(SNTC_NO) 조회
        :param      logger:                     Logger                     문장코드 ex) cl+wi
        :param      adt_dtc_sntc_no_list:       adt_dtc_sntc_no_list
        :return:                            ['f001','f002',...]
        """
        query = """
            SELECT
                SNTC_NO
            FROM
                TB_SCRT_SNTC_MST_INFO
            WHERE 1=1
                AND SNTC_DCD = '04'
        """
        if adt_dtc_sntc_no_list:
            insert_list = [value['SNTC_NO'] for value in adt_dtc_sntc_no_list]
            query += " AND SNTC_NO in ('{0}')".format("','".join(insert_list))
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
        return result

    def select_sp_to_tb_tm_cntr_sp_info(self, org_fof_c, epl_id):
        query = """
            SELECT
                IITEM
            FROM
                TB_TM_CNTR_SP_INFO
            WHERE 1=1
                AND org_fof_c = :1
                AND epl_id = :2
        """
        bind = (
            org_fof_c,
            epl_id,
        )
        self.cursor.execute(query, bind)
        result = self.rows_to_dict_list()
        if result is bool:
            return False
        if not result:
            return False
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
                DTC_SNTC_CD,
                DTC_SNTC_NM
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

    def update_qa_stta_prgst_cd(self, **kwargs):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    QA_STTA_PRGST_CD = :1
                    , TA_CM_DTM = SYSDATE
                WHERE 1=1
                    AND ORG_FOF_C = :2
                    AND EPL_ID = :3
                    AND PRPS_DATE = :4
                    AND CNTR_COUNT = :5
            """
            bind = (
                kwargs.get('qa_stta_prgst_cd'),
                kwargs.get('org_fof_c'),
                kwargs.get('epl_id'),
                kwargs.get('prps_date'),
                kwargs.get('cntr_count'),
            )
            self.cursor.execute(query, bind)
            self.logger.info("status update -> {0}".format(kwargs.get('qa_stta_prgst_cd')))
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
            query = """
                INSERT INTO
                    TB_TM_QA_TA_DTC_RST
                    (
                        ORG_FOF_C, 
                        EPL_ID,
                        PRPS_DATE,
                        QA_SCRT_LCCD,
                        QA_SCRT_MCCD,
                        SEC_NO,
                        SNTC_NO,
                        SNTC_SEQ,
                        SNTC_CD,
                        CONT_NO,
                        STT_SNTC_LIN_NO,
                        SNTC_DTC_YN,
                        SNTC_CONT,
                        SNTC_STTM,
                        SNTC_ENDTM,
                        CUST_ANYN,
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
                    :26, :27, 
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE, 'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in upload_data_dict.values():
                org_fof_c = insert_dict['ORG_FOF_C']
                epl_id = insert_dict['EPL_ID']
                prps_date = insert_dict['PRPS_DATE']
                qa_scrt_lccd = insert_dict['QA_SCRT_LCCD']
                qa_scrt_mccd = insert_dict['QA_SCRT_MCCD']
                # qa_scrt_sccd = insert_dict['QA_SCRT_SCCD']
                sec_no = insert_dict['SEC_NO']
                sntc_no = insert_dict['SNTC_NO']
                sntc_seq = insert_dict['SNTC_SEQ']
                sntc_cd = insert_dict['SNTC_CD']
                # grt_cd = insert_dict['GRT_CD']
                cont_no = insert_dict['CONT_NO']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                sntc_dtc_yn = insert_dict['SNTC_DTC_YN']
                sntc_cont = insert_dict['SNTC_CONT']
                sntc_sttm = insert_dict['SNTC_STTM']
                sntc_endtm = insert_dict['SNTC_ENDTM']
                cust_anyn = insert_dict['CUST_ANYN']
                # cust_nor_anyn = insert_dict['CUST_NOR_ANYN']
                # cust_ab_anyn = insert_dict['CUST_AB_ANYN']
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
                values_tuple = (org_fof_c, epl_id, prps_date, qa_scrt_lccd, qa_scrt_mccd, sec_no, sntc_no, sntc_seq,
                                sntc_cd, cont_no, stt_sntc_lin_no, sntc_dtc_yn, sntc_cont, sntc_sttm, sntc_endtm,
                                cust_anyn, cust_agrm_sntc_cont, dtc_kywd_lit,
                                nudtc_kywd_lit, kywd_ctgry_nm, kywd_dtc_rate, vrfc_kywd_qtty, vrfc_kywd_dtc_qtty,
                                dtc_info_crt_dtm, cntr_count, rec_id, rfile_name)
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

    def insert_tb_tm_qa_ta_adt_dtc_rst(self, upload_data_list):
        try:
            query = """
                INSERT INTO
                    TB_TM_QA_TA_ADT_DTC_RST(
                        ORG_FOF_C,
                        EPL_ID,
                        PRPS_DATE,
                        CNTR_COUNT,
                        SNTC_NO,
                        SNTC_SEQ,
                        CONT_NO,
                        SNTC_DCD,
                        STT_SNTC_LIN_NO,
                        DTC_SNTC_CD,
                        DTC_SNTC_NM,
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
                    :11, :12, :13, :14, :15,
                    :16,
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE, 
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            values_list = list()
            for insert_dict in upload_data_list:
                # poli_no = insert_dict['POLI_NO']
                # ctrdt = insert_dict['CTRDT']
                org_fof_c = insert_dict['ORG_FOF_C']
                epl_id = insert_dict['EPL_ID']
                prps_date = insert_dict['PRPS_DATE']
                cntr_count = insert_dict['CNTR_COUNT']
                sntc_no = insert_dict['SNTC_NO']
                sntc_seq = insert_dict['SNTC_SEQ']
                cont_no = insert_dict['CONT_NO']
                sntc_dcd = insert_dict['SNTC_DCD']
                stt_sntc_lin_no = insert_dict['STT_SNTC_LIN_NO']
                dtc_sntc_cd = insert_dict['DTC_SNTC_CD']
                dtc_sntc_nm = insert_dict['DTC_SNTC_NM']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                sntc_cont = insert_dict['SNTC_CONT']
                sntc_sttm = insert_dict['SNTC_STTM']
                sntc_endtm = insert_dict['SNTC_ENDTM']
                values_tuple = (
                    org_fof_c, epl_id, prps_date, cntr_count, sntc_no, sntc_seq, cont_no, sntc_dcd, stt_sntc_lin_no,
                    dtc_sntc_cd, dtc_sntc_nm, rec_id, rfile_name, sntc_cont, sntc_sttm, sntc_endtm
                )
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

    def delete_tb_tm_qa_ta_dtc_rst(self, org_fof_c, epl_id, prps_date, cntr_count):
        try:
            query = """
                DELETE FROM
                    TB_TM_QA_TA_DTC_RST
                WHERE 1=1
                    AND org_fof_c = :1
                    AND epl_id = :2
                    AND prps_date = :3
                    AND cntr_count = :4
            """
            bind = (
                org_fof_c
                , epl_id
                , prps_date
                , cntr_count
                ,
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

    def delete_tb_tm_qa_ta_adt_dtc_rst(self, org_fof_c, epl_id, prps_date, cntr_count):
        try:
            query = """
                DELETE FROM
                    TB_TM_QA_TA_ADT_DTC_RST
                WHERE 1=1
                    AND ORG_FOF_C = :1
                    AND EPL_ID = :2
                    AND PRPS_DATE = :3
                    AND CNTR_COUNT = :4
            """
            bind = (
                org_fof_c,
                epl_id,
                prps_date,
                cntr_count,
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

    def select_tcl_user(self, org_fof_c, prps_cntc_usid):
        """
        점포 정보와 상담원 정보 조회
        :param org_fof_c:           ORG_FOF_C(점포코드)
        :param prps_cntc_usid:      상담원아이디
        :return:
        """
        query = """
            SELECT 
                A.ORG_PART_C
                , A.ORG_PART_NM
                , B.USER_NM
                , (
                    SELECT ORG_FOF_NM
                      FROM {0}.TCL_ORG@D_KBLUAT_ZREAD
                     WHERE ORG_FOF_C = :1
                    ) AS ORG_FOF_NM
              FROM {0}.TCL_ORG_PART@D_KBLUAT_ZREAD A
                 , {0}.TCL_USER@D_KBLUAT_ZREAD B
             WHERE A.ORG_FOF_C = B.USER_OFFC_CD
               AND A.ORG_PART_C = B.USER_PART_CD
               AND B.USER_ID = :2
               AND ROWNUM = 1
        """.format(DB_CONFIG['tm_user'])
        bind = (
            org_fof_c,
            prps_cntc_usid,
        )
        self.cursor.execute(query, bind)
        result = self.rows_to_dict_list()
        if not result:
            temp_dict = {
                'ORG_PART_C': None
                , 'ORG_PART_NM': None
                , 'USER_NM': None
                , 'ORG_FOF_NM': None
            }
            return temp_dict
        return result[0]

    def delete_tb_tm_sec_dtc_rate(self, org_fof_c, epl_id, prps_date):
        try:
            query = """
                DELETE FROM
                    TB_TM_SEC_DTC_RATE
                WHERE 1=1
                    AND ORG_FOF_C = :1
                    AND EPL_ID = :2
                    AND PRPS_DATE = :3
            """
            bind = (
                org_fof_c,
                epl_id,
                prps_date,
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

    def insert_tb_tm_sec_dtc_rate(self, org_fof_c, epl_id, prps_date, cont_no, prps_cntc_usid, user_info_dict, result_percent):
        """
        고지탐지율 통계 저장
        :param org_fof_c:               점포코드
        :param epl_id:                  가입설계ID
        :param prps_date:               청약일
        :param cont_no:                 증권번호
        :param prps_cntc_usid:          청약상담원사용자ID
        :param user_info_dict:          select_tcl_user() 에서 조회한 점포 정보와 상담원 정보
        :param result_percent:          탐지율
        :return:
        """
        try:
            query = """
            MERGE INTO TB_TM_SEC_DTC_RATE
                USING DUAL
                ON (
                        ORG_FOF_C = :1
                        AND EPL_ID = :2
                        AND PRPS_DATE = :3
                        AND CONT_NO = :4
                )
                WHEN MATCHED THEN
                    UPDATE SET
                        ORG_FOF_C_NM = :5
                        , PRPS_USER_PART_C = :6
                        , PRPS_USER_PART_C_NM = :7
                        , PRPS_CNTC_USID = :8
                        , PRPS_CNTC_USID_NM = :9
                        , SEC_DTC_RATE = :10
                        , REGP_CD = 'TM_QA_TA'
                        , RGST_PGM_ID = 'TM_QA_TA'
                        , RGST_DTM = SYSDATE
                        , LST_CHGP_CD = 'TM_QA_TA'
                        , LST_CHG_PGM_ID = 'TM_QA_TA'
                        , LST_CHG_DTM = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT
                     (
                        ORG_FOF_C
                        , EPL_ID
                        , PRPS_DATE
                        , CONT_NO
                        , ORG_FOF_C_NM
                        , PRPS_USER_PART_C
                        , PRPS_USER_PART_C_NM
                        , PRPS_CNTC_USID
                        , PRPS_CNTC_USID_NM
                        , SEC_DTC_RATE
                        , REGP_CD
                        , RGST_PGM_ID
                        , RGST_DTM
                        , LST_CHGP_CD
                        , LST_CHG_PGM_ID
                        , LST_CHG_DTM
                    )
                VALUES (
                    :11, :12, :13, :14, :15,
                    :16, :17, :18, :19, :20,
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE, 
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            bind = (
                org_fof_c
                , epl_id
                , prps_date
                , cont_no
                , user_info_dict['ORG_FOF_NM']
                , user_info_dict['ORG_PART_C']
                , user_info_dict['ORG_PART_NM']
                , prps_cntc_usid
                , user_info_dict['USER_NM']
                , result_percent
                , org_fof_c
                , epl_id
                , prps_date
                , cont_no
                , user_info_dict['ORG_FOF_NM']
                , user_info_dict['ORG_PART_C']
                , user_info_dict['ORG_PART_NM']
                , prps_cntc_usid
                , user_info_dict['USER_NM']
                , result_percent
            )
            self.cursor.execute(query, bind)
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())


#######
# def #
#######
def error_process(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count, status):
    """
    Error process
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      org_fof_c:      ORG_FOF_C(점포 코드)
    :param      epl_id:         EPL_ID(가입설계ID)
    :param      prps_date:      PRPS_DATE(청약일)
    :param      cntr_count:     CNTR_COUNT(심사 회차)
    :param      status:         Status
    """
    logger.info("Error process ORG_FOF_C = {0}, EPL_ID = {1} PRPS_DATE = {2} CNTR_COUNT = {3}".format(org_fof_c, epl_id, prps_date, cntr_count))
    # oracle.conn.commit()
    oracle.update_qa_stta_prgst_cd(
        qa_stta_prgst_cd=status,
        org_fof_c=org_fof_c,
        epl_id=epl_id,
        prps_date=prps_date,
        cntr_count=cntr_count
    )


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
    logger.info("20. Delete garbage file")
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
    logger.info("19. Move output")
    output_dir_path = "{0}/{1}/{2}/{3}".format(QA_TA_CONFIG['ta_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8])
    output_abs_dir_path = "{0}/{1}".format(output_dir_path, OUTPUT_DIR_NAME)
    if os.path.exists(output_abs_dir_path):
        del_garbage(logger, output_abs_dir_path)
    if not os.path.exists(output_dir_path):
        os.makedirs(output_dir_path)
    shutil.move(TA_TEMP_DIR_PATH, output_dir_path)
    dir_scp_enc_file(output_abs_dir_path)


def db_insert_tb_tm_sec_dtc_rate(logger, oracle, org_fof_c, epl_id, prps_date, cont_no, prps_cntc_usid):
    """
    상담원별 고지 탐지율 통계 저장
    :param logger:
    :param oracle:
    :param org_fof_c:                   점포코드
    :param epl_id:                      가입설계ID
    :param prps_date:                   청약일
    :param cont_no:                     증권번호
    :param prps_cntc_usid:              청약상담원사용자ID
    :return:
    """
    logger.info("18. DB insert TB_TM_SEC_DTC_RATE")
    total_count = len(TARGET_CATEGORY_DICT)
    item_percent = float(1) / float(total_count) * 100
    print total_count
    print item_percent
    result_percent_float = 0.0
    for key, value in TARGET_CATEGORY_DICT.items():
        key_list = key.split("|")
        detect_check_key = '|'.join(key_list[:4])
        detect_sntc_no_list = DETECT_CATEGORY_DICT.get(detect_check_key)
        target_sntc_no_list = value
        if detect_sntc_no_list:
            item_sub_percent = float(1) / float(len(set(target_sntc_no_list))) * item_percent
            result_percent_float += item_sub_percent * len(set(detect_sntc_no_list))

    result_percent = int(round(result_percent_float))
    user_info_dict = oracle.select_tcl_user(org_fof_c, prps_cntc_usid)
    logger.debug('{0} {1} {2} {3} {4} {5} {6} '.format(
        org_fof_c, epl_id, prps_date, cont_no, prps_cntc_usid, user_info_dict, result_percent))
    oracle.insert_tb_tm_sec_dtc_rate(org_fof_c, epl_id, prps_date, cont_no, prps_cntc_usid, user_info_dict, result_percent)


def db_insert_tb_tm_qa_ta_adt_dtc_rst(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count):
    """
    DB insert TB_TM_QA_TA_ADT_DTC_RST
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      org_fof_c:      ORG_FOF_C(점포 코드)
    :param      epl_id:         EPL_ID(가입설계ID)
    :param      prps_date:      PRPS_DATE(청약일)
    :param      cntr_count:     CNTR_COUNT(심사회차)
    """
    logger.info("17. DB upload TB_TM_QA_TA_ADT_DTC_RST")
    oracle.delete_tb_tm_qa_ta_adt_dtc_rst(org_fof_c, epl_id, prps_date, cntr_count)
    oracle.insert_tb_tm_qa_ta_adt_dtc_rst(TB_TM_QA_TA_ADT_DTC_RST_LIST)


def db_insert_tb_tm_qa_ta_dtc_rst(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count):
    """
    DB insert to TB_QA_STT_TM_TA_DTC_RST
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      org_fof_c:                  ORG_FOF_C(점포코드)
    :param      epl_id:                     EPL_ID(가입설계ID)
    :param      prps_date:                  PRPS_DATE(청약일)
    :param      cntr_count:                 CNTR_COUNT(심사회차)
    """
    logger.info("16. DB upload to TB_QA_STT_TM_TA_DTC_RST")
    oracle.delete_tb_tm_qa_ta_dtc_rst(org_fof_c, epl_id, prps_date, cntr_count)
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


def set_data_for_tb_tm_qa_ta_dtc_rst(logger, oracle, dir_path, file_name, rec_info_dict, org_fof_c, epl_id, prps_date,
                                     cntr_count, cont_no):
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
    global DETECT_CATEGORY_DICT
    global TB_TM_QA_TA_DTC_RST_DICT
    global TB_TM_QA_TA_ADT_DTC_RST_LIST
    global OVERLAP_CHECK_DICT
    global OVERLAP_CHECK_DICT_VER_TWO

    # 금칙어 리스트
    fbwd_list = list()
    for item in FBWD_RESULT:
        fbwd_list.append(item[0])
    # 고객 답변 여부 리스트
    cust_anyn_list = list()
    cust_anyn_result = oracle.select_cust_anyn()
    for item in cust_anyn_result:
        cust_anyn_list.append(item[0])
    # output 결과 파일 정리
    temp_tb_tm_qa_ta_dtc_rst_dict = dict()
    sntc_seq_cnt = 0
    final_output_file = open(os.path.join(dir_path, file_name), 'r')
    for line in final_output_file:
        line = line.strip()
        line_list = line.split('\t')
        if line_list[4] == 'none' or line_list[4] == 'new_none':
            continue
        category_list = line_list[4].split("_")
        if len(category_list) < 3:
            sntc_cd = category_list[0]
            qa_scrt_lccd = category_list[0]
            qa_scrt_mccd = category_list[0]
            sec_no = ""
            sntc_no = category_list[1]
        else:
            sntc_cd = category_list[0]
            qa_scrt_list = category_list[1].split('|')
            qa_scrt_lccd = qa_scrt_list[0]
            qa_scrt_mccd = qa_scrt_list[1]
            sec_no = qa_scrt_list[2]
            sntc_no = category_list[2]
        overlap_check_key = "{0}_{1}|{2}|{3}_{4}".format(
            sntc_cd, qa_scrt_lccd, qa_scrt_mccd, sec_no, sntc_no)
        if overlap_check_key not in OVERLAP_CHECK_DICT:
            sntc_seq = str(sntc_seq_cnt)
        else:
            sntc_seq_cnt += 1
            sntc_seq = str(sntc_seq_cnt)
        OVERLAP_CHECK_DICT[overlap_check_key] = 1
        rfile_name = os.path.basename(line_list[1]).replace("_trx", "")
        rec_id = rec_info_dict[rfile_name]['REC_NO']
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
                nlp_key = '{0}|{1}|{2}'.format(line_list[1], line_num, prps_date)
                if not nlp_key in NLP_INFO_DICT:
                    continue
                sent = NLP_INFO_DICT[nlp_key][0].strip()
                if sent.startswith('[C]'):
                    cust_anyn = 'Y'
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
        tb_tm_qa_ta_adt_dtc_rst_dict = dict()
        sntc_dtc_lst_result = oracle.select_dtc_cd_and_nm(sntc_no)
        if not sntc_dtc_lst_result:
            if not sntc_no.startswith('f'):
                logger.error(
                    "Can't select SNTC_DCD and DTC_CD and DTC_CD_NM, SNTC_NO = {0}".format(sntc_no))
            sntc_dcd = ''
            dtc_sntc_cd = ''
            dtc_sntc_nm = ''
        else:
            sntc_dcd = sntc_dtc_lst_result[0]
            dtc_sntc_cd = sntc_dtc_lst_result[1]
            dtc_sntc_nm = sntc_dtc_lst_result[2]
        if sntc_no in fbwd_list:
            sntc_seq = 0
            while True:
                overlap_check_key_ver_two = '{0}_{1}_{2}_{3}_{4}'.format(org_fof_c, epl_id, prps_date, sntc_no, sntc_seq)
                if overlap_check_key_ver_two not in OVERLAP_CHECK_DICT_VER_TWO:
                    OVERLAP_CHECK_DICT_VER_TWO[overlap_check_key_ver_two] = 1
                    break
                sntc_seq += 1
            tb_tm_qa_ta_adt_dtc_rst_dict['ORG_FOF_C'] = org_fof_c
            tb_tm_qa_ta_adt_dtc_rst_dict['EPL_ID'] = epl_id
            tb_tm_qa_ta_adt_dtc_rst_dict['PRPS_DATE'] = prps_date
            tb_tm_qa_ta_adt_dtc_rst_dict['CONT_NO'] = cont_no
            tb_tm_qa_ta_adt_dtc_rst_dict['CNTR_COUNT'] = cntr_count
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_NO'] = sntc_no
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_SEQ'] = sntc_seq
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_DCD'] = sntc_dcd
            tb_tm_qa_ta_adt_dtc_rst_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
            tb_tm_qa_ta_adt_dtc_rst_dict['DTC_SNTC_CD'] = dtc_sntc_cd
            tb_tm_qa_ta_adt_dtc_rst_dict['DTC_SNTC_NM'] = dtc_sntc_nm
            tb_tm_qa_ta_adt_dtc_rst_dict['REC_ID'] = rec_id
            tb_tm_qa_ta_adt_dtc_rst_dict['RFILE_NAME'] = rfile_name
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_CONT'] = sntc_cont
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_STTM'] = modified_sntc_sttm
            tb_tm_qa_ta_adt_dtc_rst_dict['SNTC_ENDTM'] = modified_sntc_endtm
            TB_TM_QA_TA_ADT_DTC_RST_LIST.append(tb_tm_qa_ta_adt_dtc_rst_dict)
            continue
        tb_tm_qa_ta_dtc_rst_dict = dict()
        tb_tm_qa_ta_dtc_rst_dict['ORG_FOF_C'] = org_fof_c
        tb_tm_qa_ta_dtc_rst_dict['EPL_ID'] = epl_id
        tb_tm_qa_ta_dtc_rst_dict['PRPS_DATE'] = prps_date
        tb_tm_qa_ta_dtc_rst_dict['CONT_NO'] = cont_no
        tb_tm_qa_ta_dtc_rst_dict['CNTR_COUNT'] = cntr_count
        tb_tm_qa_ta_dtc_rst_dict['QA_SCRT_LCCD'] = qa_scrt_lccd
        tb_tm_qa_ta_dtc_rst_dict['QA_SCRT_MCCD'] = qa_scrt_mccd
        tb_tm_qa_ta_dtc_rst_dict['SEC_NO'] = sec_no
        tb_tm_qa_ta_dtc_rst_dict['SNTC_NO'] = sntc_no
        tb_tm_qa_ta_dtc_rst_dict['SNTC_SEQ'] = sntc_seq
        tb_tm_qa_ta_dtc_rst_dict['SNTC_CD'] = sntc_cd
        tb_tm_qa_ta_dtc_rst_dict['STT_SNTC_LIN_NO'] = stt_sntc_lin_no
        tb_tm_qa_ta_dtc_rst_dict['SNTC_DTC_YN'] = 'Y'
        tb_tm_qa_ta_dtc_rst_dict['SNTC_CONT'] = sntc_cont
        tb_tm_qa_ta_dtc_rst_dict['SNTC_STTM'] = modified_sntc_sttm
        tb_tm_qa_ta_dtc_rst_dict['SNTC_ENDTM'] = modified_sntc_endtm
        tb_tm_qa_ta_dtc_rst_dict['CUST_ANYN'] = cust_anyn
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
            org_fof_c, epl_id, prps_date, cntr_count, qa_scrt_lccd, qa_scrt_mccd, sec_no, sntc_no, sntc_seq, sntc_cd)
        if key not in temp_tb_tm_qa_ta_dtc_rst_dict:
            temp_tb_tm_qa_ta_dtc_rst_dict[key] = tb_tm_qa_ta_dtc_rst_dict
        category_key = "{0}|{1}|{2}|{3}".format(sntc_cd, qa_scrt_lccd, qa_scrt_mccd, sec_no)
        if category_key not in DETECT_CATEGORY_DICT:
            DETECT_CATEGORY_DICT[category_key] = [sntc_no]
        else:
            DETECT_CATEGORY_DICT[category_key].append(sntc_no)
        RFILE_INFO_DICT[category_key] = {
            'IP_CD': sntc_cd,
            'QA_SCRT_LCCD': qa_scrt_lccd,
            'QA_SCRT_MCCD': qa_scrt_mccd,
            'SEC_NO': sec_no,
            'REC_ID': rec_id,
            'RFILE_NAME': rfile_name,
            'SNTC_DCD': sntc_dcd,
            'DTC_CD': dtc_sntc_cd
        }
    final_output_file.close()
    dedup_dict = dict()
    for ori_key, info_dict in temp_tb_tm_qa_ta_dtc_rst_dict.items():
        sntc_cd = info_dict['SNTC_CD']
        qa_scrt_lccd = info_dict['QA_SCRT_LCCD']
        qa_scrt_mccd = info_dict['QA_SCRT_MCCD']
        sntc_no = info_dict['SNTC_NO']
        sec_no = info_dict['SEC_NO']
        stt_sntc_lin_no = info_dict['STT_SNTC_LIN_NO']
        sntc_cont = info_dict['SNTC_CONT']
        cust_anyn = info_dict['CUST_ANYN']
        key = '{0}_{1}_{2}_{3}_{4}_{5}_{6}'.format(
            sntc_cd, qa_scrt_lccd, qa_scrt_mccd, sntc_no, sec_no, stt_sntc_lin_no, sntc_cont)
        if key in dedup_dict and cust_anyn == 'Y':
            dedup_dict[key] = [ori_key, info_dict]
        elif key not in dedup_dict:
            dedup_dict[key] = [ori_key, info_dict]
    for items in dedup_dict.values():
        key = items[0]
        info_dict = items[1]
        TB_TM_QA_TA_DTC_RST_DICT[key] = info_dict


def db_insert_tb_tm_qa_ta_sec_info(logger, oracle, final_output_dir_path, rec_info_dict, org_fof_c, epl_id, prps_date,
                                   cntr_count, cont_no):
    """
    DB upload to TB_TM_QA_TA_SEC_INFO
    :param      logger:                     Logger
    :param      oracle:                     Oracle DB
    :param      final_output_dir_path:      Final output directory path
    :param      rec_info_dict:              Rec information dictionary
    :param      org_fof_c:                  ORG_FOF_C(점포코드)
    :param      epl_id:                     EPL_ID(가입설계ID)
    :param      prps_date:                  PRPS_DATE(청약일)
    :param      cntr_count:                 CNTR_COUNT(심사 회차)
    :param      cont_no:                    CONT_NO(증권번호)
    """
    global IP_SEC_SAVE_DICT
    logger.info("15. DB upload to TB_TM_QA_TA_SEC_INFO")
    w_ob = os.walk(final_output_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            set_data_for_tb_tm_qa_ta_dtc_rst(
                logger, oracle, dir_path, file_name, rec_info_dict, org_fof_c, epl_id, prps_date, cntr_count, cont_no)


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
    logger.info("14. Execute masking")
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
    logger.info("13. Modify HMD output")
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
    logger.info("12. Detect section HMD output")
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
                    if sntc_no.startswith('f'):
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
    logger.info("11. Deduplication HMD output")
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
    logger.info("10. Sorted HMD output")
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
    logger.info("9. Execute HMD")
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


def make_matrix_file(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count, iitem, iitem_name, sex_tc, fetus_yn, ip_cd, age):
    """
    Make matrix file
    :param      logger:         Logger
    :param      oracle:         Oracle DB
    :param      org_fof_c:      ORG_FOF_C(점포코드)
    :param      epl_id:         EPL_ID(청약일)
    :param      prps_date:      PRPS_DATE(청약일)
    :param      cntr_count:     CNTR_COUNT(심사회차)
    :param      iitem:          IITEM(보종코드)
    :param      iitem_name:     IITEM_NM(상품명)
    :param      sex_tc:         SEX_TC(성별)
    :param      fetus_yn:       FETUS_YN(태아여부)
    :param      ip_cd:          IP_CD(보종코드)
    :param      age:            AGE(고객나이)
    :return:                    Matrix file path
    """
    # global GRT_CD_DICT
    global MOTHER_SNTC_NO_LIST
    global TARGET_CATEGORY_DICT
    # global INT_SEC_RESULT
    global FBWD_RESULT
    global INCOMSALE_RESULT
    logger.info("8. Make matrix file")
    # Select SNTC_NO and DTC_CONT (문장 번호, 탐지 사전 내용)
    scrt_sntc_dtc_info_result_list = list()
    # 관심구간 matrix 조회

    # 상품이 어떤 종류인지(연금 / 저축 / 종신 / 기타) 분류 조회
    # sp_tp_list = oracle.select_fbwd_sp_tp_to_tb_scrt_sp_cd_mapping(iitem, 'FB')
    # if not sp_tp_list:
    #     sp_tp_list = list()
    # sp_tp_list.append({'SP_TP': 'FBWD00'})
    adt_dtc_sntc_no_list = oracle.select_tb_scrt_adt_dtc_mapping('FBWD00')
    FBWD_RESULT = oracle.select_fbwd_sntc_no(logger, adt_dtc_sntc_no_list)
    for fbwd_item in FBWD_RESULT:
        sntc_no = fbwd_item[0]
        fbwd_scrt_sntc_dtc_info_result = oracle.select_data_to_scrt_sntc_dtc_info(sntc_no)
        if not fbwd_scrt_sntc_dtc_info_result:
            continue
        for fbwd_dtc_cont_result in fbwd_scrt_sntc_dtc_info_result:
            dtc_cont = fbwd_dtc_cont_result[0]
            scrt_sntc_dtc_info_result_list.append(['FBWD', sntc_no, dtc_cont])
    # 고지 스크립트
    # . 청약건 상품 조회
    iitem_list = oracle.select_sp_to_tb_tm_cntr_sp_info(org_fof_c, epl_id)
    # . 기본 발화해야할 스크립트 조회
    scrt_sec_info_dict_list = oracle.select_tb_scrt_sec_info(ip_cd)
    # logger.debug(scrt_sec_info_dict_list)
    # . 스크립트 별 문장 조회
    scrt_sec_sntc_info_result = oracle.select_data_to_tb_scrt_sntc_by_scrt_sec_info(ip_cd, scrt_sec_info_dict_list)
    # logger.debug(scrt_sec_sntc_info_result)
    # . 상품별 추가 발화해야할 스크립트 문장 SNTC_NO 조회
    add_scrt_sec_info_dict_list = oracle.select_tb_scrt_sp_add_opt_mapping(ip_cd, iitem_list)
    # logger.debug(add_scrt_sec_info_dict_list)
    # . 추가 발화해야할 스크립트 별 문장 조회
    if add_scrt_sec_info_dict_list:
        result_select_data_to_tb_scrt_sntc_by_sntc_no = oracle.select_data_to_tb_scrt_sntc_by_sntc_no(ip_cd, add_scrt_sec_info_dict_list)
        logger.debug(result_select_data_to_tb_scrt_sntc_by_sntc_no)
        if result_select_data_to_tb_scrt_sntc_by_sntc_no:
            scrt_sec_sntc_info_result.extend(result_select_data_to_tb_scrt_sntc_by_sntc_no)
    # . 스크립트 발화 조건 조회
    logger.debug(scrt_sec_sntc_info_result)
    scrt_condition_list = oracle.select_tb_scrt_condition_mapping(ip_cd, scrt_sec_sntc_info_result)
    logger.debug(scrt_condition_list)
    for scrt_sec_sntc_info_item in scrt_sec_sntc_info_result:
        qa_scrt_lccd = scrt_sec_sntc_info_item['QA_SCRT_LCCD']
        qa_scrt_mccd = scrt_sec_sntc_info_item['QA_SCRT_MCCD']
        sec_no = scrt_sec_sntc_info_item['SEC_NO']
        sntc_no = scrt_sec_sntc_info_item['SNTC_NO']
        strt_sntc_yn = scrt_sec_sntc_info_item['STRT_SNTC_YN']

        # . 스크립트 발화 조건 체크
        logger.info('scrt_condition_list : {0}'.format(scrt_condition_list))
        if scrt_condition_list:
            for scrt_condition in scrt_condition_list:
                logger.debug(scrt_condition)
                if scrt_condition['SNTC_NO'] == sntc_no:
                    is_ad_check = False
                    if scrt_condition['FETUS_YN'] == 'Y':
                        if fetus_yn == 'Y':
                            is_ad_check = True
                        else:
                            is_ad_check = False
                    if scrt_condition['AGE_YN'] == 'Y':
                        if int(scrt_condition['AGE_OVER']) <= age < int(scrt_condition['AGE_UNDER']):
                            is_ad_check = True
                        else:
                            is_ad_check = False
                    if scrt_condition['SEX_TC_YN'] == 'Y':
                        if scrt_condition['SEX_TC'] == int(sex_tc) % 2:
                            is_ad_check = True
                        else:
                            is_ad_check = False
                    if scrt_condition['FETUS_YN'] == 'N' and scrt_condition['AGE_YN'] == 'N' and scrt_condition['SEX_TC_YN'] == 'N':
                        is_ad_check = True
                    logger.info(is_ad_check)
                    if not is_ad_check:
                        continue
        category_key = "{0}|{1}|{2}|{3}|N".format(
            ip_cd, qa_scrt_lccd, qa_scrt_mccd, sec_no)
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
                [ip_cd, qa_scrt_lccd, qa_scrt_mccd, sec_no, sntc_no, dtc_cont]
            )
    import operator
    logger.debug(sorted(scrt_sntc_dtc_info_result_list, key=operator.itemgetter(0)))
    logger.debug(sorted(MOTHER_SNTC_NO_LIST, key=operator.itemgetter(0)))
    logger.debug(sorted(TARGET_CATEGORY_DICT.items(), key=operator.itemgetter(0)))
    # Make matrix file
    matrix_dir_path = '{0}/HMD_matrix'.format(TA_TEMP_DIR_PATH)
    if not os.path.exists(matrix_dir_path):
        os.makedirs(matrix_dir_path)
    matrix_file_path = '{0}/{1}_{2}_{3}.matrix'.format(matrix_dir_path, org_fof_c, epl_id, prps_date)
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
            sec_no = str(item[3]).strip()
            sntc_no = str(item[4]).strip()
            category = '{0}_{1}|{2}|{3}_{4}'.format(
                ip_cd, qa_scrt_lccd, qa_scrt_mccd, sec_no, sntc_no)
            dtc_cont = str(item[5]).strip()
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


def nlp_output(logger, prps_date):
    """
    Copy STT output
    :param      logger:                 Logger
    :param      prps_date:              PRPS_DATE(청약일자)
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
                key = '{0}|{1}|{2}'.format(file_nm, line_num, prps_date)
                NLP_INFO_DICT[key] = [sent, nlp_sent]


def update_status_and_select_rec_file(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count):
    """
    Update status and select rec file
    :param      logger:                 Logger
    :param      oracle:                 Oracle db
    :param      org_fof_c:              ORG_FOF_C(점포코드)
    :param      epl_id:                 EPL_ID(가입설계ID)
    :param      prps_date:              PRPS_DATE(청약일)
    :param      cntr_count:             CNTR_COUNT(심사회차)
    :return:                            STT output dictionary
    """
    logger.info("2. Update status and select REC file")
    # Update TA_PRGST_CD status
    oracle.update_qa_stta_prgst_cd(
        qa_stta_prgst_cd='11',
        org_fof_c=org_fof_c,
        epl_id=epl_id,
        prps_date=prps_date,
        cntr_count=cntr_count
    )
    # Select REC file name and STT output information
    rec_info_dict = dict()
    pk_dict_list = oracle.select_pk(org_fof_c, epl_id, prps_date)
    if not pk_dict_list:
        raise Exception(
            "No data RCDG_ID [ORG_FOF_C : {0}, EPL_ID : {1}, PRPS_DATE : {2}, CNTR_COUNT : {3}]".format(
                org_fof_c, epl_id, prps_date, cntr_count))
    for pk in pk_dict_list:
        rfile_name = str(pk['RFILE_NAME']).strip()
        rec_info_dict[rfile_name] = pk
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


def setup_data(job):
    """
    Setup data and target directory
    :param      job:        Job
    :return:                Logger,
        ORG_FOF_C(점포코드), EPL_ID(가입설계ID), PRPS_DATE(청약일), CNTR_COUNT(심사회차),
        CNTC_CID (최초상담고객번호) , PRPS_CNTC_USID (청약상담원사용자ID), SEX_TC (성별), FETUS_YN (태아여부),
        QA_STTA_PRGST_CD(QA_STTA 상태코드), TA_REQ_DTM(TA 요청일시)
    """
    global NLP_DIR_PATH
    global DETAIL_DIR_PATH
    global OUTPUT_DIR_NAME
    global TA_TEMP_DIR_PATH
    global DELETE_FILE_LIST
    # Make Target directory name
    OUTPUT_DIR_NAME = '{0}_{1}_{2}_{3}_{4}'.format(job['ORG_FOF_C'], job['EPL_ID'], job['PRPS_DATE'], job['CNTR_COUNT'], job['CONT_NO'])
    TA_TEMP_DIR_PATH = "{0}/{1}".format(QA_TA_CONFIG['ta_data_path'], OUTPUT_DIR_NAME)
    DELETE_FILE_LIST.append(TA_TEMP_DIR_PATH)
    if os.path.exists(TA_TEMP_DIR_PATH):
        shutil.rmtree(TA_TEMP_DIR_PATH)
    else:
        os.makedirs(TA_TEMP_DIR_PATH)
    NLP_DIR_PATH = '{0}/modify_nlp_line_number'.format(TA_TEMP_DIR_PATH)
    DETAIL_DIR_PATH = '{0}/detail'.format(TA_TEMP_DIR_PATH)
    os.makedirs(NLP_DIR_PATH)
    os.makedirs(DETAIL_DIR_PATH)


def copy_stt_file(logger, rec_info_dict, org_fof_c, cntc_cid, prps_cntc_usid, cntr_count):
    """
    copy STT output txt, detail file to TA_TEMP_DIR_PATH
    :param      logger:             Logger
    :param      rec_info_dict:      recording information dictionary
    :param      org_fof_c:          ORG_FOF_C(점포 코드)
    :param      cntc_cid::          CNTC_CID(최초상담고객번호)
    :param      prps_cntc_usid:     PRPS_CNTC_USID(청약상담원사용자ID)
    :param      cntr_count:         CNTR_COUNT(심사회차)
    :return:
    """
    logger.info("3. copy stt txt - copy STT output txt file to TA_TEMP_DIR_PATH")
    stt_target_list = ['txt', 'detail']
    for target in rec_info_dict.keys():
        document_dt = str(rec_info_dict[target]['DOCUMENT_DT'])
        for target_name in stt_target_list:
            if rec_info_dict[target]['CHN_TP'] == 'S':
                # Stereo
                target_file_name = '{0}_trx.{1}.enc'.format(target, target_name)
            else:
                # Mono
                target_file_name = '{0}.{1}.enc'.format(target, target_name)
            target_stt_output_path = '{0}/{1}/{2}/{3}'.format(
                QA_TA_CONFIG['stt_output_path'], document_dt[:4], document_dt[5:7], document_dt[8:10])
            target_stt_file_path = '{0}/{1}/{2}'.format(target_stt_output_path, target_name, target_file_name)
            temp_stt_path = '{0}/{1}'.format(TA_TEMP_DIR_PATH, target_name)
            temp_stt_file_path = '{0}/{1}.{2}'.format(temp_stt_path, target, target_name)
            if not os.path.exists(temp_stt_path):
                os.makedirs(temp_stt_path)
            if not os.path.exists(target_stt_file_path):
                logger.error('file is not exist -> {0}'.format(target_stt_file_path))
                raise Exception(
                    "File is not exist [ORG_FOF_C : {0}, CNTC_CID : {1}, PRPS_CNTC_USID : {2}, CNTR_COUNT : {3}]".format(
                        org_fof_c, cntc_cid, prps_cntc_usid, cntr_count))
            if 0 != scp_dec_file(target_stt_file_path, temp_stt_file_path):
                logger.error('scp_dec_file ERROR ==> '.format(target_stt_file_path))
                raise Exception(
                    "File is not exist [ORG_FOF_C : {0}, CNTC_CID : {1}, PRPS_CNTC_USID : {2}, CNTR_COUNT : {3}]".format(
                        org_fof_c, cntc_cid, prps_cntc_usid, cntr_count))
            logger.debug("scp_dec_file {0} => {1}".format(target_stt_file_path, temp_stt_file_path))


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
    os.chdir(QA_TA_CONFIG['ta_bin_path'])
    target_list = glob.glob("{0}/txt/*".format(TA_TEMP_DIR_PATH))
    thread = len(target_list) if len(target_list) < int(QA_TA_CONFIG['nl_thread']) else int(QA_TA_CONFIG['nl_thread'])
    output_dir_list = ['JSON', 'JSON2', 'HMD', 'MCNT', 'NCNT', 'IDX', 'IDXVP', 'W2V']
    for dir_name in output_dir_list:
        output_dir_path = "{0}/{1}".format(TA_TEMP_DIR_PATH, dir_name)
        if not os.path.exists(output_dir_path):
            os.makedirs(output_dir_path)
    temp_new_lang_dir_path = '{0}/{1}'.format(QA_TA_CONFIG['ta_bin_path'], OUTPUT_DIR_NAME)
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


def make_morph_cnt_file(logger):
    """
    Make morph.cnt file
    :param      logger:     Logger
    """
    morph_file_list = glob.glob("{0}/MCNT/*.morph.cnt".format(TA_TEMP_DIR_PATH))
    # Load freq_except.txt file
    freq_except_dic = dict()
    freq_except_file_path = "{0}/LA/rsc/freq_except.txt".format(QA_TA_CONFIG['ta_path'])
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


def processing(job):
    """
    TA processing
    :param      job:       ORG_FOF_C(점포코드),
                            EPL_ID(가입설계ID),
                            PRPS_DATE(청약일),
                            CNTR_COUNT(심사회차),
                            CNTC_CID (최초상담고객번호) ,
                            PRPS_CNTC_USID (청약상담원사용자ID),
                            SEX_TC (성별),
                            FETUS_YN (태아여부),
                            QA_STTA_PRGST_CD(QA_STTA 상태코드),
                            TA_REQ_DTM(TA 요청일시)
                            CONT_NO(증권번호)
                            IP_CD(보종코드)
                            AGE(나이)
    """
    org_fof_c = job['ORG_FOF_C']
    epl_id = job['EPL_ID']
    prps_date = job['PRPS_DATE']
    cntr_count = job['CNTR_COUNT']
    cntc_cid = job['CNTC_CID']
    prps_cntc_usid = job['PRPS_CNTC_USID']
    iitem = job['IITEM']
    iitem_name = job['IITEM_NM']
    sex_tc = job['SEX_TC']
    fetus_yn = job['FETUS_YN']
    cont_no = job['CONT_NO']
    ip_cd = job['IP_CD']
    age = job['AGE']
    if not age:
        age = 999
    # 0. Setup data
    setup_data(job)
    # Add logging
    logger_args = {
        'base_path': QA_TA_CONFIG['log_dir_path'],
        'log_file_name': "{0}.log".format(OUTPUT_DIR_NAME),
        'log_level': QA_TA_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start QA TA')
    # 1. Connect DB
    oracle = connect_db(logger, 'Oracle')
    try:
        # 2. Update status and select REC file
        rec_info_dict = update_status_and_select_rec_file(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count)
        # 3. copy stt txt
        copy_stt_file(logger, rec_info_dict, org_fof_c, cntc_cid, prps_cntc_usid, cntr_count)
        # 4. execute new lang
        execute_new_lang(logger)
        # 5. Make statistics file
        make_statistics_file(logger)
        # 6. Modify nlp output
        modify_nlp_output_line_number(logger)
        # 7. NLP output
        nlp_output(logger, prps_date)
        # 8. Make matrix file
        matrix_file_path = make_matrix_file(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count, iitem, iitem_name,
                                            sex_tc, fetus_yn, ip_cd, age)
        logger.debug('matrix_file_path ==> {0}'.format(matrix_file_path))
        # 9. Execute HMD
        hmd_output_dir_path = execute_hmd(logger, matrix_file_path)
        # hmd_output_dir_path = "{0}/HMD_result".format(TA_TEMP_DIR_PATH)
        logger.debug('hmd_output_dir_path ==> {0}'.format(hmd_output_dir_path))
        # 10. Sorted HMD output
        sorted_hmd_output_dir_path = sort_hmd_output(logger, hmd_output_dir_path)
        logger.debug('sorted_hmd_output_dir_path ==> {0}'.format(sorted_hmd_output_dir_path))
        # 11. Deduplication HMD output
        dedup_hmd_output_dir_path = dedup_hmd_output(logger, sorted_hmd_output_dir_path)
        logger.debug('dedup_hmd_output_dir_path ==> {0}'.format(dedup_hmd_output_dir_path))
        # 12. Detect section HMD output
        detect_hmd_output_dir_path = detect_sect_hmd_output(logger, dedup_hmd_output_dir_path)
        logger.debug('detect_hmd_output_dir_path ==> {0}'.format(detect_hmd_output_dir_path))
        # 13. Modify HMD output
        final_output_dir_path = modify_hmd_output(logger, detect_hmd_output_dir_path)
        logger.debug('final_output_dir_path ==> {0}'.format(final_output_dir_path))
        # 14. Execute masking
        masking_dir_path = execute_masking(logger, final_output_dir_path)
        logger.debug('masking_dir_path ==> {0}'.format(masking_dir_path))
        # 15. DB upload TB_QA_STT_TM_TA_DTC_RST
        db_insert_tb_tm_qa_ta_sec_info(logger, oracle, masking_dir_path, rec_info_dict, org_fof_c, epl_id, prps_date,
                                       cntr_count, cont_no)
        # 16. DB insert TB_TM_QA_TA_DTC_RST
        db_insert_tb_tm_qa_ta_dtc_rst(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count)
        # 17. DB insert TB_TM_QA_TA_ADT_DTC_RST
        db_insert_tb_tm_qa_ta_adt_dtc_rst(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count)
        # 18. DB insert TB_TM_SEC_DTC_RATE
        db_insert_tb_tm_sec_dtc_rate(logger, oracle, org_fof_c, epl_id, prps_date, cont_no, prps_cntc_usid)
        # 19. Move output
        move_output(logger, prps_date)
        # 20. Delete garbage file
        delete_garbage_file(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.info("TM QA TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("----------    QA TA ERROR   ----------")
        error_process(logger, oracle, org_fof_c, epl_id, prps_date, cntr_count, '12')
        delete_garbage_file(logger)
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # Update status
    logger.info("END.. Update status to QA TA END (13)")
    oracle.update_qa_stta_prgst_cd(
        qa_stta_prgst_cd='13',
        org_fof_c=org_fof_c,
        epl_id=epl_id,
        prps_date=prps_date,
        cntr_count=cntr_count
    )
    oracle.disconnect()
    logger.info("TM QA TA END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    logger.info("Remove logger handler")
    logger.info("----------     QA TA END      ----------")
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job, config_type):
    """
    This is a program that execute TM QA TA
    :param          job:       ORG_FOF_C(점포코드),
                                EPL_ID(가입설계ID),
                                PRPS_DATE(청약일),
                                CNTR_COUNT(심사회차),
                                CNTC_CID (최초상담고객번호) ,
                                PRPS_CNTC_USID (청약상담원사용자ID),
                                SEX_TC (성별),
                                FETUS_YN (태아여부),
                                QA_STTA_PRGST_CD(QA_STTA 상태코드),
                                TA_REQ_DTM(TA 요청일시),
                                CONT_NO(증권번호)
    :param          config_type:        CONFIG type
    """
    global DT
    global ST
    global DB_CONFIG
    global QA_TA_CONFIG
    global MASKING_CONFIG
    DB_CONFIG = cfg.config.DB_CONFIG[config_type]
    QA_TA_CONFIG = cfg.config.QA_TA_CONFIG[config_type]
    MASKING_CONFIG = cfg.config.MASKING_CONFIG
    # save program start time
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
