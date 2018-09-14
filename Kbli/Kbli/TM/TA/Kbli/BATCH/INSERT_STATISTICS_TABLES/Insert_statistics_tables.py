#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-04-12, modification: 2018-04-16"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
import logging
import argparse
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timedelta
import cfg.config
from lib.openssl import decrypt_string


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#############
# constants #
#############
INSERT_CNT = 0
DELETE_CNT = 0
CONFIG = {}
DB_CONFIG = {}


#########
# class #
#########
class Oracle(object):
    def __init__(self):
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

    def select_adt_count(self):
        """
        금칙어 사용 빈도 통계 조회 (수정일 하루 전일 기준)
        :return:
        """
        sql = """
            SELECT
                ORG_FOF_C
                , EPL_ID
                , PRPS_DATE
                , CONT_NO
                , USER_OFFC_CD
                , USER_PART_CD
                , USER_ID
                , USER_NM
                , SNTC_NO
                , DTC_SNTC_NM
                , cnt
                , (SELECT B.ORG_PART_C
                      FROM {0}.TCL_ORG_PART B
                     WHERE B.ORG_FOF_C = TEMP.USER_OFFC_CD
                       AND B.ORG_PART_C = TEMP.USER_PART_CD
                       AND ROWNUM = 1) AS PRPS_USER_PART_C
                , (SELECT B.ORG_PART_NM
                      FROM {0}.TCL_ORG_PART B
                     WHERE B.ORG_FOF_C = TEMP.USER_OFFC_CD
                       AND B.ORG_PART_C = TEMP.USER_PART_CD
                       AND ROWNUM = 1) AS PRPS_USER_PART_C_NM
                , (SELECT ORG_FOF_NM
                     FROM {0}.TCL_ORG
                    WHERE ORG_FOF_C = TEMP.ORG_FOF_C) AS ORG_FOF_C_NM
            FROM
                (
                SELECT
                    A.ORG_FOF_C
                    , A.EPL_ID
                    , A.PRPS_DATE
                    , A.CONT_NO
                    , D.USER_OFFC_CD
                    , D.USER_PART_CD
                    , B.CNTC_USID AS USER_ID
                    , D.USER_NM
                    , A.SNTC_NO
                    , C.DTC_SNTC_NM
                    , COUNT(*) cnt
                FROM
                    TB_TM_QA_TA_ADT_DTC_RST A
                        LEFT OUTER JOIN TB_SCRT_SNTC_MST_INFO C
                            ON A.SNTC_NO = C.SNTC_NO
                                AND A.DTC_SNTC_CD = C.DTC_SNTC_CD
                                AND C.SNTC_DCD = '04'
                    , TB_TM_CNTR_RCDE_INFO B
                        LEFT OUTER JOIN {0}.TCL_USER D
                            ON B.CNTC_USID = D.USER_ID
                WHERE 1=1
                    AND CAST(A.PRPS_DATE AS DATE) BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE - 1
                    AND A.ORG_FOF_C = B.ORG_FOF_C
                    AND A.EPL_ID = B.EPL_ID
                    AND A.PRPS_DATE = B.PRPS_DATE
                    AND A.REC_ID = B.REC_NO
                    AND A.USER_ADT_YN = 'Y'
                GROUP BY
                    A.ORG_FOF_C
                    , A.EPL_ID
                    , A.PRPS_DATE
                    , A.CONT_NO
                    , D.USER_OFFC_CD
                    , D.USER_PART_CD
                    , B.CNTC_USID
                    , D.USER_NM
                    , A.SNTC_NO
                    , C.DTC_SNTC_NM
                ) TEMP
            ORDER BY
                USER_ID, SNTC_NO
        """.format(DB_CONFIG['tm_user'])
        self.cursor.execute(sql)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_spch_sped_avg(self):
        """
        상담원별 발화속도 통계 조회 (수정일 하루 전일 기준)
        :return:
        """
        sql = """
            SELECT
                ORG_FOF_C,
                EPL_ID,
                PRPS_DATE,
                CONT_NO,
                USER_OFFC_CD,
                USER_PART_CD,
                USER_ID,
                USER_NM,
                REC_ID,
                (
                    SELECT
                        B.ORG_PART_C
                    FROM
                        {0}.TCL_ORG_PART B
                    WHERE
                        B.ORG_FOF_C = TEMP.USER_OFFC_CD
                        AND   B.ORG_PART_C = TEMP.USER_PART_CD
                        AND   ROWNUM = 1
                ) AS PRPS_USER_PART_C,
                (
                    SELECT
                        B.ORG_PART_NM
                    FROM
                        {0}.TCL_ORG_PART B
                    WHERE
                        B.ORG_FOF_C = TEMP.USER_OFFC_CD
                        AND   B.ORG_PART_C = TEMP.USER_PART_CD
                        AND   ROWNUM = 1
                ) AS PRPS_USER_PART_C_NM,
                (
                    SELECT
                        ORG_FOF_NM
                    FROM
                        {0}.TCL_ORG
                    WHERE
                        ORG_FOF_C = TEMP.ORG_FOF_C
                ) AS ORG_FOF_C_NM,
                ROUND(AVG(TEMP.STT_SNTC_SPCH_SPED),0) AS SNTC_SPCH_SPED_AVG
            FROM
                (
                    SELECT
                        A.ORG_FOF_C,
                        A.EPL_ID,
                        A.PRPS_DATE,
                        A.CONT_NO,
                        D.USER_OFFC_CD,
                        D.USER_PART_CD,
                        B.CNTC_USID AS USER_ID,
                        D.USER_NM,
                        C.REC_ID,
                        C.STT_SNTC_SPCH_SPED
                    FROM
                        TB_TM_CNTR_INFO A
                        , TB_TM_CNTR_RCDE_INFO B
                            LEFT OUTER JOIN {0}.TCL_USER D ON B.CNTC_USID = D.USER_ID,
                        TB_TM_STT_RST C
                    WHERE
                        1 = 1
                        AND CAST(A.LST_CHG_DTM AS DATE) BETWEEN CURRENT_DATE - 2 AND CURRENT_DATE - 1
                        AND A.QA_STTA_PRGST_CD = '13'
                        AND A.ORG_FOF_C = B.ORG_FOF_C
                        AND A.EPL_ID = B.EPL_ID
                        AND A.PRPS_DATE = B.PRPS_DATE
                        AND B.REC_NO = C.REC_ID
                ) TEMP
            GROUP BY
                ORG_FOF_C,
                EPL_ID,
                PRPS_DATE,
                CONT_NO,
                USER_OFFC_CD,
                USER_PART_CD,
                USER_ID,
                USER_NM,
                REC_ID
        """.format(DB_CONFIG['tm_user'])
        self.cursor.execute(sql)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def insert_tb_tm_adt_count(self, logger, values_tuple):
        """
        금칙어 사용 빈도 통계 저장
        :param values_tuple:
        :return:
        """
        try:
            sql = """
            MERGE INTO TB_TM_ADT_COUNT
                USING DUAL
                ON (
                        ORG_FOF_C = :1
                        AND EPL_ID = :2
                        AND PRPS_DATE = :3
                        AND PRPS_CNTC_USID = :4
                        AND ADT_CD = :5
                )
                WHEN MATCHED THEN
                    UPDATE SET 
                        ORG_FOF_C_NM = :6
                        , PRPS_USER_PART_C = :7
                        , PRPS_USER_PART_C_NM = :8
                        , PRPS_CNTC_USID_NM = :9
                        , ADT_COUNT = :10
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
                            , ADT_CD
                            , ADT_COUNT
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
                        :21, 
                        'TM_QA_TA', 'TM_QA_TA', SYSDATE,
                        'TM_QA_TA', 'TM_QA_TA', SYSDATE
                    )
            """
            self.cursor.execute(sql, values_tuple)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error(values_tuple)
            logger.error(exc_info)
            return False

    def insert_tb_tm_spch_sped_avg(self, logger, values_tuple):
        """
        상담원별 발화속도 저장
        :param values_tuple:
        :return:
        """
        try:
            sql = """
            MERGE INTO TB_TM_SPCH_SPED_AVG
                USING DUAL
                ON (
                        ORG_FOF_C = :1
                        AND EPL_ID = :2
                        AND PRPS_DATE = :3
                        AND PRPS_CNTC_USID = :4
                )
                WHEN MATCHED THEN
                    UPDATE SET 
                        ORG_FOF_C_NM = :5
                        , PRPS_USER_PART_C = :6
                        , PRPS_USER_PART_C_NM = :7
                        , PRPS_CNTC_USID_NM = :8
                        , SNTC_SPCH_SPED_AVG = :9
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
                        , SNTC_SPCH_SPED_AVG
                        , REGP_CD
                        , RGST_PGM_ID
                        , RGST_DTM
                        , LST_CHGP_CD
                        , LST_CHG_PGM_ID
                        , LST_CHG_DTM
                    )
                VALUES (
                    :10, :11, :12, :13, :14, 
                    :15, :16, :17, :18, :19,
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE,
                    'TM_QA_TA', 'TM_QA_TA', SYSDATE
                )
            """
            self.cursor.execute(sql, values_tuple)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error(values_tuple)
            logger.error(exc_info)
            return False

#######
# def #
#######


class MyFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, fmt=None):
        ct = self.converter(record.created)
        if fmt:
            s = ct.strftime(fmt)
        else:
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = '%s.%03d' % (t, record.msecs)
        return s


def get_logger(fname, log_level):
    """
    Set logger
    :param      fname:          Log file name
    :param      log_level:      Log level
    :return:                    Logger
    """
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='%(asctime)s - %(levelname)s [%(lineno)d] - %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('TB_TM_CNTR_INFO_Logger')
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger


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


def connect_db(db):
    """
    Connect database
    :param      db:         Database
    :return                 SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = "Korean_Korea.KO16KSC5601"
                sql = Oracle()
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            print exc_info
            if cnt < 3:
                print "Fail connect {0}, retrying count = {1}".format(db, cnt)
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def processing():
    """
    processing
    :param      type:                   Type( Insert, Delete )
    :param      args:                   Arguments
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    log_file_path = "{0}/{1}".format(CONFIG['log_dir_path'], CONFIG['log_file_name'])
    if not os.path.exists(CONFIG['log_dir_path']):
        os.makedirs(CONFIG['log_dir_path'])
    logger = get_logger(log_file_path, logging.DEBUG)
    logger.info("----------------------------------------------"*2)
    logger.info("Start Insert statistics tables {0}".format(st))
    oracle = ''
    try:
        logger.debug("#0 Connect_db Oracle")
        oracle = connect_db('Oracle')
        if not oracle:
            logger.error("---------- Can't connect db ----------")
            sys.exit(1)

        adt_count_dict_list = oracle.select_adt_count()
        # logger.debug('adt_count_dict_list >> {0}'.format(adt_count_dict_list))
        if adt_count_dict_list:
            # 금칙어 사용 빈도 통계
            for insert_dict in adt_count_dict_list:
                org_fof_c = insert_dict['ORG_FOF_C']
                epl_id = insert_dict['EPL_ID']
                prps_date = insert_dict['PRPS_DATE']
                cont_no = insert_dict['CONT_NO']
                org_fof_c_nm = insert_dict['ORG_FOF_C_NM']
                prps_user_part_c = insert_dict['PRPS_USER_PART_C']
                prps_user_part_c_nm = insert_dict['PRPS_USER_PART_C_NM']
                prps_cntc_usid = insert_dict['USER_ID']
                prps_cntc_usid_nm = insert_dict['USER_NM']
                adt_cd = insert_dict['SNTC_NO']
                adt_count = insert_dict['CNT']
                values_tuple = (
                    org_fof_c, epl_id, prps_date, prps_cntc_usid, adt_cd, org_fof_c_nm, prps_user_part_c, prps_user_part_c_nm
                    , prps_cntc_usid_nm, adt_count, org_fof_c, epl_id, prps_date, cont_no
                    , org_fof_c_nm, prps_user_part_c, prps_user_part_c_nm, prps_cntc_usid, prps_cntc_usid_nm, adt_cd
                    , adt_count,
                )
                oracle.insert_tb_tm_adt_count(logger, values_tuple)
        spch_sped_avg_dict_list = oracle.select_spch_sped_avg()
        # logger.debug('spch_sped_avg_dict_list >> {0}'.format(spch_sped_avg_dict_list))
        if spch_sped_avg_dict_list:
            # 상담원별 발화속도
            for insert_dict in spch_sped_avg_dict_list:
                org_fof_c = insert_dict['ORG_FOF_C']
                epl_id = insert_dict['EPL_ID']
                prps_date = insert_dict['PRPS_DATE']
                cont_no = insert_dict['CONT_NO']
                org_fof_c_nm = insert_dict['ORG_FOF_C_NM']
                prps_user_part_c = insert_dict['PRPS_USER_PART_C']
                prps_user_part_c_nm = insert_dict['PRPS_USER_PART_C_NM']
                prps_cntc_usid = insert_dict['USER_ID']
                prps_cntc_usid_nm = insert_dict['USER_NM']
                sntc_spch_sped_avg = insert_dict['SNTC_SPCH_SPED_AVG']
                values_tuple = (
                    org_fof_c, epl_id, prps_date, prps_cntc_usid, org_fof_c_nm, prps_user_part_c, prps_user_part_c_nm
                    , prps_cntc_usid_nm, sntc_spch_sped_avg, org_fof_c, epl_id, prps_date, cont_no
                    , org_fof_c_nm, prps_user_part_c, prps_user_part_c_nm, prps_cntc_usid, prps_cntc_usid_nm
                    , sntc_spch_sped_avg,
                )
                oracle.insert_tb_tm_spch_sped_avg(logger, values_tuple)

        oracle.disconnect()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        oracle.disconnect()
    logger.info("END.. Start time = {0}, The time required = {1}".format(st, elapsed_time(dt)))


########
# main #
########
def main(config_type):
    """
    This is a program that Insert Script
    :param      args:       Arguments
    """
    try:
        global CONFIG
        global DB_CONFIG
        CONFIG = cfg.config.CONFIG
        DB_CONFIG = cfg.config.DB_CONFIG[config_type]
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    arguments = parser.parse_args()
    config_type = arguments.config_type
    main(config_type)
