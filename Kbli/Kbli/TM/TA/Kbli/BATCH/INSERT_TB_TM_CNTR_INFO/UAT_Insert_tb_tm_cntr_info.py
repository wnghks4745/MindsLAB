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

    def select_tcl_prps(self):
        """
        청약 완료된 정보를 불러온다. (개발개와 운영계 db_user 가 다르다.)
        B.CNM -- 고객명
        C.USER_NM -- 상담원명
        :return:
        """
        sql = """
            SELECT
                A.ORG_FOF_C
                , A.EPL_ID
                , A.PRPS_DATE
                , A.CONT_NO
                , A.CNTC_CID
                , A.PRPS_CNTC_USID
                , B.SEX_TC
                , B.AGE
                , B.CNM
                , C.USER_NM 
                ,DECODE(B.RRNO_SECU, '1111111024rYBT9sy1FWzYXOkbTOJuiw==', 'Y', 'N') AS FETUS_YN
            FROM
                {0}.TCL_PRPS@D_KBLUAT_ZREAD A LEFT OUTER JOIN {0}.TCL_USER@D_KBLUAT_ZREAD C
                    ON A.PRPS_CNTC_USID = C.USER_ID
                , {0}.TCL_EPL_CUST@D_KBLUAT_ZREAD B
            WHERE 1=1
                    AND CAST(A.PRPS_DATE AS DATE) BETWEEN CURRENT_DATE - 2 AND CURRENT_DATE - 1
                    AND B.CONT_REL_C = '21'
                    AND A.DEL_YN = 'N'
                    AND A.CONT_NO IS NOT NULL
                    AND A.ORG_FOF_C = B.ORG_FOF_C
                    AND A.EPL_ID =  B.EPL_ID
        """.format(DB_CONFIG['tm_user'])
        self.cursor.execute(sql)

        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def count_tb_tm_cntr_info(self, org_fof_c, epl_id, prps_date):
        """
        TA 완료된 데이터인지 조회
        """
        sql = """
            SELECT
                COUNT(*) cnt
            FROM
                TB_TM_CNTR_INFO
            WHERE 1=1
                AND ORG_FOF_C = :1
                AND EPL_ID = :2
                AND PRPS_DATE = :3
                AND QA_STTA_PRGST_CD = '13'
        """
        bind = (
            org_fof_c
            , epl_id
            , prps_date
        )
        self.cursor.execute(sql, bind)

        row = self.cursor.fetchone()[0]
        return row

    def select_tcl_epl_iitem(self, org_fof_c, epl_id):
        """
        청약 완료된 상품정보를 불러온다.
        :param org_fof_c:           점포코드
        :param epl_id:              가입설계ID
        :return:
        """
        sql = """
            SELECT
                A.ORG_FOF_C
                , A.EPL_ID
                , A.IITEM
                , A.IITEM_GB
                , C.IITEM_NM
            FROM {0}.TCL_EPL_IITEM@D_KBLUAT_ZREAD A
               , {0}.TCL_EPL@D_KBLUAT_ZREAD B
               , {0}.TCL_SP_ITEM_FOF@D_KBLUAT_ZREAD C
            WHERE 1=1
                AND A.ORG_FOF_C = :1
                AND A.EPL_ID = :2
                AND A.EPL_ID = B.EPL_ID
                AND A.ORG_FOF_C = B.ORG_FOF_C
                AND B.ORG_FOF_C = C.ORG_FOF_C
                AND B.PITEM = C.SP_CD
                AND A.IITEM = C.IITEM
        """.format(DB_CONFIG['tm_user'])
        bind = (
            org_fof_c
            , epl_id
        )
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_tcl_cs_cntc_hist(self, org_fof_c, cid, cntc_usid):
        """
        청약 완료된 최근 3달간 녹취 이력을 불러온다.
        :param cid:                 TCL_PRPS의 CNTC_CID 최초상담고객번호
        :param cntc_usid:           TCL_PRPS의 PRPS_CNTC_USID 청약상담원사용자ID
        :return:
        """
        sql = """
            SELECT
                ORG_FOF_C
                , CNTC_HIST_ID
                , CONT_NO
                , CID
                , CNTC_USID
                , CNM
                , REC_NO
                , CNTC_STRT_DATE
                , CNTC_STRT_TIME
                , CALL_END_DATE
                , CALL_END_TIME
                , CALL_TIME
                , DSPT_TNO_SECU
            FROM
                {0}.TCL_CS_CNTC_HIST@D_KBLUAT_ZREAD
            WHERE 1=1
                AND CNTC_STRT_DATE BETWEEN CURRENT_DATE - 91 AND CURRENT_DATE
                AND ORG_FOF_C = :1
                AND CID = :2
                AND CNTC_USID = :3
                AND REC_NO is not null
                AND DEL_YN = 'N'
        """.format(DB_CONFIG['tm_user'])
        bind = (
            org_fof_c
            , cid
            , cntc_usid
        )
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_call_meta(self, org_fof_c, mapping_org_fof_c, cntc_cid, cnm, prps_cntc_usid, cont_no):
        """
        :return:
        """
        sql = """
           SELECT
               :1 AS ORG_FOF_C
               , null AS CNTC_HIST_ID
               , :2 AS CONT_NO
               , :3 AS CID
               , A.AGENT_ID AS CNTC_USID
               , A.REC_ID AS REC_NO
               , TO_CHAR(A.START_DTM, 'YYYYMMDD') AS CNTC_STRT_DATE
               , TO_CHAR(A.START_DTM, 'HH24MISS') AS CNTC_STRT_TIME
               , TO_CHAR(A.END_DTM, 'YYYYMMDD') AS CALL_END_DATE
               , TO_CHAR(A.END_DTM, 'HH24MISS') AS CALL_END_TIME
               , A.DURATION AS CALL_TIME
               , null AS DSPT_TNO_SECU
           FROM
               CALL_META A
           WHERE 1=1
               AND A.PROJECT_CD = 'CD'
               AND A.BRANCH_CD = :4
               AND A.AGENT_ID = :5
               AND A.CUSTOMER_NM = :6
               AND A.DOCUMENT_DT BETWEEN CURRENT_DATE - 91 AND CURRENT_DATE
       """.format(DB_CONFIG['tm_user'])
        bind = (
            org_fof_c
            , cont_no
            , cntc_cid
            , mapping_org_fof_c
            , prps_cntc_usid
            , cnm
        )
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows

    def select_ip_cd_to_tb_scrt_iitem_ip_cd_mapping(self, iitem):
        """
        상품의 스크립트 문장코드 를 조회한다.
        :param iitem:                 상품코드
        :return:
        """
        sql = """
                SELECT
                    IP_CD
                FROM
                    TB_SCRT_IITEM_IP_CD_MAPPING
                WHERE 1=1
                    AND IITEM = :1
        """
        bind = (
            iitem,
        )
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        if rows is bool:
            return False
        if not rows:
            return False
        return rows[0]

    def insert_tb_tm_cntr_info(self, logger, insert_dict):
        """
        가입완료 청약 정보 저장
        :param insert_dict:
        :return:
        """
        try:
            sql = """
            MERGE INTO TB_TM_CNTR_INFO
                USING DUAL
                ON (
                        ORG_FOF_C = :1
                        AND EPL_ID = :2
                        AND PRPS_DATE = :3
                        AND CONT_NO = :4
                )
                WHEN MATCHED THEN
                    UPDATE SET
                        CNTC_CID = :5
                        , CNTC_CID_NM = :6
                        , PRPS_CNTC_USID = :7
                        , PRPS_CNTC_USID_NM = :8
                        , SEX_TC = :9
                        , FETUS_YN = :10
                        , AGE = :11
                        , LST_CHGP_CD = 'TM_TA'
                        , LST_CHG_PGM_ID  = 'TM_TA'
                        , LST_CHG_DTM = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT 
                         (  
                            ORG_FOF_C, EPL_ID, PRPS_DATE, CONT_NO, CNTC_CID
                            , PRPS_CNTC_USID, CNTR_COUNT, SEX_TC, FETUS_YN, QA_STTA_PRGST_CD
                            , REPL_YN, RPRC_YN, AGE
                            , REGP_CD, RGST_PGM_ID, RGST_DTM
                            , LST_CHGP_CD, LST_CHG_PGM_ID, LST_CHG_DTM
                        )
                    VALUES (
                        :12, :13, :14, :15, :16,
                        :17, :18, :19, :20, :21,
                        :22, :23, :24,
                        :25, :26, SYSDATE,
                        :27, :28, SYSDATE
                    )
            """
            bind = (
                insert_dict['ORG_FOF_C']
                , insert_dict['EPL_ID']
                , insert_dict['PRPS_DATE']
                , insert_dict['CONT_NO']
                , insert_dict['CNTC_CID']
                , insert_dict['CNM']
                , insert_dict['PRPS_CNTC_USID']
                , insert_dict['USER_NM']
                , insert_dict['SEX_TC']
                , insert_dict['FETUS_YN']
                , insert_dict['AGE']
                , insert_dict['ORG_FOF_C']
                , insert_dict['EPL_ID']
                , insert_dict['PRPS_DATE']
                , insert_dict['CONT_NO']
                , insert_dict['CNTC_CID']
                , insert_dict['PRPS_CNTC_USID']
                , '0'
                , insert_dict['SEX_TC']
                , insert_dict['FETUS_YN']
                , '10'
                , 'N'
                , 'N'
                , insert_dict['AGE']
                , 'TM_TA'
                , 'TM_TA'
                , 'TM_TA'
                , 'TM_TA'
            )
            self.cursor.execute(sql, bind)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            return False

    def update_ip_cd_to_tb_tm_cntr_info(self, logger, insert_dict, ip_cd):
        """
        보종코드 저장
        :param insert_dict:
        :return:
        """
        try:
            sql = """
                UPDATE
                    TB_TM_CNTR_INFO 
                SET 
                    IP_CD = :1
                WHERE 1=1
                    AND ORG_FOF_C = :2
                    AND EPL_ID = :3
                    AND PRPS_DATE = :4
            """
            bind = (
                ip_cd,
                insert_dict['ORG_FOF_C'],
                insert_dict['EPL_ID'],
                insert_dict['PRPS_DATE'],
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            return False

    def insert_tb_tm_cntr_sp_info(self, logger, result_dict_list):
        """
        가입완료 청약 상품 정보
        :param result_dict_list:        상품정보 리스트
        :return:
        """
        try:
            sql = """
            MERGE INTO TB_TM_CNTR_SP_INFO
                USING DUAL
                ON (
                        ORG_FOF_C  = :1
                        AND EPL_ID = :2
                        AND IITEM = :3
                )
                WHEN MATCHED THEN
                    UPDATE SET
                        IITEM_GB = :4
                        , IITEM_NM = :5
                        , LST_CHGP_CD = 'TM_TA'
                        , LST_CHG_PGM_ID  = 'TM_TA'
                        , LST_CHG_DTM = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT 
                         (  
                            ORG_FOF_C
                            , EPL_ID
                            , IITEM
                            , IITEM_GB
                            , IITEM_NM
                            , REGP_CD
                            , RGST_PGM_ID
                            , RGST_DTM
                            , LST_CHGP_CD
                            , LST_CHG_PGM_ID
                            , LST_CHG_DTM
                        )
                    VALUES (
                        :6, :7, :8, :9, :10, 
                        'TM_TA', 'TM_TA', SYSDATE, 
                        'TM_TA', 'TM_TA', SYSDATE
                    )
            """
            values_list = list()
            for insert_dict in result_dict_list:
                bind = (
                    insert_dict['ORG_FOF_C']
                    , insert_dict['EPL_ID']
                    , insert_dict['IITEM']
                    , insert_dict['IITEM_GB']
                    , insert_dict['IITEM_NM']
                    , insert_dict['ORG_FOF_C']
                    , insert_dict['EPL_ID']
                    , insert_dict['IITEM']
                    , insert_dict['IITEM_GB']
                    , insert_dict['IITEM_NM']
                )
                values_list.append(bind)
            self.cursor.executemany(sql, values_list)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            return False

    def insert_tb_tm_cntr_rcde_info(self, logger, epl_id, prps_date, result_dict_list):
        """
        녹취 정보
        :param
        :param logger:
        :param epl_id:                  가입설계ID
        :param prps_date:               청약일
        :param result_dict_list:        녹취 정보 리스트
        :return:
        """
        try:
            sql = """
                MERGE INTO TB_TM_CNTR_RCDE_INFO
                    USING DUAL
                    ON (
                        ORG_FOF_C = :1
                        AND EPL_ID = :2
                        AND PRPS_DATE = :3
                        AND REC_NO = :4
                    )
                WHEN MATCHED THEN
                    UPDATE SET
                        CNTC_HIST_ID = :5
                        , CID = :6
                        , CNTC_USID = :7
                        , CNTC_STRT_DATE = :8
                        , CNTC_STRT_TIME = :9
                        , CALL_END_DATE = :10
                        , CALL_END_TIME = :11
                        , DURATION = :12
                        , CONT_NO = :13
                        , DSPT_TNO_SECU = :14
                        , LST_CHGP_CD = 'TM_TA'
                        , LST_CHG_PGM_ID  = 'TM_TA'
                        , LST_CHG_DTM = SYSDATE
                    WHEN NOT MATCHED THEN
                        INSERT 
                             (  
                                ORG_FOF_C
                                , EPL_ID
                                , PRPS_DATE
                                , CNTC_HIST_ID
                                , CID
                                , CNTC_USID
                                , REC_NO
                                , CNTC_STRT_DATE
                                , CNTC_STRT_TIME
                                , CALL_END_DATE
                                , CALL_END_TIME
                                , DURATION
                                , CONT_NO
                                , DSPT_TNO_SECU
                                , REGP_CD
                                , RGST_PGM_ID
                                , RGST_DTM
                                , LST_CHGP_CD
                                , LST_CHG_PGM_ID
                                , LST_CHG_DTM
                            )
                        VALUES (
                            :15, :16, :17, :18, :19,
                            :20, :21, :22, :23, :24,
                            :25, :26, :27, :28,
                            :29, :30, SYSDATE, 
                            :31, :32, SYSDATE
                        )
                """
            values_list = list()
            for insert_dict in result_dict_list:
                bind = (
                    insert_dict['ORG_FOF_C']
                    , epl_id
                    , prps_date
                    , insert_dict['REC_NO']
                    , insert_dict['CNTC_HIST_ID']
                    , insert_dict['CID']
                    , insert_dict['CNTC_USID']
                    , insert_dict['CNTC_STRT_DATE']
                    , insert_dict['CNTC_STRT_TIME']
                    , insert_dict['CALL_END_DATE']
                    , insert_dict['CALL_END_TIME']
                    , insert_dict['CALL_TIME']
                    , insert_dict['CONT_NO']
                    , insert_dict['DSPT_TNO_SECU']
                    , insert_dict['ORG_FOF_C']
                    , epl_id
                    , prps_date
                    , insert_dict['CNTC_HIST_ID']
                    , insert_dict['CID']
                    , insert_dict['CNTC_USID']
                    , insert_dict['REC_NO']
                    , insert_dict['CNTC_STRT_DATE']
                    , insert_dict['CNTC_STRT_TIME']
                    , insert_dict['CALL_END_DATE']
                    , insert_dict['CALL_END_TIME']
                    , insert_dict['CALL_TIME']
                    , insert_dict['CONT_NO']
                    , insert_dict['DSPT_TNO_SECU']
                    , 'TM_TA'
                    , 'TM_TA'
                    , 'TM_TA'
                    , 'TM_TA'
                )
                values_list.append(bind)
            self.cursor.executemany(sql, values_list)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            exc_info = traceback.format_exc()
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
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    log_file_path = "{0}/{1}".format(CONFIG['log_dir_path'], CONFIG['log_file_name'])
    if not os.path.exists(CONFIG['log_dir_path']):
        os.makedirs(CONFIG['log_dir_path'])
    logger = get_logger(log_file_path, logging.DEBUG)
    logger.info("----------------------------------------------"*2)
    logger.info("Start Insert TB_TM_CNTR_INFO {0}".format(st))
    oracle = ''
    try:
        logger.info("#0 Connect_db Oracle")
        oracle = connect_db('Oracle')
        if not oracle:
            logger.error("---------- Can't connect db ----------")
            sys.exit(1)

        logger.info("#1 Select TCL_PRPS")
        tcl_prps_results = oracle.select_tcl_prps()
        for tcl_prps_result in tcl_prps_results:
            org_fof_c = tcl_prps_result['ORG_FOF_C']
            epl_id = tcl_prps_result['EPL_ID']
            prps_date = tcl_prps_result['PRPS_DATE']
            cont_no = tcl_prps_result['CONT_NO']
            if oracle.count_tb_tm_cntr_info(org_fof_c, epl_id, prps_date) != 0:
                logger.debug("exist {0} {1} {2}".format(org_fof_c, epl_id, prps_date))
                continue
            if not oracle.insert_tb_tm_cntr_info(logger, tcl_prps_result):
                logger.error(tcl_prps_result)
                logger.error("#1 Insert TB_TM_CNTR_INFO ")
                continue
            # logger.debug(tcl_prps_result)

            # 상품정보
            logger.info("#2 Select TCL_EPL_IITEM")
            tcl_epl_iitem_results = oracle.select_tcl_epl_iitem(org_fof_c, epl_id)

            # 보종코드 저장 : cl+wi 공통
            ip_cd = 'cl+wi'
            if tcl_epl_iitem_results:
                for item in tcl_epl_iitem_results:
                    logger.debug(item)
                    if item['IITEM_GB'] == '1':
                        temp_ip_cd = oracle.select_ip_cd_to_tb_scrt_iitem_ip_cd_mapping(item['IITEM'])
                        if temp_ip_cd:
                            ip_cd = temp_ip_cd['IP_CD']
                if not oracle.insert_tb_tm_cntr_sp_info(logger, tcl_epl_iitem_results):
                    logger.error("#2 Insert TB_TM_CNTR_SP_INFO ")
            oracle.update_ip_cd_to_tb_tm_cntr_info(logger, tcl_prps_result, ip_cd)

            # 녹취정보
            logger.info("#3 Select TCL_CS_CNTC_HIST")
            cntc_cid = tcl_prps_result['CNTC_CID']
            prps_cntc_usid = tcl_prps_result['PRPS_CNTC_USID']
            tcl_cs_cntc_hist_results = oracle.select_tcl_cs_cntc_hist(org_fof_c, cntc_cid, prps_cntc_usid)
            if tcl_cs_cntc_hist_results:
                if not oracle.insert_tb_tm_cntr_rcde_info(logger, epl_id, prps_date, tcl_cs_cntc_hist_results):
                    logger.error("#3 Insert TCL_CS_CNTC_HIST_RESULTS to TB_TM_CNTR_RCDE_INFO")

            # FIXME :: 현업에서 지점코드 매핑을 모른다고 해서 하드코딩
            temp_mapping_dict = {
                '7600': 'IN07',
                '7601': 'IN13'
            }
            # FIXME :: 현업에서 지점코드 매핑을 모른다고 해서 하드코딩
            mapping_org_fof_c = temp_mapping_dict.get(org_fof_c)
            cnm = tcl_prps_result['CNM']
            if mapping_org_fof_c:
                call_meta_results = oracle.select_call_meta(org_fof_c, mapping_org_fof_c, cntc_cid, cnm, prps_cntc_usid, cont_no)
                if call_meta_results:
                    if not oracle.insert_tb_tm_cntr_rcde_info(logger, epl_id, prps_date, call_meta_results):
                        logger.error("#4 Insert CALL_META_RESULTS to TB_TM_CNTR_RCDE_INFO")

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
