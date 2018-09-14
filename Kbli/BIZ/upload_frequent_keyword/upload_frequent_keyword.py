#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-21, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
from operator import itemgetter
from datetime import datetime, timedelta
from lib.iLogger import set_logger_period_of_time
from cfg.config import CONFIG
sys.path.append(os.path.join(os.getenv('MAUM_ROOT'), "lib/python"))
from common.config import Config
from common.openssl import decrypt_string

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ''
DT = ''
CREATOR_ID = 'CS_BAT'


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.conf = Config()
        self.conf.init('biz.conf')
        self.dsn_tns = self.conf.get('oracle.dsn').strip()
        passwd = decrypt_string(self.conf.get('oracle.passwd'))
        self.conn = cx_Oracle.connect(
            self.conf.get('oracle.user'),
            passwd,
            self.dsn_tns
        )
        self.logger = logger
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_keyword_frequent(self, call_date, call_type_code):
        query = """
            SELECT
                T2.WORD,
                SUM(T2.CNT) AS WORD_COUNT,
                T2.CNTC_USER_DEPART_C,
                T2.CNTC_USER_DEPART_NM,
                T2.CNTC_USER_PART_C,
                T2.CNTC_USER_PART_NM
            FROM
                (
                    SELECT
                        A.WORD,
                        E.CALL_ID,
                        COUNT(A.WORD) CNT,
                        E.CNTC_USER_DEPART_C,
                        E.CNTC_USER_DEPART_NM,
                        E.CNTC_USER_PART_C,
                        E.CNTC_USER_PART_NM
                    FROM
                        (
                            SELECT
                                WORD,
                                NLP_RESULT_ID
                            FROM
                                TA_NLP_RESULT_KEYWORD_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                                AND (
                                        SELECT
                                            COUNT(T.STOPWORD_CONT)
                                        FROM
                                            CM_STOPWORD_TB T
                                        WHERE
                                            TRIM(WORD) = T.STOPWORD_CONT
                                    ) = 0 
                        ) A,
                        (
                            SELECT
                                NLP_RESULT_ID,
                                STT_RESULT_DETAIL_ID
                            FROM
                                TA_NLP_RESULT_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE (:2, 'YYYY-MM-DD') 
                        ) B,
                        (
                            SELECT
                                STT_RESULT_ID,
                                STT_RESULT_DETAIL_ID
                            FROM
                                STT_RESULT_DETAIL_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE (:3, 'YYYY-MM-DD')
                                AND SPEAKER_CODE = 'ST0001'
                        ) C, 
                        (
                            SELECT
                                CALL_ID,
                                STT_RESULT_ID
                            FROM
                                STT_RESULT_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE (:4, 'YYYY-MM-DD')
                        ) D,
                        (
                            SELECT
                                CALL_ID,
                                CNTC_USER_DEPART_C,
                                CNTC_USER_DEPART_NM,
                                CNTC_USER_PART_C,
                                CNTC_USER_PART_NM
                            FROM
                                CM_CALL_META_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE (:5, 'YYYY-MM-DD')
                                AND PROJECT_CODE = 'PC0001'
                                AND CALL_TYPE_CODE = :6
                        ) E
                    WHERE 1=1
                        AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                        AND B.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                        AND C.STT_RESULT_ID = D.STT_RESULT_ID
                        AND D.CALL_ID = E.CALL_ID
                    GROUP BY
                        A.WORD,
                        E.CALL_ID,
                        E.CNTC_USER_DEPART_C,
                        E.CNTC_USER_DEPART_NM,
                        E.CNTC_USER_PART_C,
                        E.CNTC_USER_PART_NM
                ) T2
            GROUP BY
                T2.WORD,
                T2.CNTC_USER_DEPART_C,
                T2.CNTC_USER_DEPART_NM,
                T2.CNTC_USER_PART_C,
                T2.CNTC_USER_PART_NM
        """
        bind = (
            call_date,
            call_date,
            call_date,
            call_date,
            call_date,
            call_type_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_document_frequent(self, **kwargs):
        query = """
            SELECT DISTINCT
                E.CALL_ID
            FROM
                (
                    SELECT
                        NLP_RESULT_ID
                    FROM
                        TA_NLP_RESULT_KEYWORD_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        AND WORD = :2
                ) A,
                (
                    SELECT
                        NLP_RESULT_ID,
                        STT_RESULT_DETAIL_ID
                    FROM
                        TA_NLP_RESULT_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:3, 'YYYY-MM-DD')
                ) B,
                (
                    SELECT
                        STT_RESULT_ID,
                        STT_RESULT_DETAIL_ID
                    FROM
                        STT_RESULT_DETAIL_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:4, 'YYYY-MM-DD')
                        AND SPEAKER_CODE = 'ST0001'
                ) C,
                (
                    SELECT
                        CALL_ID,
                        STT_RESULT_ID
                    FROM
                        STT_RESULT_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:5, 'YYYY-MM-DD')
                ) D,
                (
                    SELECT
                        CALL_ID
                    FROM
                        CM_CALL_META_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:6, 'YYYY-MM-DD')
                        AND PROJECT_CODE = 'PC0001'
                        AND CALL_TYPE_CODE = :7
                        AND CNTC_USER_DEPART_C = :8
                        AND CNTC_USER_DEPART_NM = :9
                        AND CNTC_USER_PART_C = :10
                        AND CNTC_USER_PART_NM = :11
                ) E
            WHERE 1=1 
                AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                AND B.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                AND C.STT_RESULT_ID = D.STT_RESULT_ID
                AND D.CALL_ID = E.CALL_ID
        """
        bind = (
            kwargs.get('call_date'),
            kwargs.get('word'),
            kwargs.get('call_date'),
            kwargs.get('call_date'),
            kwargs.get('call_date'),
            kwargs.get('call_date'),
            kwargs.get('call_type_code'),
            kwargs.get('cntc_user_depart_c'),
            kwargs.get('cntc_user_depart_nm'),
            kwargs.get('cntc_user_part_c'),
            kwargs.get('cntc_user_part_nm'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return 0
        if not result:
            return 0
        return len(result)

    def select_related_keyword_sentence_step_one(self, call_date, call_type_code):
        query = """
            SELECT 
                A.WORD,
                A.NLP_RESULT_ID,
                B.STT_RESULT_DETAIL_ID,
                E.CALL_ID,
                E.CNTC_USER_DEPART_C,
                E.CNTC_USER_DEPART_NM,
                E.CNTC_USER_PART_C,
                E.CNTC_USER_PART_NM
            FROM
                (
                    SELECT
                        WORD,
                        NLP_RESULT_ID
                    FROM
                        TA_NLP_RESULT_KEYWORD_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        AND (
                                SELECT
                                    COUNT(T.STOPWORD_CONT)
                                FROM
                                    CM_STOPWORD_TB T
                                WHERE
                                    TRIM(WORD) = T.STOPWORD_CONT
                            ) = 0
                ) A,
                (
                    SELECT
                        NLP_RESULT_ID,
                        STT_RESULT_DETAIL_ID
                    FROM
                        TA_NLP_RESULT_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:2, 'YYYY-MM-DD')
                ) B,
                (
                    SELECT
                        STT_RESULT_ID,
                        STT_RESULT_DETAIL_ID
                    FROM
                        STT_RESULT_DETAIL_TB 
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:3, 'YYYY-MM-DD')
                        AND SPEAKER_CODE = 'ST0001'
                ) C, 
                (
                    SELECT
                        CALL_ID,
                        STT_RESULT_ID
                    FROM
                        STT_RESULT_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:4, 'YYYY-MM-DD')
                ) D,
                (
                    SELECT
                        CALL_ID,
                        CNTC_USER_DEPART_C,
                        CNTC_USER_DEPART_NM,
                        CNTC_USER_PART_C,
                        CNTC_USER_PART_NM
                    FROM
                        CM_CALL_META_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:5, 'YYYY-MM-DD')
                        AND PROJECT_CODE = 'PC0001'
                        AND CALL_TYPE_CODE = :6
                ) E
            WHERE 1=1
                AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                AND B.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                AND C.STT_RESULT_ID = D.STT_RESULT_ID
                AND D.CALL_ID = E.CALL_ID
        """
        bind = (
            call_date,
            call_date,
            call_date,
            call_date,
            call_date,
            call_type_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_related_keyword_sentence_step_two(self, call_date, call_type_code):
        query = """
            SELECT 
                WORD,
                NLP_FREQUENT_KEYWORD_ID,
                CNTC_USER_DEPART_C,
                CNTC_USER_DEPART_NM,
                CNTC_USER_PART_C,
                CNTC_USER_PART_NM
            FROM
                TA_NLP_FREQUENT_KEYWORD_TB
            WHERE 1=1
                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                AND CALL_TYPE_CODE = :2
                AND SPEAKER_CODE = 'ST0001'
        """
        bind = (
            call_date,
            call_type_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_related_keyword(self, call_date, call_type_code):
        query = """
            SELECT
                NLP_FREQUENT_KEYWORD_ID,
                WORD,
                COUNT(WORD) AS WORD_CNT
            FROM
                (
                    SELECT
                        CC.WORD,
                        AA.NLP_FREQUENT_KEYWORD_ID
                    FROM
                        (
                            SELECT
                                WORD,
                                NLP_RESULT_ID
                            FROM
                                TA_NLP_RESULT_KEYWORD_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) CC,
                        (
                            SELECT
                                WORD,
                                NLP_FREQUENT_KEYWORD_ID
                            FROM
                                TA_NLP_FREQUENT_KEYWORD_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:2, 'YYYY-MM-DD')
                        ) AA,
                        (
                            SELECT
                                NLP_RESULT_ID,
                                NLP_FREQUENT_KEYWORD_ID
                            FROM
                                TA_NLP_FREQUENT_KYWD_DETAIL_TB 
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:3, 'YYYY-MM-DD')
                        ) BB,
                        (
                            SELECT
                                STT_RESULT_ID,
                                STT_RESULT_DETAIL_ID
                            FROM
                                STT_RESULT_DETAIL_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:4, 'YYYY-MM-DD')
                                AND SPEAKER_CODE = 'ST0001'
                        ) DD,
                        (
                            SELECT
                                CALL_ID,
                                STT_RESULT_ID
                            FROM
                                STT_RESULT_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:5, 'YYYY-MM-DD')
                        ) EE,
                        (
                            SELECT
                                CALL_ID
                            FROM
                                CM_CALL_META_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:6, 'YYYY-MM-DD')
                                AND PROJECT_CODE = 'PC0001'
                                AND CALL_TYPE_CODE = :7
                        ) FF,
                        (
                            SELECT
                                NLP_RESULT_ID,
                                STT_RESULT_DETAIL_ID
                            FROM
                                TA_NLP_RESULT_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:8, 'YYYY-MM-DD') 
                        ) GG
                    WHERE 1=1
                        AND CC.NLP_RESULT_ID = GG.NLP_RESULT_ID
                        AND GG.STT_RESULT_DETAIL_ID = DD.STT_RESULT_DETAIL_ID
                        AND DD.STT_RESULT_ID = EE.STT_RESULT_ID
                        AND EE.CALL_ID = FF.CALL_ID
                        AND CC.NLP_RESULT_ID = BB.NLP_RESULT_ID
                        AND AA.NLP_FREQUENT_KEYWORD_ID = BB.NLP_FREQUENT_KEYWORD_ID
                        AND AA.WORD != CC.WORD
                )
            GROUP BY
                NLP_FREQUENT_KEYWORD_ID,
                WORD
        """
        bind = (
            call_date,
            call_date,
            call_date,
            call_date,
            call_date,
            call_date,
            call_type_code,
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def insert_frequent_keyword_tb(self, **kwargs):
        query = """
            INSERT INTO TA_NLP_FREQUENT_KEYWORD_TB
            (
                CALL_DATE,
                PROJECT_CODE,
                WORD,
                SPEAKER_CODE,
                WORD_FREQUENT,
                DOCUMENT_FREQUENT,
                CALL_TYPE_CODE,
                CNTC_USER_DEPART_C,
                CNTC_USER_DEPART_NM,
                CNTC_USER_PART_C,
                CNTC_USER_PART_NM,
                CREATED_DTM,
                UPDATED_DTM,
                CREATOR_ID,
                UPDATOR_ID
            )
            VALUES
            (
                TO_DATE(:1, 'YYYY-MM-DD'), 
                :2, :3, :4, :5, :6,
                :7, :8, :9, :10, :11,
                SYSDATE, SYSDATE, :12, :13
            )
        """
        bind = (
            kwargs.get('call_date'),
            kwargs.get('project_code'),
            kwargs.get('word'),
            kwargs.get('speaker_code'),
            kwargs.get('word_frequent'),
            kwargs.get('doc_frequent'),
            kwargs.get('call_type_code'),
            kwargs.get('cntc_user_depart_c'),
            kwargs.get('cntc_user_depart_nm'),
            kwargs.get('cntc_user_part_c'),
            kwargs.get('cntc_user_part_nm'),
            CREATOR_ID,
            CREATOR_ID,
        )
        self.cursor.execute(query, bind)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def insert_frequent_keyword_detail_tb(self, values_list):
        query = """
        INSERT INTO TA_NLP_FREQUENT_KYWD_DETAIL_TB
        (
            NLP_FREQUENT_KEYWORD_ID,
            NLP_RESULT_ID,
            STT_RESULT_DETAIL_ID,
            CALL_DATE,
            PROJECT_CODE,
            CALL_TYPE_CODE,
            CALL_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY-MM-DD'),
            :5, :6, :7, SYSDATE,
            SYSDATE, :8, :9
        )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def insert_related_keyword_tb(self, values_list):
        query = """
        INSERT INTO TA_RELATED_KEYWORD_TB
        (
            NLP_FREQUENT_KEYWORD_ID,
            CALL_DATE,
            RELATED_WORD,
            PROJECT_CODE,
            CALL_TYPE_CODE,
            RANK,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, TO_DATE(:2, 'YYYY-MM-DD'),
            :3, :4, :5, :6, SYSDATE,
            SYSDATE, :7, :8
        )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_frequent_data(self, call_date, call_type_code):
        self.logger.info("0-1. Delete frequency data")
        try:
            query = """
                DELETE FROM
                    TA_NLP_FREQUENT_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND CALL_TYPE_CODE = :2
            """
            bind = (
                call_date,
                call_type_code,
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

    def delete_related_data(self, call_date, call_type_code):
        self.logger.info("0-2. Delete related data")
        try:
            query = """
                DELETE FROM
                    TA_RELATED_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND CALL_TYPE_CODE = :2
            """
            bind = (
                call_date,
                call_type_code,
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

    def delete_frequent_detail_data(self, call_date, call_type_code):
        self.logger.info("0-3. Delete frequent detail data")
        try:
            query = """
                DELETE FROM
                    TA_NLP_FREQUENT_KYWD_DETAIL_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND CALL_TYPE_CODE = :2
            """
            bind = (
                call_date,
                call_type_code,
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


def connect_db(logger):
    """
    Connect database
    :param          logger:         Logger
    :return                         SQL Object
    """
    # Connect DB
    logger.debug('Connect Oracle DB ...')
    sql = False
    for cnt in range(1, 4):
        try:
            os.environ["NLS_LANG"] = ".AL32UTF8"
            sql = Oracle(logger)
            logger.debug("Success connect Oracle DB ...")
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect Oracle DB, retrying count = {0}".format(cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def extract_frequent_keyword(logger, oracle, call_date, call_type_code):
    """
    Extract frequent keyword
    :param          logger:                 Logger
    :param          oracle:                 Oracle
    :param          call_date:              CALL_DATE
    :param          call_type_code:         CALL_TYPE_CODE
    """
    logger.info("1. Select keyword frequent")
    result = oracle.select_keyword_frequent(call_date, call_type_code)
    frequent_dict = dict()
    if result:
        logger.info("Word keyword count = {0}".format(len(result)))
        for item in result:
            word = item[0]
            if '*' in word:
                continue
            word_cnt = int(item[1])
            cntc_user_depart_c = item[2]
            cntc_user_depart_nm = item[3]
            cntc_user_part_c = item[4]
            cntc_user_part_nm = item[5]
            key = "{0}!@#${1}!@#${2}!@#${3}".format(
                cntc_user_depart_c, cntc_user_depart_nm, cntc_user_part_c, cntc_user_part_nm)
            if key not in frequent_dict:
                frequent_dict[key] = {word: word_cnt}
            else:
                if word in frequent_dict[key]:
                    frequent_dict[key][word] += word_cnt
                else:
                    frequent_dict[key].update({word: word_cnt})
        logger.info("2. Insert keyword frequent")
        logger.debug("key count = {0}".format(len(frequent_dict)))
        for key, word_dict in frequent_dict.items():
            cntc_user_depart_c, cntc_user_depart_nm, cntc_user_part_c, cntc_user_part_nm = key.split("!@#$")
            sorted_frequent_dict = sorted(word_dict.iteritems(), key=itemgetter(1), reverse=True)
            sorted_frequent_dict = sorted_frequent_dict[:CONFIG['select_top_cnt'] - 1]
            logger.debug("key : {0}".format(key))
            for word, word_cnt in sorted_frequent_dict:
                logger.debug("\tword : {0}".format(word))
                doc_frequent = oracle.select_document_frequent(
                    call_date=call_date,
                    word=word,
                    call_type_code=call_type_code,
                    cntc_user_depart_c=cntc_user_depart_c,
                    cntc_user_depart_nm=cntc_user_depart_nm,
                    cntc_user_part_c=cntc_user_part_c,
                    cntc_user_part_nm=cntc_user_part_nm
                )
                logger.debug("\tselect document frequent finish ( doc_frequent = {0})".format(doc_frequent))
                oracle.insert_frequent_keyword_tb(
                    call_date=call_date,
                    project_code='PC0001',
                    word=word,
                    speaker_code='ST0001',
                    word_frequent=word_cnt,
                    doc_frequent=doc_frequent,
                    call_type_code=call_type_code,
                    cntc_user_depart_c=cntc_user_depart_c,
                    cntc_user_depart_nm=cntc_user_depart_nm,
                    cntc_user_part_c=cntc_user_part_c,
                    cntc_user_part_nm=cntc_user_part_nm
                )
    else:
        logger.info("Word keyword count = 0")


def extract_related_sentence(logger, oracle, call_date, call_type_code):
    """
    Extract related sentence
    :param          logger:                 Logger
    :param          oracle:                 Oracle
    :param          call_date:              CALL_DATE
    :param          call_type_code:         CALL_TYPE_CODE
    """
    logger.info("3. Select related keyword sentence")
    result = oracle.select_related_keyword_sentence_step_one(call_date, call_type_code)
    logger.info("3-1. Select related keyword sentence step one")
    frequent_keyword_result = oracle.select_related_keyword_sentence_step_two(call_date, call_type_code)
    logger.info("3-2. Select related keyword sentence step two")
    frequent_keyword_info_dict = dict()
    related_sentence_dict = dict()
    if result and frequent_keyword_result:
        for item in frequent_keyword_result:
            word = item[0]
            nlp_frequent_keyword_id = item[1]
            cntc_user_depart_c = item[2]
            cntc_user_depart_nm = item[3]
            cntc_user_part_c = item[4]
            cntc_user_part_nm = item[5]
            key = "{0}!@#${1}!@#${2}!@#${3}!@#${4}".format(
                word, cntc_user_depart_c, cntc_user_depart_nm, cntc_user_part_c, cntc_user_part_nm)
            if key in frequent_keyword_info_dict:
                frequent_keyword_info_dict[key].append(nlp_frequent_keyword_id)
            else:
                frequent_keyword_info_dict[key] = [nlp_frequent_keyword_id]
        for item in result:
            word = item[0]
            nlp_result_id = item[1]
            stt_result_detail_id = item[2]
            call_id = item[3]
            cntc_user_depart_c = item[4]
            cntc_user_depart_nm = item[5]
            cntc_user_part_c = item[6]
            cntc_user_part_nm = item[7]
            key = "{0}!@#${1}!@#${2}!@#${3}!@#${4}".format(
                word, cntc_user_depart_c, cntc_user_depart_nm, cntc_user_part_c, cntc_user_part_nm)
            if key in frequent_keyword_info_dict:
                for nlp_frequent_keyword_id in frequent_keyword_info_dict[key]:
                    overlap_check_key = "{0}!@#${1}!@#${2}!@#${3}".format(
                        nlp_frequent_keyword_id, nlp_result_id, stt_result_detail_id, call_id)
                    related_sentence_dict[overlap_check_key] = 1
        logger.info("4. Insert related keyword sentence")
        logger.info("Related keyword sentence count = {0}".format(len(related_sentence_dict)))
        value_list = list()
        cnt = 0
        for key in related_sentence_dict.keys():
            nlp_frequent_keyword_id, nlp_result_id, stt_result_detail_id, call_id = key.split("!@#$")
            values_tuple = (
                nlp_frequent_keyword_id,
                nlp_result_id,
                stt_result_detail_id,
                call_date,
                'PC0001',
                call_type_code,
                call_id,
                CREATOR_ID,
                CREATOR_ID
            )
            value_list.append(values_tuple)
            if len(value_list) == 100000:
                cnt += 100000
                logger.info('Insert data {0}'.format(cnt))
                oracle.insert_frequent_keyword_detail_tb(value_list)
                value_list = list()
        if len(value_list) > 0:
            logger.info('Insert rest data..')
            oracle.insert_frequent_keyword_detail_tb(value_list)
    else:
        logger.info("Related keyword sentence count = 0")


def extract_related_keyword(logger, oracle, call_date, call_type_code):
    """
    Extract related keyword
    :param          logger:                 Logger
    :param          oracle:                 Oracle
    :param          call_date:              CALL_DATE
    :param          call_type_code:         CALL_TYPE_CODE
    """
    logger.info("5. Select related keyword")
    result = oracle.select_related_keyword(call_date, call_type_code)
    frequent_dict = dict()
    if result:
        for item in result:
            nlp_frequent_keyword_id = item[0]
            word = item[1]
            word_cnt = item[2]
            if nlp_frequent_keyword_id not in frequent_dict:
                frequent_dict[nlp_frequent_keyword_id] = {word: word_cnt}
            else:
                if word in frequent_dict[nlp_frequent_keyword_id]:
                    frequent_dict[nlp_frequent_keyword_id][word] += word_cnt
                else:
                    frequent_dict[nlp_frequent_keyword_id].update({word: word_cnt})
        logger.info("6. Insert related keyword")
        cnt = 0
        value_list = list()
        for nlp_frequent_keyword_id, word_dict in frequent_dict.items():
            sorted_frequent_dict = sorted(word_dict.iteritems(), key=itemgetter(1), reverse=True)
            sorted_frequent_dict = sorted_frequent_dict[:CONFIG['relation_top_cnt']]
            rank = 1
            for word, word_cnt in sorted_frequent_dict:
                values_tuple = (
                    nlp_frequent_keyword_id,
                    call_date,
                    word,
                    'PC0001',
                    call_type_code,
                    rank,
                    CREATOR_ID,
                    CREATOR_ID
                )
                rank += 1
                value_list.append(values_tuple)
                if len(value_list) == 100000:
                    cnt += 100000
                    logger.info('Insert data {0}'.format(cnt))
                    oracle.insert_related_keyword_tb(value_list)
                    value_list = list()
        if len(value_list) > 0:
            logger.info('Insert rest data..')
            oracle.insert_related_keyword_tb(value_list)
    else:
        logger.info("Related keyword count = 0")


def processing(type_code, target_call_date):
    """
    Processing
    :param          type_code:              CALL_TYPE_CODE
    :param          target_call_date:       Target call date
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': type_code + "_" + CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger_period_of_time(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger)
        if not oracle:
            print "---------- Can't connect db ----------"
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- Can't connect db ----------"
        logger.error(exc_info)
        logger.error("---------- Can't connect db ----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        call_type_code = 'CT0001' if type_code == 'IB' else 'CT0002'
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        select_top_cnt = CONFIG['select_top_cnt']
        relation_top_cnt = CONFIG['relation_top_cnt']
        logger.info(
            "START.. Call date = {0}, Call type code = {1}, Select top count = {2}, Relation top count = {3}".format(
            call_date, call_type_code, select_top_cnt, relation_top_cnt))
        oracle.delete_frequent_data(call_date, call_type_code)
        oracle.delete_related_data(call_date, call_type_code)
        oracle.delete_frequent_detail_data(call_date, call_type_code)
        # 키워드 빈도수 추출
        extract_frequent_keyword(logger, oracle, call_date, call_type_code)
        # 키워드 문장 추출
        extract_related_sentence(logger, oracle, call_date, call_type_code)
        # 연관 키워드 추출
        extract_related_keyword(logger, oracle, call_date, call_type_code)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        logger.error(exc_info)
        logger.error("---------- ERROR ----------")
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    logger.info("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
    logger.info("-" * 100)
    oracle.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(type_code, target_call_date):
    """
    This is a program that extract frequent keyword
    :param          type_code:               CALL_TYPE_CODE
    :param          target_call_date:        Target call date
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(type_code, target_call_date)
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        if sys.argv[1].upper() not in ['IB', 'OB']:
            print "usage : python {0} [IB or OB] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} IB 20180416".format(sys.argv[0])
            sys.exit(1)
        elif len(sys.argv[2].strip()) != 8:
            print "usage : python {0} [IB or OB] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} IB 20180416".format(sys.argv[0])
            sys.exit(1)
        try:
            int(sys.argv[2])
        except Exception:
            print "usage : python {0} [IB or OB] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} IB 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip().upper(), sys.argv[2].strip())
    elif len(sys.argv) == 2:
        if sys.argv[1].upper() not in ['IB', 'OB']:
            print "usage : python {0} [IB or OB] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} IB 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip().upper(), (datetime.fromtimestamp(time.time()) - timedelta(days=1)).strftime('%Y%m%d'))
    else:
        print "usage : python {0} [IB or OB] [YYYYMMDD]".format(sys.argv[0])
        print "ex) python {0} IB 20180416".format(sys.argv[0])
        sys.exit(1)
