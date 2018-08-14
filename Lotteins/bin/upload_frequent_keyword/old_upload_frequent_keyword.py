#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 0000-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
from datetime import datetime, timedelta
from lib.iLogger import set_logger_period_of_time
from cfg.config import CONFIG, ORACLE_DB_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ""
DT = ""

#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = cx_Oracle.makedsn(
            ORACLE_DB_CONFIG['host'],
            ORACLE_DB_CONFIG['port'],
            sid=ORACLE_DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            ORACLE_DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_business_dcd(self, group_code):
        query = """
            SELECT
                FULL_CODE
            FROM
                CM_CD_DETAIL_TB
            WHERE 1=1
                AND GROUP_CODE = :1
                AND USE_YN = 'Y'
        """
        bind = (
            group_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_keyword_frequency_by_word(self, **kwargs):
        query = """
            SELECT
                WORD,
                TOTAL_CNT,
                COUNT(WORD) DOCUMENT
            FROM
            (
                SELECT
                    FF.CALL_ID,
                    COUNT(FF.CALL_ID),
                    AA.WORD,
                    AA.CNT TOTAL_CNT
                FROM
                    (
                        SELECT 
                            WORD,
                            CNT
                        FROM 
                        (
                            SELECT
                                WORD,
                                CNT 
                            FROM
                                (
                                    SELECT 
                                        B.WORD,
                                        COUNT(B.WORD) CNT
                                    FROM 
                                        TA_NLP_RESULT_TB A,
                                        TA_NLP_RESULT_KEYWORD_TB B,
                                        STT_RESULT_DETAIL_TB C,
                                        STT_RESULT_TB D, 
                                        CM_CALL_META_TB E
                                    WHERE 1=1
                                        AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                                        AND A.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                                        AND C.STT_RESULT_ID = D.STT_RESULT_ID
                                        AND D.CALL_ID = E.CALL_ID
                                        AND B.RESULT_TYPE_CODE = 'NC0001'
                                        AND LENGTH(B.WORD) > 1 
                                        AND B.KEYWORD_CLASSIFY_CODE IN('MAG', 'NNG', 'NNP', 'NP', 'VA', 'VV')
                                        AND E.PROJECT_CODE = :1
                                        AND E.BUSINESS_DCD = :2
                                        AND C.SPEAKER_CODE = :3
                                        AND E.CALL_DATE = TO_DATE(:4, 'YYYY-MM-DD')
                                    GROUP BY B.WORD
                                ) 
                            ORDER BY
                                CNT DESC
                        )
                        WHERE ROWNUM <= :5
                    ) AA,
                    TA_NLP_RESULT_KEYWORD_TB BB,
                    TA_NLP_RESULT_TB CC,
                    STT_RESULT_DETAIL_TB DD, 
                    STT_RESULT_TB EE, 
                    CM_CALL_META_TB FF
                WHERE 1=1
                    AND AA.WORD = BB.WORD
                    AND BB.NLP_RESULT_ID = CC.NLP_RESULT_ID
                    AND CC.STT_RESULT_DETAIL_ID = DD.STT_RESULT_DETAIL_ID
                    AND DD.STT_RESULT_ID = EE.STT_RESULT_ID
                    AND EE.CALL_ID = FF.CALL_ID
                    AND BB.RESULT_TYPE_CODE = 'NC0001'
                    AND LENGTH(BB.WORD) > 1 
                    AND BB.KEYWORD_CLASSIFY_CODE IN('MAG', 'NNG', 'NNP', 'NP', 'VA', 'VV')
                    AND FF.PROJECT_CODE = :6
                    AND FF.BUSINESS_DCD = :7
                    AND DD.SPEAKER_CODE = :8
                    AND FF.CALL_DATE = TO_DATE(:9, 'YYYY-MM-DD')
                GROUP BY 
                    FF.CALL_ID, 
                    AA.WORD,
                    AA.CNT 
                ORDER BY 
                    AA.WORD
            )
            GROUP BY
                WORD,
                TOTAL_CNT
            ORDER BY
                TOTAL_CNT DESC
        """
        bind = (
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('speaker_code'),
            kwargs.get('start_date'),
            kwargs.get('select_top_cnt'),
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('speaker_code'),
            kwargs.get('start_date'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_document_frequency_by_document(self, **kwargs):
        query = """
            SELECT
                WORD,
                WORD_CNT,
                CALL_CNT
            FROM
                (
                    SELECT 
                        AA.WORD,
                        COUNT(AA.WORD) WORD_CNT,
                        AA.CNT CALL_CNT
                    FROM
                        (
                            SELECT
                                WORD,
                                CNT
                            FROM 
                                (
                                    SELECT
                                        WORD,
                                        COUNT(WORD) CNT
                                    FROM
                                        (
                                            SELECT 
                                                E.CALL_ID,
                                                COUNT(E.CALL_ID) CNT1,
                                                B.WORD
                                            FROM 
                                                TA_NLP_RESULT_TB A,
                                                TA_NLP_RESULT_KEYWORD_TB B,
                                                STT_RESULT_DETAIL_TB C,
                                                STT_RESULT_TB D, 
                                                CM_CALL_META_TB E
                                            WHERE 1=1
                                                AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                                                AND A.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                                                AND C.STT_RESULT_ID = D.STT_RESULT_ID
                                                AND D.CALL_ID = E.CALL_ID
                                                AND B.RESULT_TYPE_CODE = 'NC0001'
                                                AND LENGTH(B.WORD) > 1 
                                                AND B.KEYWORD_CLASSIFY_CODE IN('MAG', 'NNG', 'NNP', 'NP', 'VA', 'VV')
                                                AND E.PROJECT_CODE = :1
                                                AND E.BUSINESS_DCD = :2
                                                AND C.SPEAKER_CODE = :3
                                                AND E.CALL_DATE = TO_DATE(:4, 'YYYY-MM-DD')
                                            GROUP BY E.CALL_ID, B.WORD
                                        )
                                    GROUP BY
                                        WORD
                                    ORDER BY
                                        CNT DESC
                                )
                            WHERE ROWNUM <= :5
                        ) AA,
                        TA_NLP_RESULT_KEYWORD_TB BB,
                        TA_NLP_RESULT_TB CC,
                        STT_RESULT_DETAIL_TB DD, 
                        STT_RESULT_TB EE, 
                        CM_CALL_META_TB FF
                    WHERE 1=1
                        AND AA.WORD = BB.WORD
                        AND BB.NLP_RESULT_ID = CC.NLP_RESULT_ID
                        AND CC.STT_RESULT_DETAIL_ID = DD.STT_RESULT_DETAIL_ID
                        AND DD.STT_RESULT_ID = EE.STT_RESULT_ID
                        AND EE.CALL_ID = FF.CALL_ID
                        AND BB.RESULT_TYPE_CODE = 'NC0001'
                        AND LENGTH(BB.WORD) > 1 
                        AND BB.KEYWORD_CLASSIFY_CODE IN('MAG', 'NNG', 'NNP', 'NP', 'VA', 'VV')
                        AND FF.PROJECT_CODE = :6
                        AND FF.BUSINESS_DCD = :7
                        AND DD.SPEAKER_CODE = :8
                        AND FF.CALL_DATE = TO_DATE(:9, 'YYYY-MM-DD')
                    GROUP BY
                        AA.WORD,
                        AA.CNT 
                    ORDER BY
                        AA.WORD
                )
            ORDER BY CALL_CNT DESC
        """
        bind = (
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('speaker_code'),
            kwargs.get('start_date'),
            kwargs.get('select_top_cnt'),
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('speaker_code'),
            kwargs.get('start_date'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_relation_keyword(self, **kwargs):
        query = """
            SELECT
                *
            FROM
                (
                    SELECT
                        BB.WORD,
                        COUNT(BB.WORD)
                    FROM
                        (
                            SELECT
                                DISTINCT NRK.NLP_RESULT_ID,
                                NR.STT_RESULT_DETAIL_ID
                            FROM
                                TA_NLP_RESULT_KEYWORD_TB NRK
                            INNER JOIN
                                TA_NLP_RESULT_TB NR
                            ON
                                NRK.NLP_RESULT_ID = NR.NLP_RESULT_ID
                            INNER JOIN
                                STT_RESULT_DETAIL_TB SRD
                            ON
                                NR.STT_RESULT_DETAIL_ID = SRD.STT_RESULT_DETAIL_ID
                                AND SRD.SPEAKER_CODE = :1
                            INNER JOIN
                                STT_RESULT_TB SR
                            ON
                                SR.STT_RESULT_ID = SRD.STT_RESULT_ID
                            INNER JOIN
                                CM_CALL_META_TB CCM
                            ON
                                SR.CALL_ID = CCM.CALL_ID
                                AND CCM.PROJECT_CODE = :2
                                AND CCM.BUSINESS_DCD = :3
                                AND (
                                    CCM.CALL_DATE BETWEEN TO_DATE(:4, 'YYYY-MM-DD')
                                    AND TO_DATE(:5, 'YYYY-MM-DD HH24:MI:SS')
                                )
                            WHERE 1=1
                                AND NRK.WORD = :6
                                AND NRK.RESULT_TYPE_CODE = 'NC0001'
                                AND LENGTH(NRK.WORD) > 1
                                AND (
                                        NRK.KEYWORD_CLASSIFY_CODE = 'MAG'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'NNG'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'NNP'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'NP'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'VA'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'VV'
                                    )
                        ) AA,
                        TA_NLP_RESULT_KEYWORD_TB BB
                    WHERE 1=1
                        AND BB.WORD != :7
                        AND LENGTH(BB.WORD) > 1
                        AND AA.NLP_RESULT_ID = BB.NLP_RESULT_ID
                        AND (
                                BB.KEYWORD_CLASSIFY_CODE = 'MAG'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'NNG'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'NNP'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'NP'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'VA'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'VV'
                            ) 
                    GROUP BY
                        BB.WORD
                    ORDER BY
                        COUNT(BB.WORD) DESC
                )
            WHERE ROWNUM <= :8
        """
        bind = (
            kwargs.get('speaker_code'),
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('start_date'),
            kwargs.get('end_date'),
            kwargs.get('word'),
            kwargs.get('word'),
            kwargs.get('relation_top_cnt'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_relation_keyword_id(self, **kwargs):
        query = """
            SELECT
                *
            FROM
                (
                    SELECT
                        AA.STT_RESULT_DETAIL_ID
                    FROM
                        (
                            SELECT
                                DISTINCT NRK.NLP_RESULT_ID,
                                NR.STT_RESULT_DETAIL_ID
                            FROM
                                TA_NLP_RESULT_KEYWORD_TB NRK
                            INNER JOIN
                                TA_NLP_RESULT_TB NR
                            ON
                                NRK.NLP_RESULT_ID = NR.NLP_RESULT_ID
                            INNER JOIN
                                STT_RESULT_DETAIL_TB SRD
                            ON
                                NR.STT_RESULT_DETAIL_ID = SRD.STT_RESULT_DETAIL_ID
                                AND SRD.SPEAKER_CODE = :1
                            INNER JOIN
                                STT_RESULT_TB SR
                            ON
                                SR.STT_RESULT_ID = SRD.STT_RESULT_ID
                            INNER JOIN
                                CM_CALL_META_TB CCM
                            ON
                                SR.CALL_ID = CCM.CALL_ID
                                AND CCM.PROJECT_CODE = :2
                                AND CCM.BUSINESS_DCD = :3
                                AND (
                                    CCM.CALL_DATE BETWEEN TO_DATE(:4, 'YYYY-MM-DD')
                                    AND TO_DATE(:5, 'YYYY-MM-DD HH24:MI:SS')
                                )
                            WHERE 1=1
                                AND NRK.WORD = :6
                                AND NRK.RESULT_TYPE_CODE = 'NC0001'
                                AND LENGTH(NRK.WORD) > 1
                                AND (
                                        NRK.KEYWORD_CLASSIFY_CODE = 'MAG'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'NNG'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'NNP'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'NP'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'VA'
                                        OR NRK.KEYWORD_CLASSIFY_CODE = 'VV'
                                    )
                        ) AA,
                        TA_NLP_RESULT_KEYWORD_TB BB
                    WHERE 1=1
                        AND BB.WORD = :7
                        AND AA.NLP_RESULT_ID = BB.NLP_RESULT_ID
                        AND (
                                BB.KEYWORD_CLASSIFY_CODE = 'MAG'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'NNG'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'NNP'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'NP'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'VA'
                                OR BB.KEYWORD_CLASSIFY_CODE = 'VV'
                            ) 
                    GROUP BY
                        AA.STT_RESULT_DETAIL_ID
                )
            WHERE ROWNUM <= 5
        """
        bind = (
            kwargs.get('speaker_code'),
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('start_date'),
            kwargs.get('end_date'),
            kwargs.get('word'),
            kwargs.get('keyword'),
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def insert_frequent_keyword_tb(self, **kwargs):
        my_seq = self.cursor.var(cx_Oracle.NUMBER)
        query = """
        INSERT INTO TA_NLP_FREQUENT_KEYWORD_TB
        (
            CALL_DATE,
            PROJECT_CODE,
            BUSINESS_DCD,
            WORD,
            SPEAKER_CODE,
            FREQUENT_CATEGORY,
            WORD_FREQUENT,
            DOCUMENT_FREQUENT,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            TO_DATE(:1, 'YYYY-MM-DD'), 
            :2, :3, :4, :5, :6, :7, :8, 
            SYSDATE, SYSDATE, '', ''
        )
        RETURNING NLP_FREQUENT_KEYWORD_ID INTO :9
        """
        bind = (
            kwargs.get('call_date'),
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('word'),
            kwargs.get('speaker_code'),
            kwargs.get('frequent_category'),
            kwargs.get('word_frequent'),
            kwargs.get('document_frequent'),
            my_seq,
        )
        self.cursor.execute(query, bind)
        nlp_frequent_keyword_id = my_seq.getvalue()
        nlp_frequent_keyword_id = int(nlp_frequent_keyword_id)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()
        return nlp_frequent_keyword_id

    def insert_related_keyword_tb(self, **kwargs):
        query = """
        INSERT INTO TA_RELATED_KEYWORD_TB
        (
            NLP_FREQUENT_KEYWORD_ID,
            CALL_DATE,
            STT_RESULT_DETAIL_ID,
            RELATED_WORD,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, TO_DATE(:2, 'YYYY-MM-DD'),
            :3, :4, SYSDATE, SYSDATE, '', ''
        )
        """
        bind = (
            kwargs.get('nlp_frequent_keyword_id'),
            kwargs.get('call_date'),
            kwargs.get('stt_result_detail_id'),
            kwargs.get('related_word'),
        )
        self.cursor.execute(query, bind)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_frequent_data(self, call_date):
        try:
            query = """
                DELETE FROM
                    TA_NLP_FREQUENT_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
            """
            bind = (
                call_date,
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

    def delete_related_data(self, call_date):
        try:
            query = """
                DELETE FROM
                    TA_RELATED_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
            """
            bind = (
                call_date,
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


def connect_db(logger, db):
    """
    Connect database
    :param          logger:         Logger
    :param          db:             Database
    :return                         SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".AL32UTF8"
                sql = Oracle(logger)
            elif db == 'MsSQL':
                sql = MSSQL(logger)
            else:
                logger.error("Unknown DB [{0}]".format(db))
                return False
            logger.debug("Success connect {0} DB ...".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0} DB, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def based_on_the_word(**kwargs):
    """
    Based on the word
    :param          kwargs:         Arguments
    """
    logger = kwargs.get('logger')
    oracle = kwargs.get('oracle')
    speaker_code = kwargs.get('speaker_code')
    project_code = kwargs.get('project_code')
    business_dcd = kwargs.get('business_dcd')
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')
    select_top_cnt = kwargs.get('select_top_cnt')
    relation_top_cnt = kwargs.get('relation_top_cnt')
    logger.info("1) Select keyword frequency by word")
    result = oracle.select_keyword_frequency_by_word(
        speaker_code=speaker_code,
        project_code=project_code,
        business_dcd=business_dcd,
        start_date=start_date,
        end_date=end_date,
        select_top_cnt=select_top_cnt
    )
    logger.info("2) Select relation keyword")
    if result:
        for item in result:
            word = item[0]
            word_cnt = item[1]
            document_cnt = item[2]
            relation_keyword = oracle.select_relation_keyword(
                speaker_code=speaker_code,
                project_code=project_code,
                business_dcd=business_dcd,
                start_date=start_date,
                end_date=end_date,
                word=word,
                relation_top_cnt=relation_top_cnt
            )
            nlp_frequent_keyword_id = oracle.insert_frequent_keyword_tb(
                call_date=start_date,
                project_code=project_code,
                business_dcd=business_dcd,
                word=word,
                speaker_code=speaker_code,
                frequent_category='WORD',
                word_frequent=word_cnt,
                document_frequent=document_cnt
            )
            if relation_keyword:
                for keyword in relation_keyword:
                    id_list = oracle.select_relation_keyword_id(
                        speaker_code=speaker_code,
                        project_code=project_code,
                        business_dcd=business_dcd,
                        start_date=start_date,
                        end_date=end_date,
                        word=word,
                        keyword=keyword[0]
                    )
                    for stt_result_detail_id in id_list:
                        oracle.insert_related_keyword_tb(
                            nlp_frequent_keyword_id=nlp_frequent_keyword_id,
                            call_date=start_date,
                            stt_result_detail_id=stt_result_detail_id[0],
                            related_word=keyword[0]
                        )


def based_on_the_document(**kwargs):
    """
    Based on the document
    :param          kwargs:         Arguments
    """
    logger = kwargs.get('logger')
    oracle = kwargs.get('oracle')
    speaker_code = kwargs.get('speaker_code')
    project_code = kwargs.get('project_code')
    business_dcd = kwargs.get('business_dcd')
    start_date = kwargs.get('start_date')
    end_date = kwargs.get('end_date')
    select_top_cnt = kwargs.get('select_top_cnt')
    relation_top_cnt = kwargs.get('relation_top_cnt')
    logger.info("1) Select document frequency by document")
    doc_result = oracle.select_document_frequency_by_document(
        speaker_code=speaker_code,
        project_code=project_code,
        business_dcd=business_dcd,
        start_date=start_date,
        end_date=end_date,
        select_top_cnt=select_top_cnt
    )
    logger.info("2) Done select document frequency by document")
    if doc_result:
        for item in doc_result:
            word = item[0]
            word_cnt = item[1]
            document_cnt = item[2]
            relation_keyword = oracle.select_relation_keyword(
                speaker_code=speaker_code,
                project_code=project_code,
                business_dcd=business_dcd,
                start_date=start_date,
                end_date=end_date,
                word=word,
                relation_top_cnt=relation_top_cnt
            )
            nlp_frequent_keyword_id = oracle.insert_frequent_keyword_tb(
                call_date=start_date,
                project_code=project_code,
                business_dcd=business_dcd,
                word=word,
                speaker_code=speaker_code,
                frequent_category='DOCUMENT',
                word_frequent=word_cnt,
                document_frequent=document_cnt
            )
            if relation_keyword:
                for keyword in relation_keyword:
                    id_list = oracle.select_relation_keyword_id(
                        speaker_code=speaker_code,
                        project_code=project_code,
                        business_dcd=business_dcd,
                        start_date=start_date,
                        end_date=end_date,
                        word=word,
                        keyword=keyword[0]
                    )
                    for stt_result_detail_id in id_list:
                        oracle.insert_related_keyword_tb(
                            nlp_frequent_keyword_id=nlp_frequent_keyword_id,
                            call_date=start_date,
                            stt_result_detail_id=stt_result_detail_id[0],
                            related_word=keyword[0]
                        )


def processing(target_call_date):
    """
    Processing
    :param          target_call_date:       Target call date
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger_period_of_time(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger, 'Oracle')
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
        start_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        end_date = "{0} 23:59:59".format(start_date)
        select_top_cnt = CONFIG['select_top_cnt']
        relation_top_cnt = CONFIG['relation_top_cnt']
        logger.info("Call date = {0}, Select top count = {1}, Relation top count = {2}".format(
            start_date, select_top_cnt, relation_top_cnt))
        oracle.delete_frequent_data(start_date)
        oracle.delete_related_data(start_date)
        # CS (PC0001) , TM (PC0002)
        for project_code in ['PC0001', 'PC0002']:
            group_code = 'CJ' if project_code == 'PC0001' else 'TD'
            business_dcd_list = oracle.select_business_dcd(group_code)
            if business_dcd_list:
                for item in business_dcd_list:
                    business_dcd = item[0]
                    for speaker_code in ['ST0001', 'ST0002']:
                        logger.info("PROJECT_CODE = {0}, SPEAKER_CODE = {1}, BUSINESS_DCD = {2}".format(
                            project_code, speaker_code, business_dcd))
                        logger.info("1. Execute base on the word")
                        oracle.disconnect()
                        oracle = connect_db(logger, 'Oracle')
                        based_on_the_word(
                            logger=logger,
                            oracle=oracle,
                            speaker_code=speaker_code,
                            project_code=project_code,
                            business_dcd=business_dcd,
                            start_date=start_date,
                            end_date=end_date,
                            select_top_cnt=select_top_cnt,
                            relation_top_cnt=relation_top_cnt
                        )
                        logger.info("2. Execute base on the document")
                        oracle.disconnect()
                        oracle = connect_db(logger, 'Oracle')
                        based_on_the_document(
                            logger=logger,
                            oracle=oracle,
                            speaker_code=speaker_code,
                            project_code=project_code,
                            business_dcd=business_dcd,
                            start_date=start_date,
                            end_date=end_date,
                            select_top_cnt=select_top_cnt,
                            relation_top_cnt=relation_top_cnt
                        )
            else:
                logger.error("Can't select business dcd")
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
def main(target_call_date):
    """
    This is a program that
    :param          target_call_date:        Target call date
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(target_call_date)
    except Exception:
        exc_info = traceback.format_exc()
        print(exc_info)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        if len(sys.argv[1]) != 8:
            print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} 20180416".format(sys.argv[0])
            sys.exit(1)
        else:
            try:
                int(sys.argv[1])
            except Exception:
                print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
                print "ex) python {0} 20180416".format(sys.argv[0])
                sys.exit(1)
            main(sys.argv[1])
    elif len(sys.argv) == 1:
        main((datetime.fromtimestamp(time.time()) - timedelta(days=1)).strftime('%Y%m%d'))
    else:
        print "usage : python {0} [YYYYMMDD]".format(sys.argv[0])
        print "ex) python {0} 20180416".format(sys.argv[0])
        sys.exit(1)