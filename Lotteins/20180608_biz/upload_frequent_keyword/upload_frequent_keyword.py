#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-06-05, modification: 0000-00-00"

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
from cfg.config import CONFIG, ORACLE_DB_CONFIG

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
CREATOR_ID = ''

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

    def select_keyword_frequency_by_word(self, call_date, project_code):
        query = """
            SELECT
                T2.WORD,
                SUM(T2.CNT) AS WORD_COUNT,
                T2.SPEAKER_CODE,
                T2.BUSINESS_DCD 
            FROM
                (
                    SELECT
                        A.WORD,
                        C.SPEAKER_CODE,
                        E.BUSINESS_DCD,
                        E.CALL_ID,
                        COUNT(A.WORD) CNT
                    FROM
                        TA_NLP_RESULT_KEYWORD_TB A,
                        TA_NLP_RESULT_TB B,
                        STT_RESULT_DETAIL_TB C, 
                        STT_RESULT_TB D,
                        CM_CALL_META_TB E
                    WHERE 1=1
                        AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                        AND B.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                        AND C.STT_RESULT_ID = D.STT_RESULT_ID
                        AND D.CALL_ID = E.CALL_ID
                        AND (
                                SELECT
                                    COUNT(T.STOPWORD_CONT)
                                FROM
                                    CM_STOPWORD_TB T
                                WHERE
                                    TRIM(A.WORD) = T.STOPWORD_CONT
                            ) = 0
                        AND E.CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        AND E.PROJECT_CODE = :2
                        AND E.BUSINESS_DCD IN (
                                                SELECT
                                                    FULL_CODE
                                                FROM
                                                    CM_CD_DETAIL_TB
                                                WHERE
                                                    USE_YN = 'Y'
                                                )
                    GROUP BY
                        A.WORD,
                        C.SPEAKER_CODE,
                        E.BUSINESS_DCD,
                        E.CALL_ID
                ) T2
            GROUP BY
                T2.WORD,
                T2.SPEAKER_CODE,
                T2.BUSINESS_DCD
            ORDER BY
                WORD_COUNT DESC
        """
        bind = (
            call_date,
            project_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_document_frequency_by_document(self, call_date, project_code):
        query = """
            SELECT
                T2.WORD,
                COUNT(T2.WORD) AS DOC_COUNT,
                T2.SPEAKER_CODE,
                T2.BUSINESS_DCD 
            FROM
                (
                    SELECT
                        A.WORD,
                        C.SPEAKER_CODE,
                        E.BUSINESS_DCD,
                        E.CALL_ID
                    FROM
                        TA_NLP_RESULT_KEYWORD_TB A,
                        TA_NLP_RESULT_TB B,
                        STT_RESULT_DETAIL_TB C, 
                        STT_RESULT_TB D,
                        CM_CALL_META_TB E
                    WHERE 1=1 
                        AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
                        AND B.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
                        AND C.STT_RESULT_ID = D.STT_RESULT_ID
                        AND D.CALL_ID = E.CALL_ID
                        AND (
                                SELECT
                                    COUNT(T.STOPWORD_CONT)
                                FROM
                                    CM_STOPWORD_TB T
                                WHERE
                                    TRIM(A.WORD) = T.STOPWORD_CONT
                            ) = 0
                        AND E.CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        AND E.PROJECT_CODE = :2
                        AND E.BUSINESS_DCD IN (
                                                SELECT
                                                    FULL_CODE
                                                FROM
                                                    CM_CD_DETAIL_TB
                                                WHERE
                                                    USE_YN = 'Y'
                                                )
                        GROUP
                            BY A.WORD,
                            C.SPEAKER_CODE,
                            E.BUSINESS_DCD,
                            E.CALL_ID
                ) T2
            GROUP BY
                T2.WORD,
                T2.SPEAKER_CODE,
                T2.BUSINESS_DCD
            ORDER BY 
                DOC_COUNT DESC
        """
        bind = (
            call_date,
            project_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_related_keyword_sentence(self, call_date, project_code):
        query = """
        SELECT 
            F.NLP_FREQUENT_KEYWORD_ID,
            A.NLP_RESULT_ID,
            B.STT_RESULT_DETAIL_ID,
            E.CALL_ID
        FROM
            TA_NLP_RESULT_KEYWORD_TB A,
            TA_NLP_RESULT_TB B,
            STT_RESULT_DETAIL_TB C, 
            STT_RESULT_TB D,
            CM_CALL_META_TB E,
            TA_NLP_FREQUENT_KEYWORD_TB F
        WHERE 1=1
            AND A.NLP_RESULT_ID = B.NLP_RESULT_ID
            AND B.STT_RESULT_DETAIL_ID = C.STT_RESULT_DETAIL_ID
            AND C.STT_RESULT_ID = D.STT_RESULT_ID
            AND D.CALL_ID = E.CALL_ID
            AND A.WORD = F.WORD
            AND (
                    SELECT
                        COUNT(T.STOPWORD_CONT)
                    FROM
                        CM_STOPWORD_TB T 
                    WHERE 
                        TRIM(A.WORD) = T.STOPWORD_CONT
                ) = 0
            AND E.CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD') 
            AND F.CALL_DATE = TO_DATE(:2, 'YYYY-MM-DD')
            AND E.PROJECT_CODE = :3
            AND F.SPEAKER_CODE = C.SPEAKER_CODE
            AND F.BUSINESS_DCD = E.BUSINESS_DCD
        """
        bind = (
            call_date,
            call_date,
            project_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_related_keyword(self, call_date, project_code):
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
                        TA_NLP_RESULT_KEYWORD_TB CC,
                        TA_NLP_FREQUENT_KEYWORD_TB AA,
                        TA_NLP_FREQUENT_KYWD_DETAIL_TB BB,
                        STT_RESULT_DETAIL_TB DD,
                        STT_RESULT_TB EE,
                        CM_CALL_META_TB FF,
                        TA_NLP_RESULT_TB GG
                    WHERE 1=1
                        AND CC.NLP_RESULT_ID = GG.NLP_RESULT_ID
                        AND GG.STT_RESULT_DETAIL_ID = DD.STT_RESULT_DETAIL_ID
                        AND DD.STT_RESULT_ID = EE.STT_RESULT_ID
                        AND EE.CALL_ID = FF.CALL_ID
                        AND CC.NLP_RESULT_ID = BB.NLP_RESULT_ID
                        AND AA.NLP_FREQUENT_KEYWORD_ID = BB.NLP_FREQUENT_KEYWORD_ID
                        AND AA.CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        AND FF.PROJECT_CODE = :2
                        AND AA.WORD != CC.WORD
                )
            GROUP BY
                NLP_FREQUENT_KEYWORD_ID,
                WORD
            ORDER BY
                NLP_FREQUENT_KEYWORD_ID,
                WORD_CNT DESC
        """
        bind = (
            call_date,
            project_code,
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
            BUSINESS_DCD,
            WORD,
            SPEAKER_CODE,
            FREQUENT_CATEGORY,
            FREQUENT,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            TO_DATE(:1, 'YYYY-MM-DD'), 
            :2, :3, :4, :5, :6, :7,
            SYSDATE, SYSDATE, :8, :9
        )
        """
        bind = (
            kwargs.get('call_date'),
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('word'),
            kwargs.get('speaker_code'),
            kwargs.get('frequent_category'),
            kwargs.get('frequent'),
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
            :5, :6, SYSDATE, SYSDATE, :7, :8
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
            RANK,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, TO_DATE(:2, 'YYYY-MM-DD'),
            :3, :4, :5, SYSDATE, SYSDATE,
            :6, :7
        )
        """
        self.cursor.executemany(query, values_list)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_frequent_data(self, call_date, project_code):
        self.logger.info("0-1. Delete frequency data")
        try:
            query = """
                DELETE FROM
                    TA_NLP_FREQUENT_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND PROJECT_CODE = :2
            """
            bind = (
                call_date,
                project_code,
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

    def delete_related_data(self, call_date, project_code):
        self.logger.info("0-2. Delete related data")
        try:
            query = """
                DELETE FROM
                    TA_RELATED_KEYWORD_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND PROJECT_CODE = :2
            """
            bind = (
                call_date,
                project_code,
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

    def delete_frequent_detail_data(self, call_date, project_code):
        self.logger.info("0-3. Delete frequent detail data")
        try:
            query = """
                DELETE FROM
                    TA_NLP_FREQUENT_KYWD_DETAIL_TB
                WHERE 1=1
                    AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                    AND PROJECT_CODE = :2
            """
            bind = (
                call_date,
                project_code,
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


def based_on_the_word(logger, oracle, call_date, project_code):
    """
    Based on the word
    :param          logger:             Logger
    :param          oracle:             Oracle
    :param          call_date:          CALL_DATE
    :param          project_code:       PROJECT_CODE
    """
    logger.info("1. Select keyword frequency by word")
    result = oracle.select_keyword_frequency_by_word(call_date, project_code)
    frequent_dict = dict()
    if result:
        logger.info("Word keyword count = {0}".format(len(result)))
        for item in result:
            word = item[0]
            if '*' in word:
                continue
            word_cnt = int(item[1])
            speaker_code = item[2]
            business_dcd = item[3]
            key = "{0}!@#${1}!@#${2}".format(speaker_code, project_code, business_dcd)
            if key not in frequent_dict:
                frequent_dict[key] = {word : word_cnt}
            else:
                if word in frequent_dict[key]:
                    frequent_dict[key][word] += word_cnt
                else:
                    frequent_dict[key].update({word:word_cnt})
        logger.info("2. Insert keyword frequency by word")
        for key, word_dict in frequent_dict.items():
            speaker_code, project_code, business_dcd = key.split('!@#$')
            sorted_frequent_dict = sorted(word_dict.iteritems(), key=itemgetter(1), reverse=True)
            sorted_frequent_dict = sorted_frequent_dict[:CONFIG['select_top_cnt'] - 1]
            for word, word_cnt in sorted_frequent_dict:
                oracle.insert_frequent_keyword_tb(
                    call_date=call_date,
                    project_code=project_code,
                    business_dcd=business_dcd,
                    word=word,
                    speaker_code=speaker_code,
                    frequent_category='WORD',
                    frequent=word_cnt
                )
    else:
        logger.info("Word keyword count = 0")


def based_on_the_document(logger, oracle, call_date, project_code):
    """
    Based on the document
    :param          logger:             Logger
    :param          oracle:             Oracle
    :param          call_date:          CALL_DATE
    :param          project_code:       PROJECT_CODE
    """
    logger.info("3. Select keyword frequency by document")
    result = oracle.select_document_frequency_by_document(call_date, project_code)
    frequent_dict = dict()
    if result:
        logger.info("Document keyword count = {0}".format(len(result)))
        for item in result:
            word = item[0]
            if '*' in word:
                continue
            document_cnt = int(item[1])
            speaker_code = item[2]
            business_dcd = item[3]
            key = "{0}!@#${1}!@#${2}".format(speaker_code, project_code, business_dcd)
            if key not in frequent_dict:
                frequent_dict[key] = {word : document_cnt}
            else:
                if word in frequent_dict[key]:
                    frequent_dict[key][word] += document_cnt
                else:
                    frequent_dict[key].update({word:document_cnt})
        logger.info("4. Insert keyword frequency by document")
        for key, word_dict in frequent_dict.items():
            speaker_code, project_code, business_dcd = key.split('!@#$')
            sorted_frequent_dict = sorted(word_dict.iteritems(), key=itemgetter(1), reverse=True)
            sorted_frequent_dict = sorted_frequent_dict[:CONFIG['select_top_cnt'] - 1]
            for word, document_cnt in sorted_frequent_dict:
                oracle.insert_frequent_keyword_tb(
                    call_date=call_date,
                    project_code=project_code,
                    business_dcd=business_dcd,
                    word=word,
                    speaker_code=speaker_code,
                    frequent_category='DOCUMENT',
                    frequent=document_cnt
                )
    else:
        logger.info("Document keyword count = 0")


def insert_related_sentence(logger, oracle, call_date, project_code):
    """
    Insert related sentence
    :param          logger:             Logger
    :param          oracle:             Oracle
    :param          call_date:          CALL_DATE
    :param          project_code:       PROJECT_CODE
    """
    logger.info("5. Select related keyword sentence")
    result = oracle.select_related_keyword_sentence(call_date, project_code)
    group_by_dict = dict()
    if result:
        for row in result:
            if row in group_by_dict:
                continue
            group_by_dict[row] = 1
        logger.info("6. Insert related keyword sentence")
        logger.info("Related keyword sentence count = {0}".format(len(group_by_dict)))
        value_list = list()
        cnt = 0
        for item in group_by_dict.keys():
            nlp_frequent_keyword_id = item[0]
            nlp_result_id = item[1]
            stt_result_detail_id = item[2]
            call_id = item[3]
            values_tuple = (
                nlp_frequent_keyword_id,
                nlp_result_id,
                stt_result_detail_id,
                call_date,
                project_code,
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


def insert_related_keyword(logger, oracle, call_date, project_code):
    """
    Insert related keyword
    :param          logger:             Logger
    :param          oracle:             Oracle
    :param          call_date:          CALL_DATE
    :param          project_code:       PROJECT_CODE
    """
    logger.info("7. Select related keyword")
    result = oracle.select_related_keyword(call_date, project_code)
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
        logger.info("8. Insert related keyword")
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
                    project_code,
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


def processing(pro_code, target_call_date):
    """
    Processing
    :param          pro_code:               PROJECT_CODE
    :param          target_call_date:       Target call date
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': pro_code + "_" + CONFIG['log_name'],
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
        project_code = 'PC0001' if pro_code == 'CS' else 'PC0002'
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        select_top_cnt = CONFIG['select_top_cnt']
        relation_top_cnt = CONFIG['relation_top_cnt']
        logger.info("Call date = {0}, Select top count = {1}, Relation top count = {2}".format(
            call_date, select_top_cnt, relation_top_cnt))
        oracle.delete_frequent_data(call_date, project_code)
        oracle.delete_related_data(call_date, project_code)
        oracle.delete_frequent_detail_data(call_date, project_code)
        based_on_the_word(logger, oracle, call_date, project_code)
        based_on_the_document(logger, oracle, call_date, project_code)
        insert_related_sentence(logger, oracle, call_date, project_code)
        insert_related_keyword(logger, oracle, call_date, project_code)
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
def main(pro_code, target_call_date):
    """
    This is a program that
    :param          pro_code:                PROJECT_CODE
    :param          target_call_date:        Target call date
    """
    global ST
    global DT
    global CREATOR_ID
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        CREATOR_ID = 'CS_BAT' if pro_code == 'CS' else 'TM_BAT'
        processing(pro_code, target_call_date)
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        if sys.argv[1].upper() not in ['CS', 'TM']:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        elif len(sys.argv[2].strip()) != 8:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        try:
            int(sys.argv[2])
        except Exception:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip().upper(), sys.argv[2].strip())
    elif len(sys.argv) == 2:
        if sys.argv[1].upper() not in ['CS', 'TM']:
            print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
            print "ex) python {0} CS 20180416".format(sys.argv[0])
            sys.exit(1)
        main(sys.argv[1].strip().upper(), (datetime.fromtimestamp(time.time()) - timedelta(days=1)).strftime('%Y%m%d'))
    else:
        print "usage : python {0} [CS or TM] [YYYYMMDD]".format(sys.argv[0])
        print "ex) python {0} CS 20180416".format(sys.argv[0])
        sys.exit(1)