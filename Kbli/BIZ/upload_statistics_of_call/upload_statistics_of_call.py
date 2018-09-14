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

    def select_agent_quality_result(self, call_date):
        query = """
            SELECT
                TT1.CALL_ID,
                TT1.RUSER_ID,
                TT1.RUSER_NAME,
                TT1.RATE,
                TT1.CNTC_USER_DEPART_C,
                TT1.CNTC_USER_DEPART_NM,
                TT1.CNTC_USER_PART_C,
                TT1.CNTC_USER_PART_NM,
                TT2.SPEED,
                TT2.SILENCE_CNT
            FROM
                (
                    SELECT
                        T1.CALL_ID,
                        T1.RUSER_ID,
                        T1.RUSER_NAME,
                        SUM(T1.CNT_Y)/SUM(T1.CNT_ALL) * 100 AS RATE,
                        T1.CNTC_USER_DEPART_C,
                        T1.CNTC_USER_DEPART_NM,
                        T1.CNTC_USER_PART_C,
                        T1.CNTC_USER_PART_NM
                    FROM
                        ( 
                            SELECT
                                CALL_ID,
                                RUSER_ID,
                                RUSER_NAME,
                                CNTC_USER_DEPART_C,
                                CNTC_USER_DEPART_NM,
                                CNTC_USER_PART_C,
                                CNTC_USER_PART_NM,
                                (
                                    SELECT
                                        COUNT(*)
                                    FROM
                                        DUAL
                                    WHERE 1=1
                                        AND SNTC_DTC_YN = 'Y'
                                ) AS CNT_Y,
                                (
                                    SELECT
                                        COUNT(*)
                                    FROM 
                                        DUAL
                                    WHERE 1=1
                                ) AS CNT_ALL
                            FROM
                                CS_AGENT_QUALITY_RESULT_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) T1
                    GROUP BY
                        T1.CALL_ID,
                        T1.RUSER_ID,
                        T1.RUSER_NAME,
                        T1.CNTC_USER_DEPART_C,
                        T1.CNTC_USER_DEPART_NM,
                        T1.CNTC_USER_PART_C,
                        T1.CNTC_USER_PART_NM
                ) TT1,
                (
                    SELECT
                        A.CALL_ID,
                        AVG(A.SPEED) AS SPEED,
                        SUM(
                            (
                                SELECT
                                    COUNT(*)
                                FROM 
                                    DUAL
                                WHERE 
                                    SILENCE_YN = 'Y'
                            )
                        ) AS SILENCE_CNT
                    FROM
                        (
                            SELECT
                                CALL_ID,
                                SPEED,
                                STT_RESULT_ID
                            FROM
                                STT_RESULT_TB
                            WHERE 1=1
                                AND SPEAKER_CODE = 'ST0002'
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) A,
                        (
                            SELECT
                                STT_RESULT_ID,
                                SILENCE_YN
                            FROM
                                STT_RESULT_DETAIL_TB
                            WHERE 1=1
                                AND SPEAKER_CODE = 'ST0002'
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) B
                    WHERE 1=1
                        AND A.STT_RESULT_ID = B.STT_RESULT_ID
                    GROUP BY 
                        A.CALL_ID
                ) TT2
            WHERE 1=1
                AND TT1.CALL_ID = TT2.CALL_ID
        """
        bind = (
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_happy_call_hmd_result(self, call_date):
        query = """
            SELECT
                T1.CALL_ID,
                T1.RUSER_ID,
                T1.RUSER_NAME,
                T1.RATE,
                T1.CNTC_USER_DEPART_C,
                T1.CNTC_USER_DEPART_NM,
                T1.CNTC_USER_PART_C,
                T1.CNTC_USER_PART_NM,
                T2.SPEED,
                T2.SILENCE_CNT
            FROM
                (
                    SELECT
                        A.CALL_ID,
                        B.RUSER_ID,
                        B.RUSER_NAME,
                        SUM(B.CNT_Y)/SUM(B.CNT_ALL) * 100 AS RATE,
                        B.CNTC_USER_DEPART_C,
                        B.CNTC_USER_DEPART_NM,
                        B.CNTC_USER_PART_C,
                        B.CNTC_USER_PART_NM
                    FROM
                        (
                            SELECT
                                CALL_ID,
                                HAPPY_CALL_ID
                            FROM
                                CS_HAPPY_CALL_MT_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) A,
                        (
                            SELECT
                                HAPPY_CALL_ID,
                                RUSER_ID,
                                RUSER_NAME,
                                CNTC_USER_DEPART_C,
                                CNTC_USER_DEPART_NM,
                                CNTC_USER_PART_C,
                                CNTC_USER_PART_NM,
                                (
                                    SELECT
                                        COUNT(*)
                                    FROM
                                        DUAL
                                    WHERE 
                                        SNTC_DTC_YN = 'Y'
                                ) AS CNT_Y,
                                (
                                    SELECT 
                                        COUNT(*)
                                    FROM 
                                        DUAL
                                ) AS CNT_ALL
                            FROM
                                CS_HAPPY_CALL_MT_DETAIL_TB
                            WHERE 1=1
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) B
                    WHERE 1=1
                        AND A.HAPPY_CALL_ID = B.HAPPY_CALL_ID
                    GROUP BY
                        A.CALL_ID,
                        B.RUSER_ID,
                        B.RUSER_NAME,
                        B.CNTC_USER_DEPART_C,
                        B.CNTC_USER_DEPART_NM,
                        B.CNTC_USER_PART_C,
                        B.CNTC_USER_PART_NM
                ) T1,
                (
                    SELECT
                        A.CALL_ID,
                        AVG(A.SPEED) AS SPEED,
                        SUM(
                            (
                                SELECT
                                    COUNT(*)
                                FROM 
                                    DUAL
                                WHERE 
                                    SILENCE_YN = 'Y'
                            )
                        ) AS SILENCE_CNT
                    FROM
                        (
                            SELECT
                                CALL_ID,
                                SPEED,
                                STT_RESULT_ID
                            FROM
                                STT_RESULT_TB
                            WHERE 1=1
                                AND SPEAKER_CODE = 'ST0002'
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) A,
                        (
                            SELECT
                                STT_RESULT_ID,
                                SILENCE_YN
                            FROM
                                STT_RESULT_DETAIL_TB
                            WHERE 1=1
                                AND SPEAKER_CODE = 'ST0002'
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        ) B
                    WHERE 1=1
                        AND A.STT_RESULT_ID = B.STT_RESULT_ID
                    GROUP BY 
                        A.CALL_ID
                ) T2
            WHERE 1=1
                AND T1.CALL_ID = T2.CALL_ID
        """
        bind = (
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def insert_statistics_of_call_data(self, call_date, statistics_type, result):
        cnt = 0
        for item in result:
            try:
                call_id = item[0]
                ruser_id = item[1]
                ruser_name = item[2]
                rate = item[3]
                cntc_user_depart_c = item[4]
                cntc_user_depart_nm = item[5]
                cntc_user_part_c = item[6]
                cntc_user_part_nm = item[7]
                speed = item[8]
                silence_cnt = item[9]
                query = """
                    INSERT INTO CS_STATISTICS_OF_CALL_TB
                    (
                        CALL_ID,
                        CALL_DATE,
                        RUSER_ID,
                        RUSER_NAME,
                        RATE,
                        TYPE,
                        SPEED,
                        SILENCE_CNT,
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
                        :1, TO_DATE(:2, 'YYYY-MM-DD'), 
                        :3, :4, :5, :6, :7, :8, :9, :10, :11,
                        :12, SYSDATE, SYSDATE, :13, :14
                    )
                """
                bind = (
                    call_id,
                    call_date,
                    ruser_id,
                    ruser_name,
                    rate,
                    statistics_type,
                    speed,
                    silence_cnt,
                    cntc_user_depart_c,
                    cntc_user_depart_nm,
                    cntc_user_part_c,
                    cntc_user_part_nm,
                    CREATOR_ID,
                    CREATOR_ID,
                )
                self.cursor.execute(query, bind)
                self.conn.commit()
                cnt += 1
            except Exception:
                self.logger.error(traceback.format_exc())
                continue
        self.logger.info("Insert rows count = {0}".format(cnt))

    def delete_statistics_data(self, call_date):
        self.logger.info("0-1. Delete statistics data")
        try:
            query = """
                DELETE FROM
                    CS_STATISTICS_OF_CALL_TB
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
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        logger.info("START.. Call date = {0}".format(call_date))
        oracle.delete_statistics_data(call_date)
        # 고객상담지원
        agent_quality_result = oracle.select_agent_quality_result(call_date)
        if agent_quality_result:
            logger.info("1. Start CS_AGENT_QUALITY_RESULT_TB [Target count = {0}]".format(len(agent_quality_result)))
            logger.info("1-1. Insert CS_STATISTICS_OF_CALL_TB")
            oracle.insert_statistics_of_call_data(call_date, 'C', agent_quality_result)
        else:
            logger.info("1. Start CS_AGENT_QUALITY_RESULT_TB [Target count = 0]")
        # 해피콜모니터링
        happy_call_result = oracle.select_happy_call_hmd_result(call_date)
        if happy_call_result:
            logger.info("2. Start CS_HAPPY_CALL_MT_DETAIL_TB [Target count = {0}]".format(len(agent_quality_result)))
            logger.info("2-1. Insert CS_STATISTICS_OF_CALL_TB")
            oracle.insert_statistics_of_call_data(call_date, 'H', happy_call_result)
        else:
            logger.info("2. Start CS_HAPPY_CALL_MT_DETAIL_TB [Target count = 0]")
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
        print traceback.format_exc()

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
