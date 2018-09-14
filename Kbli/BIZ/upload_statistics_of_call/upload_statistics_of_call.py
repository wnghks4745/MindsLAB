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
                SSA.CALL_ID,
                SSA.RUSER_ID,
                SSA.RUSER_NAME,
                SSB.RATE,
                SSA.CNTC_USER_DEPART_C,
                SSA.CNTC_USER_DEPART_NM,
                SSA.CNTC_USER_PART_C,
                SSA.CNTC_USER_PART_NM,
                SSB.SPEED,
                SSB.SILENCE_CNT
            FROM
                CM_CALL_META_TB SSA,
                (
                    SELECT
                        SA.CALL_ID,
                        SA.SPEED,
                        SA.SILENCE_CNT,
                        SB.RATE
                    FROM
                        (
                            SELECT
                                MA.CALL_ID,
                                MA.SPEED,
                                MB.SILENCE_CNT
                            FROM
                                (
                                    SELECT
                                        A1.CALL_ID,
                                        AVG(B1.SPEED) AS SPEED
                                    FROM
                                        CM_CALL_META_TB A1,
                                        STT_RESULT_TB B1
                                    WHERE 1=1
                                        AND B1.SPEAKER_CODE = 'ST0002'
                                        AND A1.CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                                        AND A1.CALL_ID = B1.CALL_ID(+)
                                    GROUP BY
                                        A1.CALL_ID
                                ) MA,
                                (
                                    SELECT
                                        A2.CALL_ID
                                        C1.SILENCE_CNT
                                    FROM
                                        CM_CALL_META_TB A2,
                                        (
                                            SELECT
                                                SA.CALL_ID,
                                                COUNT(*) AS SILENCE_CNT
                                            FROM
                                                (
                                                    SELECT
                                                        RECORD_KEY,
                                                        MAX(CALL_ID) AS CALL_ID,
                                                        MAX(CALL_TYPE_CODE) AS CALL_TYPE_CODE,
                                                        MAX(DETAIL_NAME) AS DETAIL_NAME,
                                                        MAX(START_TIME) AS START_TIME,
                                                        MAX(RUSER_ID) AS RUSER_ID,
                                                        MAX(RUSER_NAME) AS RUSER_NAME,
                                                        SUM(SILENCE_TIME) AS SILENCE_TIME,
                                                        MAX(POLY_NO) AS POLY_NO,
                                                        MAX(CU_ID) AS CU_ID,
                                                        MAX(CU_NAME) AS CU_NAME,
                                                        MAX(CONT_DATE) AS CONT_DATE,
                                                        MAX(CNTC_USER_DEPART_C) AS CNTC_USER_DEPART_C,
                                                        MAX(CNTC_USER_DEPART_NM) AS CNTC_USER_DEPART_NM,
                                                        MAX(CNTC_USER_PART_C) AS CNTC_USER_PART_C,
                                                        MAX(CNTC_USER_PART_NM) AS CNTC_USER_PART_NM
                                                    FROM
                                                        ( 
                                                            SELECT
                                                                A.RECORD_KEY,
                                                                A.CALL_ID,
                                                                CALL_TYPE_CODE,
                                                                B.DETAIL_NAME,
                                                                A.START_TIME,
                                                                RUSER_ID,
                                                                RUSER_NAME,
                                                                C.SILENCE_TIME,
                                                                POLY_NO,
                                                                CU_ID,
                                                                CU_NAME,
                                                                CONT_DATE,
                                                                CNTC_USER_DEPART_C,
                                                                CNTC_USER_DEPART_NM,
                                                                CNTC_USER_PART_C,
                                                                CNTC_USER_PART_NM,
                                                            FROM
                                                                CM_CALL_META_TB A,
                                                                CM_CD_DETAIL_TB B,
                                                                STT_RESULT_DETAIL_TB C
                                                            WHERE 1=1
                                                                AND A.CALL_TYPE_CODE = B.FULL_CODE
                                                                AND A.CALL_ID = C.CALL_ID
                                                                AND SILENCE_YN = 'Y'
                                                                AND A.START_TIME BETWEEN TO_TIMESTAMP(:1, 'YY/MM/DD')
                                                                                    AND TO_TIMESTAMP(:1 || '23:59:59', 'YY/MM/DD HH24:MI:SSFF')
                                                        ) ZA
                                                    GROUP BY
                                                        ZA.RECORD_KEY
                                                ) SA,
                                                ZCS.TCL_CS_CNTC_HIST SB
                                            WHERE 1=1
                                                AND SA.RECORD_KEY = SB.REC_NO
                                                AND SUBSTR(SA.RECORD_KEY, 9, 8) = SB.CNTC_STRT_DATE
                                            GROUP BY
                                                SA.CALL_ID
                                        ) C1
                                    WHERE 1=1
                                        AND A2.CALL_ID = C1.CALL_ID
                                ) MB
                            WHERE 1=1
                                AND MA.CALL_ID = MB.CALL_ID(+)
                        ) SA,
                        (
                            SELECT
                                A3.CALL_ID,
                                D1.RATE
                            FROM
                                CM_CALL_META_TB A3,
                                (
                                    SELECT 
                                        CALL_ID,
                                        SUM(SS.RATE)/COUNT(*) AS RATE
                                    FROM
                                        (
                                            SELECT
                                                T1.CALL_ID,
                                                T1.RUSER_ID,
                                                T1.RUSER_NAME,
                                                SUM(T1.CNT_Y)/SUM(T1.CNT_ALL) * 100 RATE,
                                                T1.CNTC_USER_DEPART_C,
                                                T1.CNTC_USER_DEPART_NM,
                                                T1.CNTC_USER_PART_C,
                                                T1.CNTC_USER_PART_NM,
                                            FROM
                                                (
                                                    SELECT
                                                        CALL_ID,
                                                        RUSER_ID,
                                                        RUSER_NAME,
                                                        CNTC_USER_DEPART_C,
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
                                        ) SS
                                    WHERE 1=1
                                        AND SS.RATE > 0
                                    GROUP BY CALL_ID
                                ) D1
                            WHERE 1=1
                                AND A3.CALL_ID = D1.CALL_ID
                        ) SB
                    WHERE 1=1
                        AND SA.CALL_ID = SB.CALL_ID(+)
                ) SSB
            WHERE 1=1
                AND SSA.CALL_ID = SSB.CALL_ID
        """
        bind = (
            call_date,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return list()
        if not result:
            return list()
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
        ruser_dict = dict()
        for item in agent_quality_result:
            ruser_id = item[1]
            ruser_name = item[2]
            rate = item[3]
            speed = item[8]
            silence_cnt = item[9]
            key = '{0}_{1}'.format(ruser_id, ruser_name)
            if key not in ruser_dict:
                ruser_dict[key] = {
                    'TOTAL_SPEED': 0,
                    'SPEED_CNT': 0,
                    'TOTAL_SILENCE': 0,
                    'TOTAL_RATE': 0,
                    'RATE_CONT': 0,
                    'RESULT': item
                }
            if speed:
                ruser_dict[key]['TOTAL_SPEED'] += float(speed)
                ruser_dict[key]['SPEED_CNT'] += 1
            if silence_cnt:
                ruser_dict[key]['TOTAL_SILENCE'] += int(silence_cnt)
            if rate:
                ruser_dict[key]['TOTAL_RATE'] += float(rate)
                ruser_dict[key]['RATE_CNT'] += 1
        modify_agent_quality_result = list()
        for ruser_dict in ruser_dict.values():
            call_id = ruser_dict['RESULT'][0]
            ruser_id = ruser_dict['RESULT'][1]
            ruser_name = ruser_dict['RESULT'][2]
            ruser_dict['RATE_CNT'] = 1 if ruser_dict['RATE_CNT'] == 0 else ruser_dict['RATE_CNT']
            rate = ruser_dict['TOTAL_RATE'] / ruser_dict['RATE_CNT']
            cntc_user_depart_c = ruser_dict['RESULT'][4]
            cntc_user_depart_nm = ruser_dict['RESULT'][5]
            cntc_user_part_c = ruser_dict['RESULT'][6]
            cntc_user_part_nm = ruser_dict['RESULT'][7]
            ruser_dict['SPEED_CNT'] = 1 if ruser_dict['SPEED_CNT'] == 0 else ruser_dict['SPEED_CNT']
            speed = ruser_dict['TOTAL_SPEED'] / ruser_dict['SPEED_CNT']
            silence_cnt = ruser_dict['TOTAL_SILENCE']
            modify_agent_quality_result.append((call_id, ruser_id, ruser_name, rate, cntc_user_depart_c,
                                                cntc_user_depart_nm, cntc_user_part_c, cntc_user_part_nm, speed,
                                                silence_cnt))
        if modify_agent_quality_result:
            logger.info("1. Start CS_AGENT_QUALITY_RESULT_TB [Target count = {0}]".format(len(modify_agent_quality_result)))
            logger.info("1-1. Insert CS_STATISTICS_OF_CALL_TB")
            oracle.insert_statistics_of_call_data(call_date, 'C', modify_agent_quality_result)
        else:
            logger.info("1. Start CS_AGENT_QUALITY_RESULT_TB [Target count = 0]")
        # # 해피콜모니터링
        # happy_call_result = oracle.select_happy_call_hmd_result(call_date)
        # if happy_call_result:
        #     logger.info("2. Start CS_HAPPY_CALL_MT_DETAIL_TB [Target count = {0}]".format(len(agent_quality_result)))
        #     logger.info("2-1. Insert CS_STATISTICS_OF_CALL_TB")
        #     oracle.insert_statistics_of_call_data(call_date, 'H', happy_call_result)
        # else:
        #     logger.info("2. Start CS_HAPPY_CALL_MT_DETAIL_TB [Target count = 0]")
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
