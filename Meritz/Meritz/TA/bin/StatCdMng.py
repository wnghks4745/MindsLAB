#!/usr/bin/python
# -*- coding: euc-kr -*-

import os
import sys
import time
import pymssql
import ConfigParser
import requests
import logging
import socket
import traceback


CONFIG_FILE = '/data1/MindsVOC/TA/Meritz/cfg/TA.cfg'
LOG_FILE = '/data1/MindsVOC/TA/Meritz/logs/StatCdMng.log'

class DB_Config:
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read(CONFIG_FILE)
        self.host = config.get('DB_CONFIG', 'host')
        self.user = config.get('DB_CONFIG', 'user')
        self.password = config.get('DB_CONFIG', 'password')
        self.database = config.get('DB_CONFIG', 'database')
        self.port = config.get('DB_CONFIG', 'port')
        self.charset = config.get('DB_CONFIG', 'charset')


# 1. 콜전체가 11번 오류, 1월 18일 이전 건들, QC에 결과를 05번 에러로 보내줘야 함
def update_sttacllreq_11():
    select_cnt = 0
    vts_trnum = []
    vts_isptms = []
    cur = conn.cursor()
    s_qry = '''
        SELECT
            STTACTR.TRANSFER_NUMBER,
            STTACTR.ISP_TMS
        FROM
            STTACTRREQ STTACTR WITH(NOLOCK),
            STTACLLREQ STTACLL WITH(NOLOCK)
        WHERE 1=1
            AND STTACTR.TRANSFER_NUMBER = STTACLL.TRANSGER_NUMBER
            AND STTACTR.TMS_FILE_CNT
            (
                SELECT
                    COUNT(*)
                FROM
                    STTACLLREQ WITH(NOLOCK)
                WHERE 1=1
                    AND STTACLLREQ.TRANSFER_NUMBER = STTACTR.TRANSFER_NUMBER
                    AND STTACLLREQ.ISP_TMS = STTACTR.ISP_TMS
                    AND STTACLLREQ.PROG_STAT_CD IN ('11')
                    AND STTACLLREQ.RECORD_START_DATE < '2017-01-18'
            )
            AND STTACTR.ISP_TMS = STTACLL.ISP_TMS
            AND STTACTR.PROG_STAT_CD = '11'
            AND STTACLL.PROG_STAT_CD = '11'
            AND STTACLL.RECORD_START_DATE < '2017-01-18'
        GROUP BY
            STTACTR.TRANSGER_NUMBER,
            STTACTR.ISP_TMS
    '''

    # debug
    logger.debug(s_qry)

    try:
        cur.execute(s_qry)
    except Exception as e:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        cur.close()
        conn.close()
        sys.exit(1)

    row = cur.fetchone()
    while row:
        vts_trnum.append(str(row[0]))
        vts_isptms.append(str(row[1]))
        # debug
        logger.debug(row)
        row = cur.fetchone()
        select_cnt += 1

    cur.close()
    cur = conn.cursor()

    for u_cnt in range(0, select_cnt):
        ctr_sql = """
            UPDATE
                STTACTRREQ
            SET
                STTACTRREQ.PROG_STAT_CD = '12'
            WHERE 1=1
                AND STTACTRREQ.TRANSFER_NUMBER = '%s'
                AND STTACTRREQ.ISP_TMS = '%s'
                AND STTACTRREQ.PROG_STAT_CD = '11'
        """ % (vts_trnum[u_cnt], vts_isptms[u_cnt])

        ctr_sql = ctr_sql.strip()

        try:
            cur.execute(ctr_sql)
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            conn.rollback()
            cur.close()
            conn.close()
            sys.exit(1)

        # debug
        logger.info(ctr_sql)

        cll_sql = '''
            UPDATE
                STTACLLREQ
            SET
                STTACLLREQ.PROG_STAT_CD = '12'
            WHERE 1=1
                AND STTACLLREQ.TRANSFER_NUMBER = '%s'
                AND STTACLLREQ.ISP_TMS = '%s'
                AND STTACLLREQ.PROG_STAT_CD = '11'
        ''' % (vts_trnum[u_cnt], vts_isptms[u_cnt])

        cll_sql = cll_sql.strip()
        try:
            cur.execute(cll_sql)
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            conn.rollback()
            cur.close()
            conn.close()
            sys.exit(1)

        conn.commit()

        # debug
        logger.info(cll_sql)

        if vts_isptms[u_cnt] == '1':
            if socket.gethostname() == 'vrstt1v':
                vs_url = 'http:.-----'
            elif socket.gethostname() == 'vrta1p' or socket.gethostname() == 'vrta2p':
                vs_url = 'http:.-----'

            try:
                requests.get(vs_url)
            except Exception as e:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                cur.close()
                conn.close()
                sys.exit(1)

            # debug
            logger.info(vs_url)
    cur.close()

    return 0


# 2. 콜전체가 22번 오류 QC에 결과를 05번 에러로 보내줘야 함
def update_sttacllreq_22():
    select_cnt = 0

    vts_trnum = []
    vts_isptms = []
    cur = conn.cursor()

    s_qry = '''
        SELECT
            STTACTR.TRANNSFER_NUMBER,
            STTACTR.ISP_TMS
        FROM
            STTACTRREQ STTACTR WITH(NOLOCK),
            STTACLLREQ STTACLL WITH(NOLOCK)
        WHERE 1=1
            AND STTACTR.TRANSFER_NUMBER = STTACLL.TRANSFER_NUMBER
            AND STTACTR.TMS_FILE_CNT = (
                                        SELECT
                                            COUNT (*)
                                        FROM
                                            STTACLLREQ WITH(NOLOCK)
                                        WHERE 1=1
                                            AND STTACLLREQ.TRANSFER_NUMBER = STTACTR.TRANSFER_NUMBER
                                            AND STTACLLREQ.ISP_TMS = STTACTR.ISP_TMS
                                            AND STTACLLREQ.PROG_STAT_CD in ('22')
                                        )
            AND STTACTR.ISP_TMS = STTACLL.ISP_TMS
            AND STTACTR.PROG_STAT_CD = '11'
            AND STTACLL.PROG_STAT_CD = '22'
        GROUP BY
            STTACTR.TRANSFER_NUMBER,
            STTACTR.ISP_TMS
    '''

    logger.debug(s_qry)

    try:
        cur.execute(s_qry)
    except Exception as e:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        cur.close()
        conn.close()
        sys.exit(1)

    row = cur.fetchone()
    while row:
        vts_trnum.append(str(row[0]))
        vts_isptms.append(str(row[1]))
        logger.debug(row)
        row = cur.fetchone()
        select_cnt += 1

    cur.close()
    cur = conn.cursor()

    for u_cnt in range(0, select_cnt):
        ctr_sql = """
            UPDATE
                STTACTRREQ
            SET
                STTACTRREQ.PROG_STAT_CD = '12'
            WHERE 1=1
                AND STTACTRREQ.TRANSFER_NUMBER = '%s'
                AND STTACTRREQ.ISP_TMS = '%s'
                AND STTACTRREQ.PROG_STAT_CD = '11'
        """ % (vts_trnum[u_cnt], vts_isptms[u_cnt])

        ctr_sql = ctr_sql.strip()

        try:
            cur.execute(ctr_sql)
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            conn.rollback()
            cur.close()
            conn.close()
            sys.exit(1)

        conn.commit()

        logger.info(ctr_sql)

        if vts_isptms[u_cnt] == '1':
            if socket.gethostname() == 'vrstt1v':
                vs_url = 'http://-----'
            elif socket.gethostname() == 'vrstt1p' or socket.gethostname() == 'vtstt2p' or socket.gethostname() == 'vrta1p' or socket.gethostname() == 'vrta2p':
                vs_url = 'https://--------'

            try:
                requests.get(vs_url)
            except Exception as e:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                conn.close()
                sys.exit(1)

            logger.info(vs_url)

    cur.close()

    return 0


# 3. 콜에 1월 18일 이전 건이 섞여 있는 건들 오류 아님
def update_sttacllreq_11_21():
    select_cnt = 0

    vts_trnum = []
    vts_isptms = []
    cur = conn.cursor()

    s_qry = """
        SELECT
            STTACTR.TRANSFER_NUMBER,
            STTACTR.ISP_TMS
        FROM
            STTACTRREQ STTACTR WITH(NOLOCK),
            STTACLLREQ STTACLL WITH(NOLOCK)
        WHERE 1-1
            AND STTACTR.TRANSFER_NUMBER = STTACLL.TRANSFER_NUMBER
            AND STTACTR.TMS_FILE_CNT > (
                                        SELECT
                                            COUNT(*)
                                        FROM
                                            STTACLLREQ WITH(NOLOCK)
                                        WHERE 1=1
                                            AND STTACLLREQ.TRANSFER_NUMBER = STTACTR.TRANSFER_NUMBER
                                            AND STTACLLREQ.ISP_TMS = STTACTR.ISP_TMS
                                            AND STTACLLREQ.PROG_STAT_CD in ('11')
                                            AND STTACLLREQ.RECORD_START_DATE < '2017-01-18'
                                        )
            AND STTACTR.ISP_TMS = STTACLL.ISP_TMS
            AND STTACTR.PROG_STAT_CD = '11'
            AND STTACLL.PROG_STAT_CD = '11'
            AND STTACLL.RECORD_START_DATE < '2017-01-18'
            GROUP BY
                STTACTR.TRANSFER_NUMBER,
                STTACTR.ISP_TMS
    """
    logger.debug(s_qry)

    try:
        cur.execute(s_qry)
    except Exception as e:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        cur.close()
        conn.close()
        sys.exit(1)

    row = cur.fetchone()
    while row:
        vts_trnum.append(str(row[0]))
        vts_isptms.append(str(row[1]))
        logger.debug(row)
        row = cur.fetchone()
        select_cnt += 1

    cur.close()
    cur = conn.cursor()

    for u_cnt in range(0, select_cnt):
        cll_sql = '''
            UPDATE
                STTACLLREQ
            SET
                STTACLLREQ.PROG_STAT_CD = '22'
            WHERE 1=1
                AND STTACLLREQ.TRANSFER_NUMBER = '%s'
                AND STTACLLREQ.ISP_TMS = '%s'
                AND STTACLLREQ.PROG_STAT_CD = '11'
                AND STTACLLREQ.RECORD_START_DATE < '2017-01-18'
        ''' % (vts_trnum[u_cnt], vts_isptms[u_cnt])

        cll_sql = cll_sql.strip()
        try:
            cur.execute(cll_sql)
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            conn.rollback()
            cur.close()
            conn.close()
            sys.exit(1)

        conn.commit()

        logger.info(cll_sql)

    cur.close()

    return 0



def main():
    # 1. 콜전체가 11번 오류, 1월 18일 이전 건들 QC에 결괄ㄹ 05번 에러로 보내줘야 함 계약을 12번 콜을 12번으로
    # 2. 콜전체가 22번 오류 QC에 결과를 05번 에러로 보내줘야함 계약을 12번으로
    # 3. 콜에 1월 18일 이전 건이 섞여 있는 건들, 오류 아님. 콜을 22번으로

    global conn
    global logger

    fmt = '[%s(asctime)s] %(levelname)s %(lineno)s %(message)s'
    # logging.basicConfig(level=logging.DEBUG, format=fmt, filename=LOG_FILE
    logging.basicConfig(level=logging.INFO, format=fmt, filename=LOG_FILE)
    logger = logging.getLogger('StatCdMng_daemon')
    logger.debug("Im Start")

    cfg = DB_Config()

    try:
        conn = pymssql.connect(host=cfg.host, user=cfg.user, password=cfg.password, database=cfg.database)
    except Exception as e:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        sys.exit(1)

    # 1. 콜전체가 11번 오류, 1월 18일 이전 건들, QC에 결과를 05번 에러로 보내줘야 함.
    v_ret = update_sttacllreq_11()
    # 2. 콜전체가 22번 오류, QC에 결과를 05번 에러로 보내줘야 함
    v_ret = update_sttacllreq_22()
    # 3. 콜에 1월 18일 이전 건이 섞여 있는 건들, 오류 아님
    v_ret = update_sttacllreq_11_21()

    conn.close()

    logger.debug("Im End")

# 2017-03-07
if __name__ == '__main__':
    main()