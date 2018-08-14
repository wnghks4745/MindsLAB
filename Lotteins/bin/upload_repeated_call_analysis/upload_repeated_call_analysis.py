#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-07, modification: 0000-00-00"

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

    def select_repeated_call(self, call_date):
        query = """
            SELECT
                AA.CU_ID,
                AA.CALL_ID,
                AA.RUSER_ID
            FROM
                CM_CALL_META_TB AA
            INNER JOIN
                (
                    SELECT
                        CU_ID,
                        CU_ID_COUNT
                    FROM
                        (
                            SELECT
                                CU_ID,
                                COUNT(CU_ID) AS CU_ID_COUNT
                            FROM
                                CM_CALL_META_TB
                            WHERE 1=1
                                AND PROJECT_CODE = 'PC0001'
                                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                            GROUP BY
                                CU_ID
                        )
                    WHERE 1=1
                       AND CU_ID_COUNT > 1
                ) BB
            ON
              AA.CU_ID = BB.CU_ID  
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

    def select_call_driver_frequency_by_multi(self, call_date, call_id_query):
        query = """
            SELECT
                CALL_ID,
                CATEGORY_1DEPTH_ID,
                CATEGORY_2DEPTH_ID,
                CATEGORY_3DEPTH_ID
            FROM
                CS_CALL_DRIVER_CLASSIFY_TB
            WHERE 1=1
                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                AND PROJECT_CODE = 'PC0001'
        """
        query += call_id_query
        query += """
            GROUP BY
                CALL_ID,
                CATEGORY_1DEPTH_ID,
                CATEGORY_2DEPTH_ID,
                CATEGORY_3DEPTH_ID
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

    def insert_repeated_call_analysis_tb(self, **kwargs):
        query = """
        INSERT INTO CS_REPEATED_CALL_ANALYSIS_TB
        (
            CALL_ID,
            CU_ID,
            RUSER_ID,
            CALL_DATE,
            CATEGORY_1DEPTH_ID,
            CATEGORY_2DEPTH_ID,
            CATEGORY_3DEPTH_ID,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'),
            :5, :6, :7, SYSDATE, SYSDATE, '', ''
        )
        """
        bind = (
            kwargs.get('call_id'),
            kwargs.get('cu_id'),
            kwargs.get('ruser_id'),
            kwargs.get('call_date'),
            kwargs.get('category_1depth_id'),
            kwargs.get('category_2depth_id'),
            kwargs.get('category_3depth_id'),
        )
        self.cursor.execute(query, bind)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_repeated_call_analysis_tb(self, call_date):
        try:
            query = """
                DELETE FROM
                    CS_REPEATED_CALL_ANALYSIS_TB
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
        call_date = "{0}-{1}-{2}".format(target_call_date[:4], target_call_date[4:6], target_call_date[6:])
        logger.info("Start repeated call analysis. [Target date -> {0}]".format(call_date))
        oracle.delete_repeated_call_analysis_tb(call_date)
        meta_result = oracle.select_repeated_call(call_date)
        if meta_result:
            id_dict = dict()
            frequency_dict = dict()
            call_id_query = 'AND ('
            for cu_id, call_id, ruser_id in meta_result:
                id_dict[call_id] = (cu_id, ruser_id)
                call_id_query += 'CALL_ID = {0} OR '.format(call_id)
            call_id_query = call_id_query[:-3]
            call_id_query += ')'
            call_driver_result = oracle.select_call_driver_frequency_by_multi(call_date, call_id_query)
            if call_driver_result:
                for item in call_driver_result:
                    call_id = item[0]
                    category_1depth_id = item[1]
                    category_2depth_id = item[2]
                    category_3depth_id = item[3]
                    cu_id = id_dict[call_id][0]
                    ruser_id = id_dict[call_id][1]
                    frequency_key = "{0}_{1}_{2}_{3}".format(
                        cu_id, category_1depth_id, category_2depth_id, category_3depth_id)
                    if frequency_key not in frequency_dict:
                        frequency_dict[frequency_key] = [1, [(call_id, ruser_id)]]
                    else:
                        frequency_dict[frequency_key][0] += 1
                        frequency_dict[frequency_key][1].append((call_id, ruser_id))
                for key, value in frequency_dict.items():
                    frequency = value[0]
                    if frequency > 1:
                        cu_id, category_1depth_id, category_2depth_id, category_3depth_id = key.split('_')
                        id_list = value[1]
                        for call_id, ruser_id in id_list:
                            oracle.insert_repeated_call_analysis_tb(
                                call_id=call_id,
                                cu_id=cu_id,
                                ruser_id=ruser_id,
                                call_date=call_date,
                                category_1depth_id=category_1depth_id,
                                category_2depth_id=category_2depth_id,
                                category_3depth_id=category_3depth_id
                            )
        else:
            logger.info("No repeated call")
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