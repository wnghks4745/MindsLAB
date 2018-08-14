#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-21, modification: 0000-00-00"

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
CREATOR_ID = 'CS_BAT'

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

    def select_call_driver_frequency_by_multi(self, group_code, call_date, project_code):
        query = """
            SELECT
                AA.BUSINESS_DCD,
                CC.OFFICE_HOUR,
                AA.CATEGORY_1DEPTH_ID,
                AA.CATEGORY_2DEPTH_ID,
                AA.CATEGORY_3DEPTH_ID,
                CC.CALL_ID,
                COUNT(*)
            FROM
                CS_CALL_DRIVER_CLASSIFY_TB AA,
                (
                    SELECT
                        FULL_CODE
                    FROM
                        CM_CD_DETAIL_TB
                    WHERE 1=1
                        AND GROUP_CODE = :1
                        AND USE_YN = 'Y'
                ) BB,
                CM_CALL_META_TB CC
            WHERE 1=1
                AND AA.CALL_DATE = TO_DATE(:2, 'YYYY-MM-DD')
                AND AA.PROJECT_CODE = :3
                AND AA.BUSINESS_DCD = BB.FULL_CODE
                AND AA.CALL_ID = CC.CALL_ID
            GROUP BY
                AA.BUSINESS_DCD,
                CC.OFFICE_HOUR,
                AA.CATEGORY_1DEPTH_ID,
                AA.CATEGORY_2DEPTH_ID,
                AA.CATEGORY_3DEPTH_ID,
                CC.CALL_ID
            ORDER BY
                COUNT(*) DESC
        """
        bind = (
            group_code,
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

    def select_call_driver_frequency_by_single(self, group_code, call_date, project_code):
        query = """
            SELECT
                DD.CALL_ID,
                AA.BUSINESS_DCD,
                CC.OFFICE_HOUR,
                AA.CATEGORY_1DEPTH_ID,
                AA.CATEGORY_2DEPTH_ID,
                AA.CATEGORY_3DEPTH_ID,
                COUNT(*)
            FROM
                CS_CALL_DRIVER_CLASSIFY_TB AA,
                (
                    SELECT
                        FULL_CODE
                    FROM
                        CM_CD_DETAIL_TB
                    WHERE 1=1
                        AND GROUP_CODE = :1
                        AND USE_YN = 'Y'
                ) BB,
                CM_CALL_META_TB CC,
                (
                    SELECT
                        DISTINCT CALL_ID,
                        BUSINESS_DCD
                    FROM
                        CS_CALL_DRIVER_CLASSIFY_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:2, 'YYYY-MM-DD')
                        AND PROJECT_CODE = :3
                ) DD
            WHERE 1=1
                AND AA.CALL_DATE = TO_DATE(:4, 'YYYY-MM-DD')
                AND AA.PROJECT_CODE = :5
                AND AA.CALL_ID = DD.CALL_ID
                AND AA.BUSINESS_DCD = BB.FULL_CODE
                AND AA.BUSINESS_DCD = DD.BUSINESS_DCD
                AND BB.FULL_CODE = DD.BUSINESS_DCD
                AND AA.CALL_ID = CC.CALL_ID
            GROUP BY
                DD.CALL_ID,
                AA.BUSINESS_DCD,
                CC.OFFICE_HOUR,
                AA.CATEGORY_1DEPTH_ID,
                AA.CATEGORY_2DEPTH_ID,
                AA.CATEGORY_3DEPTH_ID
            ORDER BY
                COUNT(*) DESC
        """
        bind = (
            group_code,
            call_date,
            project_code,
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

    def insert_frequent_summary_tb(self, **kwargs):
        query = """
        INSERT INTO CS_CALL_CLASSIFY_SUMMARY_TB
        (
            PROJECT_CODE,
            BUSINESS_DCD,
            OFFICE_HOUR,
            CALL_DATE,
            COUNT_TYPE_CODE,
            CATEGORY_1DEPTH_ID,
            CATEGORY_2DEPTH_ID,
            CATEGORY_3DEPTH_ID,
            CATEGORY_FREQUENCY,
            CREATED_DTM,
            UPDATED_DTM,
            CREATOR_ID,
            UPDATOR_ID
        )
        VALUES
        (
            :1, :2, :3,
            TO_DATE(:4, 'YYYY-MM-DD'),
            :5, :6, :7,
            :8, :9, SYSDATE,
            SYSDATE, :10, :11
        )
        """
        bind = (
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('office_hour'),
            kwargs.get('call_date'),
            kwargs.get('count_type_code'),
            kwargs.get('category_1depth_id'),
            kwargs.get('category_2depth_id'),
            kwargs.get('category_3depth_id'),
            kwargs.get('category_frequency'),
            CREATOR_ID,
            CREATOR_ID,
        )
        self.cursor.execute(query, bind)
        if self.cursor.rowcount > 0:
            self.conn.commit()
        else:
            self.conn.rollback()

    def delete_summary_data(self, call_date, project_code):
        try:
            query = """
                DELETE FROM
                    CS_CALL_CLASSIFY_SUMMARY_TB
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


def processing(target_call_date, project_code, group_code):
    """
    Processing
    :param          target_call_date:       Target call date
    :param          project_code:           PROJECT_CODE
    :param          group_code:             GROUP_CODE
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': "cs_" + CONFIG['log_name'],
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
        oracle.delete_summary_data(call_date, project_code)
        # CS(PC0001), TM(PC0002) Multi CY0001, Single CY0002
        logger.info("Target date -> {0}".format(call_date))
        logger.info("Start multi")
        multi_result = oracle.select_call_driver_frequency_by_multi(group_code, call_date, project_code)
        multi_output_dict = dict()
        if multi_result:
            for multi_item in multi_result:
                business_dcd = multi_item[0]
                office_hour = multi_item[1]
                category_1depth_id = multi_item[2]
                category_2depth_id = multi_item[3]
                category_3depth_id = multi_item[4]
                key = "{0}!@#${1}!@#${2}!@#${3}!@#${4}".format(
                    business_dcd, office_hour, category_1depth_id, category_2depth_id, category_3depth_id)
                if key not in multi_output_dict:
                    multi_output_dict[key] = 1
                else:
                    multi_output_dict[key] += 1
            for key, frequency in multi_output_dict.items():
                business_dcd, office_hour, category_1depth_id, category_2depth_id, category_3depth_id = key.split("!@#$")
                oracle.insert_frequent_summary_tb(
                    project_code=project_code,
                    business_dcd=business_dcd,
                    office_hour=office_hour,
                    call_date=call_date,
                    count_type_code='CY0001',
                    category_1depth_id=category_1depth_id,
                    category_2depth_id=category_2depth_id,
                    category_3depth_id=category_3depth_id,
                    category_frequency=frequency
                )
        logger.info("Start single")
        single_result = oracle.select_call_driver_frequency_by_single(group_code, call_date, project_code)
        if single_result:
            classify_dict = dict()
            for item in single_result:
                call_id = item[0]
                business_dcd = item[1]
                office_hour = item[2]
                category_1depth_id = item[3]
                category_2depth_id = item[4]
                category_3depth_id = item[5]
                category_frequency =item[6]
                key = "{0}!@#${1}!@#${2}".format(call_id, business_dcd, office_hour)
                if key not in classify_dict:
                    classify_dict[key] = {
                        '1depth_id': category_1depth_id,
                        '2depth_id': category_2depth_id,
                        '3depth_id': category_3depth_id,
                        'frequency': category_frequency
                    }
                else:
                    if category_frequency > classify_dict[key]['frequency']:
                        classify_dict[key] = {
                            '1depth_id': category_1depth_id,
                            '2depth_id': category_2depth_id,
                            '3depth_id': category_3depth_id,
                            'frequency': category_frequency
                        }
            merge_frequency_dict = dict()
            for classify_key, value in classify_dict.items():
                business_dcd = classify_key.split('!@#$')[1]
                office_hour = classify_key.split('!@#$')[2]
                key = "{0}!@#${1}!@#${2}!@#${3}!@#${4}".format(
                    business_dcd, office_hour, value['1depth_id'], value['2depth_id'], value['3depth_id'])
                if key not in merge_frequency_dict:
                    merge_frequency_dict[key] = 1
                else:
                    merge_frequency_dict[key] += 1
            for category, frequency in merge_frequency_dict.items():
                category_list = category.split('!@#$')
                oracle.insert_frequent_summary_tb(
                    project_code=project_code,
                    business_dcd=category_list[0],
                    office_hour=category_list[1],
                    call_date=call_date,
                    count_type_code='CY0002',
                    category_1depth_id=category_list[2],
                    category_2depth_id=category_list[3],
                    category_3depth_id=category_list[4],
                    category_frequency=frequency
                )
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
        project_code = 'PC0001'
        group_code = 'CJ'
        processing(target_call_date, project_code, group_code)
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