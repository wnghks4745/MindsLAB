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

    def select_call_driver_frequency_by_multi(self, call_date, project_code, business_dcd):
        query = """
            SELECT
                CATEGORY_1DEPTH_ID,
                CATEGORY_2DEPTH_ID,
                CATEGORY_3DEPTH_ID,
                COUNT(*)
            FROM
                CS_CALL_DRIVER_CLASSIFY_TB
            WHERE 1=1
                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                AND PROJECT_CODE = :2
                AND BUSINESS_DCD = :3
            GROUP BY
                CATEGORY_1DEPTH_ID,
                CATEGORY_2DEPTH_ID,
                CATEGORY_3DEPTH_ID
            ORDER BY
                COUNT(*) DESC
        """
        bind = (
            call_date,
            project_code,
            business_dcd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_call_id(self, call_date, project_code, business_dcd):
        query = """
            SELECT
                DISTINCT CALL_ID
            FROM
                CS_CALL_DRIVER_CLASSIFY_TB
            WHERE 1=1
                AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                AND PROJECT_CODE = :2
                AND BUSINESS_DCD = :3
        """
        bind = (
            call_date,
            project_code,
            business_dcd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_call_driver_frequency_by_single(self, call_date, project_code, call_id, business_dcd):
        query = """
            SELECT
                *
            FROM
                (
                    SELECT
                        CATEGORY_1DEPTH_ID,
                        CATEGORY_2DEPTH_ID,
                        CATEGORY_3DEPTH_ID,
                        COUNT(*)
                    FROM
                        CS_CALL_DRIVER_CLASSIFY_TB
                    WHERE 1=1
                        AND CALL_DATE = TO_DATE(:1, 'YYYY-MM-DD')
                        AND PROJECT_CODE = :2
                        AND CALL_ID = :3
                        AND BUSINESS_DCD = :4
                    GROUP BY
                        CATEGORY_1DEPTH_ID,
                        CATEGORY_2DEPTH_ID,
                        CATEGORY_3DEPTH_ID
                    ORDER BY
                        COUNT(*) DESC
                )
            WHERE ROWNUM < 2
        """
        bind = (
            call_date,
            project_code,
            call_id,
            business_dcd,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
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
            :1, :2, TO_DATE(:3, 'YYYY-MM-DD'),
            :4, :5, :6, :7, :8, SYSDATE,
            SYSDATE, '', ''
        )
        """
        bind = (
            kwargs.get('project_code'),
            kwargs.get('business_dcd'),
            kwargs.get('call_date'),
            kwargs.get('count_type_code'),
            kwargs.get('category_1depth_id'),
            kwargs.get('category_2depth_id'),
            kwargs.get('category_3depth_id'),
            kwargs.get('category_frequency'),
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
        'log_file_name': "tm_" + CONFIG['log_name'],
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
        logger.info("Count type Multi CY0001, Single CY0002")
        business_dcd_list = oracle.select_business_dcd(group_code)
        if business_dcd_list:
            for business_dcd_result in business_dcd_list:
                business_dcd = business_dcd_result[0]
                multi_result = oracle.select_call_driver_frequency_by_multi(call_date, project_code, business_dcd)
                call_id_list = oracle.select_call_id(call_date, project_code, business_dcd)
                logger.info("CALL_DATE = {0}, PROJECT_CODE = {1}, BUSINESS_DCD = {2}".format(
                    call_date, project_code, business_dcd))
                if multi_result:
                    for multi_item in multi_result:
                        oracle.insert_frequent_summary_tb(
                            project_code=project_code,
                            business_dcd=business_dcd,
                            call_date=call_date,
                            count_type_code='CY0001',
                            category_1depth_id=multi_item[0],
                            category_2depth_id=multi_item[1],
                            category_3depth_id=multi_item[2],
                            category_frequency=multi_item[3]
                        )
                if call_id_list:
                    temp_dict = dict()
                    for item in call_id_list:
                        call_id = item[0]
                        single_result = oracle.select_call_driver_frequency_by_single(
                            call_date, project_code, call_id, business_dcd)
                        if single_result:
                            key = "{0}!@#${1}!@#${2}".format(single_result[0], single_result[1], single_result[2])
                            if key not in temp_dict:
                                temp_dict[key] = single_result[3]
                            else:
                                temp_dict[key] += single_result[3]
                    for category, frequency in temp_dict.items():
                        category_list = category.split('!@#$')
                        oracle.insert_frequent_summary_tb(
                            project_code=project_code,
                            business_dcd=business_dcd,
                            call_date=call_date,
                            count_type_code='CY0002',
                            category_1depth_id=category_list[0],
                            category_2depth_id=category_list[1],
                            category_3depth_id=category_list[2],
                            category_frequency=frequency
                        )
        else:
            logger.error("Can't select business_dcd. [GROUP_CODE = {0}]".format(group_code))
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
        project_code = 'PC0002'
        group_code = 'TD'
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