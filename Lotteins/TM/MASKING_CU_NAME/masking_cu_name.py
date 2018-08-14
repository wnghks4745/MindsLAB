# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-08, modification: 2018-02-08"

###########
# imports #
###########
import os
import sys
import time
import traceback
import cx_Oracle
from datetime import datetime, timedelta
from cfg.config import CONFIG, DB_CONFIG
from lib.iLogger import set_logger

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
            DB_CONFIG['host'],
            DB_CONFIG['port'],
            sid=DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
            DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_cu_name(self, **kwargs):
        query = """
            SELECT
                REC_ID,
                RFILE_NAME,
                CU_NAME
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND CALL_START_TIME BETWEEN TO_DATE(:1, 'YYYYMMDDHH24MISS')
                                        AND TO_DATE(:2, 'YYYYMMDDHH24MISS')
                AND NOT EXISTS (
                    SELECT
                        *
                    FROM
                        TB_TM_CNTR_RCDG_INFO
                    WHERE 1=1
                        AND TB_TM_STT_RCDG_INFO.REC_ID = TB_TM_CNTR_RCDG_INFO.REC_ID
                        AND TB_TM_STT_RCDG_INFO.RFILE_NAME = TB_TM_CNTR_RCDG_INFO.RFILE_NAME
                )
        """
        bind = (
            kwargs.get('front_call_start_time'),
            kwargs.get('back_call_start_time'),
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return list()
        return results

    def update_cu_name(self, rcdg_info_dict):
        try:
            query = """
                UPDATE
                    TB_TM_STT_RCDG_INFO
                SET
                    CU_NAME = :1
                WHERE 1=1
                    AND REC_ID = :2
                    AND RFILE_NAME = :3
            """
            values_list = list()
            for insert_dict in rcdg_info_dict.values():
                cu_name = insert_dict['CU_NAME']
                rec_id = insert_dict['REC_ID']
                rfile_name = insert_dict['RFILE_NAME']
                values_tuple = (cu_name, rec_id, rfile_name)
                values_list.append(values_tuple)
            self.cursor.executemany(query, values_list)
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
    :param      sdate:      date object
    :return:                Reqruied time (type : datetime)
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
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.info('1. Connect {0} ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
                sql = Oracle(logger)
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        err_str = "Fail connect {0}".format(db)
        raise Exception(err_str)
    return sql


def processing():
    """
    Processing
    """
    count = 0
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_file_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger, 'Oracle')
        if not oracle:
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("---------- Can't connect db -----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        today_date = datetime.strptime(DT[:8], "%Y%m%d")
        front_date = today_date - timedelta(days=CONFIG['masking_date'])
        back_date = front_date + timedelta(days=1)
        front_call_start_time = str(front_date).replace(":", "").replace("-", "").replace(" ", "")
        back_call_start_time = str(back_date).replace(":", "").replace("-", "").replace(" ", "")
        rcdg_info_list = oracle.select_cu_name(
            front_call_start_time=front_call_start_time,
            back_call_start_time=back_call_start_time
        )
        if not rcdg_info_list:
            logger.info('Masking Target is not exists -> {0}'.format(front_call_start_time[:8]))
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit()
        rcdg_info_dict = dict()
        for rcdg_info in rcdg_info_list:
            rec_id = rcdg_info[0]
            rfile_name = rcdg_info[1]
            cu_name = unicode(rcdg_info[2], 'cp949')
            masking_cu_name = '{0}*{1}'.format(cu_name[:1], cu_name[2:]).encode('cp949')
            info_dict = dict()
            info_dict['REC_ID'] = rec_id
            info_dict['RFILE_NAME'] = rfile_name
            info_dict['CU_NAME'] = masking_cu_name
            key = '{0}-{1}'.format(rec_id, rfile_name)
            rcdg_info_dict[key] = info_dict
            count += 1
        if not oracle.update_cu_name(rcdg_info_dict):
            logger.error('Masking data update is Failed')
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit()
    except Exception:
        exc_info = traceback.format_exc()
        logger.info('MASKING END.. Start time = {0}, The time required = {1}'.format(ST, elapsed_time(DT)))
        logger.error(exc_info)
        logger.error("-----------   MASKING ERROR ----------")
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit()
    oracle.disconnect()
    logger.info('MASKING END.. Start time = {0}, The time required = {1}, masking cu name count = {2}'.format(
        ST, elapsed_time(DT), count))
    logger.info("-" * 100)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main():
    """
    This is a program that masking CU_NAME in TB_TM_STT_RCDG_INFO
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        print(exc_info)


if __name__ == '__main__':
    main()
