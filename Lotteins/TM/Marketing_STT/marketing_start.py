#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-24, modification: 2018-05-24"

###########
# imports #
###########
import os
import sys
import time
import cx_Oracle
import traceback
import multiprocessing
from datetime import datetime, timedelta
from cfg.config import START_CONFIG, ORACLE_DB_CONFIG
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
PROC_CNT = 0
ERR_CNT = 0
REST_CNT = 0
TARGET_CNT = 0


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

    def select_target(self):
        query = """
            SELECT
                TASK_DATE,
                FILE_NAME
            FROM
                MARKETING_REC_META_TB
            WHERE 1=1
                AND STT_PRGST_CD = '00'
            ORDER BY
                TASK_DATE
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return list()
        return results


#######
# def #
#######
def sleep_exact_time(seconds):
    """
    Sleep
    :param      seconds:        Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def do_task(job_list):
    """
    Process execute STT
    :param      job_list:        Job list
    """
    sys.path.append(START_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(job_list)


def process_execute(**kwargs):
    """
    Execute multi process
    :param          kwargs      Arguments
    :return:                    new PID list
    """
    logger = kwargs.get('logger')
    job_list = kwargs.get('job_list')
    run_count = kwargs.get('run_count')
    pid_list = kwargs.get('pid_list')
    process_max_limit = kwargs.get('process_max_limit')
    for _ in range(run_count):
        for job in job_list[:]:
            if len(pid_list) >= process_max_limit:
                logger.info('Total processing Count is MAX....')
                break
            if len(job) > 0:
                p = multiprocessing.Process(target=do_task, args=(job,))
                pid_list.append(p)
                p.start()
                logger.info('spawn new processing, pid is [{0}]'.format(p.pid))
                for item in job:
                    logger.info('\t{0}'.format(item))
                sleep_exact_time(int(START_CONFIG['process_interval']))
        job_list = list()
    return pid_list


def append_list(target_list):
    """
    Update main list
    :param      target_list:        Target item list
    :return:
    """
    main_list = list()
    if not target_list:
        return main_list
    for target in target_list:
        task_date = str(target[0]).strip()
        file_name = str(target[1]).strip()
        rec_file_dict = {
            'TASK_DATE': task_date,
            'FILE_NAME': file_name
        }
        main_list.append(rec_file_dict)
    return main_list


def make_job_list(oracle, job_max_limit):
    """
    Make job list
    :param      oracle:                 Oracle DB
    :param      job_max_limit:          Job max limit
    :return:                            Job list
    """
    global TARGET_CNT
    target_list = oracle.select_target()
    TARGET_CNT = len(target_list)
    if not  target_list:
        return list()
    pk_list = append_list(target_list)
    # TM 후심사 대상 분배
    job_list = list()
    job_list.append(list())
    cnt = 0
    for rec_file_dict in pk_list:
        if not len(job_list[cnt]) < job_max_limit:
            job_list.append(list())
            cnt += 1
        job_list[cnt].append(rec_file_dict)
    return job_list


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
    # Add logging
    logger_args = {
        'base_path': START_CONFIG['log_dir_path'],
        'log_file_name': START_CONFIG['log_name'],
        'log_level': START_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    pid_list = list()
    job_max_limit = int(START_CONFIG['job_max_limit'])
    process_max_limit = int(START_CONFIG['process_max_limit'])
    process_interval = int(START_CONFIG['process_interval'])
    logger.info("Marketing Started ...")
    logger.info("job max limit is {0}".format(job_max_limit))
    logger.info("process max limit is {0}".format(process_max_limit))
    logger.info("process interval is {0}".format(process_interval))
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
    job_list = make_job_list(oracle, job_max_limit)
    oracle.disconnect()
    while len(job_list):
        ts = time.time()
        current_hour = datetime.fromtimestamp(ts).hour
#        if 19 >= current_hour >= 9:
#            break
        for pid in pid_list[:]:
            if not pid.is_alive():
                pid_list.remove(pid)
        run_count = process_max_limit - len(pid_list)
        if run_count > 0:
            pid_list = process_execute(
                logger=logger,
                job_list=job_list[:run_count],
                run_count=run_count,
                pid_list=pid_list,
                process_max_limit=process_max_limit
            )
        job_list = job_list[run_count:]
    while True:
        for pid in pid_list[:]:
            if not pid.is_alive():
                pid_list.remove(pid)
        if len(pid_list) == 0:
            break
    logger.info("Target Count = {0}, process Count = {1}, Error Count = {2}, Rest Count = {3}".format(
        TARGET_CNT, PROC_CNT, ERR_CNT, REST_CNT))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)
    sys.exit(1)


########
# main #
########
def main():
    """
    This is a program that marketing stt start
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
        print exc_info


if __name__ == '__main__':
    main()
