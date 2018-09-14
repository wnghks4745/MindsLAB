#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-09-29, modification: 2017-12-07"


###########
# imports #
###########
import os
import sys
import time
import glob
import signal
import argparse
import shutil
import atexit
import cx_Oracle
import logging
import traceback
import multiprocessing
from datetime import datetime, timedelta
from lib.daemon import Daemon
import cfg.config
from logging.handlers import TimedRotatingFileHandler
from lib.openssl import decrypt_string

#############
# constants #
#############
DIR_PATH = ''
DAEMON_CONFIG = {}
DB_CONFIG = {}
CONFIG_TYPE = ''


###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#########
# class #
#########
class DAEMON(Daemon):
    def run(self):
        atexit.register(del_tmp)
        set_sig_handler()
        pid_list = list()
        job_max_limit = int(DAEMON_CONFIG['job_max_limit'])
        process_max_limit = int(DAEMON_CONFIG['process_max_limit'])
        process_interval = int(DAEMON_CONFIG['process_interval'])
        log_file_path = "{0}/{1}".format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("CS TA Daemon Started ...")
        logger.info("process_max_limit is {0}".format(process_max_limit))
        logger.info("process_interval is {0}".format(process_interval))
        while True:
            try:
                oracle = oracle_connect(logger)
                job_list = make_job_list(logger, process_max_limit, job_max_limit, oracle)
            except Exception:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                continue
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            for _ in range(run_count):
                for job in job_list:
                    if len(pid_list) >= process_max_limit:
                        logger.info('Processing Count is MAX....')
                        break
                    if len(job) > 0:
                        args = job
                        logger.info(args)
                        p = multiprocessing.Process(target=do_task, args=(args, ))
                        pid_list.append(p)
                        p.start()
                        logger.info('spawn new processing, pid is [{0}]'.format(p.pid))
                        sleep_exact_time(process_interval)
                job_list = list()
            time.sleep(DAEMON_CONFIG['cycle_time'])


class ORACLE(object):
    def __init__(self):
        self.dsn_tns = DB_CONFIG['dsn']
        passwd = decrypt_string(DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        
    def rows_to_dict_list(self):
        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor]

    def find_target(self, last_date):
        """
        Retrieves unprocessed rec_id in the database
        :return:    Rec id list
        """
        sql = """
            SELECT
                *
            FROM (
                SELECT
                    PROJECT_CD,
                    DOCUMENT_ID,
                    DOCUMENT_DT,
                    REC_ID, 
                    CALL_TYPE, 
                    STT_PRGST_CD, 
                    DURATION,
                    CALL_DT,
                    START_DTM,
                    END_DTM,
                    CHN_TP
                FROM
                    CALL_META
                WHERE 1=1
                    AND PROJECT_CD = 'CS'
                    AND STT_PRGST_CD = '05'
                    AND CALL_TYPE = '0'
                    AND CHAT_TA_PRGST_CD in ('10')
                    AND START_DTM BETWEEN :1 
                                    and CURRENT_DATE
                ORDER BY
                    DOCUMENT_DT ASC
                ) A
            WHERE rownum <= 200
        """
        bind = (last_date, )
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        return rows

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


class MyFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, fmt=None):
        ct = self.converter(record.created)
        if fmt:
            s = ct.strftime(fmt)
        else:
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = '%s.%03d' % (t, record.msecs)
        return s


#######
# def #
#######
def sleep_exact_time(seconds):
    """
    Sleep
    :param      seconds:    Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def do_task(args):
    """
    Process execute CS_TA
    :param      args:       Arguments
    """
    sys.path.append(DAEMON_CONFIG['ta_script_path'])
    import CS_TA
    reload(CS_TA)
    CS_TA.main(args, CONFIG_TYPE)


def init_list_of_objects(size):
    """
    Make list in list different object reference each time
    :param      size:   List index size
    :return:            List of objects
    """
    list_of_objects = list()
    for _ in range(0, size):
        # different object reference each time
        list_of_objects.append(list())
    return list_of_objects


def make_job_list(logger, process_max_limit, job_max_limit, oracle):
    """
    Make job list
    :param      logger:                 Logger
    :param      process_max_limit:      Process max limit
    :param      job_max_limit:          Job max limit
    :param      oracle:                 DB
    :return:                            Job list
    """
    global DIR_PATH
    cnt = 0
    job_list = init_list_of_objects(process_max_limit)
    ts = time.time()
    last_date = (datetime.fromtimestamp(ts) - timedelta(days=DAEMON_CONFIG['search_date_range'])).strftime('%Y-%m-%d')
    info_dic_list = oracle.find_target(last_date)
    logger.debug("info_dic_list -> {0}".format(info_dic_list))
    for info_dic in info_dic_list:
        if cnt == process_max_limit:
            cnt = 0
        if len(job_list[cnt]) < job_max_limit:
            job_list[cnt].append(info_dic)
        cnt += 1
    oracle.disconnect()
    return job_list


def oracle_connect(logger):
    """
    Attempt to connect to oracle
    :param:     logger:     Logger
    :return:                ORACLE
    """
    cnt = 0
    oracle = ''
    while True:
        try:
            oracle = ORACLE()
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Fail connect ORACLE, retrying count = {0}".format(cnt))
            if cnt > 10:
                break
            cnt += 1
            continue
    return oracle


def get_logger(fname, log_level):
    """
    Set logger
    :param      fname:          Log file name
    :param      log_level:      Log level
    :return:                    Logger
    """
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='%(asctime)s - %(levelname)s [%(lineno)d] - %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('CS_TA_Logger')
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger


def signal_handler(sig, frame):
    """
    Signal handler
    :param      sig:
    :param      frame:
    """
    if sig == signal.SIGHUP:
        return
    if sig == signal.SIGTERM or sig == signal.SIGINT:
        logger = logging.getLogger('CS_TA_Logger')
        logger.info('CS TA Daemon stop')
        sys.exit(0)


def set_sig_handler():
    """
    Set sig handler
    """
    signal.signal(signal.SIGTSTP, signal.SIG_IGN)
    signal.signal(signal.SIGTTOU, signal.SIG_IGN)
    signal.signal(signal.SIGTTIN, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def del_tmp():
    if os.path.exists('{0}.tmp'.format(DIR_PATH)):
        file_list = glob.glob('{0}.tmp/*'.format(DIR_PATH))
        print 'del_tmp execution'
        if len(file_list) == 0:
            print '  delete {0}.tmp'.format(DIR_PATH)
            shutil.rmtree('{0}.tmp'.format(DIR_PATH))
        else:
            print '  rename {0}.tmp -> {0}'.format(DIR_PATH)
            os.rename('{0}.tmp'.format(DIR_PATH), DIR_PATH)


def set_config(config_type):
    """
    active type setting
    :param      config_type:        Config Type
    """
    global DAEMON_CONFIG
    global DB_CONFIG
    global CONFIG_TYPE

    CONFIG_TYPE = config_type
    DAEMON_CONFIG = cfg.config.DAEMON_CONFIG[config_type]
    DB_CONFIG = cfg.config.DB_CONFIG[config_type]


########
# main #
########
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd', required=True, choices=['dev', 'uat', 'prd'])
    parser.add_argument('-p', action='store', dest='process', type=str, help='stop or start or restart', required=True, choices=['stop', 'start', 'restart'])
    arguments = parser.parse_args()
    set_config(arguments.config_type)
    if not os.path.exists(DAEMON_CONFIG['log_dir_path']):
        os.makedirs(DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
        os.path.join(DAEMON_CONFIG['pid_dir_path'], DAEMON_CONFIG['pid_file_name']),
        stdout='{0}/stdout_daemon.log'.format(DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(DAEMON_CONFIG['log_dir_path'])
    )
    if 'start' == arguments.process.lower():
        daemon.start()
    elif 'stop' == arguments.process.lower():
        daemon.stop()
    elif 'restart' == arguments.process.lower():
        daemon.restart()
    else:
        print "Unknown command"
        print "usage: %s start | stop | restart" % sys.argv[0]
        sys.exit(2)
    sys.exit(0)
