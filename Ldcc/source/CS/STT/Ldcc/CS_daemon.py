#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-06, modification: 2018-08-06"

###########
# imports #
###########
import os
import sys
import time
import signal
import socket
import logging
import MySQLdb
import traceback
import multiprocessing
from datetime import datetime
from lib.daemon import Daemon
from logging.handlers import TimedRotatingFileHandler
sys.path.append('/app/MindsVOC/CS')
from service.config import MYSQL_DB_CONFIG, CS_DAEMON_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class DAEMON(Daemon):
    def run(self):
        set_sig_handler()
        pid_list = list()
        job_max_limit = int(CS_DAEMON_CONFIG['job_max_limit'])
        process_max_limit = int(CS_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(CS_DAEMON_CONFIG['process_interval'])
        # log setting
        log_file_path = "{0}/{1}".format(CS_DAEMON_CONFIG['log_dir_path'], CS_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("CS Daemon Started ...")
        logger.info("job max limit is {0}".format(job_max_limit))
        logger.info("process max limit is {0}".format(process_max_limit))
        logger.info("process interval is {0}".format(process_interval))
        while True:
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            # MySQL DB connect
            mysql = connect_db(logger, 'MySQL')
            try:
                # 우선순위 잡 가져오기
                job_list = make_job_list(mysql, run_count, job_max_limit)
                mysql.disconnect()
            except Exception:
                try:
                    mysql.disconnect()
                except Exception:
                    logger.error('mysql is already disconnect')
                logger.error(traceback.format_exc())
                continue
            # 일반 프로세스 생성
            if len(job_list) > 0:
                pid_list = process_execute(
                    logger=logger,
                    job_list=job_list,
                    run_count=run_count,
                    pid_list=pid_list,
                    process_max_limit=process_max_limit
                )


class MySQL(object):
    def __init__(self, logger):
        self.logger = logger
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['passwd'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_target(self):
        query = """
            SELECT
                *
            FROM
                STT_RCDG_INFO
            WHERE 1=1
                AND STT_PRGST_CD = '00'
                AND STT_SERVER_ID = %s
            ORDER BY
                CAST(CREATED_DTM AS DATE) ASC
        """
        bind = (
            str(socket.gethostname()),
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return False
        return results


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
                    logger.info('\t{0}\t{1}\t{2}'.format(item['CON_ID'], item['RFILE_NAME'], item['CALL_DURATION']))
                sleep_exact_time(int(CS_DAEMON_CONFIG['process_interval']))
        job_list = list()
    return pid_list


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
    sys.path.append(CS_DAEMON_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(job_list)


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    cnt = 0
    sql = False
    while True:
        try:
            if db == 'MySQL':
                sql = MySQL(logger)
            else:
                raise Exception("Unknown database [{0}], MySQL".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
            cnt += 1
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def init_list_of_objects(size):
    """
    Make list in list different object reference each time
    :param      size:       List index size
    :return:                List of objects
    """
    list_of_objects = list()
    for _ in range(0, size):
        # different object reference each time
        list_of_objects.append(list())
    return list_of_objects


def make_job_list(mysql, run_count, job_max_limit):
    """
    Make job list
    :param      mysql:                  MySQL DB
    :param      run_count:              Run count
    :param      job_max_limit:          Job max limit
    :return:                            Job list
    """
    if run_count <= 0:
        return list()
    # CS 대상 가져오기
    target_list = mysql.select_target()
    if not target_list:
        return list()
    # CS 대상 분배
    job_list = init_list_of_objects(run_count)
    cnt = 0
    for rec_file_dict in target_list:
        if cnt == run_count:
            cnt = 0
        if len(job_list[cnt]) < job_max_limit:
            job_list[cnt].append(rec_file_dict)
        cnt += 1
    return job_list[:run_count]


def get_logger(fname, log_level):
    """
    Set logger
    :param      fname:          Log file name
    :param      log_level:      Log level
    :return:                    Logger
    """
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='[%(asctime)s] %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('STT_Logger')
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
        logger = logging.getLogger('STT_Logger')
        logger.info('STT Daemon stop')
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


########
# main #
########
if __name__ == '__main__':
    if not os.path.exists(CS_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(CS_DAEMON_CONFIG['log_dir_path'])
    if not os.path.exists(CS_DAEMON_CONFIG['pid_dir_path']):
        os.makedirs(CS_DAEMON_CONFIG['pid_dir_path'])
    daemon = DAEMON(
        os.path.join(CS_DAEMON_CONFIG['pid_dir_path'], CS_DAEMON_CONFIG['pid_file_name']),
        stdout='{0}/stdout_daemon.log'.format(CS_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(CS_DAEMON_CONFIG['log_dir_path'])
    )
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1].lower():
            daemon.start()
        elif 'stop' == sys.argv[1].lower():
            daemon.stop()
        elif 'restart' == sys.argv[1].lower():
            daemon.restart()
        else:
            print "Unknown command"
            print "usage: %s start | stop | restart" % sys.argv[0]
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start | stop | restart" % sys.argv[0]
        sys.exit(2)
