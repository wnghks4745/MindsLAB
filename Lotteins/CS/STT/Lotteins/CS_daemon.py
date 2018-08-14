#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-09, modification: 2018-03-19"

###########
# imports #
###########
import os
import sys
import time
import signal
import socket
import logging
import cx_Oracle
import traceback
import multiprocessing
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from lib.daemon import Daemon
from cfg.config import CS_DAEMON_CONFIG, DB_CONFIG

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
            # 실행 가능 process 수 계산
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            # Oracle DB connect
            oracle = connect_db(logger, 'Oracle')
            try:
                # Target Select
                job_list = make_job_list(oracle, run_count, job_max_limit)
                oracle.disconnect()
            except Exception:
                try:
                    oracle.disconnect()
                except Exception:
                    logger.error('oracle is already disconnect')
                logger.error(traceback.format_exc())
                continue
            pid_list = process_execute(logger, job_list, run_count, pid_list, process_max_limit)


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

    def select_target(self):
        query = """
            SELECT
                REC_ID,
                RFILE_NAME,
                CS_STTA_PRGST_CD,
                BIZ_CD,
                CALL_DURATION
            FROM
                TB_CS_STT_RCDG_INFO
            WHERE 1=1
                AND CS_STTA_PRGST_CD = '00'
                AND STT_SERVER_ID = :1
            ORDER BY
                SUBSTR(rgst_dtm, 1, 8) ASC,
                CALL_DURATION
        """
        bind = (
            socket.gethostname(),
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return list()
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
def process_execute(logger, job_list, run_count, pid_list, process_max_limit):
    """
    Execute multi process
    :param      logger:                 Logger
    :param      job_list:               Job list
    :param      run_count:              Run count
    :param      pid_list:               PID list
    :param      process_max_limit:      Process max limit
    :return:                            new PID list
    """
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
    Process execute CS
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
    cnt = 0
    sql = False
    while True:
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".AL32UTF8"
                sql = Oracle(logger)
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
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


def append_list(target_list, main_list):
    """
    Update main list
    :param      target_list:        Target item list
    :param      main_list:          Update target list
    :return:
    """
    for target in target_list:
        rec_id = str(target[0]).strip()
        rfile_name = str(target[1]).strip()
        stt_prgst_cd = str(target[2]).strip()
        biz_cd = str(target[3]).strip()
        call_duration = str(target[4]).strip()
        rec_file_dict = {
            'REC_ID': rec_id,
            'RFILE_NAME': rfile_name,
            'STT_PRGST_CD': stt_prgst_cd,
            'BIZ_CD': biz_cd,
            'CALL_DURATION': call_duration
        }
        main_list.append(rec_file_dict)
    return main_list


def make_job_list(oracle, run_count, job_max_limit):
    """
    Make job list
    :param      oracle:                 Oracle DB
    :param      run_count:              Run count
    :param      job_max_limit:          Job max limit
    :return:                            Job list
    """
    if run_count == 0:
        return list()
    target_list = oracle.select_target()
    if len(target_list) == 0:
        return list()
    pk_list = list()
    pk_list = append_list(target_list, pk_list)
    job_list = init_list_of_objects(run_count)
    cnt = 0
    for rec_file_dict in pk_list:
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
        logger.info('CS Daemon stop')
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
