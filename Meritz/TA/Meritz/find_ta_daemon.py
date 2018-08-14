#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = 'Creation: 2017-11-01, modification: 2017-11-29'

###########
# imports #
###########
import os
import sys
import time
import signal
import logging
import multiprocessing
from datetime import datetime, timedelta
from cfg.config import FIND_TA_DAEMON_CONFIG
from bin.lib.daemon import Daemon
from logging.handlers import TimedRotatingFileHandler

###########
# options #
###########
reload(sys)
sys.setdefaultencodig('utf-8')

#############
# constants #
#############
DT = ''
ST = ''
COUNT = 0

#########
# class #
#########
class DAEMON(Daemon):
    def run(self):
        global COUNT
        set_sig_handler()
        pid_list = list()
        job_max_limit = int(FIND_TA_DAEMON_CONFIG['job_max_limit'])
        process_max_limit = int(FIND_TA_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(FIND_TA_DAEMON_CONFIG['process_interval'])
        log_file_path = '{0}/{1}'.format(FIND_TA_DAEMON_CONFIG['log_dir_path'], FIND_TA_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info('TA Daemon Started...')
        logger.debug('process_max_limit is {0}'.format(process_max_limit))
        logger.debug('process_interval is {0}'.format(process_interval))
        # Get check txt file path list
        dir_path_list = get_chk_dir_path_list(logger)
        logger.debug('dir_path_list -> {0}'.format(dir_path_list))
        cnt = 0
        job_list = list()
        job_list.append(list())
        for dir_path in dir_path_list:
            if len(job_list[cnt]) < job_max_limit:
                job_list[cnt].append(dir_path)
            else:
                job_list.append(list())
                cnt += 1
                job_list[cnt].append(dir_path)
            COUNT += 1
        logger.debug('job_list : ')
        cnt = 0
        for job in job_list:
            logger.debug('job[{0}] : {1}'.format(cnt, job))
            cnt += 1
        while True:
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            for _ in range(run_count):
                cnt = 0
                play_job_list = list()
                for job in job_list:
                    if len(pid_list) >= process_max_limit:
                        logger.debug('Processing Count is MAX....')
                        break
                    if len(job) > 0:
                        play_job_list.append(cnt)
                        args = job
                        p = multiprocessing.Process(target=do_task, args=(args, ))
                        pid_list.append(p)
                        p.start()
                        logger.debug('spawn new processing, pid is [{0}]'.format(p.pid))
                        logger.debug('\t'.join(job))
                        sleep_exact_time(process_interval)
                    cnt += 1
                play_job_list.reverse()
                for play_job in play_job_list:
                    del job_list[play_job]
            if len(job_list) == 0:
                break
        logger.debug('job_list : ')
        cnt = 0
        for job in job_list:
            logger.debug('job[{0}] : {1}'.format(cnt, job))
            cnt += 1
        ts = time.time()
        et = datetime.fromtimestamp(ts)
        logger.info('TA Daemon is END...    Start time = {0}    End time = {1}  Count = {2}'.format(ST, et, COUNT))


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
    :param      seconds:        Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def do_task(args):
    """
    Process execute CS
    :param      args:       Arguments
    """
    sys.path.append(FIND_TA_DAEMON_CONFIG['find_script_path'])
    import find_ta
    reload(find_ta)
    find_ta.main(args)


def get_chk_dir_path_list(logger):
    """
    Get check dir path list
    :param      logger:     Logger
    :return:                dir path list
    """
    logger.info('Get check txt file path list')
    chk_dir_base_path = FIND_TA_DAEMON_CONFIG['chk_dir_path']
    chk_dir_date_path = '{0}/{1}/{2}/{3}'.format(chk_dir_base_path, DT[:4], DT[4:6], DT[6:8])
    if not os.path.exists(chk_dir_date_path):
        logger.info('directory is not exists -> {0}'.format(chk_dir_date_path))
        sys.exit(0)
    w_ob = os.walk(chk_dir_date_path)
    dir_name_list = list()
    dir_path_list = list()
    for dir_path, sub_dirs, files in w_ob:
        dir_name_list = sub_dirs
        break
    for dir_name in dir_name_list:
        dir_path_list.append('{0}/{1}'.format(chk_dir_date_path, dir_name))
    return dir_path_list


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
    :param sig:
    :param frame:
    """
    if sig == signal.SIGHUP:
        return
    if sig == signal.SIGTERM or sig == signal.SIGINT:
        logger = logging.getLogger('TA_Logger')
        logger.info('TA Daemon stop')
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
    ts = time.time()
    ST = datetime.fromtimestamp(ts)
    DT = (datetime.fromtimestamp(ts) - timedelta(days=1)).strftime('%Y%m%d')
    if not os.path.exists(FIND_TA_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(FIND_TA_DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
        FIND_TA_DAEMON_CONFIG['pid_file_path'],
        stdout='/dev/null',
        stderr='/dev/null'
    )
    if len(sys.argv) == 2 or len(sys.argv) == 3:
        if len(sys.argv) == 3 and len(sys.argv[2]) == 8:
            DT = sys.argv[2]
        if len(sys.argv) == 3 and not len(sys.argv[2]) == 8:
            print "date is not collect"
            print "usage: start | restart YYYYMMDD"
            sys.exit(2)
        chk_dir_date_path = '{0}/{1}/{2}/{3}'.format(FIND_TA_DAEMON_CONFIG['chk_dir_path'], DT[:4], DT[4:6], DT[6:8])
        if not os.path.exists(chk_dir_date_path):
            print 'date file is not exists -> ', chk_dir_date_path
            sys.exit(2)
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