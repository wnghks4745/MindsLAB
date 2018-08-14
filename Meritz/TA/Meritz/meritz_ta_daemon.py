#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-11-29, modification: 2017-11-29"

###########
# imports #
###########
import TA
import os
import sys
import time
import signal
import logging
import multiprocessing
from datetime import datetime
from cfg.config import DAEMON_CONFIG
from bin.lib.daemon import Daemon
from logging.handlers import TimedRotatingFileHandler


#########
# class #
#########
class MyFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = '%s.%03d' % (t, record.msecs)
        return s

class MERITZTA(Daemon):
    def run(self):
        set_sig_handler()
        # Set config
        process_max_limit = DAEMON_CONFIG['process_max_limit']
        daemon_interval_mod = DAEMON_CONFIG['daemon_interval_mod']
        process_interval = DAEMON_CONFIG['process_interval']
        # Set log
        if os.path.exists(DAEMON_CONFIG['log_dir_path']):
            os.makedirs(DAEMON_CONFIG['log_dir_path'])
        log_file_path = '{0}/{1}'.format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info('TA Daemon Started...')
        logger.info('process_max_limit is {0}'.format(process_max_limit))
        logger.info('daemon_interval_mod is {0}'.format(daemon_interval_mod))
        logger.info('process_interval is {0}'.format(process_interval))
        # Start setting
        pid_list = list()
        # daemon start
        while True:
            sleep_exact_time(60)
            # pid alive check
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
                else:
                    logger.info('pid [{0}] is alive'.format(pid.pid))
            logger.info('current process count is {0}'.format(len(pid_list)))
            # Start TA process
            run_count = process_max_limit - len(pid_list)
            for _ in range(run_count):
                now = datetime.today()
                if daemon_interval_mod != (now.minute % 2):
                    break
                if len(pid_list) >= process_max_limit:
                    logger.info('Process Count is MAX...')
                    break
                p = multiprocessing.Process(target=do_task)
                pid_list.append(p)
                p.start()
                logger.info('spawn new process, pid is [{0}]'.format(p.pid))
                time.sleep(process_interval)

#######
# def #
#######
def do_task():
    """
    Process execute TA
    """
    reload(TA)
    TA.execute_ta()

def sleep_exact_time(seconds):
    """
    Sleep
    :param      seconds:        Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)

def signal_handler(sig, frame):
    """
    Signal handler
    :param      sig:
    :param      frame:
    """
    logger = logging.getLogger('TA_Logger')
    if sig == signal.SIGHUP:
        return
    if sig == signal.SIGTERM or sig == signal.SIGINT:
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

def get_logger(fname, log_level):
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='[%(asctime)s] %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('TA_Logger')
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger

########
# main #
########
if __name__ == '__main__':
    daemon = MERITZTA(DAEMON_CONFIG['pid_path'], stdout='/dev/null', stderr='/dev/null')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1].lower():
            daemon.start()
        elif 'stop' == sys.argv[1].lower():
            daemon.stop()
        elif 'restart' == sys.argv[1].lower():
            daemon.restart()
        else:
            print 'Unknown command'
            sys.exit(2)
        sys.exit(0)
    else:
        print 'usage: {0} start | stop | restart'.format(sys.argv[0])
        sys.exit(2)