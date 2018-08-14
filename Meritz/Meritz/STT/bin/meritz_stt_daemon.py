#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "Seungphil Lee"
__email__ = "astraea@mindslab.ai"
__date__ = "creation: 0000-00-00, modification: 0000-00-00"
__copyright__ = "All Rights Reserved by MINDsLAB"

###########
# imports #
###########
import sys
import time
import datetime
import multiprocessing
from multiprocessing import Pool
from lib.daemon import Daemon
import STT
import signal
import os
import ConfigParser
import logging
from logging.handlers import TimedRotatingFileHandler

#########
# class #
#########
CONFIG_FILE = '/data1/MindsVOC/CS/Meritz/cfg/CS.cfg'
LOG_FILE = '/data1/MindsVOC/CS/Meritz/logs/STT_daemon.log'

class MyFormatter(logging.Formatter):
    converter = datetime.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = '%s.%03d' % (t, record.msecs)
        return s


def do_task(lock):
    reload(STT)
    STT.execute_stt(lock)


def sleep_exact_time(seconds):
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def signal_handler(sig, frame):
    logger = logging.getLogger('STT_Logger')
    if sig == signal.SIGHUP:
        return
    if sig == signal.SIGTERM or sig == signal.SIGINT:
        logger = logging.getLogger('STT_Logger')
        logger.info('CS Daemon stop')
        sys.exit(0)


def set_sig_handler():
    signal.signal(signal.SIGTSTP, signal.SIG_IGN)
    signal.signal(signal.SIGTTOU, signal.SIG_IGN)
    signal.signal(signal.SIGTTIN, signal.SIG_IGN)
    signal.signal(signal.SIGHUP, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def get_logger(fname, log_level):
    logHandler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    logFormatter = MyFormatter(fmt='[%(asctime)s] %(message)s')
    logHandler.setFormatter(logFormatter)
    logger = logging.getLogger('STT_Logger')
    logger.addHandler(logHandler)
    logger.setLevel(log_level)
    return logger


class MERITZSTT(Daemon):
    def run(self):
        lock = multiprocessing.Lock()
        set_sig_handler()

        config = ConfigParser.RawConfigParser()
        config.read(CONFIG_FILE)
        process_max_limit = config.getint('DAEMON_CFG', 'process_max_limit')
        daemon_interval_mod = config.getint('DAEMON_CFG', 'daemon_interval_mod')
        process_interval = config.getint('DAEMON_CFG', 'process_interval')

        logger = get_logger(LOG_FILE, logging.INFO)

        pid_list = list()

        logger.info('CS Daemon Started ...')
        logger.info('process_max_limit is %d' % process_max_limit)
        logger.info('daemon_interval_mod is %d' % daemon_interval_mod)
        logger.info('process_interval is %d' % process_interval)

        while True:
            sleep_exact_time(60)

            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
                else:
                    logger.info('pid [%d] is alive' % pid.pid)

            logger.info('current process count is %d' % len(pid_list))

            run_count = process_max_limit - len(pid_list)
            for i in range(run_count):
                now = datetime.datetime.today()
                if daemon_interval_mod != (now.minute % 2):
                    break
                if len(pid_list) >= process_max_limit:
                    logger.info('Process Count is MAX....')
                    break
                p = multiprocessing.Process(target=do_task, args=(lock,))
                pid_list.append(p)
                p.start()
                logger.info('spawn new process, pid is [%d]' % p.pid)
                time.sleep(process_interval)


if __name__ == '__main__':
    daemon = MERITZSTT('/tmp/Meritz_STT.pid', stdout='/dev/null', stderr='/dev/null')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1].lower():
            daemon.start()
        elif 'stop' == sys.argv[1].lower():
            daemon.stop()
        elif 'restart' == sys.argv[1].lower():
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(2)
        sys.exit(0)
    else:
        print "usage: %s start | stop | restart" % sys.argv[0]
        sys.exit(2)
