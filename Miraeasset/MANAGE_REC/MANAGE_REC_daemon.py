#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-09-29, modification: 2017-11-13"

###########
# imports #
###########
import os
import sys
import time
import signal
import logging
import datetime
import multiprocessing
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from lib import openssl
from lib.daemon import Daemon
from cfg.config import DAEMON_CONFIG

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#########
# class #
#########


class MANAGE(Daemon):
    def run(self):
        set_sig_handler()
        pid_list = list()
        process_interval = int(DAEMON_CONFIG['process_interval'])
        logger = get_logger(
            "{0}/{1}".format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name']), logging.INFO)
        logger.info('Manage rec daemon started ...')
        logger.info('Process interval is {0}'.format(process_interval))
        while True:
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            if len(pid_list) > 0:
                continue
            p = multiprocessing.Process(target=do_task_pre, )
            pid_list.append(p)
            p.start()
            logger.debug('Spawn new processing, pid is [{0}]'.format(p.pid))
            sleep_exact_time(process_interval)


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


def do_task_pre():
    """
    Process execute MANAGE_REC
    """
    reload(openssl)
    sys.path.append(DAEMON_CONFIG['manage_script_path'])
    import encrypt_rec_file
    reload(encrypt_rec_file)
    encrypt_rec_file.main(False)


def sleep_exact_time(seconds):
    """
    Sleep
    :param          seconds:        Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def signal_handler(sig, frame):
    """
    Signal handler
    :param          sig:
    :param          frame:
    """
    if sig == signal.SIGHUP:
        return
    if sig == signal.SIGTERM or sig == signal.SIGINT:
        logger = logging.getLogger('Logger')
        logger.info('Daemon stop')
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
    """
    Set logger
    :param          fname:                      Log file name
    :param          log_level:                  Loge level
    :return:                                    Logger
    """
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='[%(asctime)s] %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('Logger')
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger

if __name__ == '__main__':
    if not os.path.exists(DAEMON_CONFIG['log_dir_path']):
        os.makedirs(DAEMON_CONFIG['log_dir_path'])
    daemon = MANAGE(
        DAEMON_CONFIG['pid_file_path'],
        stdout='{0}/stdout_daemon.log'.format(DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(DAEMON_CONFIG['log_dir_path'])
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
