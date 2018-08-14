#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-02-09, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import signal
import logging
import multiprocessing
from datetime import datetime
from cfg.config import DAEMON_CONFIG
from lib.daemon import Daemon
from logging.handlers import TimedRotatingFileHandler


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
        set_sig_handler()
        pid_list = list()
        log_file_path = "{0}/{1}".format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("Daemon Started ...")
        logger.info("process_interval is {0}".format(DAEMON_CONFIG['process_interval']))
        while True:
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            if len(pid_list) > 0:
                logger.debug('Processing Count is MAX....')
                sleep_exact_time(DAEMON_CONFIG['process_interval'])
                continue
            p = multiprocessing.Process(target=do_task)
            pid_list.append(p)
            p.start()
            logger.debug('spawn new processing, pid is [{0}]'.format(p.pid))
            sleep_exact_time(DAEMON_CONFIG['process_interval'])


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


def do_task():
    """
    Process execute json data upsert
    """
    sys.path.append(DAEMON_CONFIG['script_path'])
    import upload_json
    reload(upload_json)
    upload_json.main()


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
    logger = logging.getLogger('Logger')
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


########
# main #
########
if __name__ == '__main__':
    if not os.path.exists(DAEMON_CONFIG['log_dir_path']):
        os.makedirs(DAEMON_CONFIG['log_dir_path'])
    if not os.path.exists(DAEMON_CONFIG['pid_dir_path']):
        os.makedirs(DAEMON_CONFIG['pid_dir_path'])
    daemon = DAEMON(
        os.path.join(DAEMON_CONFIG['pid_dir_path'], DAEMON_CONFIG['pid_file_name']),
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
