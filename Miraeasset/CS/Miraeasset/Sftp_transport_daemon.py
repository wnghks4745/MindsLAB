#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-09-14, modification: 2017-10-10"


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
from cfg.config import SFTP_DAEMON_CONFIG
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
        process_max_limit = 1
        process_interval = 1
        log_file_path = '{0}/{1}'.format(SFTP_DAEMON_CONFIG['log_dir_path'], SFTP_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info('SFTP Daemon Started ...')
        logger.info('process_max_limit is {0}'.format(process_max_limit))
        logger.info('process_interval is {0}'.format(process_interval))
        while True:
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            for _ in range(run_count):
                if len(pid_list) >= process_max_limit:
                    logger.debug('Processing Count is MAX....')
                    break
                p = multiprocessing.Process(target=do_task)
                pid_list.append(p)
                p.start()
                logger.debug('spawn new processing, pid is [{0}]'.format(p.pid))
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
    Process execute db_upload_txt_send_using_sftp
    """
    sys.path.append(SFTP_DAEMON_CONFIG['script_path'])
    import sftp_transport
    reload(sftp_transport)
    sftp_transport.main()


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
    logger = logging.getLogger('SFTP_Logger')
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
        logger = logging.getLogger('SFTP_Logger')
        logger.info('SFTP Daemon stop')
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
    if not os.path.exists(SFTP_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(SFTP_DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
        SFTP_DAEMON_CONFIG['pid_file_path'],
        stdout='{0}/sftp_stdout_daemon.log'.format(SFTP_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/sftp_stderr_daemon.log'.format(SFTP_DAEMON_CONFIG['log_dir_path'])
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