#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-07, modification: 2018-08-07"

###########
# imports #
###########
import os
import sys
import time
import glob
import signal
import logging
import multiprocessing
from datetime import datetime
from lib.daemon import Daemon
from logging.handlers import TimedRotatingFileHandler
sys.path.append('/app/MindsVOC/PAST_CS')
from service.config import PAST_CS_DAEMON_CONFIG

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
        job_max_limit = int(PAST_CS_DAEMON_CONFIG['job_max_limit'])
        process_max_limit = int(PAST_CS_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(PAST_CS_DAEMON_CONFIG['process_interval'])
        # log setting
        log_file_path = "{0}/{1}".format(PAST_CS_DAEMON_CONFIG['log_dir_path'], PAST_CS_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("PAST CS Daemon Started ...")
        logger.info("job max limit is {0}".format(job_max_limit))
        logger.info("process max limit is {0}".format(process_max_limit))
        logger.info("process interval is {0}".format(process_interval))
        while True:
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            # 잡 가져오기
            job_list = make_job_list(run_count, job_max_limit)
            # 프로세스 생성
            pid_list = process_execute(
                logger=logger,
                job_list=job_list,
                run_count=run_count,
                pid_list=pid_list,
                process_max_limit=process_max_limit
            )
            time.sleep(3)


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


def do_task(job_list):
    """
    Process execute PAST_CS
    :param      job_list:       Job list
    """
    sys.path.append(PAST_CS_DAEMON_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(job_list)


def process_execute(**kwargs):
    """
    Execute multi process
    :param      kwargs:         Arguments
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
                sleep_exact_time(int(PAST_CS_DAEMON_CONFIG['process_interval']))
        job_list = list()
    return pid_list


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
    if not target_list:
        return main_list
    for target in target_list:
        rfile_name = os.path.basename(target)
        file_name, ext = os.path.splitext(rfile_name)
        # ext = ext.replace('.', '')
        ext = 'wav'
        dir_path = os.path.dirname(target)
        biz_cd = os.path.basename(dir_path)
        rec_file_dict = {
            'RFILE_NAME': file_name,
            'BIZ_CD': biz_cd,
            'REC_EXT': ext
        }
        main_list.append(rec_file_dict)
    return main_list


def make_job_list(run_count, job_max_limit):
    """
    Make job list
    :param      run_count:          Run count
    :param      job_max_limit:      Job max limit
    :return:                        Job list
    """
    if run_count == 0:
        return list()
    # PAST CS 대상 가져오기
    rec_path_list = PAST_CS_DAEMON_CONFIG['rec_path']
    rec_file_path_list = list()
    for rec_dir_path in rec_path_list:
        rec_file_path_list += glob.glob('{0}/2018*.json'.format(rec_dir_path))
    if not rec_file_path_list:
        return list()
    pk_list = list()
    pk_list = append_list(rec_file_path_list, pk_list)
    # PAST CS 대상 분배
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
    if not os.path.exists(PAST_CS_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(PAST_CS_DAEMON_CONFIG['log_dir_path'])
    if not os.path.exists(PAST_CS_DAEMON_CONFIG['pid_dir_path']):
        os.makedirs(PAST_CS_DAEMON_CONFIG['pid_dir_path'])
    daemon = DAEMON(
        os.path.join(PAST_CS_DAEMON_CONFIG['pid_dir_path'], PAST_CS_DAEMON_CONFIG['pid_file_name']),
        stdout='{0}/stdout_daemon.log'.format(PAST_CS_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(PAST_CS_DAEMON_CONFIG['log_dir_path'])
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
