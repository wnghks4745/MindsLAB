#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-11-24, modification: 2017-11-29"


###########
# imports #
###########
import os
import sys
import time
import signal
import logging
import subprocess
import multiprocessing
from datetime import datetime, timedelta
from cfg.config import FIND_DAEMON_CONFIG
from bin.lib.daemon import Daemon
from logging.handlers import TimedRotatingFileHandler


###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


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
        job_max_limit = int(FIND_DAEMON_CONFIG['job_max_limit'])
        process_max_limit = int(FIND_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(FIND_DAEMON_CONFIG['process_interval'])
        log_file_path = '{0}/{1}'.format(FIND_DAEMON_CONFIG['log_dir_path'], FIND_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info('CS Daemon Started ...')
        logger.debug('process_max_limit is {0}'.format(process_max_limit))
        logger.debug('process_interval is {0}'.format(process_interval))
        # Get check wav file path list
        wav_file_path_list = get_chk_wav_file_path_list(logger)
        logger.debug('wav_file_path_list -> {0}'.format(wav_file_path_list))
        cnt = 0
        job_list = list()
        job_list.append(list())
        for wav_file_path in wav_file_path_list:
            if len(job_list[cnt]) < job_max_limit:
                job_list[cnt].append(wav_file_path)
            else:
                job_list.append(list())
                cnt += 1
                job_list[cnt].append(wav_file_path)
            COUNT += 1
        # Check the Job list
        logger.debug('job_list : ')
        cnt = 0
        for job in job_list:
            logger.debug('job[{0}] : {1}'.format(cnt, job))
            cnt += 1
        # Start daemon process
        while True:
            # Job list exist check
            if len(job_list) == 1 and len(job_list[0]) == 0:
                break
            # alive pid check
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            # Make the new process
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
                        logger.debug(job)
                        sleep_exact_time(process_interval)
                    cnt += 1
                play_job_list.reverse()
                for play_job in play_job_list:
                    del job_list[play_job]
            if len(job_list) == 0:
                break
            time.sleep(1)
        # Check the Job list
        logger.debug('job_list : ')
        cnt = 0
        for job in job_list:
            logger.debug('job[{0}] : {1}'.format(cnt, job))
            cnt += 1
        ts = time.time()
        et = datetime.fromtimestamp(ts)
        logger.info('CS Daemon is END...   Start time = {0}   End time = {1}   Count = {2}'.format(ST, et, COUNT))


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
    sys.path.append(FIND_DAEMON_CONFIG['find_script_path'])
    import find_cs_that_do_not_read
    reload(find_cs_that_do_not_read)
    find_cs_that_do_not_read.main(args)


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    :return:                Command out
    """
    logger.debug('Command -> {0}'.format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for subprocess to finish
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


def get_sec(s):
    """
    Calculate time minus time
    :param      s:      time
    :return:            Calculate sec
    """
    l = s.split(':')
    hour = float(l[0]) * 3600
    minute = float(l[1]) * 60
    seconds = float(l[2])
    time = hour + minute + seconds
    time = time / 2
    hour = int(time) / 3600
    time = time % 3600
    minute = int(time) / 60
    time = time % 60
    return '{0}:{1}:{2}'.format(hour, minute, time)


def get_chk_wav_file_path_list(logger):
    """
    Get check recording file path list
    :param      logger:     Logger
    :return:                Recording file path list
    """
    logger.info('Get check wav file path list')
    chk_dir_path = FIND_DAEMON_CONFIG['check_dir_path']
    wav_path_list = list()
    # bring the data
    chk_date_dir_path = '{0}/{1}/{2}/{3}'.format(chk_dir_path, DT[:4], DT[4:6], DT[6:8])
    logger.info('Check directory path -> {0}'.format(chk_date_dir_path))
    # check the 5 minute of wav file
    w_ob = os.walk(chk_date_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            logger.debug(file_name)
            extension = os.path.splitext(file_name)[1]
            if not extension == '.wav':
                continue
            file_path = os.path.join(dir_path, file_name)
            ffmpeg_cmd = FIND_DAEMON_CONFIG['tool_dir_path'] + '/ffmpeg -i ' + file_path + \
                         " 2>&1 | grep Duration | awk '{print $2}' | tr -d ,"
            time = sub_process(logger, ffmpeg_cmd)
            time = get_sec(time)
            check_start_time = datetime.strptime(FIND_DAEMON_CONFIG['check_time'][0], '%H:%M:%S')
            check_end_time = datetime.strptime(FIND_DAEMON_CONFIG['check_time'][1], '%H:%M:%S')
            date_time = datetime.strptime(time.split('.')[0], '%H:%M:%S')
            if check_end_time > date_time > check_start_time:
                file_path_dic = dict()
                file_path_dic[file_path] = str(date_time)
                wav_path_list.append(file_path_dic)
    logger.debug('list : {0}'.format(wav_path_list))
    return wav_path_list


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
    ts = time.time()
    ST = datetime.fromtimestamp(ts)
    DT = (datetime.fromtimestamp(ts) - timedelta(days=1)).strftime('%Y%m%d')
    if not os.path.exists(FIND_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(FIND_DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
        FIND_DAEMON_CONFIG['pid_file_path'],
        stdout='{0}/find_stdout_daemon.log'.format(FIND_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/find_stderr_daemon.log'.format(FIND_DAEMON_CONFIG['log_dir_path'])
    )
    if len(sys.argv) == 2 or len(sys.argv) == 3:
        if len(sys.argv) == 3 and len(sys.argv[2]) == 8:
            DT = sys.argv[2]
        if len(sys.argv) == 3 and len(sys.argv[2]) == 8:
            print 'date is not collect'
            print 'usage: YYYYMMDD'
            sys.exit(2)
        if 'start' == sys.argv[1].lower():
            daemon.start()
        elif 'stop' == sys.argv[1].lower():
            daemon.stop()
        elif 'restart' == sys.argv[1].lower():
            daemon.restart()
        else:
            print 'Unknown command'
            print 'usage: %s start | stop | restart' % sys.argv[0]
            sys.exit(2)
        sys.exit(0)
    else:
        print 'usage: %s start | stop | restart' % sys.argv[0]
        sys.exit(0)