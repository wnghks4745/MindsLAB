#!/usr/bin/python
# -*- coding: euc-kr -*-

import sys
import os
import signal
import logging
from logging.handlers import TimedRotatingFileHandler
import sched
import subprocess
import multiprocessing
import threading
from datetime import *
import time
from lib.daemon import Daemon

LOG_FILE = '/data1/MindsVOC/TA/Meritz/logs/TA_shced_daemon.log'

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


def get_logger(fname, log_level):
    logHandler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    logFormatter = MyFormatter(fmt='[%(asctime)s] %(message)s')
    logHandler.setFormatter(logFormatter)
    logger = logging.getLogger('STT_Logger')
    logger.addHandler(logHandler)
    logger.setLevel(log_level)
    return logger


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


# for multi processing log
class LogThread(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.q = queue

    def run(self):
        while True:
            msg = self.q.get()
            print msg
            if msg == 'stop':
                break


# test 하기 위한 mtime 수정하는 방법
# touch -m -d '20170202 00:00:01' test.txt
# mtime 바뀐 내용 확인
# stat test.txt

# path : 검색할 디렉토리명
#        경로는 다음 처럼 명시한다
#        /data1/MindsVOC/TA/rec_server/2*
# days : days + 1 이상 지난 파일디렉토리를 삭제
def delete_old_files_and_dirs():
    path = '/data1/MindsVOC/TA/rec_server/*'
    days = 6

    logger = logging.getLogger('SD_Logger')
    logger.info('JOB Start ...')
    # cmd = 'find %s -mtime +%d -ls' % (path, days)
    # TODO : 실제로 파일 삭제를 수행하려면 아래 주석을 풀어주세요
    cmd = 'find %s -mtime +%d -ls -exec rm -rf {} \;' % (path, days)
    sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    response_out, response_err = sp.communicate()
    print response_out
    logger.info('JOB End ...')


def update_bad_status_cd():
    logger = logging.getLogger('SD_Logger')
    logger.info('update_bad_status_cd : JOB Start ...')
    cmd = '/data1/MindsVOC/TA/Meritz/bin/StatCdMng.py'
    sp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    response_out, response_err = sp.communicate()
    logger.info(response_out)
    logger.info('update_bad_status_cd : JOB End ...')


def dummy_jon():
    time.sleep(1)


def test1():
    logger = logging.getLogger('SD_Logger')
    logger.info('test1 start')
    time.sleep(20)
    logger.info('test1 end')


def test2():
    multiprocessing.Process(target=do_task)
    logger = logging.getLogger('SD_Logger')
    logger.info('test2 start')
    time.sleep(5)
    logger.info('test2 end')


def do_task(task_name):
    p = multiprocessing.Process(target=task_name)
    p.start()


class SchedDaemon(Daemon):
    def run(self):
        set_sig_handler()
        logger = get_logger(LOG_FILE, logging.INFO)
        s = sched.scheduler(time.time, time.sleep)

        logger.info('SD Daemon start')

        # every 180 seconds ( 3 minutes )
        unit_time = 180
        while True:
            # expression of time class
            future = (int(time.time()) + unit_time) / unit_time * unit_time
            # expression of datetime class
            future_dt = datetime.fromtimestamp(future)

            time.sleep(0.1)  # for clean log ..
            logger.info('Next TA_schedule time is %s' % str(future_dt))

            ########################################################################################################
            # s.run()을 위해서 dummy job 1개가 이 반드시 필요함
            # s.enterabs(future, 1, do_task, (dummy_job, ))
            ########################################################################################################

            ########################################################################################################
            # 매일 시/분/초 를 지정 , wav 삭제 프로그램
            # if future_dt.hour == 1 and future_dt.minute == 0 and future_dt.second == 0:
                # pydoc 설명
                # enterabs(self, time, priority, action, argument)
            #   s.enterabs(future, 1, do_task, (delete_old_files_and_dirs, ))

            ########################################################################################################
            # when every 5 minute and zero second, Do 상태값 보정 배치 프로그램
            s.enterabs(future, 1, do_task, (update_bad_status_cd, ))
            ########################################################################################################

            ########################################################################################################
            # test1
            # s.enterabs(future, 1, do_task, (test1, ))

            # test2
            # s.enterabs(future, 1, do_task, (test2, ))
            ########################################################################################################

            s.run()

if __name__ == '__main__':
    # daemon = SchedDaemon('/tmp/TA_sched_daemon.pid', stdout='/dev/null', stderr='/dev/null')
    daemon = SchedDaemon('/tmp/TA_sched_daemon.pid', stdout='/tmp/sd.log', stderr='/tmp/sd.log')
    # daemon.run()
    # sys.exit(0)

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
        print "usage : %s start | stop | restart" % sys.argv[0]
        sys.exit(2)
