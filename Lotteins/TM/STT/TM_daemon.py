#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-21, modification: 2018-03-27"

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
from lib.daemon import Daemon
from cfg.config import TM_DAEMON_CONFIG, DB_CONFIG
from logging.handlers import TimedRotatingFileHandler

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
        pri_pid_list = list()
        pid_list = list()
        job_max_limit = int(TM_DAEMON_CONFIG['job_max_limit'])
        pri_process_max_limit = int(TM_DAEMON_CONFIG['pri_process_max_limit'])
        process_max_limit = int(TM_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(TM_DAEMON_CONFIG['process_interval'])
        # log setting
        log_file_path = "{0}/{1}".format(TM_DAEMON_CONFIG['log_dir_path'], TM_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("TM Daemon Started ...")
        logger.info("job max limit is {0}".format(job_max_limit))
        logger.info("priority process max limit is {0}".format(pri_process_max_limit))
        logger.info("general process max limit is {0}".format(process_max_limit))
        logger.info("process interval is {0}".format(process_interval))
        while True:
            start_time = int(TM_DAEMON_CONFIG['work_sttm'])
            end_time = int(TM_DAEMON_CONFIG['work_endtm'])
            ts = time.time()
            current_time = datetime.fromtimestamp(ts).strftime('%H')
            temp_process_cnt = process_max_limit
            if start_time <= int(current_time) < end_time:
                process_max_limit = int(TM_DAEMON_CONFIG['work_process_max_limit'])
            else:
                process_max_limit = int(TM_DAEMON_CONFIG['process_max_limit'])
            total_process_max_limit = pri_process_max_limit + process_max_limit
            if temp_process_cnt != process_max_limit:
                logger.info('general process max limit change {0} -> {1}'.format(temp_process_cnt, process_max_limit))
            # 우선순위 전용 프로세스 생성 갯수 설정
            for pri_pid in pri_pid_list[:]:
                if not pri_pid.is_alive():
                    pri_pid_list.remove(pri_pid)
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            pri_run_count = total_process_max_limit - len(pri_pid_list) - len(pid_list)
            if len(pri_pid_list) > pri_process_max_limit:
                steal_run_count = len(pri_pid_list) - pri_process_max_limit
            else:
                steal_run_count = 0
            run_count = process_max_limit - steal_run_count - len(pid_list)
            # Oracle DB connect
            oracle = connect_db(logger, 'Oracle')
            try:
                # 우선순위 잡 가져오기
                pri_job_list = make_pri_job_list(oracle, pri_run_count)
                job_list = make_job_list(oracle, run_count, job_max_limit)
                oracle.disconnect()
            except Exception:
                try:
                    oracle.disconnect()
                except Exception:
                    logger.error('oracle is already disconnect')
                logger.error(traceback.format_exc())
                continue
            # 우선순위 전용 프로세스 생성
            if len(pri_job_list) > 0:
                pri_pid_list = process_execute(
                    logger=logger,
                    job_list=pri_job_list,
                    run_count=pri_run_count,
                    pid_list=pri_pid_list,
                    pid_count=len(pid_list),
                    process_max_limit=total_process_max_limit,
                    name='priority'
                )
            # 일반 프로세스 생성
            else:
                if len(job_list) > 0:
                    pid_list = process_execute(
                        logger=logger,
                        job_list=job_list,
                        run_count=run_count,
                        pid_list=pid_list,
                        pid_count=steal_run_count,
                        process_max_limit=process_max_limit,
                        name='general'
                    )


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

    def select_target(self, cntr_proc_dcd):
        query = """
            SELECT
                REC_ID,
                RFILE_NAME,
                NQA_STTA_PRGST_CD,
                BIZ_CD,
                CALL_DURATION
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND CNTR_PROC_DCD = :1
                AND NQA_STTA_PRGST_CD = '00'
                AND STT_SERVER_ID = :2
            ORDER BY
                SUBSTR(rgst_dtm, 1, 8) ASC,
                CALL_DURATION
        """
        bind = (
            cntr_proc_dcd,
            str(socket.gethostname())
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return False
        return results

    def select_pri_target(self, cntr_proc_dcd, rec_id, rfile_name):
        query = """
            SELECT
                REC_ID,
                RFILE_NAME,
                NQA_STTA_PRGST_CD,
                BIZ_CD,
                CALL_DURATION
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND CNTR_PROC_DCD = :1
                AND ( 1=0
                    OR NQA_STTA_PRGST_CD = '00'
                    OR NQA_STTA_PRGST_CD = '90'
                )
                AND STT_SERVER_ID = :2
                AND REC_ID = :3
                AND RFILE_NAME = :4
        """
        bind = (
            cntr_proc_dcd,
            str(socket.gethostname()),
            rec_id,
            rfile_name,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
        return results

    def select_poli_no_stt_process(self, qa_stta_prgst_cd):
        query = """
            SELECT
                POLI_NO,
                CTRDT,
                CNTR_COUNT
            FROM
                TB_TM_CNTR_INFO
            WHERE 1=1
                AND QA_STTA_PRGST_CD = :1
        """
        bind = (
            qa_stta_prgst_cd,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return list()
        if not results:
            return list()
        return results

    def select_rec_id(self, poli_no, ctrdt, cntr_count):
        query = """
            SELECT
                REC_ID,
                RFILE_NAME
            FROM
                TB_TM_CNTR_RCDG_INFO
            WHERE 1=1
                AND POLI_NO = :1
                AND CTRDT = :2
                AND CNTR_COUNT = :3
        """
        bind = (
            poli_no,
            ctrdt,
            cntr_count,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
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
def process_execute(**kwargs):
    """
    Execute multi process
    :param          kwargs      Arguments
    :return:                    new PID list
    """
    logger = kwargs.get('logger')
    job_list = kwargs.get('job_list')
    run_count = kwargs.get('run_count')
    pid_list = kwargs.get('pid_list')
    pid_count = kwargs.get('pid_count')
    process_max_limit = kwargs.get('process_max_limit')
    name = kwargs.get('name')
    for _ in range(run_count):
        for job in job_list[:]:
            if len(pid_list) + pid_count >= process_max_limit:
                logger.info('Total processing Count is MAX....')
                break
            if len(job) > 0:
                p = multiprocessing.Process(target=do_task, args=(job,))
                pid_list.append(p)
                p.start()
                logger.info('spawn new {0} processing, pid is [{1}]'.format(name, p.pid))
                for item in job:
                    logger.info('\t{0}'.format(item))
                sleep_exact_time(int(TM_DAEMON_CONFIG['process_interval']))
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
    Process execute STT
    :param      job_list:        Job list
    """
    sys.path.append(TM_DAEMON_CONFIG['stt_script_path'])
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
    if not target_list:
        return main_list
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
    if run_count <= 0:
        return list()
    # TM 후심사 대상 가져오기
    first_qa_target_list = oracle.select_target('01')
    slow_qa_target_list = oracle.select_target('02')
    target_list = oracle.select_target('00')
    if not first_qa_target_list and not slow_qa_target_list and not target_list:
        return list()
    pk_list = list()
    pk_list = append_list(first_qa_target_list, pk_list)
    pk_list = append_list(slow_qa_target_list, pk_list)
    pk_list = append_list(target_list, pk_list)
    # TM 후심사 대상 분배
    job_list = init_list_of_objects(run_count)
    cnt = 0
    for rec_file_dict in pk_list:
        if cnt == run_count:
            cnt = 0
        if len(job_list[cnt]) < job_max_limit:
            job_list[cnt].append(rec_file_dict)
        cnt += 1
    return job_list[:run_count]


def make_pri_job_list(oracle, pri_run_count):
    """
    Make priority job list
    :param      oracle:                         Oracle DB
    :param      pri_run_count:                  Priority run count
    :return:                                    Priority job list
    """
    if pri_run_count == 0:
        return list()
    # TM 계약 대상 건 중 STT 처리중 콜 가져오기
    target_list = oracle.select_poli_no_stt_process('01')
    if len(target_list) < 1:
        return list()
    # TM 계약 대상 건 수만큼 job_list 생성
    job_list = init_list_of_objects(pri_run_count)
    cnt = 0
    for target in target_list:
        poli_no = target[0].strip()
        ctrdt = target[1].strip()
        cntr_count = target[2].strip()
        # 계약건에 매핑되어있는 녹취파일 정보 가져오기
        target_rec_id_list = oracle.select_rec_id(poli_no, ctrdt, cntr_count)
        if not target_rec_id_list:
            continue
        pri_pk_list = list()
        # 전체 녹취파일을 돌며 STT 미처리 파일 가져오기
        for item in target_rec_id_list:
            rec_id = item[0].strip()
            rfile_name = item[1].strip()
            pri_target_list = oracle.select_pri_target('01', rec_id, rfile_name)
            if not pri_target_list:
                continue
            pri_pk_list = append_list(pri_target_list, pri_pk_list)
        if len(pri_pk_list) < 1:
            continue
        # STT 미처리 파일 priority job list 에 추가
        for rec_file_dict in pri_pk_list:
            if not len(job_list[cnt]) < 5:
                cnt += 1
                if cnt + 1 > len(job_list):
                    break
            job_list[cnt].append(rec_file_dict)
        cnt += 1
        if cnt + 1 > len(job_list):
            break
    return job_list[:cnt]


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
        logger.info('STT Daemon stop')
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
    if not os.path.exists(TM_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(TM_DAEMON_CONFIG['log_dir_path'])
    if not os.path.exists(TM_DAEMON_CONFIG['pid_dir_path']):
        os.makedirs(TM_DAEMON_CONFIG['pid_dir_path'])
    daemon = DAEMON(
        os.path.join(TM_DAEMON_CONFIG['pid_dir_path'], TM_DAEMON_CONFIG['pid_file_name']),
        stdout='{0}/stdout_daemon.log'.format(TM_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(TM_DAEMON_CONFIG['log_dir_path'])
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
