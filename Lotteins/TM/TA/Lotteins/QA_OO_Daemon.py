#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-08, modification: 2018-03-12"

###########
# imports #
###########
import os
import sys
import time
import signal
import logging
import requests
import cx_Oracle
import traceback
import multiprocessing
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from cfg.config import QA_OO_DAEMON_CONFIG, DB_CONFIG
from lib.daemon import Daemon

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
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
        self.conn.autocommit = False
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
                POLI_NO,
                CTRDT,
                IP_CD,
                CNTR_COUNT,
                TA_REQ_DTM,
                IP_DCD
            FROM
                TB_TM_CNTR_INFO
            WHERE 1=1
                AND CNTR_PROC_DCD = :1
                AND QA_STTA_PRGST_CD = '03'
                AND ROWNUM <= 5
            FOR UPDATE SKIP LOCKED
        """
        bind = (
            cntr_proc_dcd,
        )
        self.cursor.execute(query, bind)
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
        return results

    def select_cntr_rcdg_info(self, poli_no, ctrdt):
        query = """
            SELECT DISTINCT
                REC_ID,
                RFILE_NAME
            FROM
                TB_TM_CNTR_RCDG_INFO
            WHERE 1=1
                AND POLI_NO = :1
                AND CTRDT = :2
        """
        bind = (
            poli_no,
            ctrdt,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_nqa_stta_prgst_cd(self, rec_id, rfile_name):
        query = """
            SELECT
                NQA_STTA_PRGST_CD
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND REC_ID = :1
                AND RFILE_NAME = :2
        """
        bind = (
            rec_id,
            rfile_name,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_nqa_sel_ta_prgst_cd(self):
        query = """
            SELECT
                REC_ID,
                RFILE_NAME
            FROM
                TB_TM_STT_RCDG_INFO
            WHERE 1=1
                AND NQA_STTA_PRGST_CD = '13'
                AND NQA_SEL_TA_PRGST_CD = '10'
        """
        self.cursor.execute(query)
        results = self.cursor.fetchall()
        if results is bool:
            return False
        if not results:
            return False
        return results

    def update_http_status(self, **kwargs):
        try:
            sql = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    HTTP_TRANS_CD = :1,
                    LST_CHGP_CD = 'TM_TA_DA',
                    LST_CHG_PGM_ID = 'TM_TA_DA',
                    LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND POLI_NO = :2
                    AND CTRDT = :3
                    AND CNTR_COUNT = :4
            """
            bind = (
                kwargs.get('http_trans_cd'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
                kwargs.get('cntr_count')
            )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
#                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def update_qa_stta_prgst_cd(self, poli_no, ctrdt, cntr_count, qa_stta_prgst_cd):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    QA_STTA_PRGST_CD = :1,
                    TA_CMDTM = SYSDATE
                WHERE 1=1
                    AND POLI_NO = :2
                    AND CTRDT = :3
                    AND CNTR_COUNT = :4
            """
            bind = (
                qa_stta_prgst_cd,
                poli_no,
                ctrdt,
                cntr_count,
            )
            self.logger.info('Update QA_STTA_PRGST_CD to {1} [POLI_NO : {1}, CTRDT : {2}, CNTR_COUNT : {3}'.format(
                qa_stta_prgst_cd, poli_no, ctrdt, cntr_count))
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
#                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            self.disconnect()
            raise Exception(traceback.format_exc())


class DAEMON(Daemon):
    def run(self):
        set_sig_handler()
        qa_pri_pid_list = list()
        qa_pid_list = list()
        oo_pid_list = list()
        oo_job_max_limit = int(QA_OO_DAEMON_CONFIG['oo_job_max_limit'])
        pri_process_max_limit = int(QA_OO_DAEMON_CONFIG['pri_process_max_limit'])
        process_max_limit = int(QA_OO_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(QA_OO_DAEMON_CONFIG['process_interval'])
        total_process_max_limit = pri_process_max_limit + process_max_limit
        # log setting
        log_file_path = "{0}/{1}".format(QA_OO_DAEMON_CONFIG['log_dir_path'], QA_OO_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("QA and OO Daemon Started ...")
        logger.info('OO job max limit is {0}'.format(oo_job_max_limit))
        logger.info('Priority process max limit is {0}'.format(pri_process_max_limit))
        logger.info("Process max limit is {0}".format(process_max_limit))
        logger.info("Process interval is {0}".format(process_interval))
        while True:
            # Oracle DB connect
            oracle = connect_db(logger, 'Oracle')
            if not oracle:
                logger.error('DB connect Fail')
                continue
            try:
                # bring the target
                qa_pri_job_list = make_qa_pri_job_list(logger, oracle, total_process_max_limit)
                qa_job_list = make_qa_job_list(logger, oracle, process_max_limit)
                oo_job_list = make_oo_job_list(logger, oracle, process_max_limit, oo_job_max_limit)
                # 우선순위 전용 프로세스 생성 갯수 설정
                for qa_pri_pid in qa_pri_pid_list[:]:
                    if not qa_pri_pid.is_alive():
                        qa_pri_pid_list.remove(qa_pri_pid)
                for qa_pid in qa_pid_list[:]:
                    if not qa_pid.is_alive():
                        qa_pid_list.remove(qa_pid)
                for oo_pid in oo_pid_list[:]:
                    if not oo_pid.is_alive():
                        oo_pid_list.remove(oo_pid)
                qa_pri_run_count = total_process_max_limit - len(qa_pri_pid_list) - len(qa_pid_list) - len(oo_pid_list)
                if len(qa_pri_pid_list) > pri_process_max_limit:
                    steal_run_count = len(qa_pri_pid_list) - pri_process_max_limit
                else:
                    steal_run_count = 0
                general_run_count = process_max_limit - steal_run_count - len(oo_pid_list) - len(qa_pid_list)
                # 우선순위 전용 프로세스 생성
                if len(qa_pri_job_list) > 0:
                    qa_pri_pid_list = process_execute(
                        logger=logger,
                        job_list=qa_pri_job_list,
                        run_count=qa_pri_run_count,
                        pid_list=qa_pri_pid_list,
                        pid_count=len(qa_pid_list) + len(oo_pid_list),
                        process_max_limit=total_process_max_limit,
                        do_task=qa_ta_do_task,
                        name='QA priority'
                    )
                # TM 미체결 선택 프로세스 생성
                elif len(oo_job_list) > 0:
                    oo_pid_list = process_execute(
                        logger=logger,
                        job_list=oo_job_list,
                        run_count=general_run_count,
                        pid_list=oo_pid_list,
                        pid_count=steal_run_count + len(qa_pid_list),
                        process_max_limit=process_max_limit,
                        do_task=oo_ta_do_task,
                        name='QO general'
                    )
                # General
                else:
                    qa_pid_list = process_execute(
                        logger=logger,
                        job_list=qa_job_list,
                        run_count=general_run_count,
                        pid_list=qa_pid_list,
                        pid_count=steal_run_count + len(oo_pid_list),
                        process_max_limit=process_max_limit,
                        do_task=qa_ta_do_task,
                        name='QA general'
                    )
                oracle.conn.commit()
                oracle.disconnect()
            except Exception:
                try:
                    oracle.disconnect()
                except Exception:
                    logger.error('oracle is already disconnect')
                logger.error(traceback.format_exc())
                continue

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
def oo_ta_do_task(job):
    """
    Process execute select_TA
    :param      job:       Job
    """
    sys.path.append(QA_OO_DAEMON_CONFIG['ta_script_path'])
    import OO_TA
    reload(OO_TA)
    OO_TA.main(job)


def qa_ta_do_task(job):
    """
    Process execute qa_TA
    :param      job:       Job
    """
    sys.path.append(QA_OO_DAEMON_CONFIG['ta_script_path'])
    import QA_TA
    reload(QA_TA)
    QA_TA.main(job)


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
    do_task = kwargs.get('do_task')
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
                sleep_exact_time(int(QA_OO_DAEMON_CONFIG['process_interval']))
                time.sleep(1)
        job_list = list()
    return pid_list


def sleep_exact_time(seconds):
    """
    Sleep
    :param      seconds:        Seoonds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


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


def make_oo_job_list(logger, oracle, process_max_limit, job_max_limit):
    """
    Make outstanding order TA job list
    :param      logger:                 Logger
    :param      oracle:                 Oracle DB
    :param      process_max_limit:      Process max limit
    :param      job_max_limit:          Job max limit
    :return:                            Job list
    """
    # 미체결 선택 TA 대상 가져오기
    try:
        target_list = oracle.select_nqa_sel_ta_prgst_cd()
        if not target_list:
            return list()
        pk_list = list()
        for target in target_list:
            rec_id = str(target[0]).strip()
            rfile_name = str(target[1]).strip()
            temp_dict = {
                'REC_ID': rec_id,
                'RFILE_NAME': rfile_name
            }
            pk_list.append(temp_dict)
        # TM 미체결 선택 TA 대상 분배
        job_list = init_list_of_objects(process_max_limit)
        cnt = 0
        for rec_file_dict in pk_list:
            job_list[cnt].append(rec_file_dict)
            if len(job_list[cnt]) == job_max_limit:
                if cnt + 1 < process_max_limit:
                    cnt += 1
                else:
                    break
        return job_list[:cnt+1]
    except Exception:
        exc_info = traceback.format_exc()
        logger.error("Can't make OO TA job list.")
        logger.error(exc_info)
        return list()


def check_stt_process(oracle, rec_file_dict):
    """
    Check STT process
    :param      oracle:             Oracle DB
    :param      rec_file_dict:      REC file dictionary
    :return:                        True or False
    """
    stt_fin_cnt = 0
    return True
    for info_dict in rec_file_dict.values():
        rec_id = info_dict['REC_ID']
        rfile_name = info_dict['RFILE_NAME']
        nqa_stta_prgst_cd = oracle.select_nqa_stta_prgst_cd(rec_id, rfile_name)
        if not nqa_stta_prgst_cd:
            continue
        if nqa_stta_prgst_cd[0] == '13' or nqa_stta_prgst_cd[0] == '90':
            stt_fin_cnt += 1
    if stt_fin_cnt != len(rec_file_dict):
        return False
    return True


def get_cntr_info(logger, oracle, poli_no, status, ip_dcd, ctrdt, cntr_count):
    """
    Http requests get cntr info
    :param      logger:         Logger
    :param      oracle:         Oracle
    :param      poli_no:        POLI_NO(증서번호)
    :param      status:         Status
    :param      ip_dcd:         IP_DCD(보험상품구분코드)
    :param      ctrdt:          CTRDT(청약일자)
    :param      cntr_count:     CNTR_COUNT(심하회차)
    :return:
    """
    bjgb = ''
    if ip_dcd == '01' or ip_dcd == '04':
        bjgb = 'L'
    elif ip_dcd == '02':
        bjgb = 'O'
    elif ip_dcd == '03':
        bjgb = 'A'
    url = QA_OO_DAEMON_CONFIG['http_url']
    params = {
        'bjgb': bjgb,
        'polno': poli_no,
        'sttstatus': status
    }
    try:
        requests.get(url, params=params, timeout=QA_OO_DAEMON_CONFIG['requests_timeout'])
        oracle.update_http_status(
            http_trans_cd='01',
            poli_no=poli_no,
            ctrdt=ctrdt,
            cntr_count=cntr_count
        )
    except Exception:
        logger.error('\tFail http status send ->\tpoli_no : {0}\tctrdt : {1}\tcntr_count : {2}'.format(
            poli_no, ctrdt, cntr_count))
        oracle.update_http_status(
            http_trans_cd='02',
            poli_no=poli_no,
            ctrdt=ctrdt,
            cntr_count=cntr_count
        )


def make_rec_file_dict(oracle, poli_no, ctrdt):
    """
    Select rec file
    :param      oracle:         Oracle DB
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(계약 일자)
    :return:                    REC file dictionary
    """
    # Select REC file name and REC information
    rec_file_dict = dict()
    cntr_rcdg_info_list = oracle.select_cntr_rcdg_info(poli_no, ctrdt)
    if not cntr_rcdg_info_list:
        return False
    for tm_info in cntr_rcdg_info_list:
        rec_id = str(tm_info[0]).strip()
        rfile_name = str(tm_info[1]).strip()
        result_dict = {
            'REC_ID': rec_id,
            'RFILE_NAME': rfile_name
        }
        key = '{0}_{1}'.format(rec_id, rfile_name)
        rec_file_dict[key] = result_dict
    return rec_file_dict


def make_qa_job_list(logger, oracle, process_max_limit):
    """
    Make QA_TA job list
    :param      logger:                 Logger
    :param      oracle:                 Oracle DB
    :param      process_max_limit:      Process max limit
    :return:                            QA_TA Job list
    """
    # bring the QA_TA Target
    try:
        target_list = oracle.select_target('02')
    except Exception:
        time.sleep(1)
        return list()
    if not target_list:
        return list()
    idx = 0
    job_list = list()
    for target in target_list:
        poli_no = str(target[0]).strip()
        ctrdt = str(target[1]).strip()
        cntr_count = str(target[3]).strip()
        ip_dcd = str(target[5]).strip()
        try:
            rec_file_dict = make_rec_file_dict(oracle, poli_no, ctrdt)
            # Oracle DB 에서 증서 번호, 청약 일자 기준으로 녹취 ID 가 없는 경우
            if not rec_file_dict:
                oracle.update_qa_stta_prgst_cd(poli_no, ctrdt, cntr_count, '90')
                get_cntr_info(logger, oracle, poli_no, '90', ip_dcd, ctrdt, cntr_count)
                continue
            # STT 처리 완료 수와 증서 번호, 청약 일자 기준 녹취파일 수가 다를 경우 ( STT 가 처리 중일 수 있음 )
            if not check_stt_process(oracle, rec_file_dict):
                continue
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't make rec_file_info_dict POLI_NO = {0}, CTRDT = {1}".format(poli_no, ctrdt))
            logger.error(exc_info)
            continue
        job_list.append(target)
        idx += 1
        if idx >= process_max_limit:
            break
    return job_list


def make_qa_pri_job_list(logger, oracle, total_process_max_limit):
    """
    Make priority job list
    :param      logger:                         Logger
    :param      oracle:                         Oracle DB
    :param      total_process_max_limit:        Total process max limit
    :return:                                    Priority job list
    """
    # TM 선심사 대상 가져오기
    try:
        target_list = oracle.select_target('01')
    except Exception:
        time.sleep(1)
        return list()
    if not target_list or len(target_list) < 1:
        return list()
    idx = 0
    job_list = list()
    for target in target_list:
        poli_no = str(target[0]).strip()
        ctrdt = str(target[1]).strip()
        cntr_count = str(target[3]).strip()
        ip_dcd = str(target[5]).strip()
        try:
            rec_file_dict = make_rec_file_dict(oracle, poli_no, ctrdt)
            # Oracle DB 에서 증서 번호, 청약 일자 기준으로 녹취 ID가 없는 경우
            if not rec_file_dict:
                oracle.update_qa_stta_prgst_cd(poli_no, ctrdt, cntr_count, '90')
                get_cntr_info(logger, oracle, poli_no, '90', ip_dcd, ctrdt, cntr_count)
                continue
            # STT 처리 완료 수와 증서 번호, 청약 일자 기준으로 녹취파일 수가 다를 경우 ( STT 가 처리 중일 수 있음 )
            if not check_stt_process(oracle, rec_file_dict):
                continue
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't make rec_file_info_dict POLI_NO = {0}, CTRDT = {1}".format(poli_no, ctrdt))
            logger.error(exc_info)
            continue
        job_list.append(target)
        idx += 1
        if idx >= total_process_max_limit:
            break
    return job_list


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
                sql = Oracle(logger)
            else:
                raise Exception("Unknown database [{0}], Oracle".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def get_logger(fname, log_level):
    """
    Set logger
    :param      fname:          Log file name
    :param      log_level:      Log level
    :return:                    Logger
    """
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='[%(asctime)s %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('TA_Logger')
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
        logger = logging.getLogger('TA_Logger')
        logger.info('QA OO  Daemon stop')
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
    if not os.path.exists(QA_OO_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(QA_OO_DAEMON_CONFIG['log_dir_path'])
    if not os.path.exists(QA_OO_DAEMON_CONFIG['pid_dir_path']):
        os.makedirs(QA_OO_DAEMON_CONFIG['pid_dir_path'])
    daemon = DAEMON(
        os.path.join(QA_OO_DAEMON_CONFIG['pid_dir_path'], QA_OO_DAEMON_CONFIG['pid_file_name']),
        stdout='{0}/stdout_daemon.log'.format(QA_OO_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(QA_OO_DAEMON_CONFIG['log_dir_path'])
    )
    # Check argument
    if len(sys.argv) == 2:
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
        print "usage: %s start | stop | restart" % sys.argv[0]
        sys.exit(2)
