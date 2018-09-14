#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-08, modification: 2018-03-28"

###########
# imports #
###########
import os
import sys
import time
import signal
import logging
import argparse
import cx_Oracle
import traceback
import multiprocessing
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import cfg.config
from lib.daemon import Daemon
from lib.openssl import decrypt_string

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#############
# constants #
#############
TM_QA_TA_DAEMON_CONFIG = {}
DB_CONFIG = {}
CONFIG_TYPE = ''


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = DB_CONFIG['dsn']
        passwd = decrypt_string(DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def rows_to_dict_list(self):
        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor]

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_target(self):
        query = """
            SELECT
                A.ORG_FOF_C,
                A.EPL_ID,
                A.PRPS_DATE,
                A.CNTR_COUNT,
                A.CONT_NO,
                A.CNTC_CID,
                A.IP_CD,
                A.PRPS_CNTC_USID,
                A.SEX_TC,
                A.FETUS_YN,
                A.AGE,
                A.QA_STTA_PRGST_CD,
                A.TA_REQ_DTM,
                B.IITEM,
                B.IITEM_NM
            FROM
                TB_TM_CNTR_INFO A, TB_TM_CNTR_SP_INFO B
            WHERE 1=1
                AND A.QA_STTA_PRGST_CD in ('07','10')
                AND B.IITEM_GB = '1'
                AND A.ORG_FOF_C = B.ORG_FOF_C
                AND A.EPL_ID = B.EPL_ID
                AND ROWNUM <= 5
            FOR UPDATE SKIP LOCKED
        """
        self.cursor.execute(query)
        results = self.rows_to_dict_list()
        if results is bool:
            return False
        if not results:
            return False
        return results

    def select_cntr_rcdg_info(self, org_fof_c, epl_id, prps_date):
        query = """
            SELECT DISTINCT
                A.REC_NO AS REC_ID
                , B.DOCUMENT_ID AS RFILE_NAME
                , B.STT_PRGST_CD
                , A.CONT_NO
            FROM
                TB_TM_CNTR_RCDE_INFO A, CALL_META B
            WHERE 1=1
                AND A.ORG_FOF_C = :1
                AND A.EPL_ID = :2
                AND A.PRPS_DATE = :3
                AND B.PROJECT_CD in ('TM', 'CD')
                AND A.REC_NO = B.REC_ID
        """
        bind = (
            org_fof_c,
            epl_id,
            prps_date,
        )
        self.cursor.execute(query, bind)
        results = self.rows_to_dict_list()
        if results is bool:
            return False
        if not results:
            return False
        return results

    def update_qa_stta_prgst_cd(self, org_fof_c, epl_id, prps_date, cntr_count, qa_stta_prgst_cd):
        try:
            query = """
                UPDATE
                    TB_TM_CNTR_INFO
                SET
                    QA_STTA_PRGST_CD = :1,
                    TA_CM_DTM = SYSDATE
                WHERE 1=1
                    AND org_fof_c = :2
                    AND epl_id = :3
                    AND prps_date = :4
                    AND cntr_count = :5
            """
            bind = (
                qa_stta_prgst_cd,
                org_fof_c,
                epl_id,
                prps_date,
                cntr_count,
            )
            self.logger.info(
                'Update QA_STTA_PRGST_CD to {4} [org_fof_c : {0}, epl_id : {1}, prps_date : {2}, cntr_count: {3}'
                    .format(org_fof_c, epl_id, prps_date, cntr_count, qa_stta_prgst_cd)
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            self.disconnect()
            raise Exception(traceback.format_exc())


def set_config(config_type):
    """
    active type setting
    :param config_type:
    :return:
    """
    global TM_QA_TA_DAEMON_CONFIG
    global DB_CONFIG
    global CONFIG_TYPE

    CONFIG_TYPE = config_type
    TM_QA_TA_DAEMON_CONFIG = cfg.config.TM_QA_TA_DAEMON_CONFIG[config_type]
    DB_CONFIG = cfg.config.DB_CONFIG[config_type]


class DAEMON(Daemon):

    def run(self):
        set_sig_handler()
        qa_pid_list = list()
        process_max_limit = int(TM_QA_TA_DAEMON_CONFIG['process_max_limit'])
        process_interval = int(TM_QA_TA_DAEMON_CONFIG['process_interval'])
        # log setting
        log_file_path = "{0}/{1}".format(TM_QA_TA_DAEMON_CONFIG['log_dir_path'], TM_QA_TA_DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("TM QA Daemon Started ...")
        logger.info("Process max limit is {0}".format(process_max_limit))
        logger.info("Process interval is {0}".format(process_interval))

        while True:
            # Oracle DB connect
            oracle = connect_db(logger, 'Oracle')
            if not oracle:
                logger.error('DB connect Fail')
                continue
            # bring the target
            try:
                qa_job_list = make_qa_job_list(logger, oracle, process_max_limit)
                for qa_pid in qa_pid_list[:]:
                    if not qa_pid.is_alive():
                        qa_pid_list.remove(qa_pid)
                general_run_count = process_max_limit - len(qa_pid_list)
                qa_pid_list = process_execute(
                    logger=logger,
                    job_list=qa_job_list,
                    run_count=general_run_count,
                    pid_list=qa_pid_list,
                    pid_count=len(qa_pid_list),
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
def qa_ta_do_task(job):
    """
    Process execute qa_TA
    :param      job:       Job
    """
    sys.path.append(TM_QA_TA_DAEMON_CONFIG['ta_script_path'])
    import TM_QA_TA
    reload(TM_QA_TA)
    TM_QA_TA.main(job, CONFIG_TYPE)


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
                logger.info(job)
                sleep_exact_time(int(TM_QA_TA_DAEMON_CONFIG['process_interval']))
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


def make_rec_file_dict(oracle, org_fof_c, epl_id, prps_date):
    """
    Select rec file
    :param      oracle:             Oracle DB
    :param      org_fof_c:          점포코드
    :param      epl_id:             가입설계아이디
    :param      prps_date:          청약일
    :return:                    REC file dictionary
    """
    # Select REC file name and REC information
    rec_file_dict = oracle.select_cntr_rcdg_info(org_fof_c, epl_id, prps_date)
    if not rec_file_dict:
        return False
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
        target_dict_list = oracle.select_target()
    except Exception:
        time.sleep(1)
        return list()
    if not target_dict_list:
        return list()
    idx = 0
    job_list = list()
    for target_dict in target_dict_list:
        org_fof_c = target_dict['ORG_FOF_C']
        epl_id = target_dict['EPL_ID']
        prps_date = target_dict['PRPS_DATE']
        cntr_count = target_dict['CNTR_COUNT']
        try:
            rec_file_dict_list = make_rec_file_dict(oracle, org_fof_c, epl_id, prps_date)
            # Oracle DB 에서 증서 번호, 청약 일자 기준으로 녹취 ID 가 없는 경우
            if not rec_file_dict_list:
                oracle.update_qa_stta_prgst_cd(org_fof_c, epl_id, prps_date, cntr_count, '90')
                continue

            qa_stta_prgst_cd = ''
            # 녹취 파일 전체 STT 처리가 되어있는지 확인
            for rec_file_dict in rec_file_dict_list:
                if rec_file_dict['STT_PRGST_CD'] == '01':
                    qa_stta_prgst_cd = '01'
                    break
                elif rec_file_dict['STT_PRGST_CD'] == '02':
                    qa_stta_prgst_cd = '02'
                    break
                elif rec_file_dict['STT_PRGST_CD'] == '03':
                    qa_stta_prgst_cd = '03'
                    break
                elif rec_file_dict['STT_PRGST_CD'] == '90':
                    qa_stta_prgst_cd = '90'
                    break
            if qa_stta_prgst_cd != '':
                # 녹취 파일 전체 중 STT 처리가 안되어 있는게 있다면 STT 상태(stta_prgst_cd)와 같은 값으로 세팅
                oracle.update_qa_stta_prgst_cd(org_fof_c, epl_id, prps_date, cntr_count, qa_stta_prgst_cd)

        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't make rec_file_info_dict org_fof_c = {0}, epl_id = {1}, prps_date = {2}, cntr_count = {3}"
                         .format(org_fof_c, epl_id, prps_date, cntr_count))
            logger.error(exc_info)
            continue
        job_list.append(target_dict)
        idx += 1
        if idx >= process_max_limit:
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
                os.environ["NLS_LANG"] = "Korean_Korea.KO16KSC5601"
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
    log_formatter = MyFormatter(fmt='%(asctime)s - %(levelname)s [%(lineno)d] - %(message)s')
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
        logger.info('TM QA Daemon stop')
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    parser.add_argument('-p', action='store', dest='process', type=str, help='stop or start or restart'
                        , required=True, choices=['stop', 'start', 'restart'])
    arguments = parser.parse_args()
    set_config(arguments.config_type)
    if not os.path.exists(TM_QA_TA_DAEMON_CONFIG['log_dir_path']):
        os.makedirs(TM_QA_TA_DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
        os.path.join(TM_QA_TA_DAEMON_CONFIG['pid_dir_path'], TM_QA_TA_DAEMON_CONFIG['pid_file_name']),
        stdout='{0}/stdout_daemon.log'.format(TM_QA_TA_DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(TM_QA_TA_DAEMON_CONFIG['log_dir_path'])
    )

    # Check argument
    if 'start' == arguments.process.lower():
        daemon.start()
    elif 'stop' == arguments.process.lower():
        daemon.stop()
    elif 'restart' == arguments.process.lower():
        daemon.restart()
    else:
        print 'Unknown command'
        print 'usage: %s start | stop | restart' % sys.argv[0]
        sys.exit(2)
