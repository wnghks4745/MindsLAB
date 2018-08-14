#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-11-30, modification: 2017-12-04"

###########
# imports #
###########
import os
import sys
import time
import shutil
import signal
import MySQLdb
import pymssql
import logging
import traceback
import collections
import multiprocessing
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from lib.daemon import Daemon
from cfg.config import DAEMON_CONFIG
from cfg.config import STT_CONFIG
from cfg.config import MYSQL_DB_CONFIG
from cfg.config import MSSQL_DB_CONFIG


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
        process_max_limit = int(DAEMON_CONFIG['process_max_limit'])
        process_interval = int(DAEMON_CONFIG['process_interval'])
        log_file_path = '{0}/{1}'.format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info('CS Daemon Started ...')
        logger.info('process_max_limit is {0}'.format(process_max_limit))
        logger.info('process_interval is {0}'.format(process_interval))
        while True:
            job_list = make_job_list(logger, process_max_limit)
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            for _ in range(run_count):
                for job in job_list:
                    if len(pid_list) >= process_max_limit:
                        logger.info('Processing Count is MAX....')
                        break
                    p = multiprocessing.Process(target=do_task, args=(job,))
                    pid_list.append(p)
                    p.start()
                    logger.info('spawn new processing, pid is [{0}]'.format(p.pid))
                    logger.info(job)
                    sleep_exact_time(process_interval)
                job_list = list()
            sleep_exact_time(10)


class MyFormatter(logging.Formatter):
    converter = datetime.fromtimestamp

    def formatTime(self, record, fmt=None):
        ct = self.converter(record.created)
        if fmt:
            s = ct.strftime(fmt)
        else:
            t = ct.strftime('%Y-%m-%d %H:%M:%S')
            s = '%s.%03d' % (t, record.msec)
        return s


class MSSQL(object):
    def __init__(self, logger):
        self.logger = logger
        self.conn = pymssql.connect(
            host=MSSQL_DB_CONFIG['host'],
            user=MSSQL_DB_CONFIG['user'],
            password=MSSQL_DB_CONFIG['password'],
            database=MSSQL_DB_CONFIG['database'],
            port=MSSQL_DB_CONFIG['port'],
            charset=MSSQL_DB_CONFIG['charset'],
            login_timeout=MSSQL_DB_CONFIG['login_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

    def select_tm_info(self, rcdg_id):
        query = """
            SELECT
                R_FILE_NM,
                R_DURATION,
                R_START_TM,
                R_END_TM,
                R_USR_ID
            FROM
                DBO.VREC_STT_INFO WITH(NOLOCK)
            WHERE 1=1
                AND R_KEY_CODE = %s
        """
        bind = (
            rcdg_id,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


class MySQL(object):
    def __init__(self, logger):
        self.logger = logger
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['passwd'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

    def select_target(self):
        """
        TM_STT 계약정보 테이블에서
        STT진행상태코드가 '01', '06', '07'인 건 들의
        증서번호, 계약일자, STT진행상태코드, STT요청일시를 조회한다.
        """
        query = """
            SELECT
                POLI_NO,
                CTRDT,
                STT_PRGST_CD,
                STT_REQ_DTM
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                AND ( 0=1
                    OR STT_PRGST_CD = '01'
                    OR STT_PRGST_CD = '06'
                    OR STT_PRGST_CD = '07'
                ) 
        """
        self.cursor.execute(query, )
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_rcdg_id(self, poli_no, ctrdt):
        """
        TM_STT 정보 테이블에서
        증서번호와 계약일자 및 STT처리상태코드가 00인것으로
        녹취ID 를 조회한다.
        """
        query = """
            SELECT
                RCDG_ID
            FROM
                TB_QA_STT_TM_INFO
            WHERE 1=1
                AND POLI_NO = %s
                AND CTRDT = %s
                AND STT_PRC_SCD = '00'
        """
        bind = (
            poli_no,
            ctrdt
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def update_stt_prgst_cd(self, **kwargs):
        """
        TM_STT 계약정보 테이블의
        증서번호와 계약일자를 조회하여
        STT진행상태코드와 STT요청일시를 '90', 최신으로
        업데이트한다.
        :param kwargs:
        :return:
        """
        try:
            query = """
                UPDATE
                    TB_QA_STT_TM_CNTR_INFO
                SET
                    STT_PRGST_CD = %s,
                    STT_REQ_DTM = %s
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('stt_prgst_cd'),
                kwargs.get('stt_req_dtm'),
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute('set names utf8')
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


def do_task(job):
    """
    Process execute CS
    :param      job:        Job
    """
    sys.path.append(DAEMON_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(job)


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
            if db == 'MsSQL':
                os.environ['NLS_LANG'] = '.AL32UTF8'
                sql = MSSQL(logger)
            elif db == 'MySQL':
                sql = MySQL(logger)
            else:
                raise Exception("Unknown database [{0}], MsSQL or MySQL".format(db))
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


def make_rec_file_dict(mysql, mssql, poli_no, ctrdt):
    """
    Select rec file
    :param      mysql:          MySQL
    :param      mssql:          MsSQL
    :param      poli_no:        POLI_NO(증서 번호)
    :param      ctrdt:          CTRDT(계약 일자)
    :return:                    REC file dictionary
    """
    # Select REC file name and REC information
    rec_file_dict = dict()
    rcdg_id_list = mysql.select_rcdg_id(poli_no, ctrdt)
    if not rcdg_id_list:
        return dict()
    for tm_info in rcdg_id_list:
        rcdg_id = str(tm_info[0]).strip()
        result = mssql.select_tm_info(rcdg_id)
        if not result:
            return dict()
        rcdf_file_nm = str(result[0]).strip()
        rcdg_crnc_hms = str(result[1]).strip()
        rcdg_stdtm = str(result[2]).strip()
        rcdg_edtm = str(result[3]).strip()
        cnsr_id = str(result[4]).strip()
        result_dict = {
            'rcdg_id': rcdg_id,
            'rcdg_file_nm': rcdf_file_nm,
            'rcdg_crnc_hms': rcdg_crnc_hms,
            'rcdg_stdtm': rcdg_stdtm,
            'rcdg_edtm': rcdg_edtm,
            'cnsr_id': cnsr_id
        }
        rec_file_dict[rcdf_file_nm] = result_dict
    return rec_file_dict


def check_rec_file(logger, poli_no, ctrdt, rec_file_dict):
    """
    Check record file
    :param      logger:             Logger
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      rec_file_dict:      REC file dictionary
    :return:                        True or False
    """
    for rcdf_file_nm in rec_file_dict.keys():
        # 녹취 파일은 각각의 계약 날짜가 아닌 녹취 날짜에 전송된다.
        rcdf_file_date = rcdf_file_nm[:8]
        target_mono_file_path = '{0}/{1}/{2}.enc'.format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        target_rx_file_path = '{0}/{1}/{2}.rx.enc'.format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        target_tx_file_path = '{0}/{1}/{2}.tx.enc'.format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        incident_target_mono_file_path = '{0}/{1}.enc'.format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        incident_target_rx_file_path = '{0}/{1}.rx.enc'.format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        incident_target_tx_file_path = '{0}/{1}.tx.enc'.format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        if os.path.exists(target_mono_file_path):
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, target_mono_file_path))
            continue
        elif os.path.exists(target_rx_file_path) and os.path.exists(target_tx_file_path):
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, target_rx_file_path))
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, target_tx_file_path))
            continue
        elif os.path.exists(incident_target_mono_file_path):
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, incident_target_mono_file_path))
        elif os.path.exists(incident_target_rx_file_path) and os.path.exists(incident_target_tx_file_path):
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, incident_target_rx_file_path))
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDF_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, incident_target_tx_file_path))
            continue
        else:
            logger.debug("Can't find file POLI_NO = {0}, CTRDT = {1}, RCDF_FILE_NM = {2}".format(
                poli_no, ctrdt, rcdf_file_nm))
            return False
        return True


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param      logger:                 Logger
    :param      delete_file_path:       Input path
    """
    if os.path.exists(delete_file_path):
        try:
            if os.path.isfile(delete_file_path):
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm):
    """
    Wav file not found process
    :param      logger:             Logger
    :param      mysql:              MySQL
    :param      stt_prgst_cd:       STT_PRGST_CD(CS 진행상태코드)
    :param      poli_no:            POLI_NO(증서 번호)
    :param      ctrdt:              CTRDT(계약 일자)
    :param      stt_req_dtm:        STT_REQ_DTM(CS 요청 일시)
    """
    logger.info("Wav file not found. [POLI_NO = {0}, CTRDT = {1}]".format(poli_no, ctrdt))
    mysql.update_stt_prgst_cd(
        stt_prgst_cd='90',
        stt_req_dtm=stt_req_dtm,
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    if stt_prgst_cd == '06':
        output_dir_name = '{0}_{1}_supplementation'.format(poli_no, ctrdt)
    else:
        output_dir_name = '{0}_{1}'.format(poli_no, ctrdt)
    output_dir_path = "{0}/{1}/{2}/{3}/{4}/TB_QA_STT_TM_CNTR_INFO".format(
        STT_CONFIG['stt_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8], output_dir_name)
    if os.path.exists(output_dir_path):
        del_garbage(logger, output_dir_path)
    os.makedirs(output_dir_path)
    tm_cntr_info_output_file_path = '{0}/{1}_{2}_tb_qa_stt_tm_cntr_info.txt'.format(
        output_dir_path, poli_no, ctrdt)
    tm_cntr_info_output_file = open(tm_cntr_info_output_file_path, 'a')
    output_dict = collections.OrderedDict()
    output_dict['poli_no'] = poli_no
    output_dict['ctrdt'] = ctrdt
    output_dict['stt_prgst_cd'] = '90'
    output_dict['stt_req_dtm'] = stt_req_dtm
    output_dict['ta_cmdtm'] = str(datetime.now())
    output_list = list(output_dict.values())
    print >> tm_cntr_info_output_file, '\t'.join(output_list)
    tm_cntr_info_output_file.close()
    temp_db_upload_dir_path = '{0}/{1}.tmp'.format(STT_CONFIG['db_upload_path'], output_dir_name)
    tm_cntr_info_dir_path = '{0}/{1}.tmp/TB_QA_STT_TM_CNTR_INFO'.format(STT_CONFIG['db_upload_path'], output_dir_name)
    db_upload_dir_path = '{0}/{1}'.format(STT_CONFIG['db_upload_path'], output_dir_name)
    if os.path.exists(temp_db_upload_dir_path):
        del_garbage(logger, temp_db_upload_dir_path)
    os.makedirs(tm_cntr_info_dir_path)
    shutil.copy(tm_cntr_info_output_file_path, tm_cntr_info_dir_path)
    if os.path.exists(db_upload_dir_path):
        logger.info('{0} is already exists.'.format(output_dir_name))
        del_garbage(logger, db_upload_dir_path)
    os.rename(temp_db_upload_dir_path, db_upload_dir_path)


def make_job_list(logger, process_max_limit):
    """
    Make job list
    :param      logger:                 Logger
    :param      process_max_limit:      Process max limit
    :return:                            Job list
    """
    idx = 0
    mysql = connect_db(logger, 'MySQL')
    mssql = connect_db(logger, 'MsSQL')
    if not mysql or not mssql:
        return list()
    target_list = mysql.select_target()
    job_list = list()
    if not target_list:
        return job_list
    for target in target_list:
        poli_no = str(target[0]).strip()
        ctrdt = str(target[1]).strip()
        stt_prgst_cd = str(target[2]).strip()
        stt_req_dtm = str(target[3]).strip()
        try:
            rec_file_dict = make_rec_file_dict(mysql, mssql, poli_no, ctrdt)
            if len(rec_file_dict) < 1:
                continue
            if not check_rec_file(logger, poli_no, ctrdt, rec_file_dict):
                wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm)
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
    mysql.disconnect()
    mssql.disconnect()
    return job_list


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


########
# main #
########
if __name__ == '__main__':
    if not os.path.exists(DAEMON_CONFIG['log_dir_path']):
        os.makedirs(DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
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