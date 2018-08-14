#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-11-30, modification: 2017-12-14"

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
from cfg.config import DAEMON_CONFIG, STT_CONFIG, MSSQL_DB_CONFIG, MYSQL_DB_CONFIG

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
        log_file_path = "{0}/{1}".format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("CS Daemon Started ...")
        logger.info("process_max_limit is {0}".format(process_max_limit))
        logger.info("process_interval is {0}".format(process_interval))
        while True:
            job_list = make_job_list(logger, process_max_limit)
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            for i in range(run_count):
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
            s = '%s.%03d' % (t, record.msecs)
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

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

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

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_target(self):
        query = """
            SELECT
                POLI_NO,
                CTRDT,
                STT_PRGST_CD,
                STT_REQ_DTM,
                PI_CNT
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                 AND (
                    STT_PRGST_CD = '01'
                 OR STT_PRGST_CD = '06'
                 OR STT_PRGST_CD = '07'
                 OR STT_PRGST_CD = '99'
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

    def select_pi_no(self, poli_no, ctrdt):
        query = """
            SELECT
                PI_NO
            FROM
                TB_QA_STT_TM_CNTR_SCRT_INFO
            WHERE 1=1
                 AND POLI_NO = %s
                 AND CTRDT = %s
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

    def select_ta_prc_cnt_from_scrt_info(self, poli_no, ctrdt):
        query = """
            SELECT
                COUNT(*)
            FROM
                TB_QA_STT_TM_CNTR_SCRT_INFO
            WHERE 1=1
                 AND POLI_NO = %s
                 AND CTRDT = %s
                 AND TA_PRC_YN = 'N'
        """
        bind = (
            poli_no,
            ctrdt
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_ta_prc_cnt_from_prod_info(self, poli_no, ctrdt):
        query = """
            SELECT
                COUNT(*)
            FROM
                TB_QA_STT_TM_CNTR_SCRT_PROD_INFO
            WHERE 1=1
                 AND POLI_NO = %s
                 AND CTRDT = %s
                 AND TA_PRC_YN = 'N'
        """
        bind = (
            poli_no,
            ctrdt
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_rcdg_id_cnt(self, poli_no, ctrdt):
        query = """
            SELECT
                RCDG_ID_CNT
            FROM
                TB_QA_STT_TM_CNTR_INFO
            WHERE 1=1
                 AND POLI_NO = %s
                 AND CTRDT = %s
        """
        bind = (
            poli_no,
            ctrdt
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def update_stt_prgst_cd(self, **kwargs):
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
            self.cursor.execute("set names utf8")
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

    def update_stt_prc_scd(self, **kwargs):
        try:
            query = """
                UPDATE
                    TB_QA_STT_TM_INFO
                SET
                    STT_PRC_SCD = '01'
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute("set names utf8")
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

    def update_ta_prc_yn_from_scrt_info(self, **kwargs):
        try:
            query = """
                UPDATE
                    TB_QA_STT_TM_CNTR_SCRT_INFO
                SET
                    TA_PRC_YN = 'Y'
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute("set names utf8")
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def update_ta_prc_yn_from_prod_info(self, **kwargs):
        try:
            query = """
                UPDATE
                    TB_QA_STT_TM_CNTR_SCRT_PROD_INFO
                SET
                    TA_PRC_YN = 'Y'
                WHERE 1=1
                    AND POLI_NO = %s
                    AND CTRDT = %s
            """
            bind = (
                kwargs.get('poli_no'),
                kwargs.get('ctrdt'),
            )
            self.cursor.execute("set names utf8")
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

#######
# def #
#######


def do_task(job):
    """
    Process execute CS
    :param          job:                Job
    """
    sys.path.append(DAEMON_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(job)


def sleep_exact_time(seconds):
    """
    Sleep
    :param          seconds:        Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def connect_db(logger, db):
    """
    Connect database
    :param          logger:         Logger
    :param          db:             Database
    :return:                        SQL Object
    """
    # Connect DB
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'MsSQL':
                os.environ["NLS_LANG"] = ".AL32UTF8"
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


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param          logger:                     Logger
    :param          delete_file_path:           Input path
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
    :param          logger:                 Logger
    :param          mysql:                  MySQL
    :param          stt_prgst_cd:           STT_PRGST_CD(CS 진행상태코드)
    :param          poli_no:                POLI_NO(증서 번호)
    :param          ctrdt:                  CTRDT(계약 일자)
    :param          stt_req_dtm:            STT_REQ_DTM(CS 요청 일시)
    """
    logger.info("Wav file not found. [POLI_NO = {0}, CTRDT = {1}]".format(poli_no, ctrdt))
    mysql.update_stt_prgst_cd(
        stt_prgst_cd='90',
        stt_req_dtm=stt_req_dtm,
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    mysql.update_stt_prc_scd(
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    mysql.update_ta_prc_yn_from_scrt_info(
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    mysql.update_ta_prc_yn_from_prod_info(
        poli_no=poli_no,
        ctrdt=ctrdt
    )
    if stt_prgst_cd == '06':
        output_dir_name = "{0}_{1}_supplementation".format(poli_no, ctrdt)
    else:
        output_dir_name = "{0}_{1}".format(poli_no, ctrdt)
    temp_output_dir_path = "{0}/{1}/{2}/{3}/{4}".format(
        STT_CONFIG['stt_output_path'], ctrdt[:4], ctrdt[4:6], ctrdt[6:8], output_dir_name)
    output_dir_path = "{0}/TB_QA_STT_TM_CNTR_INFO".format(temp_output_dir_path)
    if os.path.exists(temp_output_dir_path):
        del_garbage(logger, temp_output_dir_path)
    os.makedirs(output_dir_path)
    tm_cntr_info_output_file_path = "{0}/{1}_{2}_tb_qa_stt_tm_cntr_info.txt".format(
        output_dir_path, poli_no, ctrdt)
    tm_cntr_info_output_file = open(tm_cntr_info_output_file_path, 'a')
    output_dict = collections.OrderedDict()
    output_dict['poli_no'] = poli_no
    output_dict['ctrdt'] = ctrdt
    output_dict['stt_prgst_cd'] = '90'
    output_dict['stt_req_dtm'] = stt_req_dtm
    output_dict['ta_cmdtm'] = str(datetime.now())
    output_list = list(output_dict.values())
    print >> tm_cntr_info_output_file, "\t".join(output_list)
    tm_cntr_info_output_file.close()
    temp_db_upload_dir_path = "{0}/{1}.tmp".format(STT_CONFIG['db_upload_path'], output_dir_name)
    tm_cntr_info_dir_path = "{0}/{1}.tmp/TB_QA_STT_TM_CNTR_INFO".format(STT_CONFIG['db_upload_path'], output_dir_name)
    db_upload_dir_path = "{0}/{1}".format(STT_CONFIG['db_upload_path'], output_dir_name)
    if os.path.exists(temp_db_upload_dir_path):
        del_garbage(logger, temp_db_upload_dir_path)
    os.makedirs(tm_cntr_info_dir_path)
    shutil.copy(tm_cntr_info_output_file_path, tm_cntr_info_dir_path)
    if os.path.exists(db_upload_dir_path):
        logger.info("{0} is already exists.".format(output_dir_name))
        del_garbage(logger, db_upload_dir_path)
    os.rename(temp_db_upload_dir_path, db_upload_dir_path)


def check_rec_file(logger, poli_no, ctrdt, rec_file_dict):
    """
    Check record file
    :param          logger:                 Logger
    :param          poli_no:                POLI_NO(증서 번호)
    :param          ctrdt:                  CTRDT(계약 일자)
    :param          rec_file_dict:          REC file dictionary
    :return:                                True or False
    """
    for rcdf_file_nm in rec_file_dict.keys():
        # 녹취 파일은 각각의 계약 날짜가 아닌 녹취 날짜에 전송된다.
        rcdf_file_date = rcdf_file_nm[:8]
        target_mono_file_path = "{0}/{1}/{2}.enc".format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        target_rx_file_path = "{0}/{1}/{2}.rx.enc".format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        target_tx_file_path = "{0}/{1}/{2}.tx.enc".format(STT_CONFIG['rec_dir_path'], rcdf_file_date, rcdf_file_nm)
        incident_target_mono_file_path = "{0}/{1}.enc".format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        incident_target_rx_file_path = "{0}/{1}.rx.enc".format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
        incident_target_tx_file_path = "{0}/{1}.tx.enc".format(STT_CONFIG['incident_rec_dir_path'], rcdf_file_nm)
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
            continue
        elif os.path.exists(incident_target_rx_file_path) and os.path.exists(incident_target_tx_file_path):
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, incident_target_rx_file_path))
            logger.debug("POLI_NO = {0}, CTRDT = {1}, RCDG_FILE_NM = {2}, Target RCDG_FILE_NM = {3}".format(
                poli_no, ctrdt, rcdf_file_nm, incident_target_tx_file_path))
            continue
        else:
            logger.debug("Can't find file POLI_NO = {0}, CTRDT = {1}, RCDF_FILE_NM = {2}".format(
                poli_no, ctrdt, rcdf_file_nm))
            return False
    return True


def make_rec_file_dict(mysql, mssql, poli_no, ctrdt):
    """
    Select rec file
    :param          mysql:          MySQL
    :param          mssql:          MsSQL
    :param          poli_no:        POLI_NO(증서 번호)
    :param          ctrdt:          CTRDT(계약 일자)
    :return:                        REC file dictionary
    """
    # Select REC file name and REC information
    rec_file_dict = dict()
    rcdg_id_list = mysql.select_rcdg_id(poli_no, ctrdt)
    if not rcdg_id_list:
        return 'continue'
    for tm_info in rcdg_id_list:
        rcdg_id = str(tm_info[0]).strip()
        result = mssql.select_tm_info(rcdg_id)
        if not result:
            return 'reprocess'
        rcdf_file_nm = str(result[0]).strip()
        rcdg_crnc_hms = str(result[1]).strip()
        rcdg_stdtm = str(result[2]).strip()
        rcdg_edtm = str(result[3]).strip()
        cnsr_id = str(result[4]).strip()
        result_dict = {
            'rcdg_id': rcdg_id,
            'rcdf_file_nm': rcdf_file_nm,
            'rcdg_crnc_hms': rcdg_crnc_hms,
            'rcdg_stdtm': rcdg_stdtm,
            'rcdg_edtm': rcdg_edtm,
            'cnsr_id': cnsr_id
        }
        rec_file_dict[rcdf_file_nm] = result_dict
    return rec_file_dict


def make_job_list(logger, process_max_limit):
    """
    Make job list
    :param          logger:                     Logger
    :param          process_max_limit:          Process max limit
    :return:             `                       Job list
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
    if not os.path.exists(STT_CONFIG['rec_dir_path']):
        logger.error("Not existed REC directory [{0}]".format(STT_CONFIG['rec_dir_path']))
        return job_list
    for target in target_list:
        poli_no = str(target[0]).strip()
        ctrdt = str(target[1]).strip()
        stt_prgst_cd = str(target[2]).strip()
        stt_req_dtm = str(target[3]).strip()
        if str(target[4]).strip() == 'None':
            continue
        pi_cnt = int(str(target[4]).strip())
        try:
            # STT_PRGST_CD 가 99 인 경우 음성 파일을 못 찾은 경우로 처리
            if stt_prgst_cd == '99':
                logger.info("STT_PRGST_CD is 99, wav_file_not_found_process start")
                wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm)
                continue
            rec_file_dict = make_rec_file_dict(mysql, mssql, poli_no, ctrdt)
            # 녹취 ID 로 녹취 DB 조회 결과가 없을 경우
            if rec_file_dict == 'reprocess':
                wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm)
                continue
            # MySQL DB 에서 증서 번호, 계약 일자 기준으로 녹취 ID 가 없는 경우
            if not rec_file_dict or rec_file_dict == 'continue':
                continue
            # MySQL DB 에서 증서 번호, 계약 일자 기준으로 문항 번호가 없는 경우
            check_pi_no = mysql.select_pi_no(poli_no, ctrdt)
            if not check_pi_no:
                continue
            # 녹취 파일 서버에 녹취 파일이 존재 하지 않는 경우
            if not check_rec_file(logger, poli_no, ctrdt, rec_file_dict):
                wav_file_not_found_process(logger, mysql, stt_prgst_cd, poli_no, ctrdt, stt_req_dtm)
                continue
            # RCDG_ID_CNT 와 녹취 파일 수가 다를 경우
            rcdg_id_cnt_result = mysql.select_rcdg_id_cnt(poli_no, ctrdt)
            if str(rcdg_id_cnt_result[0]).strip() == 'None':
                continue
            rcdg_id_cnt = int(str(rcdg_id_cnt_result[0]).strip())
            if len(rec_file_dict) != rcdg_id_cnt:
                continue
            # PI_CNT 수와 SCRT_SNTC_INFO, PROD_SNTC_INFO 의 USE_YN 이 'Y' 가 아닌 수의 합이 다를 경우
            scrt_result = mysql.select_ta_prc_cnt_from_scrt_info(poli_no, ctrdt)
            prod_result = mysql.select_ta_prc_cnt_from_prod_info(poli_no, ctrdt)
            if pi_cnt != (int(scrt_result[0]) + int(prod_result[0])):
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
    :param          sig:
    :param          frame
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
    :param          fname:                      Log file name
    :param          log_level:                  Loge level
    :return:                                    Logger
    """
    log_handler = TimedRotatingFileHandler(fname, when='midnight', backupCount=5)
    log_formatter = MyFormatter(fmt='[%(asctime)s] %(message)s')
    log_handler.setFormatter(log_formatter)
    logger = logging.getLogger('STT_Logger')
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger

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
