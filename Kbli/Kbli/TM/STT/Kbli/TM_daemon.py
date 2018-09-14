#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2018-05-01, modification: 2018-05-01"


###########
# imports #
###########
import os
import sys
import time
import glob
import signal
import argparse
import shutil
import atexit
import cx_Oracle
import psycopg2
import psycopg2.extras
import logging
import traceback
import collections
import multiprocessing
from datetime import datetime, timedelta
from lib.daemon import Daemon
import cfg.config
from logging.handlers import TimedRotatingFileHandler
from lib.openssl import decrypt_string

#############
# constants #
#############
DIR_PATH = ''
DAEMON_CONFIG = {}
ORACLE_DB_CONFIG = {}
POSTGRESQL_DB_CONFIG = {}
CONFIG_TYPE = ''


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
        atexit.register(del_tmp)
        set_sig_handler()
        pid_list = list()
        job_max_limit = int(DAEMON_CONFIG['job_max_limit'])
        process_max_limit = int(DAEMON_CONFIG['process_max_limit'])
        process_interval = int(DAEMON_CONFIG['process_interval'])
        log_file_path = "{0}/{1}".format(DAEMON_CONFIG['log_dir_path'], DAEMON_CONFIG['log_file_name'])
        logger = get_logger(log_file_path, logging.INFO)
        logger.info("STT Daemon Started ...")
        logger.info("process_max_limit is {0}".format(process_max_limit))
        logger.info("process_interval is {0}".format(process_interval))
        while True:
            try:
                oracle = oracle_connect(logger)
                # get_view_data(logger, oracle)
                job_list = make_job_list(logger, process_max_limit, job_max_limit, oracle)
            except Exception:
                exc_info = traceback.format_exc()
                logger.error(exc_info)
                continue
            for pid in pid_list[:]:
                if not pid.is_alive():
                    pid_list.remove(pid)
            run_count = process_max_limit - len(pid_list)
            for _ in range(run_count):
                for job in job_list:
                    if len(pid_list) >= process_max_limit:
                        logger.info('Processing Count is MAX....')
                        break
                    if len(job) > 0:
                        args = job
                        p = multiprocessing.Process(target=do_task, args=(args, ))
                        pid_list.append(p)
                        p.start()
                        logger.info('spawn new processing, pid is [{0}]'.format(p.pid))
                        logger.info('\t'.join(job))
                        sleep_exact_time(process_interval)
                job_list = list()
            time.sleep(DAEMON_CONFIG['cycle_time'])


class PostgreSQL(object):
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname = POSTGRESQL_DB_CONFIG['db']
            , host = POSTGRESQL_DB_CONFIG['host']
            , user = POSTGRESQL_DB_CONFIG['user']
            , password = POSTGRESQL_DB_CONFIG['password']
            , port = POSTGRESQL_DB_CONFIG['port']
            , connect_timeout = POSTGRESQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
    def find_view_table(self, logger, before_date, before_time):
        """
        find unprocessed rec_id in the view table
        writing complete file check : compress = '1' AND mixing_complete = '1'
        :return:    Rec id list
        """
        sql = """
            SELECT * FROM 
                view_call_info_stt
            WHERE 
                call_date >= %s 
                AND call_time >= %s
                AND compress = '1'
                AND mixing_complete = '1'
        """
        bind = (before_date, before_time, )
        logger.debug('call_date : {0} , call_time : {1}'.format(before_date, before_time))
        self.cursor.execute(sql, bind)
        rows = self.cursor.fetchall()
        return rows
        
    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


class ORACLE(object):
    def __init__(self):
        self.dsn_tns = ORACLE_DB_CONFIG['dsn']
        passwd = decrypt_string(ORACLE_DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()
        
    def rows_to_dict_list(self):
        columns = [i[0] for i in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor]

    def insert_view_table_data_to_call_meta(self, logger, row):
        """
        insert_view_table_data_to_call_meta
        :param      logger:     Rec id
        :param      view table data rows:   rows
        """
        logger.debug("insert_view_table_data_to_call_meta : row - {0}".format(row['call_id']))
        try:
            sql = """
                MERGE INTO CALL_META
                    USING DUAL
                    ON (
                            PROJECT_CD = 'TM'
                            AND DOCUMENT_ID = :1
                    )
                WHEN MATCHED THEN
                    UPDATE SET 
                        REC_ID = :2
                        , LST_CHGP_CD = 'STT'
                        , LST_CHG_PGM_ID = 'STT'
                        , LST_CHG_DTM  = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT
                        (  
                            PROJECT_CD
                            , DOCUMENT_DT
                            , DOCUMENT_ID
                            , CALL_TYPE
                            , REC_ID
                            , CALL_DT
                            , START_DTM
                            , END_DTM
                            , DURATION
                            , STATUS
                            , EXTENSION_PHONE_NO
                            , STT_PRGST_CD
                            , CHN_TP
                            , REGP_CD
                            , RGST_PGM_ID
                            , RGST_DTM
                            , LST_CHGP_CD
                            , LST_CHG_PGM_ID
                            , LST_CHG_DTM
                        )
                    VALUES (
                        :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15
                        ,'STT', 'STT', SYSDATE, 'STT', 'STT', SYSDATE
                    )
            """
            bind = (
                row['call_id']
                , row['etc1']
                , 'TM'
                , row['call_date']
                , row['call_id']
                , row['call_type']
                , row['etc1']
                , row['call_date']
                , datetime.combine(row['call_date'], row['call_time'])
                , row['end_time']
                , row['duration']
                , row['status']
                , row['phone_no']
                , '01'
                , 'S'
            )
            self.cursor.execute(sql, bind)
            self.conn.commit()
            return True
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            self.conn.rollback()

    def find_call_data(self, last_date):
        """
        Retrieves unprocessed rec_id in the database
        STT_PRGST_CD = '01' // init code
        STT_PRGST_CD = '90' // not exist file code
        STT_PRGST_CD = '07' // re request
        :return:    Rec id list
        """
        sql = """
            SELECT * FROM (
                SELECT
                    PROJECT_CD,
                    DOCUMENT_ID,
                    DOCUMENT_DT,
                    REC_ID, 
                    CALL_TYPE, 
                    STT_PRGST_CD, 
                    DURATION,
                    CALL_DT,
                    START_DTM,
                    END_DTM,
                    CHN_TP
                FROM
                    CALL_META
                WHERE 1=1
                    AND (STT_PRGST_CD = '01' OR STT_PRGST_CD = '90' OR STT_PRGST_CD = '07')
                    AND ((PROJECT_CD = 'TM' AND DOCUMENT_DT BETWEEN :1 and CURRENT_DATE) OR PROJECT_CD = 'CD')
                    AND (REC_ID <> '' OR  REC_ID is not null)
                ORDER BY DOCUMENT_DT ASC ) A
            WHERE rownum <= 2000
        """
        bind = (last_date, )
        self.cursor.execute(sql, bind)
        rows = self.rows_to_dict_list()
        info_dic_list = list()
        
        for row in rows:
            rec_info_dic = dict()
            rec_info_dic['PK'] = '{0}####{1}####{2}'.format(row['DOCUMENT_ID'], row['DOCUMENT_DT'], row['STT_PRGST_CD'])
            rec_info_dic['PROJECT_CD'] = row['PROJECT_CD']
            rec_info_dic['DOCUMENT_ID'] = row['DOCUMENT_ID']
            rec_info_dic['DOCUMENT_DT'] = row['DOCUMENT_DT']
            rec_info_dic['REC_ID'] = row['REC_ID']
            rec_info_dic['CALL_TYPE'] = row['CALL_TYPE']
            rec_info_dic['STT_PRGST_CD'] = row['STT_PRGST_CD']
            rec_info_dic['DURATION'] = row['DURATION']
            rec_info_dic['CALL_DT'] = row['CALL_DT']
            rec_info_dic['START_DTM'] = row['START_DTM']
            rec_info_dic['END_DTM'] = row['END_DTM']
            info_dic_list.append(rec_info_dic)
        return info_dic_list

    def update_stt_prgst_cd(self, logger, info_dic, prgst_cd):
        """
        Progress code Update
        :param      logger:     Rec id
        :param      info_dic:   Status
        :param      prgst_cd:   Information dictionary of Rec id
        """
        project_cd = info_dic['PROJECT_CD']
        document_id = info_dic['DOCUMENT_ID']
        document_dt = info_dic['DOCUMENT_DT']
        try:
            sql = """
                UPDATE
                    CALL_META
                SET
                    STT_PRGST_CD = :1
                    , LST_CHGP_CD = 'STT'
                    , LST_CHG_PGM_ID = 'STT'
                    , LST_CHG_DTM = SYSDATE
                WHERE 1=1
                    AND PROJECT_CD = :2
                    AND DOCUMENT_ID = :3
                    AND DOCUMENT_DT = :4
            """
            self.cursor.execute(sql, (prgst_cd, project_cd, document_id, document_dt,))
            if self.cursor.rowcount > 0:
                logger.info('status of rec_id {0} is upgrade -> {1}'.format(document_id, prgst_cd))
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            self.conn.rollback()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


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
def calculate(seconds):
    if seconds is bool:
        return False
    hour = seconds / 3600
    seconds = seconds % 3600
    minute = seconds / 60
    seconds = seconds % 60
    times = '%02d%02d%02d' % (hour, minute, seconds)
    return times


def del_tmp():
    if os.path.exists('{0}.tmp'.format(DIR_PATH)):
        file_list = glob.glob('{0}.tmp/*'.format(DIR_PATH))
        print 'del_tmp execution'
        if len(file_list) == 0:
            print '  delete {0}.tmp'.format(DIR_PATH)
            shutil.rmtree('{0}.tmp'.format(DIR_PATH))
        else:
            print '  rename {0}.tmp -> {0}'.format(DIR_PATH)
            os.rename('{0}.tmp'.format(DIR_PATH), DIR_PATH)


def get_view_data(logger, oracle):
    logger.info('get_view_data !')
    ts = time.time()
#     before_dt = datetime.fromtimestamp(ts) - timedelta(minutes=30)
    before_dt = datetime.fromtimestamp(ts) - timedelta(hours=2)
    before_date = before_dt.strftime('%Y-%m-%d')
    before_time = before_dt.strftime('%H:%M:%S')
    postgresql = postgresql_connect(logger)
    rows = postgresql.find_view_table(logger, before_date, before_time)
    postgresql.disconnect()
    
    for row in rows: 
        oracle.insert_view_table_data_to_call_meta(logger, row)
    

def make_job_list(logger, process_max_limit, job_max_limit, oracle):
    """
    Make job list
    :param      logger:                 Logger
    :param      process_max_limit:      Process max limit
    :param      job_max_limit:          Job max limit
    :return:                            Job list
    """
    global DIR_PATH
    cnt = 0
    job_list = init_list_of_objects(process_max_limit)
    ts = time.time()
    last_date = (datetime.fromtimestamp(ts) - timedelta(days=DAEMON_CONFIG['search_date_range'])).strftime('%Y-%m-%d')
    info_dic_list = oracle.find_call_data(last_date)
    # logger.info("info_dic_list -> {0}".format(info_dic_list))
    for info_dic in info_dic_list:
        if not rec_server_check(logger, info_dic):
            if info_dic['STT_PRGST_CD'] == '90':
                continue
            else:
                oracle.update_stt_prgst_cd(logger, info_dic, '90')
                continue
        if cnt == process_max_limit:
            cnt = 0
        if len(job_list[cnt]) < job_max_limit:
            job_list[cnt].append(info_dic['PK'])
        cnt += 1
    oracle.disconnect()
    return job_list


def sleep_exact_time(seconds):
    """
    Sleep
    :param      seconds:    Seconds
    """
    now = time.time()
    expire = int(now + seconds) / seconds * seconds
    time.sleep(expire - now)


def do_task(args):
    """
    Process execute STT
    :param      args:       Arguments
    """
    sys.path.append(DAEMON_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(args, CONFIG_TYPE)


def is_writing_file(logger, file_path):
    file_size = os.path.getsize(file_path)
    logger.debug("{0} before ==> {1}".format(file_path, file_size))
    time.sleep(1)
    file_size_after = os.path.getsize(file_path)
    logger.debug("{0} after ==> {1}".format(file_path, file_size_after))
    if file_size != file_size_after:
        logger.debug('## different file size => {0} , {1} != {2}'.format(file_path, file_size, file_size_after))
        return False
    else:
        return True


def rec_server_check(logger, info_dic):
    """
    exist check the file_name in rec_server
    :param      logger:         Logger
    :param      info_dic:       Information Dictionary of REC file
    :return:                    Bool
    """
    file_name = "{0}.wav.enc".format(info_dic['DOCUMENT_ID'])
    rec_start_date = str(info_dic['DOCUMENT_DT'])
    project_cd = info_dic['PROJECT_CD']
    if project_cd == 'TM':
        base_path = '{0}/{1}'.format(
            DAEMON_CONFIG['rec_server_path'], rec_start_date[:4] + rec_start_date[5:7] + rec_start_date[8:10])
    elif project_cd == 'CD':
        base_path = '{0}/{1}/{2}/{3}'.format(
            DAEMON_CONFIG['card_rec_server_path'], rec_start_date[:4], rec_start_date[5:7], rec_start_date[8:10])
    file_path = '{0}/{1}'.format(base_path, file_name)
    if not os.path.exists(base_path):
        logger.error('directory is not exists -> {0}'.format(base_path))
   
    if os.path.exists(file_path):
        logger.debug('file is exists -> {0}'.format(file_path))
        # if info_dic['PROJECT_CD'] == 'CD':
        #     return True
        # if not is_writing_file(logger, file_path):
        #     logger.error('file is writing -> {0}'.format(file_path))
        #     return False
        return True
    else:
        logger.error('file is not exist -> {0}'.format(file_path))
        return False


def init_list_of_objects(size):
    """
    Make list in list different object reference each time
    :param      size:   List index size
    :return:            List of objects
    """
    list_of_objects = list()
    for _ in range(0, size):
        # different object reference each time
        list_of_objects.append(list())
    return list_of_objects


def oracle_connect(logger):
    """
    Attempt to connect to oracle
    :param:     logger:     Logger
    :return:                ORACLE
    """
    cnt = 0
    while True:
        try:
            oracle = ORACLE()
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Fail connect ORACLE, retrying count = {0}".format(cnt))
            if cnt > 10:
                break
            cnt += 1
            continue
    return oracle


def postgresql_connect(logger):
    """
    Attempt to connect to postgresql
    :param:     logger:     Logger
    :return:                postgresql
    """
    cnt = 0
    while True:
        try:
            postgresql = PostgreSQL()
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Fail connect postgresql, retrying count = {0}".format(cnt))
            if cnt > 10:
                break
            cnt += 1
            continue
    return postgresql


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


def set_config(config_type):
    """
    active type setting
    :param config_type:
    :return:
    """
    global DAEMON_CONFIG
    global ORACLE_DB_CONFIG
    global POSTGRESQL_DB_CONFIG
    global CONFIG_TYPE

    CONFIG_TYPE = config_type
    DAEMON_CONFIG = cfg.config.DAEMON_CONFIG[config_type]
    ORACLE_DB_CONFIG = cfg.config.ORACLE_DB_CONFIG[config_type]
    POSTGRESQL_DB_CONFIG = cfg.config.POSTGRESQL_DB_CONFIG


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
    if not os.path.exists(DAEMON_CONFIG['log_dir_path']):
        os.makedirs(DAEMON_CONFIG['log_dir_path'])
    daemon = DAEMON(
        DAEMON_CONFIG['pid_file_path'],
        stdout='{0}/stdout_daemon.log'.format(DAEMON_CONFIG['log_dir_path']),
        stderr='{0}/stderr_daemon.log'.format(DAEMON_CONFIG['log_dir_path'])
    )
    if 'start' == arguments.process.lower():
        daemon.start()
    elif 'stop' == arguments.process.lower():
        daemon.stop()
    elif 'restart' == arguments.process.lower():
        daemon.restart()
    else:
        print "Unknown command"
        print "usage: %s start | stop | restart" % sys.argv[0]
        sys.exit(2)
    sys.exit(0)
