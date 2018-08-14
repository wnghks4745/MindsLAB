#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-09-29, modification: 2017-12-07"


###########
# imports #
###########
import os
import sys
import time
import glob
import signal
import shutil
import atexit
import MySQLdb
import logging
import traceback
import collections
import multiprocessing
from datetime import datetime, timedelta
from lib.daemon import Daemon
from cfg.config import DAEMON_CONFIG, MYSQL_DB_CONFIG
from logging.handlers import TimedRotatingFileHandler

#############
# constants #
#############
DIR_PATH = ''

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
        logger.info("CS Daemon Started ...")
        logger.info("process_max_limit is {0}".format(process_max_limit))
        logger.info("process_interval is {0}".format(process_interval))
        while True:
            try:
                job_list = make_job_list(logger, process_max_limit, job_max_limit)
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


class MySQL(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['password'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def select_rec_id_list_to_tb_qa_stt_recinfo(self, last_date):
        """
        Retrieves unprocessed rec_id in the database
        :return:    Rec id list
        """
        sql = """
            SELECT
                RCDG_ID, RCDG_FILE_NM, CHN_TP_CD, PRGST_CD, DURATION_HMS, REC_STDT, REC_SDTM, REC_EDTM
            FROM
                TB_QA_STT_RECINFO
            WHERE 1=1
                AND (PRGST_CD = '01' OR PRGST_CD = '90')
                AND RCDG_TP_CD = 'CS'
                AND REC_STDT BETWEEN %s and NOW()
        """
        bind = (last_date, )
        self.cursor.execute(sql, bind)
        rows = self.cursor.fetchall()
        info_dic_list = list()
        for row in rows:
            rec_info_dic = dict()
            rec_info_dic['PK'] = '{0}####{1}####{2}'.format(row['RCDG_ID'], row['RCDG_FILE_NM'], row['PRGST_CD'])
            rec_info_dic['RCDG_ID'] = row['RCDG_ID']
            rec_info_dic['RCDG_FILE_NM'] = row['RCDG_FILE_NM']
            rec_info_dic['rec_file_name'] = row['RCDG_FILE_NM'].replace('.', '_')
            rec_info_dic['CHN_TP_CD'] = row['CHN_TP_CD']
            rec_info_dic['PRGST_CD'] = row['PRGST_CD']
            rec_info_dic['DURATION_HMS'] = row['DURATION_HMS']
            rec_info_dic['REC_STDT'] = row['REC_STDT']
            rec_info_dic['REC_SDTM'] = row['REC_SDTM']
            rec_info_dic['REC_EDTM'] = row['REC_EDTM']
            info_dic_list.append(rec_info_dic)
        return info_dic_list

    def update_prgst_cd_to_tb_qa_stt_recinfo(self, logger, info_dic, prgst_cd):
        """
        Progress code Update
        :param      logger:     Rec id
        :param      info_dic:   Status
        :param      prgst_cd:   Information dictionary of Rec id
        """
        rcdg_id = info_dic['RCDG_ID']
        rcdg_file_nm = info_dic['RCDG_FILE_NM']
        try:
            sql = """
                UPDATE
                    TB_QA_STT_RECINFO
                SET
                    PRGST_CD = %s
                WHERE 1=1
                    AND RCDG_ID = %s
                    AND RCDG_FILE_NM = %s
            """
            self.cursor.execute(sql, (prgst_cd, rcdg_id, rcdg_file_nm, ))
            if self.cursor.rowcount > 0:
                logger.info('status of rec_id {0} is upgrade -> {1}'.format(rcdg_id, prgst_cd))
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
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


def none_check(argument):
    if 0 == len(argument) is bool:
        return 'None'
    return argument


def make_job_list(logger, process_max_limit, job_max_limit):
    """
    Make job list
    :param      logger:                 Logger
    :param      process_max_limit:      Process max limit
    :param      job_max_limit:          Job max limit
    :return:                            Job list
    """
    global DIR_PATH
    mysql = mysql_connect(logger)
    cnt = 0
    job_list = init_list_of_objects(process_max_limit)
    ts = time.time()
    last_date = (datetime.fromtimestamp(ts) - timedelta(days=DAEMON_CONFIG['search_date_range'])).strftime('%Y-%m-%d')
    info_dic_list = mysql.select_rec_id_list_to_tb_qa_stt_recinfo(last_date)
    # Make sftp target directory
    #sftp_target_dir_path = DAEMON_CONFIG['sftp_dir_path']
    #if not os.path.exists(sftp_target_dir_path):
    #    os.makedirs(sftp_target_dir_path)
    # determine directory name and make
    #while True:
    #    ts = time.time()
    #    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    #    # Adding a server-specific unique variable when adding a server
    #    sftp_set_dir_path = '{0}/{1}'.format(sftp_target_dir_path, dt)
    #    if os.path.exists(sftp_set_dir_path) or os.path.exists(sftp_set_dir_path + '.tmp'):
    #        logger.info('filename {0} is already exist'.format(sftp_set_dir_path))
    #        time.sleep(1)
    #        continue
    #    else:
    #        DIR_PATH = sftp_set_dir_path
    #        os.makedirs('{0}.tmp'.format(sftp_set_dir_path))
    #        break
    logger.debug("info_dic_list -> {0}".format(info_dic_list))
    for info_dic in info_dic_list:
        if not rec_server_check(logger, info_dic) and info_dic['PRGST_CD'] == '01':
            mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, info_dic, '90')
            #make_tb_qa_stt_cs_info(logger, info_dic, sftp_set_dir_path)
            continue
        elif not rec_server_check(logger, info_dic) and info_dic['PRGST_CD'] == '90':
            continue
        if cnt == process_max_limit:
            cnt = 0
        if len(job_list[cnt]) < job_max_limit:
            job_list[cnt].append(info_dic['PK'])
        cnt += 1
    # Check the make upload txt
    #if os.path.exists('{0}.tmp'.format(sftp_set_dir_path)):
    #    file_list = glob.glob('{0}.tmp/*'.format(sftp_set_dir_path))
    #    if len(file_list) == 0:
    #        shutil.rmtree('{0}.tmp'.format(sftp_set_dir_path))
    #    else:
    #        logger.info('rename {0}.tmp -> {0}'.format(sftp_set_dir_path))
    #        os.rename('{0}.tmp'.format(sftp_set_dir_path), sftp_set_dir_path)
    mysql.disconnect()
    return job_list


def make_tb_qa_stt_cs_info(logger, info_dic, sftp_set_dir_path):
    """
    Make TB_QA_STT_CS_INFO table for status code change
    :param      logger:                 Logger
    :param      info_dic:               Information dictionary
    :param      sftp_set_dir_path:      Directory path for Set of sftp
    """
    logger.info('Make DB upload output for status code change')
    # make info table directory
    info_dir_path = '{0}.tmp/TB_QA_STT_CS_INFO'.format(sftp_set_dir_path)
    if not os.path.exists(info_dir_path):
        os.makedirs(info_dir_path)
    # ready for db upload information setting
    ts = time.time()
    curr_time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    rec_stdt = str(info_dic['REC_STDT'])
    rcdg_crnc_hms = calculate(int(info_dic['DURATION_HMS']))
    # db upload information setting
    output_dict = collections.OrderedDict()
    output_dict['RCDG_ID'] = info_dic['RCDG_ID']
    output_dict['RCDG_FILE_NM'] = info_dic['RCDG_FILE_NM']
    output_dict['RCDG_FILE_PATH_NM'] = 'None'
    output_dict['CHN_TP_CD'] = info_dic['CHN_TP_CD']
    output_dict['RCDG_DT'] = none_check('{0}{1}{2}'.format(rec_stdt[:4], rec_stdt[5:7], rec_stdt[8:10]))
    output_dict['RCDG_STDTM'] = str(info_dic['REC_SDTM'])
    output_dict['RCDG_EDTM'] = str(info_dic['REC_EDTM'])
    output_dict['RCDG_CRNC_HMD'] = none_check(rcdg_crnc_hms)
    output_dict['STT_PRGST_CD'] = '90'
    output_dict['STT_REQ_DTM'] = curr_time
    output_dict['STT_CMDTM'] = curr_time
    insert_data = '\t'.join(output_dict.values())
    # Create db upload txt
    sftp_txt_file_path = '{0}/{1}_{2}_TB_QA_STT_CS_INFO.txt'.format(
        info_dir_path, output_dict['RCDG_ID'], output_dict['RCDG_FILE_NM'])
    logger.debug('  make db upload txt -> {0}'.format(sftp_txt_file_path))
    sftp_txt_file = open(sftp_txt_file_path, 'a')
    print >> sftp_txt_file, insert_data
    sftp_txt_file.close()


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
    Process execute CS
    :param      args:       Arguments
    """
    sys.path.append(DAEMON_CONFIG['stt_script_path'])
    import STT
    reload(STT)
    STT.main(args)


def rec_server_check(logger, info_dic):
    """
    exist check the file_name in rec_server
    :param      logger:         Logger
    :param      info_dic:       Information Dictionary of REC file
    :return:                    Bool
    """
    file_name = info_dic['RCDG_FILE_NM']
    rec_start_date = str(info_dic['REC_STDT'])
    base_path = '{0}/{1}'.format(
        DAEMON_CONFIG['rec_server_path'], rec_start_date[:4] + rec_start_date[5:7] + rec_start_date[8:10])
    if not os.path.exists(base_path):
        logger.error('directory is not exists -> {0}'.format(base_path))
    abs_rx_file_path = '{0}/{1}.rx.enc'.format(base_path, file_name)
    abs_tx_file_path = '{0}/{1}.tx.enc'.format(base_path, file_name)
    incident_file_dir_path = '{0}/incident_file'.format(DAEMON_CONFIG['rec_server_path'])
    incident_file_rx_file_path = '{0}/{1}.rx.enc'.format(incident_file_dir_path, file_name)
    incident_file_tx_file_path = '{0}/{1}.tx.enc'.format(incident_file_dir_path, file_name)
    if os.path.exists(abs_rx_file_path) and os.path.exists(abs_tx_file_path):
        logger.debug('file is exists -> {0}'.format(file_name))
        return True
    elif os.path.exists(incident_file_rx_file_path) and os.path.exists(incident_file_tx_file_path):
        logger.debug('file is exists -> {0}'.format(file_name))
        return True
    else:
        logger.debug('file is not exist -> {0}'.format(file_name))
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


def mysql_connect(logger):
    """
    Attempt to connect to MySQL
    :param:     logger:     Logger
    :return:                Mysql
    """
    cnt = 0
    while True:
        try:
            mysql = MySQL()
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Fail connect MySQL, retrying count = {0}".format(cnt))
            cnt += 1
            continue
    return mysql


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
