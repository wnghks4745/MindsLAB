#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-10-12, modification: 2017-12-08"

###########
# imports #
###########
import os
import sys
import time
import MySQLdb
import pymssql
import commands
import traceback
from datetime import date, datetime, timedelta
from lib.iLogger import set_logger
from cfg.config import CONFIG, MYSQL_DB_CONFIG, MSSQL_DB_CONFIG

#########
# class #
#########


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

    def select_data(self, target_date, r_comp_type):
        query = """
            SELECT
                *
            FROM
                DBO.VREC_STT_INFO WITH(NOLOCK)
            WHERE 1=1
                 AND R_FILE_NM like %s
                 AND R_COMP_TYPE = %s
        """
        bind = (
            "{0}%".format(target_date),
            r_comp_type,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
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

    def select_data(self, mysql_date):
        query = """
            SELECT
                RCDG_ID,
                RCDG_FILE_NM
            FROM
                TB_QA_STT_RECINFO
            WHERE
                 DATE(REC_STDT) = %s
        """
        bind = (mysql_date, )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        return result

    def insert_data_to_tb_qa_stt_recinfo(self, **kwargs):
        try:
            query = """
                INSERT INTO TB_QA_STT_RECINFO
                (
                    RCDG_ID,
                    RCDG_FILE_NM,
                    CHN_TP_CD,
                    RCDG_TP_CD,
                    PRGST_CD,
                    DURATION_HMS,
                    REC_STDT,
                    REC_SDTM,
                    REC_EDTM,
                    USER_ID,
                    EXT_NO,
                    REGP_CD,
                    RGST_PGM_ID,
                    RGST_DTM,
                    LST_CHGP_CD,
                    LST_CHG_PGM_ID
                )
                VALUES
                (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s
                )
            """
            bind = (
                kwargs.get('rcdg_id'),
                kwargs.get('rcdg_file_nm'),
                kwargs.get('chn_tp_cd'),
                kwargs.get('rcdg_tp_cd'),
                kwargs.get('prgst_cd'),
                kwargs.get('duration_hms'),
                kwargs.get('rec_stdt'),
                kwargs.get('rec_sdtm'),
                kwargs.get('rec_edtm'),
                kwargs.get('user_id'),
                kwargs.get('ext_no'),
                kwargs.get('regp_cd'),
                kwargs.get('rgst_pgm_id'),
                kwargs.get('rgst_dtm'),
                kwargs.get('lst_chgp_cd'),
                kwargs.get('lst_chg_pgm_id'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            self.logger.error("Can't insert data to TB_QA_STT_RECINFO")
            self.logger.error(exc_info)
            self.logger.error(kwargs.values())
            self.conn.rollback()


#######
# def #
#######


def elapsed_time(sdate):
    """
    elapsed time
    :param          sdate:          date object
    :return                         Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]),
                          int(sdate[8:10]), int(sdate[10:12]), int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def upload_data_to_mysql(logger, mysql, mysql_rcdg_id_dict, mssql_data, log_cnt):
    """
    Upload data to MySQL
    :param          logger:                     Logger
    :param          mysql:                      MySQL
    :param          mysql_rcdg_id_dict:         RCDG_ID dictionary
    :param          mssql_data:                 MsSQL data
    :param          log_cnt:                    Log count
    """
    logger.info('4-{0}). Upload data to MySQL'.format(log_cnt))
    insert_cnt = 0
    for item in mssql_data:
        try:
            r_key_code = item[0]
            r_file_nm = item[1]
            r_duration = item[2]
            r_start_tm = item[3]
            r_end_tm = item[4]
            r_usr_id = item[5]
            r_ext_id = item[6]
    #        r_comp_type = item[7]
    #        r_comp_type_nm = item[8]
    #        r_group_id = item[9]
    #        r_group_id_nm = item[10]
    #        r_team_id = item[11]
    #        r_team_id_nm = item[12]
            logger.debug("r_key_code = {0}, r_file_nm = {1}. r_duration = {2}, r_start_tm = {3}, r_end_tm = {4},"
                        " r_usr_id = {5}, r_ext_id = {6}".format(r_key_code, r_file_nm, r_duration, r_start_tm,
                                                                 r_end_tm, r_usr_id, r_ext_id))
            start_tm = "{0}{1}".format(r_file_nm[:8], r_start_tm)
            end_tm = "{0}{1}".format(r_file_nm[:8], r_end_tm)
            key = "{0}_{1}".format(r_key_code, r_file_nm)
            if key in mysql_rcdg_id_dict:
                continue
            mysql.insert_data_to_tb_qa_stt_recinfo(
                rcdg_id=r_key_code,
                rcdg_file_nm=r_file_nm,
                chn_tp_cd=CONFIG['chn_tp_cd'],
                rcdg_tp_cd=CONFIG['rcdg_tp_cd'],
                prgst_cd=CONFIG['prgst_cd'],
                duration_hms=r_duration,
                rec_stdt=date(int(r_file_nm[:4]), int(r_file_nm[4:6]), int(r_file_nm[6:8])),
                rec_sdtm=datetime.strptime(start_tm, "%Y%m%d%H%M%S"),
                rec_edtm=datetime.strptime(end_tm, "%Y%m%d%H%M%S"),
                user_id=r_usr_id,
                ext_no=r_ext_id,
                regp_cd='',
                rgst_pgm_id='',
                rgst_dtm=datetime.now(),
                lst_chgp_cd='',
                lst_chg_pgm_id=''
            )
            insert_cnt += 1
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't upload data to MySQL")
            logger.error(exc_info)
            continue
    return insert_cnt


def make_mysql_rcdg_id_dict(logger, mysql, target_date, log_cnt):
    """
    Make MySQL RCDG_ID dictionary
    :param          logger:             Logger
    :param          mysql:              MySQL
    :param          target_date:        Target date
    :param          log_cnt:            Log count
    :return:                            RCDG_ID dictionary
    """
    logger.info('3-{0}). Make MySQL RCDG_ID dictionary'.format(log_cnt))
    mysql_rcdg_id_dict = dict()
    mysql_date = "{0}-{1}-{2}".format(target_date[:4], target_date[4:6], target_date[6:8])
    logger.info('Select RCDG_ID from MySQL target date -> {0}'.format(mysql_date))
    mysql_rcdg_id = mysql.select_data(mysql_date)
    for item in mysql_rcdg_id:
        rcdg_id = item[0]
        rcdg_file_nm = item[1]
        key = "{0}_{1}".format(rcdg_id, rcdg_file_nm)
        mysql_rcdg_id_dict[key] = 1
    return mysql_rcdg_id_dict


def connect_db(logger, db):
    """
    Connect database
    :param          logger:         Logger
    :param          db:             Database
    :return:                        SQL Object
    """
    # Connect DB
    logger.info('Connect {0} ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'MsSQL':
                sql = MSSQL(logger)
            elif db == 'MySQL':
                sql = MySQL(logger)
            else:
                raise Exception("Fail connect {0}".format(db))
            logger.info("Success connect ".format(db))
            break
        except Exception as e:
            print e
            if cnt < 3:
                print "Fail connect {0}, retrying count = {1}".format(db, cnt)
                logger.error("Fail connect {0}, retrying count = {1}".format(db, cnt))
            continue
    if not sql:
        raise Exception("Fail connect {0}".format(db))
    return sql


def processing(input_date):
    """
    Processing
    :param          input_date:        Target date
    """
    ts = time.time()
    target_date_list = list()
    if not input_date:
        for cnt in range(int(CONFIG['pre_hours']), -1, -1):
            target_datetime = datetime.now() - timedelta(hours=cnt)
            temp_target_date = target_datetime.strftime('%Y%m%d%H')
            target_date_list.append(temp_target_date)
    else:
        target_date_list.append(input_date)
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_file_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start upload REC information')
    logger.info("1. Connect DB ..")
    # Connect MsSQL
    mssql = connect_db(logger, 'MsSQL')
    # Connect MySQL
    mysql = connect_db(logger, 'MySQL')
    total_insert_cnt = 0
    log_cnt = 1
    for target_date in target_date_list:
        # Select MsSQL data
        logger.info('2-{0}). Select MsSQL data'.format(log_cnt))
        logger.info('Target date -> {0}'.format(target_date))
        mssql_data = mssql.select_data(target_date, CONFIG['r_comp_type'])
        # Make MySQL RCDG_ID dictionary
        mysql_rcdg_id_dict = make_mysql_rcdg_id_dict(logger, mysql, target_date, log_cnt)
        # Upload data to MySQL
        insert_cnt = upload_data_to_mysql(logger, mysql, mysql_rcdg_id_dict, mssql_data, log_cnt)
        total_insert_cnt += insert_cnt
        logger.info("MySQL DB upload count is {0}".format(insert_cnt))
        log_cnt += 1
    logger.info("Total MySQL DB upload count is {0}".format(total_insert_cnt))
    # Disconnect DB and remove logger
    logger.info("5. Disconnect DB and remove logger")
    mssql.disconnect()
    mysql.disconnect()
    logger.info("END.. Start time = {0}, The time required = {1}".format(st, elapsed_time(dt)))
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)

########
# main #
########


def main(input_date):
    """
    This is a program that insert MsSQL data to MySQL
    :param          input_date:        Target date
    """
    try:
        # Stop if already running
        script_name = os.path.basename(__file__)
        l = commands.getstatusoutput(
            "ps aux | grep -e '%s' | grep -v grep | awk '{print $2}'| awk '{print $2}'" % script_name)
        if l[1]:
            print "Script is already running."
            sys.exit(1)
        processing(input_date)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        try:
            int(sys.argv[1])
        except Exception:
            print 'Target date have to number.'
            print 'Error target date. ex) 2017...'
            sys.exit(1)
        main(sys.argv[1])
    elif len(sys.argv) == 1:
        main(False)
    else:
        print "usage : python upload_rec_info.py [target_date, default=NOW]"
        print "Ex) python upload_rec_info.py or upload_rec_info.py 20170704"
        sys.exit(1)
