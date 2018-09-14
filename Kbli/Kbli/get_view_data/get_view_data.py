#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-31, modification: 2018-03-13"

###########
# imports #
###########
import os
import sys
import time
import psycopg2
import psycopg2.extras
import cx_Oracle
import traceback
from datetime import datetime, timedelta
from cfg.config import CONFIG, ORACLE_DB_CONFIG, TM_POSTGRESQL_DB_CONFIG, CS_POST_GRESQL_DB_CONFIG
from lib.iLogger import set_logger
from lib.openssl import decrypt_string
from lib.damo import scp_enc_file

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ""
DT = ""


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = ORACLE_DB_CONFIG['dsn']
        passwd = decrypt_string(ORACLE_DB_CONFIG['passwd'])
        #self.logger.info(passwd)
        #self.logger.info(ORACLE_DB_CONFIG['passwd'])
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            passwd,
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def insert_view_table_data_to_call_meta(self, row, project_cd, status):
        """
        Insert view table data to call meta
        :param              row:            view table data rows
        :param              project_cd:     Project code
        :param              status:         Status
        """
        self.logger.debug("insert view table data to call meta : row - {0}".format(row['call_id']))
        try:
            sql = """
                MERGE INTO CALL_META
                    USING
                        DUAL
                    ON ( 1=1
                        AND PROJECT_CD = :1
                        AND DOCUMENT_ID = :2
                    )
                WHEN MATCHED THEN
                    UPDATE SET
                        REC_ID = :3,
                        LST_CHGP_CD = 'MAP',
                        LST_CHG_PGM_ID = 'MAP',
                        LST_CHG_DTM = SYSDATE
                WHEN NOT MATCHED THEN
                    INSERT (
                        PROJECT_CD,
                        DOCUMENT_DT,
                        DOCUMENT_ID,
                        CALL_TYPE,
                        REC_ID,
                        CALL_DT,
                        START_DTM,
                        END_DTM,
                        DURATION,
                        STATUS,
                        EXTENSION_PHONE_NO,
                        STT_PRGST_CD,
                        CHN_TP,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                    VALUES (
                        :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, 
                        :15, :16, 'MAP', 'MAP', SYSDATE, 'MAP', 'MAP', SYSDATE
                    )
            """
            bind = (
                project_cd,
                row['call_id'],
                row['etc1'],
                project_cd,
                row['call_date'],
                row['call_id'],
                row['call_type'],
                row['etc1'],
                row['call_date'],
                datetime.combine(row['call_date'], row['call_time']),
                row['end_time'],
                row['duration'],
                row['status'],
                row['phone_no'],
                status,
                'S',
            )
            self.cursor.execute(sql, bind)
            self.conn.commit()
            return True
        except Exception as e:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            self.conn.rollback()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


class PostgreSQL(object):
    def __init__(self, logger, project_cd):
        self.logger = logger
        db_config = TM_POSTGRESQL_DB_CONFIG if project_cd == 'TM' else CS_POST_GRESQL_DB_CONFIG
        self.conn = psycopg2.connect(
            dbname=db_config['db'],
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            port=db_config['port'],
            connect_timeout=db_config['connect_timeout']
        )
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def find_view_table(self, before_date, before_time):
        """
        Find upprocessed rec_id in the view table writing complete file check : COMPRESS = '1' AND MIXING_COMPLETE = '1'
        :param          before_date:            before date
        :param          before_time:            before time
        :return:                                Rec id list
        """
        sql = """
            SELECT
                *
            FROM
                VIEW_CALL_INFO_STT
            WHERE 1=1
                AND CALL_DATE >= %s
                AND CALL_TIME >= %s
                AND COMPRESS = '1'
                AND MIXING_COMPLETE = '1'
        """
        bind = (
            before_date,
            before_time,
        )
        self.logger.debug('CALL_DATE : {0}, CALL_TIME : {1}'.format(before_date, before_time))
        self.cursor.execute(sql, bind)
        rows = self.cursor.fetchall()
        return rows

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


#######
# def #
#######
def encrypt_and_insert_rows(logger, oracle, rows, project_cd):
    """
    Encrypt Record and Insert CALL_META
    :param          logger:                 Logger
    :param          oracle:                 Oracle
    :param          rows:                   REC CALL META row
    :param          project_cd:             Project code
    """
    for row in rows:
        file_name = ''
        try:
            document_dt = str(row.get('call_date'))
            directory_name = document_dt[:4] + document_dt[5:7] + document_dt[8:10]
            directory_path = '{0}/{1}'.format(CONFIG['target_rec_path'][project_cd], directory_name)
            file_name = row.get('call_id')
            wav_file_name = '{0}.wav'.format(file_name)
            wav_file_path = '{0}/{1}'.format(directory_path, wav_file_name)
            enc_file_path = '{0}.enc'.format(wav_file_path)
            if os.path.exists(wav_file_path):
                # 녹취파일 암호화
                result = scp_enc_file(wav_file_path, enc_file_path)
                logger.debug('wav : {0}'.format(wav_file_path))
                logger.debug('enc : {0}'.format(enc_file_path))
                logger.debug('result : {0}'.format(result))
                if not result == 0:
                    logger.error('{0}'.format(result))
                    raise Exception
                # os.remove(wav_file_path)
                tmp_file_name = '{0}.tmp'.format(wav_file_path)
                os.remove(wav_file_path)
            elif not os.path.exists(enc_file_path):
                logger.error('Rec file or Encrypt rec file is not exists -> {0}'.format(wav_file_path))
                # CALL META 90 UPSERT
                oracle.insert_view_table_data_to_call_meta(row, project_cd, '90')
            # CALL META SUCCESS INSERT
            oracle.insert_view_table_data_to_call_meta(row, project_cd, '01')
        except Exception:
            # CALL META ERROR INSERT
            oracle.insert_view_table_data_to_call_meta(row, project_cd, '13')
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Encrypt Error file name is {0}".format(file_name))


def connect_db(logger, db):
    """
    Connect database
    :param          logger:             Logger
    :param          db:                 Database
    :return:                            SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".AL32UTF8"
                sql = Oracle(logger)
            elif db == 'TM_postgresql':
                sql = PostgreSQL(logger, 'TM')
            elif db == 'CS_postgresql':
                sql = PostgreSQL(logger, 'CS')
            else:
                logger.error("Unknown DB [{0}]".format(db))
                return False
            logger.debug("Success connect {0} DB ...".format(db))
            break
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            if cnt < 3:
                logger.error("Fail connect {0} DB, retrying count = {1}".format(db, cnt))
            time.sleep(10)
            continue
    if not sql:
        return False
    return sql


def get_view_data(logger, oracle):
    """
    Get view data
    :param          logger:             Logger
    :param          oracle:             Oracle
    """
    logger.info("Get view data!")
    ts = time.time()
    before_dt = datetime.fromtimestamp(ts) - timedelta(hours=3)
    before_date = before_dt.strftime('%Y-%m-%d')
    before_time = before_dt.strftime('%H:%M:%S')
    postgresql = connect_db(logger, 'TM_postgresql')
    rows = postgresql.find_view_table(before_date, before_time)
    postgresql.disconnect()
    encrypt_and_insert_rows(logger, oracle, rows, 'TM')
    postgresql = connect_db(logger, 'CS_postgresql')
    rows = postgresql.find_view_table(before_date, before_time)
    postgresql.disconnect()
    encrypt_and_insert_rows(logger, oracle, rows, 'CS')


def processing():
    """
    Processing
    """
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    # Connect db
    try:
        oracle = connect_db(logger, 'Oracle')
        if not oracle:
            print "---------- Can't connect db ----------"
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- Can't connect db ----------"
        logger.error(exc_info)
        logger.error("---------- Can't connect db ----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    # get view data
    get_view_data(logger, oracle)
    oracle.disconnect()


########
# main #
########
def main():
    """
    This is program that Encrypt Recording file
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info


if __name__ == '__main__':
    main()
