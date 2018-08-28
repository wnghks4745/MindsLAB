#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-07-31, modification: 2018-07-31"

###########
# imports #
###########
import os
import sys
import time
import glob
import json
import shutil
import socket
import MySQLdb
import traceback
import collections
from datetime import datetime
from lib.iLogger import set_logger
from lib.openssl import encrypt_file
sys.path.append('/app/MindsVOC/CS')
from service.config import JSON_CONFIG, MYSQL_DB_CONFIG


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ''
DT = ''
ERROR_CNT = 0
UPLOAD_CNT = 0
ID = 'REC'


#########
# class #
#########
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
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def check_stt_rcdg_info(self, con_id, rfile_name):
        try:
            query = """
                SELECT
                    *
                FROM
                    STT_RCDG_INFO
                WHERE 1=1
                    AND CON_ID = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                con_id,
                rfile_name,
            )
            self.cursor.execute(query, bind)
            result = self.cursor.fetchone()
            if result is bool:
                return False
            if result:
                return True
            return False
        except Exception:
            exc_info = traceback.format_exc()
            raise Exception(exc_info)

    def update_data_to_stt_rcdg_info(self, **kwargs):
        try:
            query = """
                UPDATE
                    STT_RCDG_INFO
                SET
                    CHANNEL = %s,
                    RECORDKEY = %s,
                    CENTER = %s,
                    PROJECT_CD = %s,
                    STT_PRGST_CD = %s,
                    STT_REQ_DTM = NOW(),
                    STT_SERVER_ID = %s,
                    CALL_TYPE = %s,
                    DATE = %s,
                    CALL_START_TIME = %s,
                    CALL_END_TIME = %s,
                    CALL_DURATION = %s, 
                    RUSER_ID = %s,
                    RUSER_NAME = %s,
                    RUSER_NUMBER = %s,
                    CU_NUMBER = %s,
                    BIZ_CD = %s,
                    CHN_TP = %s,
                    FILE_SPRT = %s,
                    REC_EXT = %s,
                    UPDATOR_ID = %s,
                    UPDATED_DTM = NOW()
                WHERE 1=1
                    AND CON_ID = %s
                    AND RFILE_NAME = %s
            """
            bind = (
                kwargs.get('channel'),
                kwargs.get('recordkey'),
                kwargs.get('center'),
                kwargs.get('project_cd'),
                kwargs.get('stt_prgst_cd'),
                kwargs.get('stt_server_id'),
                kwargs.get('call_type'),
                kwargs.get('date'),
                kwargs.get('call_start_time'),
                kwargs.get('call_end_time'),
                kwargs.get('call_duration'),
                kwargs.get('ruser_id'),
                kwargs.get('ruser_name'),
                kwargs.get('ruser_number'),
                kwargs.get('cu_number'),
                kwargs.get('biz_cd'),
                kwargs.get('chn_tp'),
                kwargs.get('file_sprt'),
                kwargs.get('rec_ext'),
                ID,
                kwargs.get('con_id'),
                kwargs.get('rfile_name'),
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
            raise Exception(traceback.format_exc())

    def insert_data_to_stt_rcdg_info(self, **kwargs):
        try:
            query = """
                INSERT INTO STT_RCDG_INFO
                (
                    CON_ID,
                    RFILE_NAME,
                    CHANNEL,
                    RECORDKEY,
                    CENTER,
                    PROJECT_CD,
                    STT_PRGST_CD,
                    STT_REQ_DTM,
                    STT_SERVER_ID,
                    CALL_TYPE,
                    DATE,
                    CALL_START_TIME,
                    CALL_END_TIME,
                    CALL_DURATION,
                    RUSER_ID,
                    RUSER_NAME,
                    RUSER_NUMBER,
                    CU_NUMBER,
                    BIZ_CD,
                    CHN_TP,
                    FILE_SPRT,
                    REC_EXT,
                    CREATED_DATE,
                    CREATOR_ID,
                    CREATED_DTM,
                    UPDATOR_ID,
                    UPDATED_DTM
                )
                VALUES (
                    %s, %s, %s, %s, %s, 
                    %s, %s, NOW(), %s, %s, %s, 
                    %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, 
                    %s, NOW(), %s, NOW(), %s,
                    NOW()
                )
            """
            bind = (
                kwargs.get('con_id'),
                kwargs.get('rfile_name'),
                kwargs.get('channel'),
                kwargs.get('recordkey'),
                kwargs.get('center'),
                kwargs.get('project_cd'),
                kwargs.get('stt_prgst_cd'),
                kwargs.get('stt_server_id'),
                kwargs.get('call_type'),
                kwargs.get('date'),
                kwargs.get('call_start_time'),
                kwargs.get('call_end_time'),
                kwargs.get('call_duration'),
                kwargs.get('ruser_id'),
                kwargs.get('ruser_name'),
                kwargs.get('ruser_number'),
                kwargs.get('cu_number'),
                kwargs.get('biz_cd'),
                kwargs.get('chn_tp'),
                kwargs.get('file_sprt'),
                kwargs.get('rec_ext'),
                ID,
                ID,
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            exc_info = traceback.format_exc()
            self.conn.rollback()
            raise Exception(exc_info)

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


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


def error_process(logger, file_path):
    """
    Error file process
    :param      logger:         Logger
    :param      file_path:      Json file path
    """
    global ERROR_CNT
    ERROR_CNT += 1
    try:
        now_dt = datetime.now()
        logger.error("Error process. [Json = {0}]".format(file_path))
        error_dir_path = '{0}/error_data/{1}/{2}/{3}'.format(
            os.path.dirname(file_path), now_dt.strftime("%Y"), now_dt.strftime("%m"), now_dt.strftime("%d"))
        if not os.path.exists(error_dir_path):
            os.makedirs(error_dir_path)
        error_json_file_path = '{0}/{1}'.format(error_dir_path, os.path.basename(file_path))
        enc_error_json_file_path = '{0}.enc'.format(error_json_file_path)
        if os.path.exists(enc_error_json_file_path):
            del_garbage(logger, enc_error_json_file_path)
        logger.error('Error json file move {0} -> {1}'.format(file_path, error_dir_path))
        shutil.move(file_path, error_dir_path)
        logger.error('Error json file encrypt {0} -> {1}'.format(error_json_file_path, enc_error_json_file_path))
        encrypt_file([error_json_file_path])
        file_name = os.path.splitext(os.path.basename(file_path))[0]
        if file_name:
            record_file_list = glob.glob('{0}/{1}.*'.format(os.path.dirname(file_path), file_name))
            record_file_list += glob.glob('{0}/{1}_*'.format(os.path.dirname(file_path), file_name))
            if len(record_file_list) > 0:
                for record_file_path in record_file_list:
                    error_file_path = "{0}/{1}".format(error_dir_path, os.path.basename(record_file_path))
                    enc_error_file_path = '{0}.enc'.format(error_file_path)
                    if os.path.exists(enc_error_file_path):
                        del_garbage(logger, enc_error_file_path)
                    shutil.move(record_file_path, error_dir_path)
                    logger.error('Error record file move {0} -> {1}'.format(record_file_path, error_dir_path))
                    encrypt_file([error_file_path])
                    logger.error('Error record file encrypt {0} -> {1}'.format(error_file_path, enc_error_file_path))
    except Exception:
        exc_info = traceback.format_exc()
        logger.critical("Critical error {0}".format(exc_info))


def upload_data_to_db(logger, mysql, checked_json_data_dict):
    """
    Upload data to database
    :param      logger:                         Logger
    :param      mysql:                          MySQL
    :param      checked_json_data_dict:         Checked json data dictionary
    """
    global UPLOAD_CNT
    logger.debug("Json data upload to DB")
    for file_path, json_data in checked_json_data_dict.items():
        try:
            logger.debug("Target json file = {0}".format(file_path))
            if mysql.check_stt_rcdg_info(json_data.get('con_id'), json_data.get('r_file_name')):
                mysql.update_data_to_stt_rcdg_info(
                    con_id=json_data.get('con_id'),
                    rfile_name=json_data.get('r_file_name'),
                    channel=json_data.get('channel'),
                    project_cd=json_data.get('project_cd'),
                    stt_prgst_cd='00',
                    stt_server_id=str(socket.gethostname()),
                    call_type=json_data.get('call_type'),
                    call_start_time=json_data.get('start_time'),
                    call_end_time=json_data.get('end_time'),
                    call_duration=json_data.get('duration'),
                    ruser_id=json_data.get('r_user_id'),
                    ruser_name=json_data.get('r_user_name'),
                    ruser_number=json_data.get('r_user_number'),
                    cu_number=json_data.get('cust_number'),
                    biz_cd=json_data.get('biz'),
                    chn_tp=json_data.get('chn_tp'),
                    file_sprt=json_data.get('sprt'),
                    rec_ext=json_data.get('rec_ext'),
                    date=json_data.get('date'),
                    recordkey=json_data.get('recordkey'),
                    center=json_data.get('center')
                )
            else:
                mysql.insert_data_to_stt_rcdg_info(
                    con_id=json_data.get('con_id'),
                    rfile_name=json_data.get('r_file_name'),
                    channel=json_data.get('channel'),
                    project_cd=json_data.get('project_cd'),
                    stt_prgst_cd='00',
                    stt_server_id=str(socket.gethostname()),
                    call_type=json_data.get('call_type'),
                    call_start_time=json_data.get('start_time'),
                    call_end_time=json_data.get('end_time'),
                    call_duration=json_data.get('duration'),
                    ruser_id=json_data.get('r_user_id'),
                    ruser_name=json_data.get('r_user_name'),
                    ruser_number=json_data.get('r_user_number'),
                    cu_number=json_data.get('cust_number'),
                    biz_cd=json_data.get('biz'),
                    chn_tp=json_data.get('chn_tp'),
                    file_sprt=json_data.get('sprt'),
                    rec_ext=json_data.get('rec_ext'),
                    date=json_data.get('date'),
                    recordkey=json_data.get('recordkey'),
                    center=json_data.get('center')
                )
            json_output_dir_path = '{0}/{1}/{2}'.format(
                JSON_CONFIG['json_output_path'], json_data.get('start_time')[:10], json_data.get('biz'))
            json_output_file_path = os.path.join(json_output_dir_path, os.path.basename(file_path))
            if not os.path.exists(json_output_dir_path):
                os.makedirs(json_output_dir_path)
            if os.path.exists(json_output_file_path):
                del_garbage(logger, json_output_file_path)
            logger.debug("Move json file to output directory.")
            shutil.move(file_path, json_output_dir_path)
            encrypt_file([json_output_file_path])
            mysql.conn.commit()
            UPLOAD_CNT += 1
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            mysql.conn.rollback()
            error_process(logger, file_path)
            continue


def check_json_data(logger, json_data):
    """
    Check json data
    :param      logger:         Logger
    :param      json_data:      Json data
    :return:                    True or False
    """
    project_cd = str(json_data['ProjectCD']).strip()
    if len(project_cd) < 1:
        logger.error("Error ProjectCD length is 0")
        return False
    con_id = str(json_data['ConID']).strip()
    if len(con_id) < 1:
        logger.error('Error ConID length is 0')
        return False
    biz = str(json_data['Biz']).strip()
    if len(biz) < 1:
        logger.error("Error BiZ length is 0")
        return False
    channel = str(json_data['Channel']).strip()
    if len(channel) < 1:
        logger.error("Error Channel length is 0")
        return False
    if channel == 'None':
        channel = ''
    call_type = str(json_data['CallType']).strip()
    if len(call_type) < 1:
        logger.error("Error CallType length is 0")
        return False
    start_time = str(json_data['StartTime']).strip()
    if len(start_time) != 14:
        logger.error("Error StartTime length is not 14")
        return False
    end_time = str(json_data['EndTime']).strip()
    if len(end_time) != 14:
        logger.error("Error EndTime length is not 14")
        return False
    duration = str(json_data['Duration']).strip()
    if len(duration) != 6:
        logger.error("Error Duration length is not 6")
        return False
    r_user_id = str(json_data['RUserID']).strip()
    if len(r_user_id) < 1:
        logger.error("Error RUserID length is 0")
        return False
    r_user_name = str(json_data['RUserName']).strip()
    if len(r_user_name) < 1:
        logger.error("Error RUserName length is 0")
        return False
    r_user_number = str(json_data['RUserNumber']).strip()
    if len(r_user_number) < 1:
        logger.error("Error RUserNumber length is 0")
        return False
    cust_number = str(json_data['CuNumber']).strip()
    if len(cust_number) < 1:
        logger.error("Error CuNumber length is 0")
        return False
    r_file_name = str(json_data['RFileName']).strip()
    if len(r_file_name) < 1:
        logger.error("Error RFileName length is 0")
        return False
    chn_tp = str(json_data['Chn_tp']).strip().upper()
    if chn_tp not in ['S', 'M']:
        logger.error("Error Chn_tp is not 'S' or 'M'")
        return False
    sprt = str(json_data['Sprt']).strip().upper()
    if sprt not in ['Y', 'N']:
        logger.error("Error Sprt is not 'Y' or 'N'")
        return False
    rec_ext = str(json_data['Rec_ext']).strip()
    if len(rec_ext) < 1:
        logger.error("Error Rec_ext length is 0")
        return False
    date = str(json_data['Date']).strip()
    if len(date) != 8:
        logger.error("Error Date length is not 8")
        return False
    recordkey = str(json_data['RecordKey']).strip()
    if len(recordkey) < 1:
        logger.error("Error RecordKey length is 0")
        return False
    center = str(json_data['Center']).strip()
    if len(center) < 1:
        logger.error("Error Center length is 0")
        return False
    # modified_date = '{0}/{1}/{2}'.format(date[:4], date[4:6], date[6:8])
    rcdg_stdtm = '{0}/{1}/{2} {3}:{4}:{5}'.format(
        start_time[:4], start_time[4:6], start_time[6:8], start_time[8:10], start_time[10:12], start_time[12:14])
    rcdg_edtm = '{0}/{1}/{2} {3}:{4}:{5}'.format(
        end_time[:4], end_time[4:6], end_time[6:8], end_time[8:10], end_time[10:12], end_time[12:14])
    json_data_dict = {
        'con_id': con_id,
        'r_file_name': r_file_name,
        'channel': channel,
        'project_cd': project_cd,
        'call_type': call_type,
        'start_time': rcdg_stdtm,
        'end_time': rcdg_edtm,
        'duration': duration,
        'r_user_id': r_user_id,
        'r_user_name': r_user_name,
        'r_user_number': r_user_number,
        # 'cust_number': cust_number,
        'biz': biz,
        'chn_tp': chn_tp,
        'sprt': sprt,
        'rec_ext': rec_ext,
        'date': date,
        'recordkey': recordkey,
        'center': center
    }
    return json_data_dict


def check_rec_file(logger, file_path, checked_json_data):
    """
    Check record file
    :param      logger:                 Logger
    :param      file_path:              Json file path
    :param      checked_json_data:      Json data
    :return:                            True or False
    """
    rec_dir_path = os.path.dirname(file_path)
    chn_tp = checked_json_data['chn_tp']
    sprt = checked_json_data['sprt']
    file_name = checked_json_data['r_file_name']
    extension = checked_json_data['rec_ext']
    logger.debug("Record directory = {0}, Chn_tp = {1}, Sprt = {2}, RFileName = {3}, Rec_ext = {4}".format(
        rec_dir_path, chn_tp, sprt, file_name, extension
    ))
    if chn_tp == 'S' and sprt == 'Y':
        rx_file_path = "{0}/{1}_rx.{2}".format(rec_dir_path, file_name, extension)
        tx_file_path = "{0}/{1}_tx.{2}".format(rec_dir_path, file_name, extension)
        if not os.path.exists(rx_file_path) or not os.path.exists(tx_file_path):
            logger.error('Record file is not exist -> {0} or {1}'.format(rx_file_path, tx_file_path))
            return False
        if os.stat(rx_file_path)[6] == 0 or os.stat(tx_file_path)[6] == 0:
            logger.error('Record file size is 0 -> {0} or {1}'.format(rx_file_path, tx_file_path))
            return False
    elif (chn_tp == 'S' and sprt == 'N') or chn_tp == 'M':
        trx_file_path = "{0}/{1}.{2}".format(rec_dir_path, file_name, extension)
        if not os.path.exists(trx_file_path):
            logger.error('Record file is not exist -> {0}'.format(trx_file_path))
            return False
        if os.stat(trx_file_path)[6] == 0:
            logger.error('Record file size is 0 -> {0}'.format(trx_file_path))
            return False
    else:
        logger.error("Not found record file = {0}".format(file_path))
        return False
    return True


def check_raw_data(logger, sorted_json_list):
    """
    Check record file and json data
    :param      logger:                 Logger
    :param      sorted_json_list:       json file list
    :return:                            Checked json data
    """
    logger.debug("-" * 100)
    logger.debug("Check raw data (JSON, RECORD)")
    json_data_dict = collections.OrderedDict()
    for file_path in sorted_json_list:
        try:
            logger.debug("Open json file. [{0}]".format(file_path))
            try:
                json_file = open(file_path, 'r')
                logger.debug("Load json data. [encoding = euc-kr]")
                json_data = json.load(json_file, encoding='euc-kr')
                json_file.close()
            except Exception:
                logger.error("Retry load json data.")
                time.sleep(1)
                json_file = open(file_path, 'r')
                logger.debug("Load json data. [encoding = euc-kr]")
                json_data = json.load(json_file, encoding='euc-kr')
                json_file.close()
            # Check json data
            logger.debug("Check json data.")
            checked_json_data = check_json_data(logger, json_data)
            if not checked_json_data:
                error_process(logger, file_path)
                continue
            # Check record file
            # logger.debug("Check record file.")
            # if not check_rec_file(logger, file_path, checked_json_data):
            #     error_process(logger, file_path)
            #     continue
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            error_process(logger, file_path)
            continue
        json_data_dict[file_path] = checked_json_data
    return json_data_dict


def make_json_list(logger):
    """
    Make sorted json file list
    :param      logger:         Logger
    :return:                    Sorted json list
    """
    logger.debug("Make json list")
    target_dir_list = JSON_CONFIG['json_dir_path']
    logger.debug("Json directory list = {0}".format(", ".join(target_dir_list)))
    # 녹취 업체별 json 파일 리스트 생성
    file_list = list()
    for target_dir_path in target_dir_list:
        if not os.path.exists(target_dir_path):
            err_str = "Record directory is not exist. -> {0}".format(target_dir_path)
            logger.error(err_str)
            continue
        logger.debug("Extract json file list from {0}".format(target_dir_path))
        file_list += glob.glob("{0}/*.json".format(target_dir_path))
    # 최종 변경 시간 기준 녹취 파일 정렬
    logger.debug("Json file list, sorted by last modified time.")
    sorted_json_list = sorted(file_list, key=os.path.getmtime, reverse=False)
    return sorted_json_list


def connect_db(logger, db):
    """
    Connect database
    :param      logger:         Logger
    :param      db:             Database
    :return:                    SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'MySQL':
                sql = MySQL(logger)
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
        err_str = "Fail connect {0}".format(db)
        return False
    return sql


def processing():
    """
    Processing
    """
    # Add logging
    logger_args = {
        'base_path': JSON_CONFIG['log_dir_path'],
        'log_file_name': JSON_CONFIG['log_name'],
        'log_level': JSON_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    # Connect db
    try:
        mysql = connect_db(logger, 'MySQL')
        if not mysql:
            logger.error("---------- Can't connect db ----------")
            for handler in logger.handlers:
                handler.close()
                logger.removeHandler(handler)
            sys.exit(1)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("---------- Can't connect db ----------")
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    try:
        # Make json file list
        sorted_json_list = make_json_list(logger)
        if len(sorted_json_list) > 0:
            # Record file exist check
            checked_json_data_dict = check_raw_data(logger, sorted_json_list)
            # Upload json data to db
            upload_data_to_db(logger, mysql, checked_json_data_dict)
            logger.info(
                "Total json target count = {0}, upload count = {1}, error_count = {2}, The time required = {3}".format(
                    len(sorted_json_list), UPLOAD_CNT, ERROR_CNT, elapsed_time(DT)))
        else:
            logger.debug("No json file")
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error("---------- ERROR ----------")
        mysql.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    mysql.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main():
    """
    This is a program that upload JSON data to MySQL DB
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
        print (exc_info)


if __name__ == '__main__':
    main()