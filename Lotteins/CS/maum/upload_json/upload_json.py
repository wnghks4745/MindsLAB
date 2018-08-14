#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-01-31, modification: 2018-04-23"

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
import pymssql
import traceback
import cx_Oracle
import subprocess
import collections
from datetime import datetime
from lib.iLogger import set_logger
from lib.openssl import encrypt_file
from cfg.config import CONFIG, ORACLE_DB_CONFIG, MSSQL_DB_CONFIG

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
UPLOAD_CNT = 0
ERROR_CNT = 0


#########
# class #
#########
class Oracle(object):
    def __init__(self, logger):
        self.logger = logger
        self.dsn_tns = cx_Oracle.makedsn(
            ORACLE_DB_CONFIG['host'],
            ORACLE_DB_CONFIG['port'],
            sid=ORACLE_DB_CONFIG['sid']
        )
        self.conn = cx_Oracle.connect(
            ORACLE_DB_CONFIG['user'],
            ORACLE_DB_CONFIG['passwd'],
            self.dsn_tns
        )
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_business_dcd_from_cm_cd_detail_tb(self, meta_code):
        query = """
            SELECT
                FULL_CODE
            FROM
                CM_CD_DETAIL_TB
            WHERE 1=1
                AND META_CODE = :1
                AND USE_YN = 'Y'
        """
        bind = (
            meta_code,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_cs_business_dcd(self, user_id, user_name):
        query = """
            SELECT
                JOBCD
            FROM
                GROUP_INFO_CS_USER_TB
            WHERE 1=1
                AND USER_ID LIKE :1
                AND USERNAME = :2
        """
        bind = (
            "{0}%".format(user_id),
            user_name
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def select_tm_business_dcd(self, user_id, user_name):
        query = """
            SELECT
                TM_DCD
            FROM
                GROUP_INFO_TM_USER_TB
            WHERE 1=1
                AND USER_ID LIKE :1
                AND USERNAME = :2
        """
        bind = (
            "{0}%".format(user_id),
            user_name
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result

    def insert_data_to_cm_call_meta_tb(self, **kwargs):
        try:
            query = """
                INSERT INTO CM_CALL_META_TB
                (
                    CALL_DATE,
                    PROJECT_CODE,
                    FILE_NAME,
                    CALL_TYPE_CODE,
                    CONTRACT_NO,
                    RECORD_KEY,
                    START_TIME,
                    END_TIME,
                    DURATION,
                    CTI_CALL_ID,
                    RUSER_ID,
                    RUSER_NAME,
                    RUSER_NUMBER,
                    BUSINESS_DCD,
                    CU_ID,
                    CU_NAME,
                    CU_NAME_HASH,
                    CU_NUMBER,
                    IN_CALL_NUMBER,
                    BIZ_CD,
                    CHN_TP,
                    FILE_SPRT,
                    REC_EXT,
                    CREATED_DTM,
                    UPDATED_DTM,
                    CREATOR_ID,
                    UPDATOR_ID
                )
                VALUES (
                    TO_DATE(:1, 'YYYY/MM/DD'),
                    :2, :3, :4, '', :5,
                    TO_DATE(:6, 'YYYY/MM/DD HH24:MI:SS'),
                    TO_DATE(:7, 'YYYY/MM/DD HH24:MI:SS'),
                    :8, :9, :10, :11, :12, :13, :14,
                    M2U_MASKING_NAME(:15), M2U_CRYPTO_SHA256_ENCRYPT(:16),
                    :17, :18, :19, :20, :21, :22, SYSDATE, SYSDATE, :23, :24
                )
            """
            bind = (
                kwargs.get('start_time')[:10],
                kwargs.get('project_code'),
                kwargs.get('file_name'),
                kwargs.get('call_type_code'),
                kwargs.get('record_key'),
                kwargs.get('start_time'),
                kwargs.get('end_time'),
                kwargs.get('duration'),
                kwargs.get('cti_call_id'),
                kwargs.get('ruser_id'),
                kwargs.get('ruser_name'),
                kwargs.get('ruser_number'),
                kwargs.get('business_dcd'),
                kwargs.get('cu_id'),
                kwargs.get('cu_name'),
                kwargs.get('cu_name'),
                kwargs.get('cu_number'),
                kwargs.get('in_call_number'),
                kwargs.get('biz_cd'),
                kwargs.get('chn_tp'),
                kwargs.get('file_sprt'),
                kwargs.get('rec_ext'),
                kwargs.get('creator_id'),
                kwargs.get('updator_id'),
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

    def delete_cm_call_meta_tb(self, file_name):
        try:
            query = """
                DELETE
                    CM_CALL_META_TB
                WHERE 1=1
                    AND FILE_NAME = :1
            """
            bind = (file_name,)
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

    def select_cust_info(self, cti_call_id, start_time, r_user_number):
        query = """
            SELECT
                CallID,
                Tag2,
                Tag3,
                Tag8
            FROM
                t_callinfo WITH(NOLOCK)
            WHERE 1=1
                AND R_CallID = %s
                AND StartTime like %s
                AND Ext = %s
        """
        bind = (
            cti_call_id,
            "{0}%".format(start_time[:8]),
            r_user_number,
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchone()
        if result is bool:
            return False
        if not result:
            return False
        return result


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


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)


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
    :param          logger:             Logger
    :param          file_path:          Json file path
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


def upload_data_to_db(logger, oracle, checked_json_data_dict):
    """
    Upload data to database
    :param          logger:                         Logger
    :param          oracle:                         Oracle
    :param          checked_json_data_dict:         Checked json data dictionary
    """
    global UPLOAD_CNT
    logger.debug("Json data upload to DB")
    for file_path, json_data in checked_json_data_dict.items():
        try:
            logger.debug("Target json file = {0}".format(file_path))
            project_code = 'PC0001' if json_data.get('project_cd') == 'CS' else 'PC0002'
            if json_data.get('call_type') == '1':
                call_type = 'CT0001'
            elif json_data.get('call_type') == '2':
                call_type = 'CT0002'
            else:
                call_type = 'CT0000'
            hour = json_data.get('duration')[:2]
            minute = json_data.get('duration')[2:4]
            sec = json_data.get('duration')[4:6]
            duration = int(hour) * 3600 + int(minute) * 60 + int(sec)
            record_key = '' if json_data.get('rec_id') == 'None' else json_data.get('rec_id')
            # CS BUSINESS_DCD
            if project_code == 'PC0001':
                user_id = json_data.get('r_user_id')[:7]
                user_name = json_data.get('r_user_name')
                if user_id == 'None':
                    business_dcd = ''
                else:
                    result = oracle.select_cs_business_dcd(user_id, user_name)
                    if result:
                        business_dcd = result[0]
                    else:
                        business_dcd = ''
            # TM BUSINESS_DCD
            else:
                user_id = json_data.get('r_user_id')[:7]
                user_name = json_data.get('r_user_name')
                if user_id == 'None':
                    business_dcd = ''
                else:
                    result = oracle.select_tm_business_dcd(user_id, user_name)
                    if result:
                        business_dcd = result[0]
                    else:
                        business_dcd = ''
            if business_dcd:
                meta_business_dcd = oracle.select_business_dcd_from_cm_cd_detail_tb(business_dcd)
                if meta_business_dcd:
                    business_dcd = meta_business_dcd[0]
            #oracle.delete_cm_call_meta_tb(json_data.get('r_file_name'))
            oracle.insert_data_to_cm_call_meta_tb(
                project_code=project_code,
                file_name=json_data.get('r_file_name'),
                call_type_code=call_type,
                record_key=record_key,
                start_time=json_data.get('start_time'),
                end_time=json_data.get('end_time'),
                duration=duration,
                cti_call_id=json_data.get('cti_call_id'),
                stt_cmdtm='',
                stt_server_id=str(socket.gethostname()),
                ruser_id='' if json_data.get('r_user_id') == 'None' else json_data.get('r_user_id'),
                ruser_name='' if json_data.get('r_user_name') == 'None' else json_data.get('r_user_name'),
                ruser_number=json_data.get('r_user_number'),
                business_dcd=business_dcd,
                cu_id='' if json_data.get('cust_id') == 'None' else json_data.get('cust_id'),
                cu_name='' if json_data.get('cust_name') == 'None' else json_data.get('cust_name'),
                cu_number='' if json_data.get('cust_number') == 'None' else json_data.get('cust_number'),
                in_call_number=json_data.get('ani'),
                biz_cd=json_data.get('biz'),
                chn_tp=json_data.get('chn_tp'),
                file_sprt=json_data.get('sprt'),
                rec_ext=json_data.get('rec_ext'),
                creator_id='CS_REC',
                updator_id='CS_REC',
            )
            json_output_dir_path = '{0}/{1}/{2}'.format(
                CONFIG['json_output_path'], json_data.get('start_time')[:10], json_data.get('biz'))
            json_output_file_path = os.path.join(json_output_dir_path, os.path.basename(file_path))
            if not os.path.exists(json_output_dir_path):
                os.makedirs(json_output_dir_path)
            if os.path.exists(json_output_file_path):
                del_garbage(logger, json_output_file_path)
            logger.debug("Move json file to output directory.")
            shutil.move(file_path, json_output_dir_path)
            rec_dir_path = os.path.dirname(file_path)
            rec_move_dir_path = CONFIG['rec_move_dir_path']
            chn_tp = json_data.get('chn_tp')
            sprt = json_data.get('sprt')
            file_name = json_data.get('r_file_name')
            extension = json_data.get('rec_ext')
            target_file_list = list()
            if chn_tp == 'S' and sprt == 'Y':
                target_file_list.append("{0}/{1}_rx.{2}".format(rec_dir_path, file_name, extension))
                target_file_list.append("{0}/{1}_tx.{2}".format(rec_dir_path, file_name, extension))
            elif (chn_tp == 'S' and sprt == 'N') or chn_tp == 'M':
                target_file_list.append("{0}/{1}.{2}".format(rec_dir_path, file_name, extension))
            for target_file_path in target_file_list:
                rec_move_file_path = os.path.join(rec_move_dir_path, os.path.basename(target_file_path))
                if not os.path.exists(rec_move_dir_path):
                    os.makedirs(rec_move_dir_path)
                if os.path.exists(rec_move_file_path):
                    del_garbage(logger, rec_move_file_path)
                logger.info("Move rec file to collector directory. {0} -> {1}".format(
                    target_file_path, rec_move_dir_path))
                shutil.move(target_file_path, '{0}.tmp'.format(rec_move_file_path))
                shutil.move('{0}.tmp'.format(rec_move_file_path), rec_move_file_path)
            encrypt_file([json_output_file_path])
            oracle.conn.commit()
            UPLOAD_CNT += 1
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            oracle.conn.rollback()
            error_process(logger, file_path)
            continue


def check_json_data(logger, json_data):
    """
    Check json data
    :param          logger:             Logger
    :param          json_data:          Json data
    :return:                            True or False
    """
    project_cd = str(json_data['ProjectCD']).strip()
    if len(project_cd) < 1:
        logger.error("Error ProjectCD length is 0")
        return False
    rec_id = str(json_data['RecID']).strip()
    if len(rec_id) < 1:
        logger.error("Error RecID length is 0")
        return False
    cti_call_id = str(json_data['CTICallID']).strip()
    if len(cti_call_id) < 1:
        logger.error("Error CTICallID length is 0")
        return False
    if cti_call_id == 'None':
        cti_call_id = None
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
    ani = str(json_data['ANI']).strip()
    if len(ani) < 1:
        logger.error("Error ANI length is 0")
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
    r_file_name = str(json_data['RFileName']).strip()
    if len(r_file_name) < 1:
        logger.error("Error RFileName length is 0")
        return False
    cust_name = str(json_data['CuName']).strip()
    if len(cust_name) < 1:
        logger.error("Error CuName length is 0")
        return False
    cust_number = str(json_data['CuNumber']).strip()
    if len(cust_number) < 1:
        logger.error("Error CuNumber length is 0")
        return False
    cust_id = str(json_data['CuID']).strip()
    if len(cust_id) < 1:
        logger.error("Error CuID length is 0")
        return False
    biz = str(json_data['Biz']).strip()
    if len(biz) < 1:
        logger.error("Error Biz length is 0")
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
    rcdg_stdtm = '{0}/{1}/{2} {3}:{4}:{5}'.format(
        start_time[:4], start_time[4:6], start_time[6:8], start_time[8:10], start_time[10:12], start_time[12:14])
    rcdg_edtm = '{0}/{1}/{2} {3}:{4}:{5}'.format(
        end_time[:4], end_time[4:6], end_time[6:8], end_time[8:10], end_time[10:12], end_time[12:14])
    # Voicestore 경우 고객 이름, 전화번호, 주민 번호 정보를 Script 에서 직접 Voicestore DB 에 접속해 가져온다.
    if biz == 'VT':
        mssql = connect_db(logger, 'MsSQL')
        results = mssql.select_cust_info(cti_call_id, start_time, r_user_number)
        if not results:
            rec_id = "None"
            cust_id = "None"
            cust_name = "None"
            cust_number = "None"
        else:
            rec_id = str(results[0]).strip()
            cust_id = str(results[3]).strip()
            cust_name = str(results[1]).strip()
            cust_number = str(results[2]).strip()
    json_data_dict = {
        'rec_id': rec_id,
        'r_file_name': r_file_name,
        'project_cd': project_cd,
        'cti_call_id': cti_call_id,
        'call_type': call_type,
        'start_time': rcdg_stdtm,
        'end_time': rcdg_edtm,
        'duration': duration,
        'r_user_id': r_user_id,
        'r_user_name': r_user_name,
        'r_user_number': r_user_number,
        'cust_id': cust_id[:7],
        'cust_name': cust_name,
        'cust_number': cust_number,
        'ani': ani,
        'biz': biz,
        'chn_tp': chn_tp,
        'sprt': sprt,
        'rec_ext': rec_ext
    }
    return json_data_dict


def check_rec_file(logger, file_path, checked_json_data):
    """
    Check record file
    :param          logger:                     Logger
    :param          file_path:                  Json file path
    :param          checked_json_data:          Json data
    :return:                                    True or False
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
    :param              logger                          Logger
    :param              sorted_json_list                Json file list
    :return:                                            Checked json data
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
                time.sleep(10)
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
            logger.debug("Check record file.")
            if not check_rec_file(logger, file_path, checked_json_data):
                error_process(logger, file_path)
                continue
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
    :param          logger:         Logger
    :return:                        Sorted json list
    """
    logger.debug('Make json list')
    target_dir_list = CONFIG['rec_dir_path']
    logger.debug("Record directory list = {0}".format(", ".join(target_dir_list)))
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
    :param          logger:         Logger
    :param          db:             Database
    :return                         SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".AL32UTF8"
                sql = Oracle(logger)
            elif db == 'MsSQL':
                sql = MSSQL(logger)
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
    try:
        # Make json file list
        sorted_json_list = make_json_list(logger)
        if len(sorted_json_list) > 0:
            # Record file exist check
            checked_json_data_dict = check_raw_data(logger, sorted_json_list)
            # Upload json data to db
            upload_data_to_db(logger, oracle, checked_json_data_dict)
            logger.info(
                "Total json target count = {0}, upload count = {1}, error count = {2},"
                " The time required = {3}".format(len(sorted_json_list), UPLOAD_CNT, ERROR_CNT, elapsed_time(DT)))
            logger.debug("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
        else:
            logger.debug("No json file")
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        logger.error(exc_info)
        logger.error("---------- ERROR ----------")
        oracle.disconnect()
        for handler in logger.handlers:
            handler.close()
            logger.removeHandler(handler)
        sys.exit(1)
    oracle.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main():
    """
    This is a program that upload JSON data to Oracle DB
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
        print(exc_info)


if __name__ == '__main__':
    main()
