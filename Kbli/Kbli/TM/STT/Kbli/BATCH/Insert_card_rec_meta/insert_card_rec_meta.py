#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-05-16, modification: 2018-05-16"

###########
# imports #
###########
import os
import sys
import glob
import time
import shutil
import cx_Oracle
import traceback
import argparse
from datetime import datetime
import cfg.config
from lib.iLogger import set_logger
from lib.openssl import decrypt_string
from lib.damo import scp_enc_file


#############
# CONSTANTS #
#############
INSERT_CNT = 0
ERROR_CNT = 0
CONFIG = {}
DB_CONFIG = {}


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

    def insert_card_rec_meta(self, meta_info_dict):
        global INSERT_CNT
        try:
            query = """
                MERGE INTO
                    CALL_META
                USING
                    DUAL
                ON ( 1=1
                    AND PROJECT_CD = :1
                    AND DOCUMENT_DT = TO_DATE(:2, 'YYYYMMDDHH24MISS')
                    AND DOCUMENT_ID = :3
                )
                WHEN NOT MATCHED THEN
                    INSERT (
                        PROJECT_CD,
                        DOCUMENT_DT,
                        DOCUMENT_ID,
                        CALL_TYPE,
                        AGENT_ID,
                        AGENT_NM,
                        CUSTOMER_ID,
                        CUSTOMER_NM,
                        BRANCH_CD,
                        CALL_DT,
                        START_DTM,
                        END_DTM,
                        DURATION,
                        CHN_TP,
                        REC_ID,
                        STT_PRGST_CD,
                        REGP_CD, 
                        RGST_PGM_ID, 
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM 
                    )
                    VALUES (
                        :4, TO_DATE(:5, 'YYYYMMDDHH24MISS'), :6, :7, :8, :9, :10, :11, :12, TO_DATE(:13, 'YYYY-MM-DD')
                        , TO_DATE(:14, 'YYYYMMDDHH24MISS'), TO_DATE(:15, 'YYYYMMDDHH24MISS'), :16, :17, :18, :19
                        , 'CD', 'CD', SYSDATE, 'CD', 'CD', SYSDATE
                    )
            """
            bind = (
                meta_info_dict.get('PROJECT_CD'),
                meta_info_dict.get('DOCUMENT_DT'),
                meta_info_dict.get('DOCUMENT_ID'),
                meta_info_dict.get('PROJECT_CD'),
                meta_info_dict.get('DOCUMENT_DT'),
                meta_info_dict.get('DOCUMENT_ID'),
                meta_info_dict.get('CALL_TYPE'),
                meta_info_dict.get('AGENT_ID'),
                meta_info_dict.get('AGENT_NM'),
                meta_info_dict.get('CUSTOMER_ID'),
                meta_info_dict.get('CUSTOMER_NM'),
                meta_info_dict.get('BRANCH_CD'),
                meta_info_dict.get('CALL_DT'),
                meta_info_dict.get('START_DTM'),
                meta_info_dict.get('END_DTM'),
                meta_info_dict.get('DURATION'),
                meta_info_dict.get('CHN_TP'),
                meta_info_dict.get('REC_ID'),
                '01',
            )
            self.cursor.execute(query, bind)
            self.conn.commit()
            INSERT_CNT += 1
            return True
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


#######
# def #
#######
def elapsed_time(sdt):
    """
    elapsed times
    :param      sdt:        date object
    :return:                Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdt or len(sdt) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdt[:4]), int(sdt[4:6]), int(sdt[6:8]), int(sdt[8:10]), int(sdt[10:12]), int(sdt[12:14]))
    required_time = end_time - start_time
    return required_time


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
            if db == 'Oracle':
                os.environ['NLS_LANG'] = "Korean_Korea.KO16KSC5601"
                sql = Oracle(logger)
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
    global ERROR_CNT
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_file_name'],
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
    logger.debug('-' * 100)
    logger.debug('Start Insert Card Recording meta information')
    logger.debug('Target directory path : {0}'.format(CONFIG['target_dir_path']))
    wav_file_list = glob.glob('{0}/*.wav'.format(CONFIG['target_dir_path']))
    for wav_file_path in wav_file_list:
        wav_file_name, ext = os.path.splitext(wav_file_path)
        wav_file_name = os.path.basename(wav_file_name)
        meta_info_list = wav_file_name.split('_')
        while True:
            now_time = time.time()
            # FIXME :: 카드사 지점코드는 IN07와 같은 형태인데 kb생명은 7600으로 사용하나 매핑 데이터를 못찾는다고 해서 하드코딩
            branch_cd_mapping_dict = {
                'IN07': '7600',
                'IN13': '7601'
            }
            # FIXME :: 카드사 지점코드는 IN07와 같은 형태인데 kb생명은 7600으로 사용하나 매핑 데이터를 못찾는다고 해서 하드코딩
            new_file_name = '{0}{1}'.format(branch_cd_mapping_dict.get(meta_info_list[6]), datetime.fromtimestamp(now_time).strftime('%Y%m%d%H%M%S%f'))
            # 지점 + 상담원아이디 + 날짜 + 시분초
            rec_id = '{0}{1}{2}'.format(branch_cd_mapping_dict.get(meta_info_list[6]), meta_info_list[2], meta_info_list[0])
            new_file_path = '{0}/{1}/{2}/{3}/{4}{5}'.format(
                CONFIG['output_dir_path'], meta_info_list[0][:4], meta_info_list[0][4:6], meta_info_list[0][6:8],
                new_file_name, ext)
            if not os.path.exists(new_file_path):
                break
        try:
            meta_info_dict = dict()
            meta_info_dict['PROJECT_CD'] = 'CD'
            meta_info_dict['DOCUMENT_DT'] = meta_info_list[0]
            meta_info_dict['DOCUMENT_ID'] = new_file_name
            meta_info_dict['CALL_TYPE'] = '1'
            meta_info_dict['AGENT_ID'] = meta_info_list[2]
            meta_info_dict['AGENT_NM'] = meta_info_list[3]
            meta_info_dict['CUSTOMER_NM'] = meta_info_list[4]
            meta_info_dict['CUSTOMER_ID'] = meta_info_list[5]
            meta_info_dict['BRANCH_CD'] = meta_info_list[6]
            meta_info_dict['CALL_DT'] = '{0}-{1}-{2}'.format(
                meta_info_list[0][:4], meta_info_list[0][4:6], meta_info_list[0][6:8])
            meta_info_dict['START_DTM'] = meta_info_list[0]
            meta_info_dict['END_DTM'] = meta_info_list[1]
            meta_info_dict['DURATION'] = (datetime.strptime(meta_info_list[1], '%Y%m%d%H%M%S') - datetime.strptime(
                meta_info_list[0], '%Y%m%d%H%M%S')).seconds
            meta_info_dict['CHN_TP'] = 'M'
            meta_info_dict['REC_ID'] = rec_id
            new_dir_path = os.path.dirname(new_file_path)
            if not os.path.exists(new_dir_path):
                os.makedirs(new_dir_path)
            oracle.insert_card_rec_meta(meta_info_dict)
            shutil.move(wav_file_path, new_file_path)
            logger.info('success file move {0} -> {1}'.format(wav_file_path, new_file_path))
            wav_file_path = new_file_path
            wav_enc_file_path = '{0}.enc'.format(wav_file_path)
            if 0 != scp_enc_file(new_file_path, wav_enc_file_path):
                logger.error('scp_enc_file ERROR ==> '.format(new_file_path))
                continue
            os.remove(wav_file_path)
            logger.info('INSERT SUCCESS - DOCUMENT_ID : {0}, DOCUMENT_DT : {1}'.format(meta_info_list[0], new_file_name))
        except Exception:
            logger.error(traceback.format_exc())
            logger.error('error file original name is {0}'.format(os.path.basename(wav_file_path)))
            new_file_name = '{0}{1}'.format(new_file_name, ext)
            logger.error('error file new name is {0}'.format(new_file_name))
            error_file_path = '{0}/error_data/{1}'.format(CONFIG['output_dir_path'], new_file_name)
            error_dir_path = os.path.dirname(error_file_path)
            if not os.path.exists(error_dir_path):
                os.makedirs(error_dir_path)
            logger.error('error file is move {0} -> {1}'.format(wav_file_path, error_file_path))
            shutil.move(wav_file_path, error_file_path)
            ERROR_CNT += 1
            continue
    oracle.disconnect()
    logger.info('END.. Start time = {0}, The time required = {1}, INSERT Count = {2}, ERROR Count = {3}'.format(
        st, elapsed_time(dt), INSERT_CNT, ERROR_CNT))
    logger.info('-' * 100)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(config_type):
    """
    Programs that insert data to card rec meta information
    """
    try:
        global CONFIG
        global DB_CONFIG
        CONFIG = cfg.config.CONFIG
        DB_CONFIG = cfg.config.DB_CONFIG[config_type]
        if not os.path.exists(CONFIG['output_dir_path']):
            os.makedirs(CONFIG['output_dir_path'])
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        print(exc_info)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    arguments = parser.parse_args()
    config_type = arguments.config_type
    main(config_type)
