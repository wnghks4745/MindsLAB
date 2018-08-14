#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-03-06, modification: 2018-03-07"

###########
# imports #
###########
import os
import sys
import time
import shutil
import traceback
import cx_Oracle
import collections
from datetime import datetime
from cfg.config import CONFIG, ORACLE_DB_CONFIG
from lib.iLogger import set_logger

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
UPSERT_CNT = 0
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

    def upsert_data_to_pasu0001(self, **kwargs):
        try:
            query = """
                MERGE INTO 
                    GROUP_INFO_PASU0001_TB
                USING 
                    DUAL
                ON  ( 1=1
                    AND USER_ID = :1
                    )
                WHEN MATCHED THEN
                    UPDATE SET
                        USERNAME = :2,
                        USEREMAIL = :3,
                        USERPHONE = :4,
                        DEPTCODE = :5,
                        POSITONCODE = :6,
                        ROLECODE = :7,
                        STATUS = :8,
                        JIKCHECK_YN = :9,
                        JIKWE_CD = :10,
                        ALIMI_CD = :11,
                        OFFICEPHONE = :12,
                        MSN_USE_YN = :13,
                        ENTER_DATE = :14,
                        MANAGE_YN = :15,
                        UPDATED_DTM = SYSDATE,
                        UPDATOR_ID = :16
                WHEN NOT MATCHED THEN
                    INSERT (
                        USER_ID,
                        USERNAME,
                        USEREMAIL,
                        USERPHONE,
                        DEPTCODE,
                        POSITONCODE,
                        ROLECODE,
                        STATUS,
                        JIKCHECK_YN,
                        JIKWE_CD,
                        ALIMI_CD,
                        OFFICEPHONE,
                        MSN_USE_YN,
                        ENTER_DATE,
                        MANAGE_YN,
                        CREATED_DTM,
                        UPDATED_DTM,
                        CREATOR_ID,
                        UPDATOR_ID
                    )
                    VALUES (
                        :1, :2, :3, :4, :5,
                        :6, :7, :8, :9, :10,
                        :11, :12, :13, :14, :15,
                        SYSDATE, SYSDATE, :16, :16
                    )
            """
            bind = (
                kwargs.get('user_id'),
                kwargs.get('username'),
                kwargs.get('useremail'),
                kwargs.get('userphone'),
                kwargs.get('deptcode'),
                kwargs.get('positoncode'),
                kwargs.get('rolecode'),
                kwargs.get('status'),
                kwargs.get('jikcheck_yn'),
                kwargs.get('jikwe_cd'),
                kwargs.get('alimi_cd'),
                kwargs.get('officephone'),
                kwargs.get('msn_use_yn'),
                kwargs.get('enter_date'),
                kwargs.get('manage_yn'),
                kwargs.get('regp_cd'),
            )
            self.cursor.execute(query, bind)
            self.conn.commit()
            if self.cursor.rowcount > 0:
                return True
            else:
                self.conn.rollback()
                return False
        except Exception:
            self.conn.rollback()
            raise Exception(traceback.format_exc())

    def upsert_data_to_pasu0008(self, **kwargs):
        try:
            query = """
                MERGE INTO 
                    GROUP_INFO_PASU0008_TB
                USING 
                    DUAL
                ON  ( 1=1
                    AND DEPTCODE = :1
                    )
                WHEN MATCHED THEN
                    UPDATE SET
                        DEPTNAME = :2,
                        PARENTCODE = :3,
                        MANAGERID = :4,
                        DEPTORDER = :5,
                        CLOSE_YN = :6,
                        OPEN_DATE = :7,
                        CLOSE_DATE = :8,
                        UPDATED_DTM = SYSDATE,
                        UPDATOR_ID = :9
                WHEN NOT MATCHED THEN
                    INSERT (
                        DEPTCODE,
                        DEPTNAME,
                        PARENTCODE,
                        MANAGERID,
                        DEPTORDER,
                        CLOSE_YN,
                        OPEN_DATE,
                        CLOSE_DATE,
                        CREATED_DTM,
                        UPDATED_DTM,
                        CREATOR_ID,
                        UPDATOR_ID
                    )
                    VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, SYSDATE, SYSDATE, :9, :9
                    )
            """
            bind = (
                kwargs.get('deptcode'),
                kwargs.get('deptname'),
                kwargs.get('parentcode'),
                kwargs.get('managerid'),
                kwargs.get('deptorder'),
                kwargs.get('close_yn'),
                kwargs.get('open_date'),
                kwargs.get('close_date'),
                kwargs.get('regp_cd'),
            )
            self.cursor.execute(query, bind)
            self.conn.commit()
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


def error_process(logger, file_path):
    """
    Error file process
    :param      logger:         Logger
    :param      file_path:      Org file path
    """
    global ERROR_CNT
    ERROR_CNT += 1
    try:
        logger.error("Error process. [file = {0}]".format(file_path))
        error_dir_path = '{0}/error_data/{1}/{2}/{3}'.format(os.path.dirname(file_path), DT[:4], DT[4:6], DT[6:8])
        if not os.path.exists(error_dir_path):
            os.makedirs(error_dir_path)
        error_org_file_path = '{0}/{1}'.format(error_dir_path, os.path.basename(file_path))
        if os.path.exists(error_org_file_path):
            del_garbage(logger, error_org_file_path)
        logger.error('Error org file move {0} -> {1}'.format(file_path, error_dir_path))
        shutil.move(file_path, error_dir_path)
    except Exception:
        exc_info = traceback.format_exc()
        logger.critical("Critical error {0}".format(exc_info))


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


def upload_data_to_db(logger, oracle, org_file_dict):
    """
    Upload data to database
    :param      logger:             Logger
    :param      oracle:             Oracle
    :param      org_file_dict:      Organization file dictionary
    """
    global UPSERT_CNT
    logger.debug("ORG data upload to DB")
    for table_name, file_path in org_file_dict.items():
        try:
            logger.debug('Open file. [{0}]'.format(file_path))
            org_file = open(file_path, 'r')
            if table_name == 'PASU0001':
                for line in org_file:
                    line_list = line.split('|')
                    user_id = line_list[0].strip()
                    logger.debug('UPSERT {0} -> USER_ID {1}'.format(table_name, user_id))
                    oracle.upsert_data_to_pasu0001(
                        user_id=user_id,                    # 사용자코드
                        username=line_list[1].strip(),      # 사용자명
                        useremail=line_list[2].strip(),     # 메일주소
                        userphone=line_list[3].strip(),     # 전화번호
                        deptcode=line_list[4].strip(),      # 부서코드
                        positoncode=line_list[5].strip(),   # 직위코드
                        rolecode=line_list[6].strip(),      # 역할코드
                        status=line_list[7].strip(),        # 재직여부
                        jikcheck_yn=line_list[8].strip(),   # 직책여부
                        jikwe_cd=line_list[9].strip(),      # 직위코드
                        alimi_cd=line_list[10].strip(),     # 알리미코드
                        officephone=line_list[11].strip(),  # 사무실번호
                        msn_use_yn=line_list[12].strip(),   # 메신저사용여부
                        enter_date=line_list[13].strip(),   # 입사일자
                        manage_yn=line_list[14].strip(),    # 부서장여부
                        regp_cd='ORG_BAT'
                    )
            elif table_name == 'PASU0008':
                for line in org_file:
                    line_list = line.split('|')
                    deptcode = line_list[0].strip()
                    logger.debug('UPSERT {0} -> DEPTCODE {1}'.format(table_name, deptcode))
                    oracle.upsert_data_to_pasu0008(
                        deptcode=deptcode,                  # 부서코드
                        deptname=line_list[1].strip(),      # 부서명
                        parentcode=line_list[2].strip(),    # 상위소속코드
                        managerid=line_list[3].strip(),     # 관리자사번
                        deptorder=line_list[4].strip(),     # 순서
                        close_yn=line_list[5].strip(),
                        open_date=line_list[6].strip(),     # 오픈일자
                        close_date=line_list[7].strip(),     # 폐쇄일자
                        regp_cd='ORG_BAT'
                    )
            else:
                logger.error('Table name is not exists -> {0}'.format(table_name))
            org_file.close()
            org_output_dir_path = '{0}/{1}/{2}/{3}'.format(CONFIG['org_output_path'], DT[:4], DT[4:6], DT[6:8])
            org_output_file_path = os.path.join(org_output_dir_path, os.path.basename(file_path))
            if not os.path.exists(org_output_dir_path):
                os.makedirs(org_output_dir_path)
            if os.path.exists(org_output_file_path):
                del_garbage(logger, org_output_file_path)
            logger.debug("Move org file to output directory")
            shutil.move(file_path, org_output_dir_path)
            oracle.conn.commit()
            UPSERT_CNT += 1
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            oracle.conn.rollback()
            error_process(logger, file_path)
            continue


def make_file_dict(logger):
    """
    Make file path dict
    :param      logger:     Logger
    :return:                file path dict
    """
    logger.debug('Make file list')
    target_file_dict = CONFIG['org_file_path_list']
    logger.debug("Target file list = {0}".format(", ".join(target_file_dict.keys())))
    # 파일 존재 유무 확인
    output_file_dict = dict()
    for target_table, target_file_path in target_file_dict.items():
        if os.path.exists(target_file_path):
            output_file_dict[target_table] = target_file_path
        else:
            logger.error('Path is not exist -> {0}'.format(target_file_path))
    return output_file_dict


def connect_db(logger, db):
    """
    Connect database
    :param      logger:     Logger
    :param      db:         Database
    :return:                SQL Object
    """
    # Connect DB
    logger.debug('Connect {0} DB ...'.format(db))
    sql = False
    for cnt in range(1, 4):
        try:
            if db == 'Oracle':
                os.environ["NLS_LANG"] = ".KO16MSWIN949"
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
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': CONFIG['log_name'],
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    # connect db
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
        # Make org file dict
        org_file_dict = make_file_dict(logger)
        # Upload org data to db
        upload_data_to_db(logger, oracle, org_file_dict)
        logger.info(
            "Total ORG target count = {0}, upsert count = {1}, error count = {2}, The time required = {3}".format(
                len(org_file_dict), UPSERT_CNT, ERROR_CNT, elapsed_time(DT)))
        logger.debug("END.. Start time = {0}, The time required = {1}".format(ST, elapsed_time(DT)))
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
    This is a program that update organization data to Oracle DB
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
