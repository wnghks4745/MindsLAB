#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-05, modification: 2017-12-11"

###########
# imports #
###########
import os
import sys
import time
import shutil
import pymssql
import traceback
from datetime import datetime, timedelta
from cfg.config import MSSQL_DB_CONFIG, DELETE_CONFIG
from lib.iLogger import set_logger

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DT = ''
ST = ''


#########
# class #
#########
class MSSQL(object):
    def __init__(self):
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
        self.cursor = self.conn.cursor(as_dict=True)

    def select_rcdg_file_nm_and_rec_stdt(self, date, r_comp_type):
        """
        Select rcdg_file_nm
        :param      date:               Select date
        :param      r_comp_type:        r_comp_type
        :return:                        Recording file name and Recording start date dictionary list
        """
        sql = """
            SELECT
                r_file_nm
            FROM
                DBO.VREC_STT_INFO WITH(NOLOCK)
            WHERE 1=1
                AND R_FILE_NM like %s
                AND R_COMP_TYPE = %s
        """
        bind = ('{0}%'.format(date), r_comp_type, )
        self.cursor.execute(sql, bind)
        rows = self.cursor.fetchall()
        if rows is bool or not rows:
            return list()
        target_list = list()
        for row in rows:
            item = dict()
            item['RCDG_FILE_NM'] = row['r_file_nm']
            item['REC_STDT'] = '{0}-{1}-{2}'.format(row['r_file_nm'][:4], row['r_file_nm'][4:6], row['r_file_nm'][6:8])
            target_list.append(item)
        return target_list

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


def mssql_connect(logger):
    """
    Trying Connect to MsSQL
    :param      logger:     LOGGER
    :return:                MsSQL
    """
    mssql = False
    logger.info('1. MsSQL setting')
    logger.info('\thost : {0}'.format(MSSQL_DB_CONFIG['host']))
    logger.info('\tport : {0}'.format(MSSQL_DB_CONFIG['port']))
    logger.info('\tuser : {0}'.format(MSSQL_DB_CONFIG['user']))
    logger.info('\tpassword : {0}'.format(MSSQL_DB_CONFIG['password']))
    for cnt in range(1, 4):
        try:
            mssql = MSSQL()
            break
        except Exception as e:
            print e
            if cnt < 3:
                logger.error('Fail connect MsSQL, retrying count = {0}'.format(cnt))
            continue
    if not mssql:
        raise Exception("Fail connect MsSQL")
    logger.info('=` Success connect MsSQL =')
    return mssql


def processing(start_date, end_date):
    """
    DELETE processing
    :param      start_date:     SELECT start date
    :param      end_date:       SELECT end date
    """
    # Add logging
    logger_args = {
        'base_path': DELETE_CONFIG['log_dir_path'],
        'log_file_name': DELETE_CONFIG['log_name'],
        'log_level': DELETE_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info('START.. DELETE CS file')
    try:
        # 1. MySQL connect
        mssql = mssql_connect(logger)
        # 2. Select rcdg_file & rec_stdt
        logger.info('2. Select RCDG_FILE_NM & REC_STDT')
        start_datetime = datetime.strptime(start_date, '%Y%m%d')
        end_datetime = datetime.strptime(end_date, '%Y%m%d')
        if start_datetime > end_datetime:
            temp_date = start_date
            start_date = end_date
            end_date = temp_date
        select_date = start_date
        target_list = list()
        while True:
            target_list += mssql.select_rcdg_file_nm_and_rec_stdt(select_date, DELETE_CONFIG['r_comp_type'])
            if select_date == end_date:
                break
            select_date = (datetime.strptime(select_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        mssql.disconnect()
        logger.info('= Success Select RCDG_FILE_NM & REC_STDT = ')
        # 3. FIND & DELETE RCDG_FILE
        logger.info('3. FIND & DELETE RCDG_FILE')
        delete_cnt = 0
        for target in target_list:
            rec_stdt = str(target['REC_STDT'])
            target_path = '{0}/{1}'.format(DELETE_CONFIG['rec_path'], rec_stdt[:4] + rec_stdt[5:7] + rec_stdt[8:10])
            delete_path_list = list()
            delete_path_list.append('{0}/{1}.tx.enc'.format(target_path, target['RCDG_FILE_NM']))
            delete_path_list.append('{0}/{1}.rx.enc'.format(target_path, target['RCDG_FILE_NM']))
            delete_path_list.append('{0}/comp_{1}_tx.wav.enc'.format(
                target_path, target['RCDG_FILE_NM'].replace('.', '_')))
            delete_path_list.append('{0}/comp_{1}_rx.wav.enc'.format(
                target_path, target['RCDG_FILE_NM'].replace('.', '_')))
            target_path = '{0}/incident_file'.format(DELETE_CONFIG['rec_path'])
            delete_path_list.append('{0}/{1}.tx.enc'.format(target_path, target['RCDG_FILE_NM']))
            delete_path_list.append('{0}/{1}.rx.enc'.format(target_path, target['RCDG_FILE_NM']))
            delete_path_list.append('{0}/comp_{1}_tx.wav.enc'.format(
                target_path, target['RCDG_FILE_NM'].replace('.', '_')))
            delete_path_list.append('{0}/comp_{1}_rx.wav.enc'.format(
                target_path, target['RCDG_FILE_NM'].replace('.', '_')))
            for delete_path in delete_path_list:
                if os.path.exists(delete_path):
                    logger.info('\tFIND RCDG_FILE! -> {0}'.format(delete_path))
                    logger.info('\tDELETE RCDG_FILE')
                    del_garbage(logger, delete_path)
                    delete_cnt += 1
        logger.info('= Success FIND & DELETE RCDG_FILE =')
        logger.info('END..\tStart time = {0}, The time required= {1}, Count = {2}'.format(
            ST, elapsed_time(DT), delete_cnt))
        logger.info('-'*100)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(start_date, end_date):
    """
    This is a program that delete CS file
    :param      start_date:     select Start date
    :param      end_date:       select End date
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(start_date, end_date)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info


if __name__ == '__main__':
    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        print 'usage : python {0} [start_date] [end_date]'.format(sys.argv[0])
        print 'usage : ex) python {0} 20171201 20180101'.format(sys.argv[0])
