#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-09-14, modification: 2017-12-07"

###########
# imports #
###########
import os
import sys
import time
import shutil
import paramiko
import cx_Oracle
import traceback
import subprocess
import argparse
from datetime import datetime, timedelta
import cfg.config
from lib.iLogger import set_logger
from lib.openssl import decrypt_string


#############
# CONSTANTS #
#############
TRANSPORT_CNT = 0
RE_TRANSPORT_CNT = 0
SUCCESS_CNT = 0
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
        self.cursor = self.conn.cursor()

    def select_file_info(self, file_path):
        meta_info_list = os.path.splitext(os.path.basename(file_path))[0].split('_')
        document_dt = meta_info_list[0]
        agent_id = meta_info_list[2]
        branch_cd = meta_info_list[6]
        end_dtm = meta_info_list[1]
        query = """
            SELECT
                *
            FROM
                CALL_META
            WHERE 1=1
                AND DOCUMENT_DT = TO_DATE(:1, 'YYYYMMDDHH24MISS')
                AND AGENT_ID = :2
                AND BRANCH_CD = :3
                AND END_DTM = TO_DATE(:4, 'YYYYMMDDHH24MISS')
        """
        bind = (
            document_dt,
            agent_id,
            branch_cd,
            end_dtm
        )
        self.cursor.execute(query, bind)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if not result:
            return False
        if len(result) < 1:
            return False
        return True

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
    Delete file
    :param      logger:                 Logger
    :param      delete_file_path:       Input path
    """
    if os.path.exists(delete_file_path):
        try:
            if os.path.isfile(delete_file_path):
                logger.debug('delete file -> {0}'.format(delete_file_path))
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                logger.debug('delete directory -> {0}'.format(delete_file_path))
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def dir_exist_check(logger, sftp, ssh, dir_path):
    """
    Check the directory path
    :param      logger:         Logger
    :param      sftp:           Sftp
    :param      ssh:            Ssh
    :param      dir_path:       Directory path
    """
    try:
        sftp.stat(dir_path)
    except Exception:
        logger.info('directory is not exist -> {0}'.format(dir_path))
        raise Exception('remote directory is not exists')
        '''
        logger.info('make directory -> {0}'.format(dir_path))
        ssh_cmd = 'mkdir {0}'.format(dir_path)
        logger.info('ssh > {0}'.format(ssh_cmd))
        ssh_stdin, ssh_stdout, stderr = ssh.exec_command(ssh_cmd)
        if len(ssh_stdout) > 0:
            logger.debug(ssh_stdout)
        if len(stderr) > 0:
            logger.debug(stderr)
        '''


def exec_cmd(logger, ssh, cmd):
    """
    Exec command
    :param      logger:     Logger
    :param      ssh:        SSH
    :param      cmd:        Command
    :return                 standard out
    """
    logger.info('ssh > {0}'.format(cmd))
    ssh_stdin, ssh_stdout, stderr = ssh.exec_command(cmd)
    if len(ssh_stdout) > 0:
        logger.debug(ssh_stdout)
    if len(stderr) > 0:
        logger.debug(stderr)
    return ssh_stdout


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    """
    logger.info('Command -> {0}'.format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for subprocess to finish
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


def download_dir(logger, ssh, sftp, remote_dir, local_dir, oracle):
    """
    Download directory
    :param      logger:         Logger
    :param      ssh:            SSH
    :param      sftp:           SFTP
    :param      remote_dir:     Remote directory
    :param      local_dir:      Local directory
    :param      oracle:         DB
    """
    global TRANSPORT_CNT
    global RE_TRANSPORT_CNT
    global SUCCESS_CNT
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    sftp.chdir(remote_dir)
    # dir_items = sftp.listdir()
    dir_items = sftp.listdir_attr(remote_dir)
    for item in dir_items:
        filename = item.filename.encode('euc-kr')
        remote_path = '{0}/{1}'.format(remote_dir, filename)
        local_path = os.path.join(local_dir, filename)
        if os.path.isdir(remote_path):
            download_dir(logger, ssh, sftp, remote_path, local_path, oracle)
        else:
            #logger.debug('target path is {0}'.format(remote_path))
            ext = os.path.splitext(remote_path)[1]
            #print logger.debug('ext is {0}'.format(ext))
            if ext != '.wav':
                continue
            file_info = sftp.stat(remote_path)
            remote_file_size = file_info.st_size
            if os.path.exists('{0}.tmp'.format(local_path)):
                local_file_size = os.stat('{0}.tmp'.format(local_path)).st_size
                if remote_file_size == local_file_size:
                    shutil.move('{0}.tmp'.format(local_path), local_path)
                    logger.info('SUCCESS : {0}'.format(local_path))
                    SUCCESS_CNT += 1
                else:
                    del_garbage(logger, local_path)
                    sftp.get(remote_path, '{0}.tmp'.format(local_path))
                    logger.info('RE TRANSPORT : {0}'.format(remote_path))
                    RE_TRANSPORT_CNT += 1
            else:
                if os.path.exists(local_path):
                    continue
                elif oracle.select_file_info(remote_path):
                    continue
                else:
                    sftp.get(remote_path, '{0}.tmp'.format(local_path))
                    logger.info('TRANSPORT : {0}'.format(remote_path))
                    TRANSPORT_CNT += 1


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
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    # Add logging
    logger_args = {
        'base_path': SFTP_CONFIG['log_dir_path'],
        'log_file_name': SFTP_CONFIG['log_file_name'],
        'log_level': SFTP_CONFIG['log_level']
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
    logger.debug('Start Get Card Recording using sftp')
    logger.debug('Target host -> {0}'.format(SFTP_CONFIG['host']))
    # paramiko setting
    host = SFTP_CONFIG['host']
    username = SFTP_CONFIG['username']
    password = SFTP_CONFIG['passwd']
    logger.debug('1. Paramiko setting')
    logger.debug(' host : {0}   username : {1}'.format(host, username))
    # ssh & sftp connect using ssh_key
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_host_keys(os.path.expanduser(os.path.join('~', '.ssh', 'known_hosts')))
        # ssh.connect(host, username=username, allow_agent=True, look_for_keys=True, timeout=10)
        ssh.connect(host, username=username, password=password, timeout=10)
        logger.debug('ssh connect Success')
        sftp = ssh.open_sftp()
        logger.debug('sftp connect Success')
        # Get directory
        remote_dir_path = SFTP_CONFIG['remote_dir_path']
        output_dir_path = SFTP_CONFIG['output_dir_path']
        logger.debug('remote directory path : {0}'.format(remote_dir_path))
        dir_exist_check(logger, sftp, ssh, remote_dir_path)
        download_dir(logger, ssh, sftp, remote_dir_path, output_dir_path, oracle)
        # Close
        ssh.close()
        sftp.close()
        # remove logger
        logger.info('END.. Start time = {0}, The time required = {1}, Transport Count = {2}, Re Transport Count = {3}, SUCCESS Count = {4}'.format(
            st, elapsed_time(dt), TRANSPORT_CNT, RE_TRANSPORT_CNT, SUCCESS_CNT))
        logger.info('-' * 100)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        raise Exception(exc_info)
    oracle.disconnect()
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(config_type):
    """
    Programs that transfer all card recording files in a directory using sftp
    """
    try:
        global SFTP_CONFIG
        global DB_CONFIG
        SFTP_CONFIG = cfg.config.SFTP_CONFIG
        DB_CONFIG = cfg.config.DB_CONFIG[config_type]
        if not os.path.exists(SFTP_CONFIG['output_dir_path']):
            os.makedirs(SFTP_CONFIG['output_dir_path'])
        processing()
    except Exception:
        exc_info = traceback.format_exc()
        ts = time.time()
        error_time = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
        print '-----    ERROR   -----'
        print error_time
        print exc_info
        print '----------------------'
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ct', action='store', dest='config_type', type=str, help='dev or uat or prd'
                        , required=True, choices=['dev', 'uat', 'prd'])
    arguments = parser.parse_args()
    config_type = arguments.config_type
    main(config_type)
