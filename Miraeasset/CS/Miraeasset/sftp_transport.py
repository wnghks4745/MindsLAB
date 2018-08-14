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
import traceback
import subprocess
from datetime import datetime
from cfg.config import SFTP_CONFIG
from lib.iLogger import set_logger

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
        logger.info('make directory -> {0}'.format(dir_path))
        ssh_cmd = 'mkdir {0}'.format(dir_path)
        logger.info('ssh > {0}'.format(ssh_cmd))
        ssh_stdin, ssh_stdout, stderr = ssh.exec_command(ssh_cmd)
        if len(ssh_stdout) > 0:
            logger.debug(ssh_stdout)
        if len(stderr) > 0:
            logger.debug(stderr)


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


def file_compression(logger, target_dir_path):
    """
    Directory compression
    :param      logger:             Logger
    :param      target_dir_path     Target directory path
    """
    logger.debug('File compression')
    # Make sub directory list
    sftp_target_dir_name_list = list()
    w_ob = os.walk(target_dir_path)
    for dir_path, sub_dir, files in w_ob:
        sftp_target_dir_name_list = sub_dir
        break
    # loop of sub directory
    for sftp_target_dir_name in sftp_target_dir_name_list:
        sftp_target_dir_path = '{0}/{1}'.format(target_dir_path, sftp_target_dir_name)
        os.chdir(sftp_target_dir_path)
        extension = os.path.splitext(sftp_target_dir_path)[1]
        if extension == '.tmp' or extension == '.zip':
            continue
        logger.info('Directory compression -> {0}'.format(sftp_target_dir_path))
        sftp_target_dir_name = os.path.basename(sftp_target_dir_path)
        sftp_zip_name = '{0}.zip'.format(sftp_target_dir_name)
        zip_cmd = 'zip -r ../{0} *'.format(sftp_zip_name)
        sub_process(logger, zip_cmd)
        del_garbage(logger, sftp_target_dir_path)


def processing(target_dir_path):
    """
    Processing
    :param      target_dir_path:        Target directory path
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    transport_cnt = 0
    # Add logging
    logger_args = {
        'base_path': SFTP_CONFIG['log_dir_path'],
        'log_file_name': SFTP_CONFIG['log_file_name'],
        'log_level': SFTP_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.debug('-' * 100)
    logger.debug('Start uploading the database upload text file using sftp')
    logger.debug('Target directory path -> {0}'.format(target_dir_path))
    # paramiko setting
    host = SFTP_CONFIG['host']
    username = SFTP_CONFIG['username']
    logger.debug('1. Paramiko setting')
    logger.debug(' host : {0}   username : {1}'.format(host, username))
    # ssh & sftp connect using ssh_key
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.load_host_keys(os.path.expanduser(os.path.join('~', '.ssh', 'known_hosts')))
        ssh.connect(host, username=username, allow_agent=True, look_for_keys=True, timeout=10)
        logger.debug('ssh connect Success')
        sftp = ssh.open_sftp()
        logger.debug('sftp connect Success')
        # Create tar file
        file_compression(logger, target_dir_path)
        # process_check
        process_check = False
        # check file name and transport
        w_ob = os.walk(target_dir_path)
        for dir_path, sub_dirs, files in w_ob:
            # sftp transport
            for file_name in files:
                extension = os.path.splitext(file_name)[1]
                if extension == '.zip':
                    process_check = True
                    file_path = '{0}/{1}'.format(target_dir_path, file_name)
                    remote_dir_path = SFTP_CONFIG['remote_dir_path']
                    # file exist check
                    dir_exist_check(logger, sftp, ssh, remote_dir_path)
                    remote_file_path = '{0}/{1}'.format(remote_dir_path, file_name)
                    # file transport
                    logger.info('sftp > {0} >>> {1}.tmp'.format(file_path, remote_file_path))
                    try:
                        sftp.put(file_path, remote_file_path + '.tmp')
                        logger.info('sftp transport success')
                        print 'sftp transport success'
                        transport_cnt += 1
                    except Exception:
                        logger.error('sftp transport fail')
                        print 'sftp transport fail'
                        continue
                    # file rename
                    # check the file exist
                    try:
                        # rename target name이 존재하는지 check
                        sftp.stat(remote_file_path)
                        sftp.remove(remote_file_path)
                        sftp.rename(remote_file_path + '.tmp', remote_file_path)
                    except Exception:
                        sftp.rename(remote_file_path + '.tmp', remote_file_path)
                    logger.info('{0}.tmp -> {0}'.format(remote_file_path))
                    # file delete
                    del_garbage(logger, file_path)
        # Close
        ssh.close()
        sftp.close()
        # remove logger
        if process_check:
            logger.info('END.. Start time = {0}, The time required = {1}, Count = {2}'.format(
                st, elapsed_time(dt), transport_cnt))
            logger.info('-' * 100)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        raise Exception
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main():
    """
    Programs that transfer all db_upload_txt files in a directory using sftp
    """
    try:
        if os.path.exists(SFTP_CONFIG['target_dir_path']):
            w_ob = os.walk(SFTP_CONFIG['target_dir_path'])
            dir_list = list()
            for dir_path, sub_dirs, files in w_ob:
                dir_list = sub_dirs
                break
            tmp_cnt = 0
            if len(dir_list) > 0:
                for dir_name in dir_list:
                    if dir_name.endswith('.tmp'):
                        tmp_cnt += 1
                if len(dir_list) != tmp_cnt:
                    processing(SFTP_CONFIG['target_dir_path'])
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
    main()