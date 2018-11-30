#!/usr/bin/python
# -*- coding: utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-10-15, modification: 0000-00-00"

###########
# imports #
###########
import sys
import time
import socket
import traceback
from datetime import datetime
from subprocess import check_output
from cfg import config
from lib import logger, util, db_connection

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#######
# def #
#######
def elapsed_time(start_time):
    """
    elapsed time
    :param          start_time:          date object
    :return                              Required time (type : datetime)
    """
    end_time = datetime.fromtimestamp(time.time())
    required_time = end_time - start_time
    return required_time


def main():
    """
    This program that check process is alive
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts)
    conf = config.TAMNTConfig
    # Add logging
    log = logger.get_timed_rotating_logger(
        logger_name=conf.logger_name,
        log_dir_path=conf.log_dir_path,
        log_file_name=conf.log_file_name,
        backup_count=conf.backup_count,
        log_level=conf.log_level
    )
    log.info("-" * 100)
    log.info("Start TA process monitoring")
    host_nm = socket.gethostname()
    oracle = db_connection.Oracle(config.OracleConfig, failover=True, service_name=True)
    check_process_list = [
        ('collector_process.py', 'COLLECTOR'),
        ('daemon_process.py', 'TA'),
        ('brain-hmd', 'HMD'),
        ('brain-nlp', 'NLP'),
    ]
    for process_name, proc_nm in check_process_list:
        try:
            log.info('Check process [{0}]'.format(process_name))
            process_list = check_output('ps -edf | grep {0}'.format(process_name), shell=True).split('\n')
            if len(process_list) >= 4:
                log.info('--> Normal operation')
                util.merge_into_process_status(
                    log=log,
                    oracle=oracle,
                    host_nm=host_nm,
                    proc_nm=proc_nm,
                    status='1'
                )
            else:
                log.info('--> Abnormal operation')
                util.merge_into_process_status(
                    log=log,
                    oracle=oracle,
                    host_nm=host_nm,
                    proc_nm=proc_nm,
                    status='0'
                )
        except Exception:
            exc_info = traceback.format_exc()
            log.error(exc_info)
            continue
    oracle.disconnect()
    log.info("END.. Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
    for handler in log.handlers:
        handler.close()
        log.removeHandler(handler)


if __name__ == '__main__':
    main()
