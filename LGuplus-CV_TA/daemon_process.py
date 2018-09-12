#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-24, modification: 0000-00-00"

###########
# imports #
###########
import sys
import time
import socket
import traceback
import multiprocessing
from cfg import config
from lib import logger, util

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#########
# class #
#########
class Daemon(object):
    def __init__(self):
        self.conf = config.DaemonConfig
        self.logger = logger.get_timed_rotating_logger(
            logger_name=self.conf.logger_name,
            log_dir_path=self.conf.log_dir_path,
            log_file_name=self.conf.log_file_name,
            backup_count=self.conf.backup_count,
            log_level=self.conf.log_level
        )

    def make_job_list(self):
        result = util.select_ta_target(self.logger, socket.gethostname())
        if not result:
            return list()
        return result[:self.conf.process_max_limit]

    def run(self):
        try:
            self.logger.info('[START] Daemon process started')
            pid_list = list()
            while True:
                try:
                    job_list = self.make_job_list()
                except Exception:
                    self.logger.error(traceback.format_exc())
                    time.sleep(10)
                    continue
                for pid in pid_list[:]:
                    if not pid.is_alive():
                        pid_list.remove(pid)
                for job in job_list:
                    if len(pid_list) >= self.conf.process_max_limit:
                        self.logger.info('Processing Count is MAX....')
                        break
                    p = multiprocessing.Process(target=do_task, args=(job,))
                    p.daemon = None
                    pid_list.append(p)
                    p.start()
                    # Job -> (rest_send_key, start_date, start_time, file_name, svc_type)
                    rest_send_key, start_date, start_time, file_name, svc_type = job
                    # Update status
                    util.update_status(self.logger, '1', rest_send_key)
                    log_str = 'Execute rest_send_key = {0},'.format(rest_send_key)
                    log_str += ' start_date = {0},'.format(start_date)
                    log_str += ' start_time = {0},'.format(start_time)
                    log_str += ' file_name = {0},'.format(file_name)
                    log_str += ' svc_type = {0} [PID={1}]'.format(svc_type, p.pid)
                    self.logger.info(log_str)
                    time.sleep(self.conf.process_interval)
        except KeyboardInterrupt:
            self.logger.info('Daemon stopped by interrupt')
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            self.logger.info('[E N D] Daemon process stopped')


#######
# def #
#######
def do_task(job):
    """
    Process execute TA
    :param          job:        Job
    """
    import ta
    reload(ta)
    ta.main(job)


#######
# def #
#######
def main():
    """
    This is a program that TA Daemon process
    """
    try:
        daemon_process = Daemon()
        daemon_process.run()
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    main()
