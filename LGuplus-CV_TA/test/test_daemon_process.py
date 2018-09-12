#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-00-00, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import test_ta
import traceback
import multiprocessing
from cfg import test_config
from lib import logger

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
        self.conf = test_config.DaemonConfig
        self.logger = logger.get_timed_rotating_logger(
            logger_name=self.conf.logger_name,
            log_dir_path=self.conf.log_dir_path,
            log_file_name=self.conf.log_file_name,
            backup_count=self.conf.backup_count,
            log_level=self.conf.log_level
        )

    def make_job_list(self):
        if not os.path.isabs(self.conf.target_dir_path):
            err_str = "Target directory path must be absolute path. ({0})".format(self.conf.target_dir_path)
            self.logger.error(err_str)
            raise Exception(err_str)
        if not os.path.exists(self.conf.target_dir_path):
            err_str = "Can't find directory.({0})".format(self.conf.target_dir_path)
            self.logger.error(err_str)
            raise Exception(err_str)
        target_file_list = glob.glob("{0}/*".format(self.conf.target_dir_path))
        sorted_file_list = sorted(target_file_list, key=os.path.getmtime, reverse=False)
        return sorted_file_list

    def run(self):
        self.logger.info('[START] Daemon Process started')
        flag = True
        pid_list = list()
        while flag:
            try:
                job_list = self.make_job_list()
                for pid in pid_list[:]:
                    if not pid.is_alive():
                        pid_list.remove(pid)
                run_count = self.conf.process_max_limit - len(pid_list)
                for _ in range(run_count):
                    for job in job_list:
                        target_file_name = os.path.basename(job)
                        if not os.path.isfile(job):
                            continue
                        if len(pid_list) >= self.conf.process_max_limit:
                            self.logger.info('Processing count is max ....')
                            break
                        if not os.path.exists(self.conf.processed_dir_path):
                            os.makedirs(self.conf.processed_dir_path)
                        if os.path.exists(os.path.join(self.conf.processed_dir_path, target_file_name)):
                            os.remove(os.path.join(self.conf.processed_dir_path, target_file_name))
                        shutil.move(job, self.conf.processed_dir_path)
                        time.sleep(1)
                        p = multiprocessing.Process(target=do_task_pre, args=(target_file_name,))
                        p.daemon = None
                        pid_list.append(p)
                        p.start()
                        self.logger.info('Execute {0} [PID={1}]'.format(target_file_name, p.pid))
                        time.sleep(self.conf.process_interval)
                    job_list = list()
            except KeyboardInterrupt:
                self.logger.info('Daemon stopped by Interrupt')
                flag = False
            except Exception:
                self.logger.error(traceback.format_exc())
        self.logger.info('[E N D] Daemon Process Stopped...')


#######
# def #
#######
def do_task_pre(target_file_name):
    """
    Target
    :param      target_file_name:        Target file name
    """
    reload(test_ta)
    test_ta.main(target_file_name)


#######
# def #
#######
def main():
    """
    This is a program that CV TA process
    """
    try:
        daemon = Daemon()
        daemon.run()
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    main()
