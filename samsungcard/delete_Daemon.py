#!/usr/bin/python
# -*- coding:utf-8

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation:2018-12-19, modification: 2018-12-19"

###########
# imports #
###########
import sys
import traceback
import multiprocessing
from cfg import config
from lib import logger
from datetime import datetime

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
        self.conf = config.DELDaemonConfig
        self.logger = logger.get_timed_rotating_logger(
            logger_name=self.conf.logger_name,
            log_dir_path=self.conf.log_dir_path,
            log_file_name=self.conf.log_file_name,
            backup_count=self.conf.backup_count,
            log_level=self.conf.log_level
        )

    def run(self):
        try:
            self.logger.info('[START] Daemon process started')
            pid_list = list()
            day = -1
            while True:
                current_day = datetime.now().day
                if current_day == day:
                    continue
                day = current_day
                p = multiprocessing.Process(target=do_task)
                pid_list.append(p)
                p.start()
        except KeyboardInterrupt:
            self.logger.info('Daemon stopped by interrupt')
        except Exception:
            self.logger.info(traceback.format_exc())
        finally:
            self.logger.info('[E N D] Daemon process stopped')


class args(object):
    dir_path = False
    mtn_period = False


#######
# def #
#######
def do_task():
    """
    Process execute STT get
    """
    import delete_file
    reload(delete_file)
    argu = args()
    delete_file.main(argu)


########
# main #
########
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
