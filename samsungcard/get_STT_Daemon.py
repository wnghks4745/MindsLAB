#!/user/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__data__ = "creation: 0000-00-00, modification: 0000-00-00"

###########
# imports #
###########
import sys
import time
import signal
import traceback
import multiprocessing
from cfg import config
from lib import logger, util

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


##########
# class #
##########
class Daemon(object):
    def __init__(self):
        self.conf = config.STTDaemonConfig
        self.logger = logger.get_timed_rotating_logger(
            logger_name=self.conf.logger_name,
            log_dir_path=self.conf.log_dir_path,
            log_file_name=self.conf.log_file_name,
            backup_count=self.conf.backup_count,
            log_level=self.conf.log_level
        )

    def run(self):
        target_list = list()
        oracle_dev = object()
        try:
            self.set_sig_handler()
            self.logger.info('[START] Daemon process started')
            pid_list = list()
            try:
                oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev')
            except Exception:
                oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev', retry=True)
            while True:
                for pid in pid_list[:]:
                    if not pid.is_alive():
                        pid_list.remove(pid)
                if len(pid_list) > self.conf.process_max_limit:
                    self.logger.debug('Processing Count is MAX....')
                    time.sleep(self.conf.process_interval)
                    continue
                target_list = util.select_stt_target(self.logger, oracle_dev, cfg_dev)
                count = len(pid_list)
                for i in range(self.conf.process_max_limit - count):
                    if len(target_list) == 0:
                        break
                    job = target_list[:320]
                    target_list = target_list[320:]
                    p = multiprocessing.Process(target=do_task, args=(count, job))
                    pid_list.append(p)
                    p.start()
                    time.sleep(self.conf.process_interval)
                for info_dict in target_list:
                    util.update_stt_status(self.logger, info_dict, '01', oracle_dev)
        except KeyboardInterrupt:
            self.logger.info('Daemon stopped by interrupt')
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            for info_dict in target_list:
                util.update_stt_status(self.logger, info_dict, '01', oracle_dev)
            self.logger.info('[E N D] Daemon process stopped')

    def signal_handler(self, sig, frame):
        """
        Signal handler
        :param      sig:
        :param      frame:
        """
        if sig == signal.SIGHUP:
            return
        if sig == signal.SIGTERM or sig == signal.SIGINT:
            logger = self.logger
            logger.info('Daemon stop')
            sys.exit(0)

    def set_sig_handler(self):
        """
        Set sig handler
        """
        signal.signal(signal.SIGTSTP, signal.SIG_IGN)
        signal.signal(signal.SIGTTOU, signal.SIG_IGN)
        signal.signal(signal.SIGTTIN, signal.SIG_IGN)
        signal.signal(signal.SIGQUIT, signal.SIG_IGN)
        signal.signal(signal.SIGHUP, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)


#######
# def #
#######
def do_task(count, job):
    """
    Process execute STT get
    :param      count:      Count
    :param      job:        Job
    """
    import get_stt_rst_data
    reload(get_stt_rst_data)
    get_stt_rst_data.main(count, job)


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
