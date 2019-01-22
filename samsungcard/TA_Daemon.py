#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation:2018-12-11, modification: 2019-01-02"

###########
# imports #
###########
import sys
import time
import signal
import traceback
import multiprocessing
from cfg import config
from lib import logger, util, summary

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
        self.conf = config.TADaemonConfig
        self.logger = logger.get_timed_rotating_logger(
            logger_name=self.conf.logger_name,
            log_dir_path=self.conf.log_dir_path,
            log_file_name=self.conf.log_file_name,
            backup_count=self.conf.backup_count,
            log_level=self.conf.log_level
        )

    def make_job_list(self, oracle, cfg):
        oracle, result = util.select_ta_target(self.logger, 100, oracle, cfg)
        if not result:
            return oracle, list()
        return oracle, result[:self.conf.process_max_limit]

    def run(self):
        try:
            self.logger.info('[START] Daemon process started')
            self.set_sig_handler()
            pid_list = list()
            day = 40
            day_flag = False
            try:
                oracle, cfg = util.db_connect(user='mlsta_dev')
            except Exception:
                oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
            while True:
                try:
                    result = util.select_oracle_server_days(self.logger, oracle, cfg)
                    current_day = int(result[0])
                    current_time = int(result[1])
                    if day != 40 and current_day != day and current_time == 9:
                        day_flag = True
                        # call_info 상태 변경
                        util.update_stt_status_ready_to_start(self.logger, oracle, cfg)
                        # offer_info 상태 변경
                        util.update_ta_status_cd_ready_to_start(self.logger, oracle, cfg)
                    oracle, job_list = self.make_job_list(oracle, cfg)
                except Exception:
                    self.logger.error(traceback.format_exc())
                    time.sleep(10)
                    continue
                for pid in pid_list[:]:
                    if not pid.is_alive():
                        pid_list.remove(pid)
                flag = False
                for job in job_list:
                    if len(pid_list) < self.conf.process_max_limit:
                        flag = True
                    if len(pid_list) >= self.conf.process_max_limit:
                        if flag:
                            self.logger.info('Processing Count is MAX....')
                        break
                    ready_call_list = util.select_call_info(
                        self.logger, job['INSRPS_CMP_ID'], job['INSR_SBSCRP_DT'], oracle, cfg, status='01')
                    process_call_list = util.select_call_info(
                        self.logger, job['INSRPS_CMP_ID'], job['INSR_SBSCRP_DT'], oracle, cfg, status='02')
                    if ready_call_list or process_call_list:
                        continue
                    p = multiprocessing.Process(target=do_task, args=(job,))
                    p.daemon = None
                    pid_list.append(p)
                    p.start()
                    # Update status
                    util.update_status(self.logger, '02', job, daemon=True, oracle=oracle, cfg=cfg)
                    log_str = 'Execute INSRPS_CMP_ID = {0},'.format(job['INSRPS_CMP_ID'])
                    log_str += ' [PID={0}]'.format(p.pid)
                    self.logger.info(log_str)
                    time.sleep(self.conf.process_interval)
                if type(oracle) is not bool:
                    oracle.conn.commit()
                if day_flag:
                    monitoring_list = util.select_ta_monitoring(oracle, cfg)
                    count = 1
                    for monitoring_dict in monitoring_list:
                        count = int(monitoring_dict['COUNT'])
                    if count == 0:
                        day_flag = False
                        summary.summary('7', self.logger)
                if type(oracle) is bool:
                    try:
                        oracle, cfg = util.db_connect(user='mlsta_dev')
                    except Exception:
                        oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
        except KeyboardInterrupt:
            self.logger.info('Daemon stopped by interrupt')
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
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
def do_task(job):
    """
    Process execute TA
    :param      job:        Job
    """
    import ta
    reload(ta)
    ta.main(job)


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

