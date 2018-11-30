#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-24, modification: 2018-11-14"

###########
# imports #
###########
import sys
import time
import socket
import traceback
import multiprocessing
from flashtext.keyword import KeywordProcessor
from cfg import config
from lib import logger, util, db_connection

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

    def make_job_list(self, oracle):
        job_list = util.select_ta_target(oracle, socket.gethostname())
        if not job_list:
            return list()
        return job_list[:self.conf.process_max_limit]

    def run(self):
        oracle = db_connection.Oracle(config.OracleConfig, failover=True, service_name=True)
        try:
            self.logger.info('[START] TA daemon process started')
            pid_list = list()
            # 브랜드명 keyword
            brand_keyword_dict = util.select_brand_keyword(self.logger, oracle)
            brand_keyword_processor = KeywordProcessor()
            for brand_keyword in brand_keyword_dict.keys():
                brand_keyword_processor.add_keyword(brand_keyword)
            # 불용어 keyword
            del_keyword_dict = util.select_del_keyword(self.logger, oracle)
            # 감성 keyword
            senti_keyword_dict = util.select_senti_keyword(self.logger, oracle)
            # HMD category overlap check rank
            category_rank_dict = util.select_hmd_category_rank(self.logger, oracle)
            while True:
                try:
                    job_list = self.make_job_list(oracle)
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
                        for pid in pid_list[:]:
                            if not pid.is_alive():
                                pid_list.remove(pid)
                        continue
                    # Job -> (rest_send_key, start_date, start_time, file_name, svc_type, team_id)
                    rest_send_key, start_date, start_time, file_name, svc_type, team_id = job
                    p = multiprocessing.Process(
                        target=do_task,
                        args=(
                            job,
                            brand_keyword_processor,
                            del_keyword_dict,
                            senti_keyword_dict,
                            category_rank_dict
                        )
                    )
                    p.daemon = None
                    pid_list.append(p)
                    p.start()
                    # Update status
                    util.update_status(self.logger, oracle, '1', rest_send_key)
                    log_str = 'Execute rest_send_key = {0},'.format(rest_send_key)
                    log_str += ' start_date = {0},'.format(start_date)
                    log_str += ' start_time = {0},'.format(start_time)
                    log_str += ' file_name = {0},'.format(file_name)
                    log_str += ' svc_type = {0},'.format(svc_type)
                    log_str += ' team_id = {0},'.format(team_id)
                    log_str += ' [PID={0}]'.format(p.pid)
                    self.logger.info(log_str)
                    time.sleep(self.conf.process_interval)
                time.sleep(0.1)
        except KeyboardInterrupt:
            self.logger.info('Daemon stopped by interrupt')
        except Exception:
            self.logger.error(traceback.format_exc())
        finally:
            try:
                oracle.disconnect()
            except Exception:
                pass
            finally:
                self.logger.info('[E N D] Daemon process stopped')


#######
# def #
#######
def do_task(job, brand_keyword_processor, del_keyword_dict, senti_keyword_dict, category_rank_dict):
    """
    Process execute TA
    :param      job:                         (rest_send_key, start_date, start_time, file_name, svc_type, team_id)
    :param      brand_keyword_processor:     Brand keyword flash text object
    :param      del_keyword_dict:            Delete keyword dictionary
    :param      senti_keyword_dict:          Sensitivity keyword dictionary
    :param      category_rank_dict:          Category rank dictionary
    """
    import ta
    reload(ta)
    ta.main(
        job,
        brand_keyword_processor,
        del_keyword_dict,
        senti_keyword_dict,
        category_rank_dict
    )


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
