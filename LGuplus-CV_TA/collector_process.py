#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-08-24, modification: 2018-09-03"

###########
# imports #
###########
import os
import sys
import json
import socket
import shutil
import traceback
from cfg import config
from lib import logger, util

###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#########
# class #
#########
class Collector(object):
    def __init__(self):
        self.conf = config.CollectorConfig
        self.logger = logger.get_timed_rotating_logger(
            logger_name=self.conf.logger_name,
            log_dir_path=self.conf.log_dir_path,
            log_file_name=self.conf.log_file_name,
            backup_count=self.conf.backup_count,
            log_level=self.conf.log_level
        )

    def do_job(self):
        # Watching target directory
        for target_dir_path in self.conf.collector_work_dir_list:
            if not os.path.isabs(target_dir_path):
                self.logger.error("Target directory path must be absolute path. ({0})".format(
                    target_dir_path))
                continue
            if not os.path.exists(target_dir_path):
                self.logger.error("Can't find directory.({0})".format(target_dir_path))
                continue
            # Get json file list (Max 10000EA)
            target_file_list = list()
            w_ob = os.walk(target_dir_path)
            for dir_path, sub_dirs, files in w_ob:
                escape = False
                for file_name in files:
                    if file_name.endswith('json'):
                        target_file_list.append(os.path.join(dir_path, file_name))
                        if len(target_file_list) == 10000:
                            escape = True
                            break
                if escape:
                    break
            # Sorted file by modify time
            sorted_file_list = sorted(target_file_list, key=os.path.getmtime, reverse=False)
            for json_file_path in sorted_file_list:
                # Set up directory paths and file names
                start_date = 'non_date'
                json_dir_path = os.path.dirname(json_file_path)
                json_file_name = os.path.basename(json_file_path)
                file_name, extension = os.path.splitext(json_file_name)
                trx_file_name = '{0}_trx.txt'.format(file_name)
                trx_file_path = os.path.join(json_dir_path, trx_file_name)
                error_dir = os.path.join(self.conf.error_dir_path, start_date)
                processed_dir = os.path.join(self.conf.processed_dir_path, start_date)
                # Execute
                if os.path.exists(json_file_path) and os.path.exists(trx_file_path):
                    try:
                        # Extract meta information
                        json_file = open(json_file_path)
                        json_data = json.load(json_file)
                        cnid = json_data['cnid']
                        start_date = json_data['start_date']
                        start_time = json_data['start_time']
                        end_date = json_data['end_date']
                        end_time = json_data['end_time']
                        rec_server_id = json_data['rec_server_id']
                        rec_channel = json_data['rec_channel']
                        agent_id = json_data['agent_id']
                        talk_time = json_data['talk_time']
                        station = json_data['station']
                        direction = json_data['direction']
                        phone_number = json_data['phone_number']
                        group_id = json_data['group_id'] if 'group_id' in json_data else ''
                        team_id = json_data['team_id'] if 'team_id' in json_data else ''
                        svc_type = json_data['svc_type'] if 'svc_type' in json_data else ''
                        cust_cd = json_data['cust_cd'] if 'cust_cd' in json_data else ''
                        cust_no = json_data['cust_no'] if 'cust_no' in json_data else ''
                        if cust_cd.strip().upper() == 'E':
                            entr_no = cust_no
                            cust_no = ''
                        elif cust_cd.strip().upper() == 'C':
                            entr_no = ''
                            cust_no = cust_no
                        else:
                            if len(cust_cd.strip()) != 1:
                                cust_cd = ''
                            entr_no = ''
                            cust_no = ''
                        hostname = socket.gethostname()
                        rest_send_key = cnid + start_time + rec_channel
                        json_file.close()
                        # Make output directory
                        base_dir_path = "{0}/{1}".format(start_date, start_time[:2])
                        processed_dir = os.path.join(self.conf.processed_dir_path, base_dir_path)
                        error_dir = os.path.join(self.conf.error_dir_path, base_dir_path)
                        if not os.path.exists(processed_dir):
                            os.makedirs(processed_dir)
                        if os.path.exists(os.path.join(processed_dir, json_file_name)):
                            os.remove(os.path.join(processed_dir, json_file_name))
                        if os.path.exists(os.path.join(processed_dir, trx_file_name)):
                            os.remove(os.path.join(processed_dir, trx_file_name))
                        shutil.move(json_file_path, processed_dir)
                        shutil.move(trx_file_path, processed_dir)
                        # Insert meta information
                        util.upsert_meta_info(
                            log=self.logger,
                            rest_send_key=rest_send_key,
                            start_date=start_date,
                            start_time=start_time,
                            end_date=end_date,
                            end_time=end_time,
                            talk_time=talk_time,
                            station=station,
                            direction=direction,
                            phone_number=phone_number,
                            entr_no=entr_no,
                            cust_no=cust_no,
                            agent_id=agent_id,
                            rec_server_id=rec_server_id,
                            rec_channel=rec_channel,
                            group_id=group_id,
                            team_id=team_id,
                            svc_type=svc_type,
                            cust_cd=cust_cd,
                            retry=False
                        )
                        # Insert status register
                        util.upsert_status_register(
                            log=self.logger,
                            rest_send_key=rest_send_key,
                            ta_hostname=hostname,
                            ta_status=0,
                            start_date=start_date,
                            start_time=start_time,
                            stt_filename=file_name,
                            svc_type=svc_type,
                            retry=False
                        )
                    except Exception:
                        if not os.path.exists(error_dir):
                            os.makedirs(error_dir)
                        if os.path.exists(os.path.join(error_dir, json_file_name)):
                            os.remove(os.path.join(error_dir, json_file_name))
                        if os.path.exists(os.path.join(error_dir, trx_file_name)):
                            os.remove(os.path.join(error_dir, trx_file_name))
                        if os.path.exists(json_file_path):
                            shutil.move(json_file_path, error_dir)
                        if os.path.exists(trx_file_path):
                            shutil.move(trx_file_path, error_dir)
                        if os.path.exists(os.path.join(processed_dir, json_file_name)):
                            shutil.move(os.path.join(processed_dir, json_file_name), error_dir)
                        if os.path.exists(os.path.join(processed_dir, trx_file_name)):
                            shutil.move(os.path.join(processed_dir, trx_file_name), error_dir)
                        self.logger.error(traceback.format_exc())
                        self.logger.error('Json file -> {0}'.format(json_file_path))
                        continue
                    self.logger.info("[DONE] File name = {0}, rest_send_key = {1}".format(
                        file_name, rest_send_key))

    def run(self):
        try:
            self.logger.info('[START] Collector process started')
            # Main loop
            while True:
                self.do_job()
        except KeyboardInterrupt:
                self.logger.info('Collector stopped by interrupt')
        except Exception:
                self.logger.error(traceback.format_exc())
        finally:
            self.logger.info('[E N D] Collector Process stopped')


#######
# def #
#######
def main():
    """
    This is a program that collector process
    """
    try:
        collector = Collector()
        collector.run()
    except Exception:
        print traceback.format_exc()


if __name__ == '__main__':
    main()
