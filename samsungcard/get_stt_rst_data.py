#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-10-15, modification: 2018-10-15"

###########
# imports #
###########
import sys
import time
import signal
import traceback
from cfg import config
from lib import util
from datetime import datetime
from lib import logger

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
    :param      start_time:         date object
    :return:                        Required time (type : datetime)
    """
    end_time = datetime.fromtimestamp(time.time())
    required_time = end_time - start_time
    return required_time


def processing(conf, count, target_list):
    """
    processing
    :param      conf:               Config
    :param      count:              Count
    :param      target_list:        Target list
    """
    ts = time.time()
    st = datetime.fromtimestamp(ts)
    # Add logging
    log = logger.get_timed_rotating_logger(
        logger_name=conf.logger_name,
        log_dir_path=conf.log_dir_path,
        log_file_name='{0}_{1}.log'.format(conf.log_file_name, count),
        backup_count=conf.backup_count,
        log_level=conf.log_level
    )
    # Ignore kill signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGQUIT, signal.SIG_IGN)
    log.debug("-" * 100)
    log.debug("Start get stt rst data")
    flag = False
    oracle = False
    try:
        try:
            oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev')
        except Exception:
            oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev', retry=True)
        try:
            oracle, cfg = util.db_connect(user='mlsta')
        except Exception:
            oracle, cfg = util.db_connect(user='mlsta', retry=True)
        # Target_list = util.select_stt_target(log, oracle_dev, cfg_dev)
        if len(target_list) > 0:
            log.info("Target Count : {0}".format(len(target_list)))
            flag = True
        for info_dict in target_list:
            try:
                log.info("\tTarget UCID : {0}".format(info_dict['UCID']))
                ucid_gkey_list = util.select_ucid_gkey(log, info_dict['UCID'], oracle, cfg_dev)
                log.info("\tResult UCID_GKEY Count : {0}".format(len(ucid_gkey_list)))
                if len(ucid_gkey_list) == 0:
                    log.error("\tUCID GKEY COUNT IS 0")
                    util.update_stt_status(log, info_dict, '90', oracle_dev)
                    continue
                target_ucid_gkey = ucid_gkey_list[0]
                info_dict['UCID_GKEY'] = target_ucid_gkey['UCID_GKEY']
                # for target_ucid_gkey in ucid_gkey_list:
                log.info("\t\tTarget UCID_GKEY : {0}".format(target_ucid_gkey['UCID_GKEY']))
                info_dict.update(util.select_sa_data(log, target_ucid_gkey, oracle, cfg))
                resultsentence_list = util.select_vw_resultsentence(log, target_ucid_gkey['UCID_GKEY'], oracle, cfg)
                log.info("\t\tResult Count : {0}".format(len(resultsentence_list)))
                if len(resultsentence_list) == 0:
                    log.error("\t\tSTT Result Count is Zero")
                    util.update_stt_status(log, info_dict, '90', oracle_dev)
                    continue
                # 데이터 Insert 전 Delete 작업
                util.delete_data_to_tb_based_stt_info(log, target_ucid_gkey, oracle_dev, cfg_dev)
                total_sntc_len = 0
                total_during_time = 0
                log.info("\t\tStart STT result DB INSERT..")
                for idx in range(len(resultsentence_list)):
                    resultsentence = resultsentence_list[idx]
                    insert_data = dict()
                    # Insert 할 데이터 정제 작업
                    insert_data.update(resultsentence)
                    stt_stereo_yn = 'Y'
                    if resultsentence['RXTX_KIND'] == 1:
                        insert_data['SPEAKER_KIND'] = 'A'
                    elif resultsentence['RXTX_KIND'] == 2:
                        insert_data['SPEAKER_KIND'] = 'C'
                    elif resultsentence['RXTX_KIND'] == 3:
                        continue
                    else:
                        insert_data['SPEAKER_KIND'] = 'M'
                        stt_stereo_yn = 'N'
                    insert_data['LINE_NUM'] = idx
                    info_dict['STT_STEREO_YN'] = stt_stereo_yn
                    during_time = 0
                    for check_idx in range(idx+1, len(resultsentence_list)):
                        if resultsentence['RXTX_KIND'] == resultsentence_list[check_idx]['RXTX_KIND']:
                            during_time = resultsentence_list[check_idx]['ARMSOFFSET'] - resultsentence['ARMSOFFSET']
                            break
                        if check_idx + 1 == len(resultsentence_list):
                            during_time = target_ucid_gkey['CALL_DUR'] * 1000 - resultsentence['ARMSOFFSET']
                            break
                    if resultsentence['SENTENCE'] == '(SILENCE)':
                        sntc_len = 0
                    else:
                        sntc_len = len(resultsentence['SENTENCE'].replace(' ', '').decode('utf-8'))
                        total_during_time += during_time
                    total_sntc_len += sntc_len
                    if during_time > 0:
                        spch_sped = round(sntc_len/(float(during_time)/1000), 2)
                        insert_data['SPCH_SPED'] = spch_sped if spch_sped < 1000 else 999.99
                    else:
                        insert_data['SPCH_SPED'] = 0
                    # 데이터 Insert 작업
                    # log.debug('during_time : {0}'.format(during_time))
                    # log.debug('sntc_len: {0}'.format(sntc_len))
                    # log.debug('sentence : {0}'.format(resultsentence['SENTENCE']))
                    # log.debug('line_num : {0}'.format(idx))
                    util.insert_data_to_tb_based_stt_info(log, insert_data, oracle_dev, cfg_dev)
                if total_during_time > 0:
                    spch_sped = round(total_sntc_len/(float(total_during_time)/1000), 2)
                    info_dict['SPCH_SPED'] = spch_sped if spch_sped < 1000 else 999.99
                else:
                    info_dict['SPCH_SPED'] = 0
                util.update_stt_status(log, info_dict, '04', oracle_dev)
            except Exception:
                log.error(traceback.format_exc())
                util.update_stt_status(log, info_dict, '03', oracle_dev)
    except Exception:
        exc_info = traceback.format_exc()
        log.error(exc_info)
    finally:
        oracle.disconnect()
        oracle_dev.disconnect()
        if flag:
            log.info("END.. Start time = {0}, The time required = {1}".format(st, elapsed_time(st)))
        for handler in log.handlers:
            handler.close()
            log.removeHandler(handler)


########
# main #
########
def main(count, job):
    """
    This is a program that Get STT result
    :param      count:      Count
    :param      job:        Job
    """
    try:
        conf = config.GETSTTConfig
        processing(conf, count, job)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)


if __name__ == '__main__':
    main(2)
