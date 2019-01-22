#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-12-26, modification: 2018-12-27"

###########
# imports #
###########
import os
import sys
import time
import argparse
import traceback
import subprocess
from datetime import datetime, timedelta
from cfg import config
from lib import logger, util

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
ST = ''
START_DATE = ""
INSERT_CNT = 0
ERROR_CNT = 0


#######
# def #
#######
def elapsed_time(start_time):
    """
    elapsed time
    :param      start_time:         date object
    :return:                        Required time (type: datetime)
    """
    end_time = datetime.fromtimestamp(time.time())
    required_time = end_time - start_time
    return required_time


def sub_process(cmd):
    """
    Execute subprocess
    :param      cmd:        Command
    """
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        print response_out
    if len(response_err) > 0:
        print response_err


def execute_convert(args, target_file_path):
    """
    Execute convert file
    :param      args:                   Arguments
    :param      target_file_path:       Target file path
    """
    target_dir_path = os.path.dirname(target_file_path)
    output_file_name = '{0}_{1}'.format(args['con_format'].replace('-', ''), os.path.basename(target_file_path))
    output_file_path = os.path.join(target_dir_path, output_file_name)
    cmd = 'iconv -c -f {0} -t {1} {2} > {3}'.format(
        args['src_format'], args['con_format'], target_file_path, output_file_path)
    sub_process(cmd)
    return output_file_path


def upload_data_00014_version(log, target_file_path):
    """
    Upload data to database
    :param      log:                    Logger
    :param      target_file_path:       Target file path
    """
    global INSERT_CNT
    global ERROR_CNT
    log.info('Upload data 00014 file')
    log.debug('Open file. [{0}]'.format(target_file_path))
    args = {'con_format': 'utf8', 'src_format': 'utf8'}
    change_target_file_path = execute_convert(args, target_file_path)
    sam_file = open(change_target_file_path, 'r')
    update_tb_based_goods_match_list = list()
    update_tb_based_goods_match_dict = dict()
    update_tb_based_center_list = list()
    update_tb_based_center_dict = dict()
    try:
        oracle, cfg = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
    for line in sam_file:
        try:
            tm_insr_pd_c = line[0:10].strip()
            tm_insr_pd_nm = line[10:85].strip()
            tm_cnr_c = line[85:87].strip()
            info_dict = dict()
            info_dict['TM_INSR_PD_C'] = tm_insr_pd_c
            info_dict['TM_INSR_PD_NM'] = tm_insr_pd_nm
            if len(tm_insr_pd_c) > 0:
                value_dict_list = util.select_tb_based_goods_match(log, oracle, cfg, info_dict)
                count = -1
                for value_dict in value_dict_list:
                    count = int(value_dict['COUNT'])
                if count == 0:
                    log.info('Insert TM_INSR_PD_C({0}) in TB_BASED_GOODS_MATCH'.format(info_dict['TM_INSR_PD_C']))
                    if info_dict['TM_INSR_PD_C'] not in update_tb_based_goods_match_dict:
                        update_tb_based_goods_match_dict[info_dict['TM_INSR_PD_C']] = True
                        update_tb_based_goods_match_list.append(info_dict)
                else:
                    log.debug('TM_INSR_PD_C({0}) is already exists in TB_BASED_GOODS_MATCH'.format(
                        info_dict['TM_INSR_PD_C']))
            info_dict = dict()
            info_dict['TM_CNR_C'] = tm_cnr_c
            if len(tm_cnr_c) > 0:
                value_dict_list = util.select_tb_based_center_using_tm_cnr_c(log, oracle, cfg, info_dict)
                count = -1
                for value_dict in value_dict_list:
                    count = int(value_dict['COUNT'])
                if count == 0:
                    log.info('Insert TM_CNR_C({0}) in TB_BASED_CENTER'.format(info_dict['TM_CNR_C']))
                    if info_dict['TM_CNR_C'] not in update_tb_based_center_dict:
                        update_tb_based_center_dict[info_dict['TM_CNR_C']] = True
                        update_tb_based_center_list.append(info_dict)
                else:
                    log.debug('TM_CNR_C({0}) is already exists in TB_BASED_CENTER'.format(
                        info_dict['TM_CNR_C']))
        except Exception:
            log.error(traceback.format_exc())
            log.error('Line Error -> {0}'.format(line))
            ERROR_CNT += 1
            continue
    sam_file.close()
    for info_dict in update_tb_based_goods_match_list:
        try:
            log.debug('UPDATE {0} : TM_INSR_PD_C {1}'.format('00014', info_dict['TM_INSR_PD_C']))
            util.insert_data_to_tb_based_goods_match(log, oracle, cfg, info_dict)
            INSERT_CNT += 1
        except Exception:
            log.error(traceback.format_exc())
            log.error('UPDATE Error -> {0}'.format(info_dict))
            ERROR_CNT += 1
    for info_dict in update_tb_based_center_list:
        try:
            log.debug('UPDATE {0} : TM_MNGT_BZS_C {1}'.format('00014', info_dict['TM_CNR_C']))
            util.insert_data_to_tb_based_center(log, oracle, cfg, info_dict)
            INSERT_CNT += 1
        except Exception:
            log.error(traceback.format_exc())
            log.error("UPDATE Error -> {0}".format(info_dict))
            ERROR_CNT += 1
    oracle.disconnect()
    os.remove(change_target_file_path)


def upload_data_00013_version(log, target_file_path):
    """
    Upload data to database
    :param      log:                    Logger
    :param      target_file_path:       Target file path
    """
    global INSERT_CNT
    global ERROR_CNT
    log.info('Upload data 00013 file')
    log.debug('Open file. [{0}]'.format(target_file_path))
    sam_file = open(target_file_path, 'r')
    update_list = list()
    try:
        oracle, cfg = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
    for line in sam_file:
        try:
            info_dict = dict()
            insr_scno = line[0:20].strip()
            insr_qa_yn = line[20:21].strip()
            info_dict['INSR_SCNO'] = insr_scno
            info_dict['INSR_QA_YN'] = insr_qa_yn
            update_list.append(info_dict)
        except Exception:
            log.error(traceback.format_exc())
            log.error('Line Error -> {0}'.format(line))
            ERROR_CNT += 1
            continue
    sam_file.close()
    for info_dict in update_list:
        try:
            log.debug('UPDATE {0} : INSR_SCNO {1}'.format('00013', info_dict['INSR_SCNO']))
            util.update_insr_qa_yn(log, info_dict, oracle, cfg)
        except Exception:
            log.error(traceback.format_exc())
            log.error("UPDATE Error -> {0}".format(info_dict))
            ERROR_CNT += 1
    oracle.disconnect()


def upload_data_00012_version(log, target_file_path):
    """
    Upload data to datatbase
    :param      log:                    Logger
    :param      target_file_path:       Target file path
    """
    global INSERT_CNT
    global ERROR_CNT
    log.info('Upload data 00012 file')
    log.debug('Open file. [{0}]'.format(target_file_path))
    sam_file = open(target_file_path, 'r')
    insert_list = list()
    try:
        oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev', retry=True)
    try:
        oracle, cfg = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
    for line in sam_file:
        try:
            info_dict = dict()
            insrps_cmp_id = line[27:47].strip()
            insr_sbscrp_dt = line[8:16].strip()
            insrps_cst_mngt_no = line[16:27].strip()
            insrps_cst_nm = line[47:97].strip()
            tm_insr_pd_c = line[97:107].strip()
            tm_insr_pd_nm = ''
            tm_cnr_c = line[107:109].strip()
            insr_scno = line[109:129].strip()
            insr_sbscrp_seqn = int(line[129:139].strip())
            insr_sbscrp_rgr_empno = line[:8].strip()
            insr_sbscrp_rgr_fnm = ''
            cqiftbcqa027_list = util.select_cqiftbcqa027(log, insr_sbscrp_rgr_empno, oracle, cfg)
            for cqiftbcqa027_dict in cqiftbcqa027_list:
                insr_sbscrp_rgr_fnm = cqiftbcqa027_dict['AGENT_NAME']
            tm_mngt_bzs_nm = ''
            tm_cnr_nm = ''
            tb_based_center_list = util.select_tb_based_center(log, tm_cnr_c, oracle_dev, cfg_dev)
            for tb_based_center_dict in tb_based_center_list:
                tb_based_company_list = util.select_tb_based_company(log, tb_based_center_dict['TM_MNGT_BZS_C'], oracle_dev, cfg_dev)
                for tb_based_company_dict in tb_based_company_list:
                    tm_mngt_bzs_nm = tb_based_company_dict['TM_MNGT_BZS_NM']
                tm_cnr_nm = tb_based_center_dict['TM_CNR_NM']
            insrco_nm = ''
            goods_match_list = util.select_goods_match(log, {'TM_INSR_PD_C': tm_insr_pd_c}, oracle_dev, cfg_dev)
            for goods_match_dict in goods_match_list:
                goods_list = util.select_goods(log, goods_match_dict, oracle_dev, cfg_dev)
                for goods_dict in goods_list:
                    tm_insr_pd_nm = goods_dict['GOODS_NM']
                    tb_based_insurer_list = util.select_tb_based_insurer(log, goods_dict['INSRCO_C'], oracle_dev, cfg_dev)
                    for tb_based_insurer_dict in tb_based_insurer_list:
                        insrco_nm = tb_based_insurer_dict['INSRCO_NM']
            ta_status_cd = '00'
            tm_type = '01'
            info_dict['INSRPS_CMP_ID'] = '{0}{1}'.format(insrps_cmp_id, insrps_cst_mngt_no)
            info_dict['INSR_SBSCRP_DT'] = insr_sbscrp_dt
            info_dict['INSRPS_CST_MNGT_NO'] = insrps_cst_mngt_no
            info_dict['INSRPS_CST_NM'] = insrps_cst_nm
            info_dict['TM_INSR_PD_C'] = tm_insr_pd_c
            info_dict['TM_INSR_PD_NM'] = tm_insr_pd_nm
            info_dict['TM_CNR_C'] = tm_cnr_c
            info_dict['INSR_SCNO'] = insr_scno
            info_dict['INSR_SBSCRP_SEQN'] = insr_sbscrp_seqn
            info_dict['INSR_SBSCRP_RGR_EMPNO'] = insr_sbscrp_rgr_empno
            info_dict['INSR_QA_YN'] = 'N'
            info_dict['TA_STATUS_CD'] = ta_status_cd
            info_dict['TM_TYPE'] = tm_type
            info_dict['INSR_SBSCRP_RGR_FNM'] = insr_sbscrp_rgr_fnm
            info_dict['TM_MNGT_BZS_NM'] = tm_mngt_bzs_nm
            info_dict['TM_CNR_NM'] = tm_cnr_nm
            info_dict['INSRCO_NM'] = insrco_nm
            insert_list.append(info_dict)
        except Exception:
            log.error(traceback.format_exc())
            log.error('Line Error -> {0}'.format(line))
            ERROR_CNT += 1
            continue
        try:
            log.debug('DELETE & INSERT {0} : INSRPS_CMP_ID {1}'.format('00012', info_dict['INSRPS_CMP_ID']))
            offer_list = util.select_tb_based_offer_info(log, info_dict['INSRPS_CMP_ID'], oracle_dev, cfg_dev)
            if len(offer_list) > 0:
                for offer_info in offer_list:
                    if offer_info['INSR_SBSCRP_SEQN'] < info_dict['INSR_SBSCRP_SEQN']:
                        util.delete_tb_ta_result_evluation(log, info_dict['INSRPS_CMP_ID'], oracle_dev, cfg_dev)
                        util.delete_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
                        util.insert_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
                        INSERT_CNT += 1
                    else:
                        log.debug('data is already exists')
                    break
            else:
                util.delete_tb_ta_result_evluation(log, info_dict['INSRPS_CMP_ID'], oracle_dev, cfg_dev)
                util.delete_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
                util.insert_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
                INSERT_CNT += 1
        except Exception:
            log.error(traceback.format_exc())
            log.error('Line Error -> {0}'.format(line))
            log.error("Delete & INSERT Error -> {0}".format(info_dict))
            ERROR_CNT += 1
    sam_file.close()
    oracle.disconnect()
    oracle_dev.disconnect()
    # for info_dict in insert_list:
    #     try:
    #         log.debug('DELETE & INSERT {0} : INSRPS_CMP_ID {1}'.format('00012', info_dict['INSRPS_CMP_ID']))
    #         offer_list = util.select_tb_based_offer_info(log, info_dict['INSRPS_CMP_ID'], oracle_dev, cfg_dev)
    #         if len(offer_list) > 0:
    #             for offer_info in offer_list:
    #                 if offer_info['INSR_SBSCRP_SEQN'] < info_dict['INSR_SBSCRP_SEQN']:
    #                     util.delete_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
    #                     util.insert_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
    #                     INSERT_CNT += 1
    #                 else:
    #                     log.debug('data is already exists')
    #                 break
    #         else:
    #             util.delete_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
    #             util.insert_tb_based_offer_info(log, info_dict, oracle_dev, cfg_dev)
    #             INSERT_CNT += 1
    #     except Exception:
    #         log.error(traceback.format_exc())
    #         log.error("Delete & INSERT Error -> {0}".format(info_dict))
    #         ERROR_CNT += 1


def upload_data_00011_version(log, target_file_path):
    """
    Upload data to database
    :param      log:                    Logger
    :param      target_file_path:       Target file path
    """
    global INSERT_CNT
    global ERROR_CNT
    log.info('Upload data 00011 file')
    log.debug('Open file. [{0}]'.format(target_file_path))
    sam_file = open(target_file_path, 'r')
    insert_list = list()
    ucid_dict = dict()
    cnsl_empno_dict = dict()
    insrps_cmp_id_dict = dict()
    try:
        oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev', retry=True)
    try:
        oracle, cfg = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle, cfg = util.db_connect(user='mlsta_dev', retry=True)
    for line in sam_file:
        try:
            info_dict = dict()
            ucid = line[0:20].strip()
            tm_type = '01'
            insrps_cmp_id = line[31:51].strip()
            if not ucid in ucid_dict:
                call_info_list = util.select_cqiftbcqa001(log, ucid, oracle, cfg)
                ucid_dict[ucid] = call_info_list
            tm_cal_dt = ''
            cal_strt_hh = ''
            cal_end_hh = ''
            cst_nm = ''
            cnsl_empno = ''
            fnm = ''
            tm_cnr_c = ''
            duration_minute = 0
            for call_info in ucid_dict[ucid]:
                tm_cal_dt = call_info['TM_CAL_DT']
                cal_strt_hh = call_info['CAL_STRT_HH']
                cal_end_hh = call_info['CAL_END_HH']
                cst_nm = call_info['TM_CST_NM']
                cnsl_empno = call_info['TM_EMPNO']
                if str(cal_end_hh) != 'None':
                    duration = str(datetime.strptime(cal_end_hh, '%H%M%S') - datetime.strptime(
                        cal_strt_hh, '%H%M%S')).replace(':', '')
                    if len(duration) > 6:
                        duration = str(datetime.strptime(cal_end_hh, '%H%M%S') + timedelta(days=1) - datetime.strptime(
                            cal_strt_hh, '%H%M%S')).replace(':', '')
                    duration = '{0:0=6d}'.format(int(duration))
                    duration_minute = int(duration[0:2]) * 60 + int(duration[2:4])
                if not cnsl_empno in cnsl_empno_dict:
                    cqiftbcqa027_list = util.select_cqiftbcqa027(log, cnsl_empno, oracle, cfg)
                    cnsl_empno_dict[cnsl_empno] = cqiftbcqa027_list
                for cqiftbcqa027_info in cnsl_empno_dict[cnsl_empno]:
                    fnm = cqiftbcqa027_info['AGENT_NAME']
                    break
                tm_cnr_c = call_info['TM_CNR_C']
                break
            cst_mngt_no = line[20:31].strip()
            stt_status = '01'
            key = '{0}{1}'.format(insrps_cmp_id, cst_mngt_no)
            if not key in insrps_cmp_id_dict:
                insrps_cmp_id_dict[key] = util.select_tb_based_offer_info(log, key, oracle_dev, cfg_dev, tm_cal_dt)
            insr_sbscrp_dt = ''
            for offer_info in insrps_cmp_id_dict[key]:
                insr_sbscrp_dt = offer_info['INSR_SBSCRP_DT']
                break
            info_dict['UCID'] = ucid
            info_dict['TM_TYPE'] = tm_type
            info_dict['INSRPS_CMP_ID'] = '{0}{1}'.format(insrps_cmp_id, cst_mngt_no)
            info_dict['TM_CAL_DT'] = tm_cal_dt
            info_dict['CAL_STRT_HH'] = cal_strt_hh
            info_dict['CAL_END_HH'] = cal_end_hh
            info_dict['CST_MNGT_NO'] = cst_mngt_no
            info_dict['CST_NM'] = cst_nm
            info_dict['CNSL_EMPNO'] = cnsl_empno
            info_dict['FNM'] = fnm
            info_dict['TM_CNR_C'] = tm_cnr_c
            info_dict['STT_STATUS'] = stt_status
            info_dict['INSR_SBSCRP_DT'] = insr_sbscrp_dt
            info_dict['DURATION_MINUTE'] = duration_minute
            insert_list.append(info_dict)
        except Exception:
            log.error(traceback.format_exc())
            log.error('Line Error -> {0}'.format(line))
            ERROR_CNT += 1
            continue
        try:
            log.debug('DELETE & INSERT {0} : UCID {1}'.format('00011', info_dict['UCID']))
            util.delete_tb_based_call_info(log, info_dict, oracle_dev, cfg_dev)
            util.insert_tb_based_call_info(log, info_dict, oracle_dev, cfg_dev)
            INSERT_CNT += 1
        except Exception:
            log.error(traceback.format_exc())
            log.error('Line Error -> {0}'.format(line))
            log.error("Delete & INSERT Error -> {0}".format(info_dict))
            ERROR_CNT += 1
    sam_file.close()
    # for info_dict in insert_list:
    #     try:
    #         log.debug('DELETE & INSERT {0} : UCID {1}'.format('00011', info_dict['UCID']))
    #         util.delete_tb_based_call_info(log, info_dict, oracle_dev, cfg_dev)
    #         util.insert_tb_based_call_info(log, info_dict, oracle_dev, cfg_dev)
    #         INSERT_CNT += 1
    #     except Exception:
    #         log.error(traceback.format_exc())
    #         log.error("Delete & INSERT Error -> {0}".format(info_dict))
    #         ERROR_CNT += 1
    util.record_count_update(log, oracle_dev, cfg_dev)
    # call_info 상태 변경
    util.update_stt_status_ready_to_start(log, oracle_dev, cfg_dev)
    # offer_info 상태 변경
    util.update_ta_status_cd_ready_to_start(log, oracle_dev, cfg_dev)
    oracle.disconnect()
    oracle_dev.disconnect()


def upload_data_to_db(log, target_file_path):
    """
    Upload data to database
    :param      log:                    Logger
    :param      target_file_path:       Target file path
    """
    log.debug("SAM data upload to DB")
    target_file_name = os.path.basename(target_file_path)
    try:
        if target_file_name.startswith('ETM_E_CQA_00011'):
            target_file_type = '00011'
        elif target_file_name.startswith('ETM_E_CQA_00012'):
            target_file_type = '00012'
        elif target_file_name.startswith('ETM_E_CQA_00013'):
            target_file_type = '00013'
        elif target_file_name.startswith('ETM_E_CQA_00014'):
            target_file_type = '00014'
        else:
            log.error("File name is strange : {0}".format(target_file_path))
            log.error("END.. Start time = {0}, The time required = {1}".format(START_DATE, elapsed_time(ST)))
            sys.exit(1)
        if target_file_type == '00011':
            upload_data_00011_version(log, target_file_path)
        elif target_file_type == '00012':
            upload_data_00012_version(log, target_file_path)
        elif target_file_type == '00013':
            upload_data_00013_version(log, target_file_path)
        elif target_file_type == '00014':
            upload_data_00014_version(log, target_file_path)
    except Exception:
        exc_info = traceback.format_exc()
        log.error(exc_info)


def processing(args):
    """
    Processing
    :param      args:       Arguments
    """
    # Set logger
    log = logger.set_logger(
        logger_name=config.BatchConfig.logger_name,
        log_dir_path=os.path.join(config.BatchConfig.log_dir_path, START_DATE),
        log_file_name='{0}_{1}.log'.format(config.BatchConfig.log_file_name, args.target_file_type),
        log_level=config.BatchConfig.log_level
    )
    log.info("[START] Execute Batch ..")
    try:
        # exists check
        target_file = '{0}_{1}.SAM'.format(args.target_file_type, args.target_file_date)
        target_file_path = os.path.join(config.BatchConfig.target_dir_path, target_file)
        if not os.path.exists(target_file_path):
            log.error("File is not exists : {0}".format(target_file_path))
            log.error("END.. Start time = {0}, The time required = {1}".format(START_DATE, elapsed_time(ST)))
            sys.exit(1)
        # Upload sam data to db
        upload_data_to_db(log, target_file_path)
        log.info(
            "Total file name = {0}, upsert count = {1}, error count = {2}, The time required = {3}".format(
                target_file, INSERT_CNT, ERROR_CNT, elapsed_time(ST)))
        # 처리파일 삭제
        log.info("Remove last file ")
        last_file_date = datetime.strptime(args.target_file_date, '%Y%m%d') - timedelta(days=1)
        remove_target_file_date = datetime.strftime(last_file_date, '%Y%m%d')
        remove_target_file_path = '{0}_{1}.SAM'.format(args.target_file_type, remove_target_file_date)
        log.info("\ttarget file : {0}".format(remove_target_file_path))
        # if os.path.exists(remove_target_file_path):
        #     os.remove(remove_target_file_path)
        #     log.info("\tDelete Complete")
        # else:
        #     log.info("\tNot exists file")
        log.debug("END.. Start time = {0}, The time required = {1}".format(START_DATE, elapsed_time(ST)))
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        log.error(exc_info)
        log.error("---------- ERROR ----------")
        sys.exit(1)


########
# main #
########
def main(args):
    """
    This is a program that insert etm_e_cqa_00011 data to Oracle DB
    :param      args:       Arguments
    """
    global ST
    global START_DATE
    ST = datetime.fromtimestamp(time.time())
    START_DATE = datetime.strftime(ST, '%Y%m%d')
    try:
        processing(args)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', nargs='?', action='store', dest='target_file_type', type=str, default=False,
                        required=True, help="Input target file\n[ ex) ETM_E_CQA_00011 ]")
    parser.add_argument('-d', nargs='?', action='store', dest='target_file_date', type=str, default=False,
                        required=True, help="Input target date\n[ ex) 20181025 ]")
    arguments = parser.parse_args()
    main(arguments)
