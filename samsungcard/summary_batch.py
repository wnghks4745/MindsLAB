#!/usr/bin/python
# -*- coding:utf-8 -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2018-12-11, modification: 2019-01-08"

###########
# imports #
###########
import os
import sys
import time
import argparse
import traceback
from datetime import datetime, timedelta
from cfg import config
from lib import logger, util

###########
# options #
###########
reload(sys)
sys.setdefaultencodig("utf-8")

#############
# constants #
#############
ST = ''
START_DATE = ""


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


def insert_data_to_db_days(log, oracle, cfg, target_dict_list, args):
    """
    Insert Days data to database
    :param      log:                    Logger
    :param      oracle:                 Oracle
    :param      cfg:                    Config
    :param      target_dict_list:       Target Dictionary List
    :param      args:                   Arguments
    """
    tb_statistics_ob_ma_dict = dict()
    tb_statistics_ob_ma_ag_dict = dict()
    # 조회된 데이터 정리
    for target_dict in target_dict_list:
        if str(target_dict['TM_MNGT_BZS_C']) == 'None':
            target_dict['TM_MNGT_BZS_C'] = str(target_dict['TM_MNGT_BZS_C'])
        # PK key 생성
        tb_statistics_ob_ma_key = '{0}_{1}_{2}'.format(
            args.date, target_dict['TM_MNGT_BZS_C'], target_dict['TM_CNR_C'])
        tb_statistics_ob_ma_ag_key = '{0}_{1}_{2}_{3}'.format(
            args.date, target_dict['TM_MNGT_BZS_C'], target_dict['TM_CNR_C'], target_dict['INSR_SBSCRP_RGR_EMPNO'])
        # PK 체크 및 dict setting
        if tb_statistics_ob_ma_key not in tb_statistics_ob_ma_dict:
            temp_dict = dict()
            temp_dict['STD_DT'] = args.date
            temp_dict['TM_MNGT_BZS_C'] = target_dict['TM_MNGT_BZS_C']
            temp_dict['TM_CNR_C'] = target_dict['TM_CNR_C']
            temp_dict['SELLING_DICT'] = dict()
            temp_dict['SELLING_COUNT'] = 0
            temp_dict['TA_EVLU_DICT'] = dict()
            temp_dict['TA_EVLU_COUNT'] = 0
            temp_dict['QA_EVLU_COUNT'] = 0
            temp_dict['MIS_SELLING_COUNT'] = 0
            temp_dict['QA_MIS_SELLING_COUNT'] = 0
            temp_dict['PER_MIS_SELLING'] = 0
            temp_dict['QA_PER_MIS_SELLING'] = 0
            temp_dict['OBJECTION_COUNT'] = 0
            temp_dict['OBJECTION_CONFM_COUNT'] = 0
            temp_dict['OBJECTION_RETURN_COUNT'] = 0
            temp_dict['MANAGT_TRGET_COUNT'] = 0
            temp_dict['UN_MANGT_TRGET_COUNT'] = 0
            temp_dict['MANAGT_TRGET_CM_COUNT'] = 0
            temp_dict['SPLEMNT_COUNT'] = 0
            temp_dict['SPLEMNT_CM_COUNT'] = 0
            temp_dict['SPLEMNT_UN_MANAGT_COUNT'] = 0
            temp_dict['EDC_SPLEMNT_COUNT'] = 0
            temp_dict['EDC_SPLEMNT_CM_COUNT'] = 0
            temp_dict['EDC_SPLEMNT_UN_MANAGT_COUNT'] = 0
            temp_dict['EDC_COUNT'] = 0
            temp_dict['EDC_CM_COUNT'] = 0
            temp_dict['EDC_UN_MANAGT_COUNT'] = 0
        else:
            temp_dict = tb_statistics_ob_ma_dict[tb_statistics_ob_ma_key]
        if tb_statistics_ob_ma_ag_key not in tb_statistics_ob_ma_ag_dict:
            temp_ag_dict = dict()
            temp_ag_dict['STD_DT'] = args.date
            temp_ag_dict['TM_MNGT_BZS_C'] = target_dict['TM_MNGT_BZS_C']
            temp_ag_dict['TM_CNR_C'] = target_dict['TM_CNR_C']
            temp_ag_dict['AGENT_ID'] = target_dict['INSR_SBSCRP_RGR_EMPNO']
            temp_ag_dict['AGENT_NM'] = target_dict['INSR_SBSCRP_RGR_FNM']
            temp_ag_dict['SELLING_DICT'] = dict()
            temp_ag_dict['SELLING_COUNT'] = 0
            temp_ag_dict['TA_EVLU_DICT'] = dict()
            temp_ag_dict['TA_EVLU_COUNT'] = 0
            temp_ag_dict['QA_EVLU_COUNT'] = 0
            temp_ag_dict['MIS_SELLING_COUNT'] = 0
            temp_ag_dict['QA_MIS_SELLING_COUNT'] = 0
            temp_ag_dict['OBJECTION_COUNT'] = 0
            temp_ag_dict['OBJECTION_CONFM_COUNT'] = 0
            temp_ag_dict['MANAGT_TRGET_COUNT'] = 0
            temp_ag_dict['UN_MANAT_TRGET_COUNT'] = 0
            temp_ag_dict['MANAGT_TRGET_CM_COUNT'] = 0
        else:
            temp_ag_dict = tb_statistics_ob_ma_ag_dict[tb_statistics_ob_ma_ag_key]
        if target_dict['INSRPS_CMP_ID'] not in temp_dict['SELLING_DICT']:
            temp_dict['SELLING_DICT'][target_dict['INSRPS_CMP_ID']] = 1
            temp_dict['SELLING_COUNT'] += 1
        if target_dict['INSRPS_CMP_ID'] not in temp_ag_dict['SELLING_DICT']:
            temp_ag_dict['SELLING_DICT'][target_dict['INSRPS_CMP_ID']] = 1
            temp_ag_dict['SELLING_COUNT'] += 1
        if target_dict['TA_STATUS_CD'] == '04':
            if target_dict['INSRPS_CMP_ID'] not in temp_dict['TA_EVLU_DICT']:
                temp_dict['TA_EVLU_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                temp_dict['TA_EVLU_COUNT'] += 1
                # 불완전판매인 경우
                if target_dict['LAST_RESULT_CD'] == '08':
                    temp_dict['MIS_SELLING_COUNT'] += 1
                # 조치대상건수
                if target_dict['MEASURE_RESULT_CD'] in ('02', '03', '04'):
                    temp_dict['MANAGT_TRGET_COUNT'] += 1
                # 조치대상완료건수
                if target_dict['MEASURE_STATUS_CD'] == '06':
                    temp_dict['MANAGT_TRGET_CM_COUNT'] += 1
                # 미조치대상건수
                elif target_dict['MEASURE_STATUS_CD'] == '07':
                    temp_dict['UN_MANGT_TRGET_COUNT'] += 1
                # QA 처리가 완료 된 건
                if str(target_dict['QA_FNM']) != 'None':
                    temp_dict['QA_EVLU_COUNT'] += 1
                    # 불완전판매인 경우
                    if target_dict['LAST_RESULT_CD'] == '08':
                        temp_dict['QA_MIS_SELLING_COUNT'] += 1
                    # 이의제기 신청이 아닌 경우
                    if target_dict['OBJECTION_CD'] != '01':
                        temp_dict['OBJECTION_COUNT'] += 1
                    # 이의제기 승인인 경우
                    if target_dict['OBJECTION_CD'] == '03':
                        temp_dict['OBJECTION_CONFM_COUNT'] += 1
                    # 이의제기 반려인 경우
                    elif target_dict['OBJECTION_CD'] == '04':
                        temp_dict['OBJECTION_RETURN_COUNT'] += 1
                    # 조치대상건수
                    if target_dict['MEASURE_RESULT_CD'] in ('02', '03', '04'):
                        if target_dict['MEASURE_RESULT_CD'] == '02':
                            temp_dict['EDC_COUNT'] += 1
                        elif target_dict['MEASURE_RESULT_CD'] == '03':
                            temp_dict['SPLEMNT_COUNT'] += 1
                        elif target_dict['MEASURE_RESULT_CD'] == '04':
                            temp_dict['EDC_SPLEMNT_COUNT'] += 1
                    # 조치대상완료건수
                    if target_dict['MEASURE_STATUS_CD'] == '06':
                        # 보완완료건수
                        if target_dict['MEASURE_RESULT_CD'] == '03':
                            temp_dict['SPLEMNT_CM_COUNT'] += 1
                        # 교육/보완완료건수
                        elif target_dict['MEASURE_RESULT_CD'] == '04':
                            temp_dict['EDC_SPLEMNT_CM_COUNT'] += 1
                        # 교육완료건수
                        elif target_dict['MEASURE_RESULT_CD'] == '02':
                            temp_dict['EDC_CM_COUNT'] += 1
                    # 미조치대상건수
                    elif target_dict['MEASURE_STATUS_CD'] == '07':
                        # 보완미조치건수
                        if target_dict['MEASURE_RESULT_CD'] == '03':
                            temp_dict['SPLEMNT_UN_MANAGT_COUNT'] += 1
                        # 교육/보완미조치건수
                        elif target_dict['MEASURE_RESULT_CD'] == '04':
                            temp_dict['EDC_SPLEMNT_UN_MANAGT_COUNT'] += 1
                        # 교육미조치건수
                        elif target_dict['MEASURE_RESULT_CD'] == '02':
                            temp_dict['EDC_UN_MANAGT_COUNT'] += 1
            if target_dict['INSRPS_CMP_ID'] not in temp_dict['TA_EVLU_DICT']:
                temp_ag_dict['TA_EVLU_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                temp_ag_dict['TA_EVLU_COUNT'] += 1
                # 불완전판매인 경우
                if target_dict['LAST_RESULT_CD'] == '08':
                    temp_ag_dict['MIS_SELLING_COUNT'] += 1
                # 이의제기 신청이 아닌 경우
                if target_dict['OBJECTION_CD'] != '01':
                    temp_ag_dict['OBJECTION_COUNT'] += 1
                # 이의제기 승인인 경우
                if target_dict['OBJECTION_CD'] == '03':
                    temp_ag_dict['OBJECTION_CONFM_COUNT'] += 1
                # 조치대상 건수
                if target_dict['MEASURE_RESULT_CD'] in ('02', '03', '04'):
                    temp_ag_dict['MANAGT_TRGET_COUNT'] += 1
                # 조치대상완료건수
                if target_dict['MEASURE_STATUS_CD'] == '06':
                    temp_ag_dict['MANAGT_TRGET_CM_COUNT'] += 1
                # 미조치대상건수
                elif target_dict['MEASURE_STATUS_CD'] == '07':
                    temp_ag_dict['UN_MANGT_TRGET_COUNT'] += 1
                # QA 처리가 완료 된 건
                if str(target_dict['QA_FNM']) != 'None':
                    temp_ag_dict['QA_EVLU_COUNT'] += 1
                    # 불완전판매인 경우
                    if target_dict['LAST_RESULT_CD'] == '08':
                        temp_ag_dict['QA_MIS_SELLING_COUNT'] += 1
        if temp_dict['TA_EVLU_COUNT'] > 0:
            temp_dict['PER_MIS_SELLING'] = (float(temp_dict['MIS_SELLING_COUNT']) / temp_dict['TA_EVLU_COUNT']) * 100
        if temp_dict['QA_EVLU_COUNT'] > 0:
            temp_dict['QA_PER_MIS_SELLING'] = (float(temp_dict['QA_MIS_SELLING_COUNT']) / temp_dict['QA_EVLU_COUNT']) * 100
        tb_statistics_ob_ma_dict[tb_statistics_ob_ma_key] = temp_dict
        tb_statistics_ob_ma_ag_dict[tb_statistics_ob_ma_ag_key] = temp_ag_dict
    util.delete_data_to_tb_statistics_ob_ma(log, oracle, cfg, args.date)
    for item_dict in tb_statistics_ob_ma_dict.values():
        util.insert_data_to_tb_statistics_ob_ma(log, oracle, cfg, item_dict)
    util.delete_data_to_tb_statistics_ob_ma_ag(log, oracle, cfg, args.date)
    for item_dict in tb_statistics_ob_ma_ag_dict.values():
        util.insert_data_to_tb_statistics_ob_ma_ag(log, oracle, cfg, item_dict)


def insert_data_to_db_month(log, oracle, cfg, target_dict_list, args):
    """
    Insert Month data to database
    :param      log:                    Logger
    :param      oracle:                 Oracle
    :param      cfg:                    Config
    :param      target_dict_list:       Target Dictionary List
    :param      args:                   Arguments
    """
    tb_statistics_company_dict = dict()
    tb_statistics_company_ag_dict = dict()
    # 조회된 데이터 정리
    for target_dict in target_dict_list:
        if str(target_dict['TM_MNGT_BZS_C']) == 'None':
            target_dict['TM_MNGT_BZS_C'] = str(target_dict['TM_MNGT_BZS_C'])
        # PK key 생성
        tb_statistics_company_key = '{0}_{1}_{2}'.format(
            args.date, target_dict['TM_MNGT_BZS_C'], target_dict['TM_CNR_C'])
        # PK 체크 및 dict setting
        if tb_statistics_company_key not in tb_statistics_company_dict:
            temp_dict = {
                'STD_MT': args.date,
                'TM_MNGT_BZS_C': target_dict['TM_MNGT_BZS_C'],
                'TM_CNR_C': target_dict['TM_CNR_C'],
                'SELLING_DICT': dict(),
                'SELLING_COUNT': 0,
                'TA_EVLU_DICT': dict(),
                'TA_EVLU_COUNT': 0,
                'TA_TOTAL_SCORE': 0,
                'TA_AVG_SCORE': 0,
                'TA_EVLU_CONFIRM': 0,
                'QA_EVLU_DICT': dict(),
                'QA_EVLU_COUNT': 0,
                'QA_TOTAL_SCORE': 0,
                'QA_AVG_SCORE': 0,
                'QA_EVLU_CONFIRM': 0,
                'MIS_SELLING_COUNT': 0,
                'TOTAL_MIS_SELLING_SCORE': 0,
                'AVG_MIS_SELLING_SCORE': 0,
                'PER_MIS_SELLING': 0,
                'QA_MIS_SELLING_COUNT': 0,
                'QA_TOTAL_MIS_SELLING_COUNT': 0,
                'QA_AVG_MIS_SELLING_SCORE': 0,
                'QA_PER_MIS_SELLING': 0,
                'SCORE_85_OVER_COUNT': 0,
                'SCORE_85_BELO_COUNT': 0,
                'DISCL_MAN_DICT': dict(),
                'DISCL_MAN_COUNT': 0,
                'UNSIGNED_COUNT': 0,
                'SCRIPT_DICT': dict(),
                'SCRIPT_COUNT': 0,
                'FLSHD_FACT_GUIDANCE_DICT': dict(),
                'FLSHD_FACT_GUIDANCD_COUNT': 0,
                'LMTT_SELLING_MARKT_DICT': dict(),
                'LMTT_SELLING_MARKT_COUNT': 0,
                'P_001': 0, 'P_002': 0, 'P_003': 0, 'P_004': 0, 'P_005': 0, 'P_006': 0, 'P_007': 0, 'P_008': 0,
                'P_009': 0, 'P_010': 0, 'P_011': 0, 'P_012': 0, 'P_013': 0, 'P_014': 0, 'P_015': 0, 'P_016': 0,
                'P_017': 0, 'P_018': 0, 'P_019': 0, 'P_020': 0, 'P_021': 0, 'P_022': 0, 'P_023': 0, 'P_024': 0,
                'P_025': 0, 'P_026': 0, 'P_027': 0, 'P_028': 0, 'P_029': 0, 'P_030': 0, 'P_031': 0, 'P_032': 0,
                'P_033': 0, 'P_034': 0
            }
        else:
            temp_dict = tb_statistics_company_dict[tb_statistics_company_key]
        if target_dict['INSRPS_CMP_ID'] not in temp_dict['SELLING_DICT']:
            temp_dict['SELLING_DICT'][target_dict['ISNRPS_CMP_ID']] = 1
            temp_dict['SELLING_COUNT'] += 1
        # TA 정상처리 된 경우
        if target_dict['TA_STATUS_CD'] == '04':
            # 최초발견 1회 실행
            if target_dict['INSRPS_CMP_ID'] not in temp_dict['TA_EVLU_DICT']:
                temp_dict['TA_EVLU_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                temp_dict['TA_EVLU_COUNT'] += 1
                temp_dict['TA_TOTAL_SCORE'] += target_dict['TA_SCORE']
                # 불완전판매인 경우
                if target_dict['LAST_RESULT_CD'] == '08':
                    temp_dict['MIS_SELLING_COUNT'] += 1
                    temp_dict['TOTAL_MIS_SELLING_SCORE'] += target_dict['QA_SCORE']
                    if int(target_dict['QA_SCORE']) >= 85:
                        temp_dict['SCORE_85_OVER_COUNT'] += 1
                    else:
                        temp_dict['SCORE_85_BELO_COUNT'] += 1
                    if target_dict['INSR_SBSCRP_RGR_EMPNO'] not in temp_dict['DISCL_MAN_DICT']:
                        temp_dict['DISCL_MAN_DICT'][target_dict['INSR_SBSCRP_RGR_EMPNO']] = 1
                        temp_dict['DISCL_MAN_COUNT'] += 1
                    tb_statistics_company_ag_key = '{0}_{1}_{2}_{3}_{4}'.format(
                        args.date, target_dict['TM_MNGT_BZS_C'], target_dict['TM_CNR_C'], target_dict['INSRPS_CMP_ID'],
                        target_dict['INSR_SBSCRP_RGR_EMPNO'])
                    if tb_statistics_company_ag_key not in tb_statistics_company_ag_dict:
                        temp_ag_dict = dict()
                        temp_ag_dict['STD_DT'] = args.date
                        temp_ag_dict['TM_MNGT_BZS_C'] = target_dict['TM_MNGT_BZS_C']
                        temp_ag_dict['TM_CNR_C'] = target_dict['TM_CNR_C']
                        temp_ag_dict['INSRPS_CMP_ID'] = target_dict['INSRPS_CMP_ID']
                        temp_ag_dict['AGENT_ID'] = target_dict['INSR_SBSCRP_RGR_EMPNO']
                        temp_ag_dict['AGENT_NM'] = target_dict['INSR_SBSCRP_RGR_FNM']
                        temp_ag_dict['INSRCO_C'] = target_dict['INSRCO_C']
                        temp_ag_dict['INSR_SBSCRP_DT'] = target_dict['INSR_SBSCRP_DT']
                        temp_ag_dict['INSRPS_CST_MNGT_NO'] = target_dict['INSRPS_CST_MNGT_NO']
                        temp_ag_dict['QA_ITEM_SCORE'] = target_dict['QA_SCORE']
                        temp_ag_dict['MEASURE_RESULT_CD'] = target_dict['MEASURE_RESULT_CD']
                        tb_statistics_company_ag_dict[tb_statistics_company_ag_key] = temp_ag_dict
                # 가입미인지인 경우
                if target_dict['UNSIGNED_YN'] == 'Y':
                    temp_dict['UNSIGNED_COUNT'] += 1
            # 발견된 건이 TA처리완료 및 불완전판매일 경우 실행 (key 상관없이 중복체크)
            if target_dict['SUCCESS_YN'] != 'Y' and target_dict['LAST_RESULT_CD'] == '08':
                # 항목별 카운트 및 저장
                if target_dict['EVLU_CRITERIA_CD'] in temp_dict:
                    temp_dict[target_dict['EVLU_CRITERIA_CD']] += 1
                # 청약별 스크립트건수 체크
                if target_dict['CATEGORY_1'] == '스크립트':
                    if target_dict['INSRPS_CMP_ID'] not in temp_dict['SCRIPT_DICT']:
                        temp_dict['SCRIPT_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                        temp_dict['SCRIPT_COUNT'] += 1
                # 청약별 허위사실안내건수 체크
                if target_dict['CATEGORY_2'] == '허위사실안내':
                    if target_dict['INSRPS_CMP_ID'] not in temp_dict['FLSHD_FACT_GUIDANCE_DICT']:
                        temp_dict['FLSHD_FACT_GUIDANCE_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                        temp_dict['FLSHD_FACT_GUIDANCD_COUNT'] += 1
                # 청약별 절판마케팅건수 체크
                if target_dict['CATEGORY_2'] == '절판마케팅':
                    if target_dict['INSRPS_CMP_ID'] not in temp_dict['LMTT_SELLING_MARKT_DICT']:
                        temp_dict['LMTT_SELLING_MARKT_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                        temp_dict['LMTT_SELLING_MARKT_COUNT'] += 1
            # QA 처리가 완료 된 건
            if str(target_dict['QA_FNM']) != 'None':
                # 최초발견 1회 실행
                if target_dict['INSRPS_CMP_ID'] not in temp_dict['QA_EVLU_DICT']:
                    temp_dict['QA_EVLU_DICT'][target_dict['INSRPS_CMP_ID']] = 1
                    temp_dict['QA_EVLU_COUNT'] += 1
                    temp_dict['QA_TOTAL_SCORE'] += target_dict['QA_SCORE']
                    if target_dict['LAST_RESULT_CD'] == '08':
                        temp_dict['QA_MIS_SELLING_COUNT'] += 1
                        temp_dict['QA_TOTAL_MIS_SELLING_COUNT'] += target_dict['QA_SCORE']
        if temp_dict['TA_EVLU_COUNT'] > 0:
            temp_dict['TA_AVG_SCORE'] = round(temp_dict['TA_TOTAL_SCORE'] / temp_dict['TA_EVLU_COUNT'], 2)
        if temp_dict['QA_EVLU_COUNT'] > 0:
            temp_dict['QA_AVG_SCORE'] = round(temp_dict['QA_TOTAL_SCORE'] / temp_dict['QA_EVLU_COUNT'], 2)
            temp_dict['QA_PER_MIS_SELLING'] = (float(
                temp_dict['QA_MIS_SELLING_COUNT']) / temp_dict['QA_EVLU_COUNT']) * 100
        if temp_dict['MIS_SELLING_COUNT'] > 0:
            temp_dict['AVG_MIS_SELLING_SCORE'] = round(
                temp_dict['TOTAL_MIS_SELLING_SCORE'] / temp_dict['MIS_SELLING_COUNT'], 2)
        if temp_dict['QA_MIS_SELLING_COUNT'] > 0:
            temp_dict['QA_AVG_MIS_SELLING_SCORE'] = round(
                temp_dict['QA_TOTAL_MIS_SELLING_SCORE'] / temp_dict['QA_MIS_SELLING_COUNT'], 2)
        if temp_dict['SELLING_COUNT'] > 0:
            temp_dict['TA_EVLU_CONFIRM'] = (float(temp_dict['TA_EVLU_COUNT']) / temp_dict['SELLING_COUNT']) * 100
            temp_dict['QA_EVLU_CONFIRM'] = (float(temp_dict['QA_EVLU_COUNT']) / temp_dict['SELLING_COUNT']) * 100
            temp_dict['PER_MIS_SELLING'] = (float(temp_dict['MIS_SELLING_COUNT']) / temp_dict['SELLING_COUNT']) * 100
        tb_statistics_company_dict[tb_statistics_company_key] = temp_dict
    util.delete_data_to_tb_statistics_company(log, oracle, cfg, args.date)
    for item_dict in tb_statistics_company_dict.values():
        util.insert_data_to_tb_statistics_company(log, oracle, cfg, item_dict)
    util.delete_data_to_tb_statistics_company_ag(log, oracle, cfg, args.date)
    for item_dict in tb_statistics_company_ag_dict.values():
        util.insert_data_to_tb_statistics_company_ag(log, oracle, cfg, item_dict)


def upload_data_to_tb(log, args):
    """
    Upload data to database
    :param      log:        Logger
    :param      args:       Arguments
    """
    log.debug("SELECT and DELETE/INSERT data")
    try:
        oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev')
    except Exception:
        oracle_dev, cfg_dev = util.db_connect(user='mlsta_dev', retry=True)
    log.debug("\tSELECT data -> target : {0}".format(args.date))
    # 조건으로 데이터 조회
    target_dict_list = util.select_summary_target_data(log, oracle_dev, cfg_dev, args.date)
    if args.type == 'M':
        insert_data_to_db_month(log, oracle_dev, cfg_dev, target_dict_list, args)
    elif args.type == 'D':
        insert_data_to_db_days(log, oracle_dev, cfg_dev, target_dict_list, args)
    oracle_dev.disconnect()


def processing(args):
    """
    Processing
    :param      args:       Arguments
    """
    # Set logger
    log = logger.set_logger(
        logger_name=config.SummaryBatchConfig.logger_name,
        log_dir_path=os.path.join(config.SummaryBatchConfig.log_dir_path, START_DATE),
        log_file_name='{0}_{1}.log'.format(config.SummaryBatchConfig.log_file_name, args.type),
        log_level=config.SummaryBatchConfig.log_level
    )
    log.info("[START] Execute Batch ..")
    if args.type not in ('M', 'D'):
        log.error("-t is not available format : {0}".format(args.type))
        sys.exit(1)
    log.info("  Type : {0}".format(args.type))
    if not args.date:
        target_format = '%Y%m' if args.type == 'M' else '%Y%m%d'
        if ST.day == 15 and args.type == 'M':
            target_date = ST - timedelta(days=30)
        elif args.type == 'M':
            log.error("Month Batch process 15 day")
            sys.exit(0)
        else:
            target_date = ST - timedelta(days=14)
        args.date = datetime.strftime(target_date, target_format)
    else:
        if args.type == 'D' and len(args.date) != 8:
            log.error("Day Batch -d option is not available format : {0}".format(args.date))
            log.error("Input target date(optional)\n[ ex) 20181101 ]")
            sys.exit(1)
        if args.type == 'M' and len(args.date) != 6:
            log.error("Month Batch -d option is not available format : {0}".format(args.date))
            log.error("Input target date(optional)\n[ ex) 201811 ]")
            sys.exit(1)
    log.info("  Date : {0}".format(args.date))
    try:
        upload_data_to_tb(log, args)
        log.info("Total type = {0}, date = {1}, The time required = {2}".format(
            args.type, args.date, elapsed_time(ST)))
        log.debug("END.. Start time = {0}, The time required = {1}".format(START_DATE, elapsed_time(ST)))
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print "---------- ERROR ----------"
        log.error(exc_info)
        log.error("---------- ERROR -----------")
        sys.exit(1)


########
# main #
########
def main(args):
    """
    This is a program that [TABLE] data to Oracle DB
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
    parser.add_argument('-t', nargs='?', action='store', dest='type', type=str, default=False, required=True,
                        help="Input Batch Type\n[ ex) (M: Months, D: Days)")
    parser.add_argument('-d', nargs='?', action='store', dest='date', type=str, default=False, required=False,
                        help="Input target date(optional)\n[ ex) 201811 ]")
    arguments = parser.parse_args()
    main(arguments)
