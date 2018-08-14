#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-09-14, modification: 2018-01-12"

###########
# imports #
###########
import os
import re
import sys
import time
import glob
import shutil
import MySQLdb
import traceback
import subprocess
import collections
from datetime import datetime, timedelta
from operator import itemgetter
from cfg.config import MYSQL_DB_CONFIG, CONFIG, MASKING_CONFIG
from lib.iLogger import set_logger
from lib.openssl import decrypt


###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")


#############
# constants #
#############
DT = ''
ST = ''
TARGET_DIR_NAME = ''
TARGET_DIR_PATH = ''
DELETE_FILE_LIST = list()
PCM_CNT = 0
TOTAL_PCM_TIME = 0
RESULT_CNT = 0


#########
# class #
#########
class MySQL(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=MYSQL_DB_CONFIG['host'],
            user=MYSQL_DB_CONFIG['user'],
            passwd=MYSQL_DB_CONFIG['password'],
            db=MYSQL_DB_CONFIG['db'],
            port=MYSQL_DB_CONFIG['port'],
            charset=MYSQL_DB_CONFIG['charset'],
            connect_timeout=MYSQL_DB_CONFIG['connect_timeout']
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)

    def select_rcdg_id_info_to_tb_qa_stt_recinfo(self, logger, rcdg_id, rcdg_file_nm):
        """
        Find Information of recording file using RCDG_ID
        :param      logger:             Logger
        :param      rcdg_id:            Recording id
        :param      rcdg_file_nm:       Recording file name
        :return:                        Information list of rec id
        """
        logger.debug('find recording id information : target -> {0} & {1}'.format(rcdg_id, rcdg_file_nm))
        sql = """
            SELECT
                *
            FROM
                TB_QA_STT_RECINFO
            WHERE 1=1
                AND RCDG_ID = %s
                AND RCDG_FILE_NM = %s
                AND RCDG_TP_CD = 'CS'
        """
        bind = (rcdg_id, rcdg_file_nm, )
        self.cursor.execute(sql, bind)
        row = self.cursor.fetchone()
        if row is bool or not row:
            return False
        return row

    def update_prgst_cd_to_tb_qa_stt_recinfo(self, logger, rcdg_id_info_dic, pk, prgst_cd):
        """
        Update progress code
        :param      logger:             Logger
        :param      rcdg_id_info_dic:   Information dictionary of recording id
        :param      pk:                 Primary key
        :param      prgst_cd:           Progress code
        :return                         Bool
        """
        rcdg_id, rcdg_file_nm, before_prgst_cd = pk.split('####')
        logger.info('Update progress code of Recording ID [{0}] File name [{1}]-> {2}'.format(
            rcdg_id, rcdg_file_nm, prgst_cd))
        rcdg_id_info_dic[pk]['PRGST_CD'] = prgst_cd
        try:
            sql = """
                UPDATE
                    TB_QA_STT_RECINFO
                SET
                    PRGST_CD = %s
                WHERE 1=1
                    AND RCDG_ID = %s
                    AND RCDG_FILE_NM = %s
            """
            bind = (prgst_cd, rcdg_id, rcdg_file_nm, )
            self.cursor.execute(sql, bind)
            if self.cursor.rowcount > 0:
                logger.info('Update is success')
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                exc_info = traceback.format_exc()
                logger.error('Update is Fail')
                logger.error(exc_info)
                return False
        except Exception:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error('Update is Fail')
            logger.error(exc_info)
            return False

    def insert_tb_qa_stt_cs_info(self, logger, output_dict):
        """
        Insert information on TB_QA_STT_CS_INFO table
        :param      logger:             Logger
        :param      output_dict:        Output dictionary
        """
        logger.info('Insert  TB_QA_STT_CS_INFO information of Recording ID [{0}] File name [{1}] wav path [{2}]'.format(
            output_dict['RCDG_ID'], output_dict['RCDG_FILE_NM'], output_dict['RCDG_FILE_PATH_NM']
        ))
        sql = ''
        try:
            sql = """
                INSERT INTO
                    TB_QA_STT_CS_INFO (
                        RCDG_ID,
                        RCDG_FILE_NM,
                        RCDG_FILE_PATH_NM,
                        CHN_TP_CD,
                        RCDG_DT,
                        RCDG_STDTM,
                        RCDG_EDTM,
                        RCDG_CRNC_HMS,
                        STT_PRGST_CD,
                        STT_REQ_DTM,
                        STT_CMDTM,
                        REGP_CD,
                        RGST_PGM_ID,
                        RGST_DTM,
                        LST_CHGP_CD,
                        LST_CHG_PGM_ID,
                        LST_CHG_DTM
                    )
                VALUES
                    (%s, %s, %s, %s, %s, 
                     %s, %s, %s, %s, %s, 
                     %s, 
                     'CS', 'CS', NOW(), 'CS', 'CS', NOW())
                ON DUPLICATE KEY UPDATE
                    RCDG_ID = %s, 
                    RCDG_FILE_NM = %s, 
                    RCDG_FILE_PATH_NM = %s,
                    CHN_TP_CD = %s,
                    RCDG_DT = %s,
                    RCDG_STDTM = %s,
                    RCDG_EDTM = %s,
                    RCDG_CRNC_HMS = %s,
                    STT_PRGST_CD = %s,
                    STT_REQ_DTM = %s,
                    STT_CMDTM = %s,
                    REGP_CD = 'CS',
                    RGST_PGM_ID = 'CS',
                    RGST_DTM = NOW(),
                    LST_CHGP_CD = 'CS',
                    LST_CHG_PGM_ID = 'CS',
                    LST_CHG_DTM = NOW()
            """
            bind = (
                output_dict['RCDG_ID'],
                output_dict['RCDG_FILE_NM'],
                output_dict['RCDG_FILE_PATH_NM'],
                output_dict['CHN_TP_CD'],
                output_dict['RCDG_DT'],
                output_dict['RCDG_STDTM'],
                output_dict['RCDG_EDTM'],
                output_dict['RCDG_CRNC_HMS'],
                output_dict['STT_PRGST_CD'],
                output_dict['STT_REQ_DTM'],
                output_dict['STT_CMDTM'],
            ) * 2
            self.cursor.execute(sql, bind)
            logger.info('Insert is success')
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            exc_info = traceback.format_exc()
            logger.error('Insert is Fail')
            logger.error('sql = {0}'.format(sql))
            logger.error(exc_info)
            raise Exception

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())


#######
# def #
#######
## error 프로세스 만들기

def calculate(seconds):
    if seconds is bool:
        return False
    hour = seconds / 3600
    seconds = seconds % 3600
    minute = seconds / 60
    seconds = seconds % 60
    times = '%02d%02d%02d' % (hour, minute, seconds)
    return times


def masking(input_line_list):
    """
    Masking
    :param          input_line_list:        Input line list
    :return:                                Output dictionary
    """
    line_cnt = 0
    number_rule = MASKING_CONFIG['number_rule']
    birth_rule = MASKING_CONFIG['birth_rule']
    etc_rule = MASKING_CONFIG['etc_rule']
    email_rule = MASKING_CONFIG['email_rule']
    address_rule = MASKING_CONFIG['address_rule']
    name_rule = MASKING_CONFIG['name_rule']
    next_line_cnt = int(MASKING_CONFIG['next_line_cnt'])
    line_dict = collections.OrderedDict()
    for line in input_line_list:
        line = line.strip()
        line_list = line.split("\t")
        sent = line_list[3].strip()
        line_dict[line_cnt] = sent.decode('euc-kr')
        line_cnt += 1
    line_re_rule_dict = collections.OrderedDict()
    for line_num, line in line_dict.items():
        re_rule_dict = dict()
        if u'성함' in line or u'이름' in line:
            if u'확인' in line or u'어떻게' in line or u'여쭤' in line or u'맞으' in line or u'부탁' in line:
                if 'name_rule' not in re_rule_dict:
                    re_rule_dict['name_rule'] = name_rule
        if u'핸드폰' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'휴대폰' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'전화' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'팩스' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'tel_number_rule' not in re_rule_dict:
                    re_rule_dict['tel_number_rule'] = number_rule
        if u'카드' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'card_number_rule' not in re_rule_dict:
                    re_rule_dict['card_number_rule'] = number_rule
        if u'주민' in line and u'번호' in line and u'앞자리' in line:
            if 'id_number_rule' not in re_rule_dict:
                re_rule_dict['id_number_rule'] = birth_rule
        if u'주민' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'id_number_rule' not in re_rule_dict:
                    re_rule_dict['id_number_rule'] = number_rule
        if u'계좌' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'account_number_rule' not in re_rule_dict:
                    re_rule_dict['account_number_rule'] = number_rule
        if u'사업자' in line and u'번호' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'id_number_rule' not in re_rule_dict:
                    re_rule_dict['id_number_rule'] = number_rule
        if u'이메일' in line and u'주소' in line:
            if 'email_rule' not in re_rule_dict:
                re_rule_dict['email_rule'] = email_rule
        if u'주소' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line:
                if 'address_rule' not in re_rule_dict:
                    re_rule_dict['address_rule'] = address_rule
        if u'서울' in line or u'경기' in line or u'부산' in line or u'광주' in line or u'대구' in line or u'울산' in line or u'대전' in line or u'충청' in line or u'충북' in line or u'충남' in line or u'경상' in line or u'경북' in line or u'경남' in line or u'제주' in line:
            if 'address_rule' not in re_rule_dict:
                re_rule_dict['address_rule'] = address_rule
        if u'생년월일' in line:
            if u'확인' in line or u'어떻게' in line or u'말씀' in line or u'부탁' in line or u'여쭤' in line or u'맞으' in line or u'불러' in line or u'구요' in line:
                if 'birth_rule' not in re_rule_dict:
                    re_rule_dict['birth_rule'] = birth_rule
        else:
            if 'etc_rule' not in re_rule_dict:
                re_rule_dict['etc_rule'] = etc_rule

        if line_num in line_re_rule_dict:
            line_re_rule_dict[line_num].update(re_rule_dict)
        else:
            line_re_rule_dict[line_num] = re_rule_dict

        for cnt in range(1, next_line_cnt + 1):
            next_line_num = line_num + cnt
            if next_line_num in line_dict:
                if next_line_num in line_re_rule_dict:
                    line_re_rule_dict[next_line_num].update(re_rule_dict)
                else:
                    line_re_rule_dict[next_line_num] = re_rule_dict
    output_dict = collections.OrderedDict()
    for re_line_num, re_rule_dict in line_re_rule_dict.items():
        if len(line_dict[re_line_num].decode('utf-8')) < int(MASKING_CONFIG['minimum_length']):
            continue
        for rule_name, re_rule in re_rule_dict.items():
            if rule_name == 'name_rule':
                masking_code = "10"
            elif rule_name == 'birth_rule':
                masking_code = "20"
            elif rule_name == 'id_number_rule':
                masking_code = "30"
            elif rule_name == 'card_number_rule':
                masking_code = "40"
            elif rule_name == 'account_number_rule':
                masking_code = "50"
            elif rule_name == 'tel_number_rule':
                masking_code = "60"
            elif rule_name == 'address_rule':
                masking_code = "70"
            elif rule_name == 'email_rule':
                masking_code = "100"
            else:
                masking_code = "110"
            p = re.compile(re_rule.decode('euc-kr'))
            re_result = p.finditer(line_dict[re_line_num].decode('utf-8'))
            output_str = ""
            for item in re_result:
                idx_tuple = item.span()
                start = idx_tuple[0]
                end = idx_tuple[1]
                output_str += "{0},{1},{2};".format(start, end, masking_code)
            if len(output_str) > 0:
                if re_line_num not in output_dict:
                    output_dict[re_line_num] = output_str
                else:
                    output_dict[re_line_num] += output_str
    return output_dict


def mysql_connect():
    """
    Trying Connect to MySQL
    :return:    MySQL
    """
    mysql = False
    for cnt in range(1, 4):
        try:
            mysql = MySQL()
            break
        except Exception as e:
            print e
            if cnt < 3:
                print "Fail connect MySQL, retrying count = {0}".format(cnt)
            continue
    if not mysql:
        raise Exception("Fail connect MySQL")
    return mysql


def check_file(name_form, file_name):
    """
    Check file name
    :param      name_form:      Check file name form
    :param      file_name:      Input file name
    :return:                    True or False
    """
    return file_name.endswith(name_form)


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    """
    logger.info("Command -> {0}".format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for subprocess to finish
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


def modify_time_info(logger, speaker, file_name, output_dict):
    """
    Modify time info
    :param      logger:         Logger
    :param      speaker:        Speaker
    :param      file_name:      File name
    :param      output_dict:    Output dict
    :return:                    Output dict
    """
    for line in file_name:
        try:
            line_list = line.split(',')
            if len(line_list) != 3:
                continue
            st = line_list[0].strip()
            et = line_list[1].strip()
            start_time = str(timedelta(seconds=float(st.replace("ts=", "")) / 100))
            end_time = str(timedelta(seconds=float(et.replace("te=", "")) / 100))
            sent = line_list[2].strip()
            modified_st = st.replace("ts=", "").strip()
            if int(modified_st) not in output_dict:
                output_dict[int(modified_st)] = "{0}\t{1}\t{2}\t{3}".format(speaker, start_time, end_time, sent)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            logger.error("Error modify_time_info")
            logger.error(line)
            logger.error(exc_info)
            continue
    return output_dict


def none_check(argument):
    if 0 == len(argument) is bool:
        return 'None'
    return str(argument)


def get_sec(s):
    """
    Calculate time minus time
    :param      s   time
    :return:        Calculate sec
    """
    l = s.split(':')
    hour = float(l[0]) * 3600
    minute = float(l[1]) * 60
    seconds = float(l[2])
    return hour + minute + seconds


def make_stt_info_table_txt(logger, pk, info_dic, sftp_set_dir_path, mysql):
    """
    Make information text of TB_QA_STT_CS_INFO table
    :param      logger:                 Logger
    :param      pk:                     Primary key
    :param      info_dic:               Information dictionary
    :param      sftp_set_dir_path:      Directory of sftp set
    :param      mysql:                  MySQL
    """
    try:
        db_upload_dir_path = '{0}/db_upload'.format(TARGET_DIR_PATH)
        if not os.path.exists(db_upload_dir_path):
            logger.info('8-1.Create db_upload txt of TB_QA_STT_CS_INFO table')
            os.makedirs(db_upload_dir_path)
        info_dir_path = '{0}/TB_QA_STT_CS_INFO'.format(db_upload_dir_path)
        if not os.path.exists(info_dir_path):
            os.makedirs(info_dir_path)
        sftp_info_dir_path = '{0}.tmp/TB_QA_STT_CS_INFO'.format(sftp_set_dir_path)
        if not os.path.exists(sftp_info_dir_path):
            os.makedirs(sftp_info_dir_path)
        # ready for db upload information setting
        ts = time.time()
        rcdg_id, rcdg_file_nm, before_prgst_cd = pk.split('####')
        rec_stdt = str(info_dic['REC_STDT'])
        rcdg_crnc_hms = calculate(int(info_dic['DURATION_HMS']))
        # db upload information setting
        output_dict = collections.OrderedDict()
        output_dict['RCDG_ID'] = rcdg_id
        output_dict['RCDG_FILE_NM'] = rcdg_file_nm
        output_dict['RCDG_FILE_PATH_NM'] = '{0}/{1}/{2}/{3}.wav'.format(
            rec_stdt[:4], rec_stdt[5:7], rec_stdt[8:10], info_dic['rcdg_file_name'])
        output_dict['CHN_TP_CD'] = none_check(info_dic['CHN_TP_CD'])
        output_dict['RCDG_DT'] = none_check('{0}{1}{2}'.format(rec_stdt[:4], rec_stdt[5:7], rec_stdt[8:10]))
        output_dict['RCDG_STDTM'] = str(info_dic['REC_SDTM'])
        output_dict['RCDG_EDTM'] = str(info_dic['REC_EDTM'])
        output_dict['RCDG_CRNC_HMS'] = none_check(rcdg_crnc_hms)
        output_dict['STT_PRGST_CD'] = none_check(info_dic['PRGST_CD'])
        output_dict['STT_REQ_DTM'] = '{0}-{1}-{2} {3}:{4}:{5}'.format(
            DT[:4], DT[4:6], DT[6:8], DT[8:10], DT[10:12], DT[12:14])
        output_dict['STT_CMDTM'] = str(datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'))
        mysql.insert_tb_qa_stt_cs_info(logger, output_dict)
        insert_data = '\t'.join(output_dict.values())
        # Create db upload txt
        txt_file_path = '{0}/{1}_{2}_TB_QA_STT_CS_INFO.txt'.format(info_dir_path, info_dic['rcdg_file_name'], rcdg_id)
        sftp_txt_file_path = '{0}/{1}_{2}_TB_QA_STT_CS_INFO.txt'.format(
            sftp_info_dir_path, info_dic['rcdg_file_name'], rcdg_id)
        txt_file = open(txt_file_path, 'w')
        sftp_txt_file = open(sftp_txt_file_path, 'w')
        print >> txt_file, insert_data
        print >> sftp_txt_file, insert_data
        txt_file.close()
        sftp_txt_file.close()
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        raise Exception


def make_stt_result_table_txt(logger, pk, info_dic, sftp_set_dir_path):
    """
    Make information text of TB_QA_STT_CS_RST table
    :param      logger:                 Logger
    :param      pk:                     Primary key
    :param      info_dic:               Information dictionary
    :param      sftp_set_dir_path       Sftp set directory
    """
    rst_dir_path = '{0}/db_upload/TB_QA_STT_CS_RST'.format(TARGET_DIR_PATH)
    sftp_rst_dir_path = '{0}.tmp/TB_QA_STT_CS_RST'.format(sftp_set_dir_path)
    if not os.path.exists(rst_dir_path):
        logger.info('8-2. Create db upload txt of TB_QA_STT_CS_RST table')
        os.makedirs(rst_dir_path)
    if not os.path.exists(sftp_rst_dir_path):
        os.makedirs(sftp_rst_dir_path)
    # ready for db upload information setting
    stt_sntc_lin_no = -1
    # detail file analysis
    detail_file_path = '{0}/detail/{1}_trx.detail'.format(TARGET_DIR_PATH, info_dic['rcdg_file_name'])
    detail_file = open(detail_file_path, 'r')
    lines = detail_file.readlines()
    masking_output_dict = masking(lines)
    txt_file_path = '{0}/{1}_{2}_TB_QA_STT_CS_RST.txt'.format(
        rst_dir_path, info_dic['rcdg_file_name'], info_dic['RCDG_ID'])
    sftp_txt_file_path = '{0}/{1}_{2}_TB_QA_STT_CS_RST.txt'.format(
        sftp_rst_dir_path, info_dic['rcdg_file_name'], info_dic['RCDG_ID'])
    for line in lines:
        # ready for db upload information setting
        line = line.split('\t')
        start_time = line[1]
        end_time = line[2]
        sntc_sttm = start_time.split('.')[0].replace(':', '')
        sntc_endtm = end_time.split('.')[0].replace(':', '')
        during_time = get_sec(end_time) - get_sec(start_time)
        if during_time == 0:
            during_time = 0.01
        speaker = line[0][1]
        # db upload information setting
        stt_sntc_lin_no += 1
        rcdg_id, rcdg_file_nm, before_prgst_cd = pk.split('####')
        chn_tp_cd = none_check(info_dic['CHN_TP_CD'])
        sntc_cont = line[3].replace('\n', '')
        sntc_len = len(sntc_cont.replace(' ', '').decode('euc_kr'))
        sntc_sttm = sntc_sttm if len(sntc_sttm) == 6 else '0' + sntc_sttm
        sntc_endtm = sntc_endtm if len(sntc_endtm) == 6 else '0' + sntc_endtm
        sntc_spch_hms = str(round(during_time, 2))
        sntc_spch_sped = str(round(float(sntc_len)/during_time, 2))
        stt_sntc_spkr_dcd = 'S' if speaker == 'A' else speaker
        msk_dtd_yn = 'Y' if stt_sntc_lin_no in masking_output_dict else 'N'
        msk_info_lit = str(masking_output_dict[stt_sntc_lin_no]).replace("'", '"') if msk_dtd_yn == 'Y' else 'None'
        insert_data = '{STT_SNTC_LIN_NO}\t{RCDG_ID}\t{RCDG_FILE_NM}\t{CHN_TP_CD}\t{SNTC_CONT}\t{SNTC_LEN}\t' \
                      '{SNTC_STTM}\t{SNTC_ENDTM}\t{SNTC_SPCH_HMS}\t{SNTC_SPCH_SPED}\t{STT_SNTC_SPKR_DCD}\t' \
                      '{MSK_DTC_YN}\t{MSK_INFO_LIT}'.format(
            STT_SNTC_LIN_NO=stt_sntc_lin_no, RCDG_ID=rcdg_id, RCDG_FILE_NM=rcdg_file_nm, CHN_TP_CD=chn_tp_cd,
            SNTC_CONT=sntc_cont, SNTC_LEN=sntc_len, SNTC_STTM=sntc_sttm, SNTC_ENDTM=sntc_endtm,
            SNTC_SPCH_HMS=sntc_spch_hms, SNTC_SPCH_SPED=sntc_spch_sped, STT_SNTC_SPKR_DCD=stt_sntc_spkr_dcd,
            MSK_DTC_YN=msk_dtd_yn, MSK_INFO_LIT=msk_info_lit)
        # Create db upload txt
        sftp_txt_file = open(sftp_txt_file_path, 'a')
        txt_file = open(txt_file_path, 'a')
        print >> txt_file, insert_data
        print >> sftp_txt_file, insert_data
        txt_file.close()
        sftp_txt_file.close()
    if not os.path.exists(txt_file_path):
        sftp_txt_file = open(sftp_txt_file_path, 'a')
        txt_file = open(txt_file_path, 'a')
        sftp_txt_file.close()
        txt_file.close()


def make_delete_target_cs_txt(logger, info_dic, sftp_set_dir_path):
    """
    Make primary key list text of delete target
    :param      logger:                 Logger
    :param      info_dic:               Information dictionary
    :param      sftp_set_dir_path       Sftp set directory
    """
    rec_stdt = str(info_dic['REC_STDT'])
    rec_path = '{0}/{1}/{2}'.format(rec_stdt[:4], rec_stdt[5:7], rec_stdt[8:10])
    delete_dir_path = '{0}/{1}/db_upload/DELETE_TARGET'.format(CONFIG['stt_output_path'], rec_path)
    sftp_set_dir_name = os.path.basename(sftp_set_dir_path)
    dir_path = '{0}/{1}'.format(delete_dir_path, sftp_set_dir_name)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    del_dir_path = '{0}/DELETE_TARGET_CS'.format(dir_path)
    sftp_del_dir_path = '{0}.tmp/DELETE_TARGET_CS'.format(sftp_set_dir_path)
    if not os.path.exists(del_dir_path):
        logger.info('8-3. Create db delete txt of DELETE_TARGET_CS')
        os.makedirs(del_dir_path)
    if not os.path.exists(sftp_del_dir_path):
        os.makedirs(sftp_del_dir_path)
    # db upload information setting
    output_dict = collections.OrderedDict()
    output_dict['RCDG_ID'] = info_dic['RCDG_ID']
    output_dict['RCDG_FILE_NM'] = info_dic['rcdg_file_name']
    insert_data = '\t'.join(output_dict.values())
    # Create db upload txt
    txt_file_path = '{0}/{1}_{2}.txt'.format(del_dir_path, info_dic['RCDG_ID'], info_dic['rcdg_file_name'])
    sftp_txt_file_path = '{0}/{1}_{2}.txt'.format(sftp_del_dir_path, info_dic['RCDG_ID'], info_dic['rcdg_file_name'])
    txt_file = open(txt_file_path, 'a')
    sftp_txt_file = open(sftp_txt_file_path, 'a')
    print >> txt_file, insert_data
    print >> sftp_txt_file, insert_data
    txt_file.close()
    sftp_txt_file.close()


def elapsed_time(sdate):
    """
    elapsed time
    :param      sdate:      date object
    :return:                Required time (type : datetime)
    """
    end_time = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    start_time = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]), int(sdate[8:10]), int(sdate[10:12]),
                          int(sdate[12:14]))
    required_time = end_time - start_time
    return required_time


def statistical_data(logger):
    """
    Statistical data to print
    :param      logger:     Logger
    :return:    logger:     Logger
    """
    required_time = elapsed_time(DT)
    end_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H:%M.%S')
    total_wav_duration = timedelta(seconds=TOTAL_PCM_TIME)
    if PCM_CNT == 0:
        division_pcm_cnt = 1
    else:
        division_pcm_cnt = PCM_CNT
    total_wav_average_duration = timedelta(seconds=TOTAL_PCM_TIME / float(division_pcm_cnt))
    xrt = (int(timedelta(seconds=TOTAL_PCM_TIME).total_seconds() / required_time.total_seconds()))
    logger.info('10. Statistical data print')
    logger.info('Start time                 = {0}'.format(ST))
    logger.info('End time                   = {0}'.format(end_time))
    logger.info('The time required          = {0}'.format(required_time))
    logger.info('WAV count                  = {0}'.format(PCM_CNT))
    logger.info('Result count               = {0}'.format(RESULT_CNT))
    logger.info('Total WAV duration         = {0}'.format(total_wav_duration))
    logger.info('Total WAV average duration = {0}'.format(total_wav_average_duration))
    logger.info('xRT                        = {0} xRT'.format(xrt))
    logger.info('Done CS')
    logger.info('Remove logger handler')
    logger.info('CS END.. Start time = {0}, The time required = {1}'.format(ST, elapsed_time(DT)))


def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param      logger:             Logger
    :param      delete_file_path:   Input path
    """
    if os.path.exists(delete_file_path):
        # noinspection PyBroadException
        try:
            if os.path.isfile(delete_file_path):
                logger.debug('delete file -> {0}'.format(delete_file_path))
                os.remove(delete_file_path)
            if os.path.isdir(delete_file_path):
                logger.debug('delete directory -> {0}'.format(delete_file_path))
                shutil.rmtree(delete_file_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't delete {0}".format(delete_file_path))
            logger.error(exc_info)


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info('10. Delete garbage file')
    for list_file in DELETE_FILE_LIST:
        try:
            logger.info('del_garbage : {0}'.format(list_file))
            del_garbage(logger, list_file)
        except Exception as e:
            logger.error(e)
            continue


def move_output(logger, rcdg_id_info_dic, mysql):
    """
    Move output to CS output path
    :param      logger:             Logger
    :param      rcdg_id_info_dic:   Information dictionary of Recording id
    :param      mysql:              MySQL
    """
    logger.info("9. Move output to CS output path")
    # Create an output directory for each recording file and move the file.
    for pk in rcdg_id_info_dic:
        # Create an output directory
        rec_stdt = str(rcdg_id_info_dic[pk]['REC_STDT'])
        # Make sure the wav directory exists.
        wav_output_path = '{0}/{1}/{2}/{3}/'.format(
            CONFIG['wav_output_path'], rec_stdt[:4], rec_stdt[5:7], rec_stdt[8:10])
        if not os.path.exists(wav_output_path):
            os.makedirs(wav_output_path)
        output_dir_path = '{0}/{1}/{2}/{3}'.format(CONFIG['stt_output_path'], rec_stdt[:4], rec_stdt[5:7],
                                                   rec_stdt[8:10])
        output_list = ['mlf', 'unseg', 'do_space', 'txt', 'detail', 'result', 'db_upload/TB_QA_STT_CS_INFO',
                       'db_upload/TB_QA_STT_CS_RST']
        if not os.path.exists(output_dir_path + '/db_upload'):
            os.makedirs(output_dir_path + '/db_upload')
        # Move the file
        for target in output_list:
            output_target_path = '{0}/{1}'.format(output_dir_path, target)
            if not os.path.exists(output_target_path):
                os.makedirs(output_target_path)
            path_list = glob.glob('{0}/{1}/{2}*'.format(
                TARGET_DIR_PATH, target, rcdg_id_info_dic[pk]['rcdg_file_name']))
            for path in path_list:
                file_name = os.path.basename(path)
                if os.path.exists('{0}/{1}'.format(output_target_path, file_name)):
                    del_garbage(logger, '{0}/{1}'.format(output_target_path, file_name))
                logger.debug('move file {0} -> {1}'.format(path, output_target_path))
                shutil.move(path, output_target_path)
        # Move the wav file
        if os.path.exists('{0}/{1}.wav'.format(wav_output_path, rcdg_id_info_dic[pk]['rcdg_file_name'])):
            del_garbage(logger, '{0}/{1}.wav'.format(wav_output_path, rcdg_id_info_dic[pk]['rcdg_file_name']))
        wav_file_path = '{0}/{1}.wav'.format(TARGET_DIR_PATH, rcdg_id_info_dic[pk]['rcdg_file_name'])
        if not os.path.exists(wav_file_path):
            # 파일 떨구기
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, '03'):
                raise Exception
            continue
        else:
            shutil.move(wav_file_path, wav_output_path)


def make_db_upload_output(logger, rcdg_id_info_dic, mysql, sftp_set_dir_path):
    """
    Make DB upload output
    :param      logger:                 Logger
    :param      rcdg_id_info_dic        Information dictionary of Recording id
    :param      mysql:                  MySQL
    :param      sftp_set_dir_path:      Directory path of sftp set
    :return:                            Modify Information dictionary of Recording id
    """
    logger.info('8. Make DB upload output')
    del_rcdg_id_list = list()
    # Create DB upload file.
    for pk in rcdg_id_info_dic:
        rcdg_id, rcdg_file_name, before_prgst_cd = pk.split('####')
        # noinspection PyBroadException
        try:
            rcdg_id_info_dic[pk]['PRGST_CD'] = '05'
            # 8-1. Create db_upload txt of TB_QA_STT_CS_INFO table
            make_stt_info_table_txt(logger, pk, rcdg_id_info_dic[pk], sftp_set_dir_path, mysql)
            # 8-2. Create db upload txt of TB_QA_STT_CS_RST table
            make_stt_result_table_txt(logger, pk, rcdg_id_info_dic[pk], sftp_set_dir_path)
            # 8-3. Create db delete txt of DELETE_TARGET_CS
            if '90' == before_prgst_cd:
                make_delete_target_cs_txt(logger, rcdg_id_info_dic[pk], sftp_set_dir_path)
        except Exception:
            rcdg_id_info_dic[pk]['PRGST_CD'] = '03'
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, '03'):
                raise Exception
            make_stt_info_table_txt(logger, pk, rcdg_id_info_dic[pk], sftp_set_dir_path, mysql)
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            delete_garbage_file(logger)
    logger.info('rename {0}.tmp -> {0}'.format(sftp_set_dir_path))
    os.rename('{0}.tmp'.format(sftp_set_dir_path), sftp_set_dir_path)
    # Recreation of recording file dictionary
    for pk in del_rcdg_id_list:
        del rcdg_id_info_dic[pk]
    return rcdg_id_info_dic


def set_output(logger):
    """
    Set output directory
    :param      logger:     Logger
    """
    global RESULT_CNT
    logger.info("7. Set output directory")
    file_path_list = glob.glob('{0}/*'.format(TARGET_DIR_PATH))
    pcm_dir_path = '{0}/pcm'.format(TARGET_DIR_PATH)
    result_dir_path = '{0}/result'.format(TARGET_DIR_PATH)
    # Moving files
    if not os.path.exists(pcm_dir_path):
        os.makedirs(pcm_dir_path)
    if not os.path.exists(result_dir_path):
        os.makedirs(result_dir_path)
    for file_path in file_path_list:
        if check_file('.pcm', file_path):
            shutil.move(file_path, pcm_dir_path)
        if check_file('.result', file_path):
            shutil.move(file_path, result_dir_path)
    # Calculate result value
    RESULT_CNT = len(glob.glob('{0}/*.result'.format(result_dir_path)))


def make_output(logger, rcdg_id_info_dic, do_space_dir_path):
    """
    Make txt file and detail file
    :param      logger:                 Logger
    :param      rcdg_id_info_dic:       Information dictionary of Recording id
    :param      do_space_dir_path:      directory path of do space result
    """
    logger.info('6. Make output [txt file and detailed file]')
    # Create directory
    logger.info('Create directory')
    txt_dir_path = '{0}/txt'.format(TARGET_DIR_PATH)
    detail_dir_path = '{0}/detail'.format(TARGET_DIR_PATH)
    if not os.path.exists(txt_dir_path):
        os.makedirs(txt_dir_path)
    if not os.path.exists(detail_dir_path):
        os.makedirs(detail_dir_path)
    # Create txt & detail file
    logger.info('Create txt & detail file')
    for pk in rcdg_id_info_dic:
        output_dict = dict()
        # Check that both stt files exist.
        logger.info('Check that two stt files exist')
        rx_file_path = '{0}/{1}_rx.stt'.format(do_space_dir_path, rcdg_id_info_dic[pk]['rcdg_file_name'])
        tx_file_path = '{0}/{1}_tx.stt'.format(do_space_dir_path, rcdg_id_info_dic[pk]['rcdg_file_name'])
        if os.path.exists(rx_file_path) and os.path.exists(tx_file_path):
            logger.info('two stt files are exist')
            # Save the necessary information.
            rx_file = open(rx_file_path, 'r')
            tx_file = open(tx_file_path, 'r')
            output_dict = modify_time_info(logger, '[A]', tx_file, output_dict)
            output_dict = modify_time_info(logger, '[C]', rx_file, output_dict)
            tx_file.close()
            rx_file.close()
        else:
            logger.error("{0} don't have tx or rx file.".format(rcdg_id_info_dic[pk]['rcdg_file_name']))
            continue
        # Detailed txt & detail file creation.
        output_dict_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
        txt_output_file = open('{0}/{1}_trx.txt'.format(txt_dir_path, rcdg_id_info_dic[pk]['rcdg_file_name']), 'w')
        detail_output_file = open(
            '{0}/{1}_trx.detail'.format(detail_dir_path, rcdg_id_info_dic[pk]['rcdg_file_name']), 'w')
        for line_list in output_dict_list:
            detail_line = line_list[1]
            detail_line_list = detail_line.split('\t')
            trx_txt = "{0}{1}".format(detail_line_list[0], detail_line_list[3])
            print >> txt_output_file, trx_txt
            print >> detail_output_file, detail_line
        txt_output_file.close()
        detail_output_file.close()


def execute_unseg_and_do_space(logger, mysql, rcdg_id_info_dic):
    """
    Execute unseg.exe and do_space.exe
    :param      logger:                 Logger
    :param      mysql:                  Mysql
    :param      rcdg_id_info_dic        Information dictionary of recording id
    :return:    Output                  directory path
    """
    logger.info("5. Execute unseg.exe and do_space.exe")
    mlf_file_path_list = glob.glob('{0}/*.mlf'.format(TARGET_DIR_PATH))
    target_cnt = len(rcdg_id_info_dic.keys())
    mlf_cnt = len(mlf_file_path_list)
    del_rcdg_list = list()
    if not target_cnt*2 == mlf_cnt:
        logger.info(' mt_long Engine error occurred')
        for pk in rcdg_id_info_dic:
            rcdg_id, rcdg_file_nm, before_prgst_cd = pk.split('####')
            search_target_mlf = glob.glob('{0}/{1}*.mlf'.format(
                TARGET_DIR_PATH, rcdg_id_info_dic[pk]['rcdg_file_name']))
            if len(search_target_mlf) == 2:
                continue
            logger.info('  rcdg_id: {0}    rcdg_file_name : {1}'.format(rcdg_id, rcdg_file_nm))
            logger.info('   create mlf file : {0}'.format(search_target_mlf))
            mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, before_prgst_cd)
            del_rcdg_list.append(pk)
    for pk in del_rcdg_list:
        del rcdg_id_info_dic[pk]
    mlf_dir_path = '{0}/mlf'.format(TARGET_DIR_PATH)
    unseg_dir_path = '{0}/unseg'.format(TARGET_DIR_PATH)
    do_space_dir_path = '{0}/do_space'.format(TARGET_DIR_PATH)
    os.chdir(CONFIG['stt_tool_path'])
    # Moving the mlf file
    logger.info('Moving the mlf file')
    if not os.path.exists(mlf_dir_path):
        os.makedirs(mlf_dir_path)
    for mlf_file_path in mlf_file_path_list:
        # noinspection PyBroadException
        try:
            shutil.move(mlf_file_path, mlf_dir_path)
        except Exception:
            exc_info = traceback.format_exc()
            logger.error("Can't move mlf file {0} -> {1}".format(mlf_file_path, mlf_dir_path))
            logger.error(exc_info)
            continue
    # Run ./unseg.exe
    logger.info('Run ./unseg.exe')
    if not os.path.exists(unseg_dir_path):
        os.makedirs(unseg_dir_path)
    unseg_cmd = './unseg.exe -d {mp} {up} 300'.format(mp=mlf_dir_path, up=unseg_dir_path)
    sub_process(logger, unseg_cmd)
    # Run ./do_space.exe
    logger.info('Run ./do_space.exe')
    if not os.path.exists(do_space_dir_path):
        os.makedirs(do_space_dir_path)
    do_space_cmd = './do_space.exe {up} {dp}'.format(up=unseg_dir_path, dp=do_space_dir_path)
    sub_process(logger, do_space_cmd)
    return do_space_dir_path, rcdg_id_info_dic


def execute_dnn(logger, thread_cnt):
    """
    Execute DNN (mt_long_utt_dnn_support.gpu.exe)
    :param      logger:         Logger
    :param      thread_cnt:     Thread count
    """
    logger.info("4. Execute DNN (mt_long_utt_dnn_support.gpu.exe)")
    os.chdir(CONFIG['stt_path'])
    dnn_thread = thread_cnt if thread_cnt < CONFIG['thread'] else CONFIG['thread']
    cmd = "./mt_long_utt_dnn_support.gpu.exe {tn} {th} 1 1 {gpu} 128 0.8".format(
        tn=TARGET_DIR_NAME, th=dnn_thread, gpu=CONFIG['gpu'])
    sub_process(logger, cmd)


def make_list_file(logger):
    """
    Make list file
    :param      logger:     Logger
    :return:                Thread count
    """
    global PCM_CNT
    global TOTAL_PCM_TIME
    global DELETE_FILE_LIST
    logger.info("3. Do make list file")
    list_file_cnt = 0
    max_list_file_cnt = 0
    w_ob = os.walk(TARGET_DIR_PATH)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            check_file_name = os.path.splitext(file_name)[0][:-3]
            rx_pcm_file_path = '{0}/{1}_rx.pcm'.format(TARGET_DIR_PATH, check_file_name)
            tx_pcm_file_path = '{0}/{1}_tx.pcm'.format(TARGET_DIR_PATH, check_file_name)
            logger.debug('rx: {0}'.format(rx_pcm_file_path))
            logger.debug('tx: {0}'.format(tx_pcm_file_path))
            # Runs if two pcm files exist.
            if os.path.exists(rx_pcm_file_path) and os.path.exists(tx_pcm_file_path):
                logger.info('tx and rx pcm file is exist -> {0}'.format(check_file_name))
                rx_s16_file_path = '{0}.s16'.format(os.path.splitext(rx_pcm_file_path)[0])
                tx_s16_file_path = '{0}.s16'.format(os.path.splitext(tx_pcm_file_path)[0])
                # Change the extension name of the pcm file.
                logger.info('Change the extension name of the pcm file -> {0} -> s16 file'.format(check_file_name))
                os.rename(rx_pcm_file_path, rx_s16_file_path)
                os.rename(tx_pcm_file_path, tx_s16_file_path)
                # Merge the two files.
                trx_s16_file_path = '{0}/{1}_trx.s16'.format(TARGET_DIR_PATH, check_file_name)
                logger.info('Merge the two files -> {0} + {1} = {2}'.format(
                    rx_s16_file_path, tx_s16_file_path, trx_s16_file_path))
                sox_cmd = 'sox -M -r 8000 -c 1 {0} -r 8000 -c 1 {1} -r 8000 -c 1 {2}'.format(
                    rx_s16_file_path, tx_s16_file_path, trx_s16_file_path)
                sub_process(logger, sox_cmd)
                # wav file creation
                logger.debug('wav file creation')
                wav_file_path = '{0}/{1}.wav'.format(TARGET_DIR_PATH, check_file_name)
                sox_cmd = 'sox -r 8000 -c 1 {0} -r 8000 -c 1 -e gsm {1}'.format(trx_s16_file_path, wav_file_path)
                sub_process(logger, sox_cmd)
                # Change the extension name of the s16 file.
                logger.debug('Change the extension name of the s16 file -> {0} -> pcm file'.format(TARGET_DIR_PATH))
                cmd = 'rename .s16 .pcm {0}/*'.format(TARGET_DIR_PATH)
                sub_process(logger, cmd)
                # Enter the PCM file name in the List file.
                logger.debug('Enter the PCM file name in the List file')
                list_file_path = "{sp}/{tn}_n{cnt}.list".format(
                    sp=CONFIG['stt_path'], tn=TARGET_DIR_NAME, cnt=list_file_cnt)
                curr_list_file_path = "{sp}/{tn}_n{cnt}_curr.list".format(
                    sp=CONFIG['stt_path'], tn=TARGET_DIR_NAME, cnt=list_file_cnt)
                DELETE_FILE_LIST.append(list_file_path)
                DELETE_FILE_LIST.append(curr_list_file_path)
                output_file_div = open(list_file_path, 'a')
                print >> output_file_div, tx_pcm_file_path
                print >> output_file_div, rx_pcm_file_path
                output_file_div.close()
                # Calculate the result value.
                logger.debug('Calculate the result value')
                PCM_CNT += 2
                TOTAL_PCM_TIME += os.stat(tx_pcm_file_path)[6] / 16000.0
                TOTAL_PCM_TIME += os.stat(rx_pcm_file_path)[6] / 16000.0
                # Calculate the thread
                if list_file_cnt > max_list_file_cnt:
                    max_list_file_cnt = list_file_cnt
                if list_file_cnt + 1 == CONFIG['thread']:
                    list_file_cnt = 0
                    continue
                list_file_cnt += 1
    # Last Calculate the thread count
    logger.debug('Calculate the thread count')
    if max_list_file_cnt == 0:
        thread_cnt = 1
    else:
        thread_cnt = max_list_file_cnt + 1
    return thread_cnt


def load_pcm_file(logger, mysql, rcdg_id_info_dic):
    """
    Load pcm file
    :param      logger:             Logger
    :param      mysql:              MySQL
    :param      rcdg_id_info_dic:   Information dictionary of recording id
    :return:                        modified Information dictionary of recording id
    """
    logger.info("2. Load pcm file")
    del_rcdg_id_list = list()
    for pk in rcdg_id_info_dic:
        rcdg_id, rcdg_file_nm, before_prgst_cd = pk.split('####')
        # pcm directory exist check
        rec_stdt = str(rcdg_id_info_dic[pk]['REC_STDT'])
        rec_stdt = rec_stdt[:4] + rec_stdt[5:7] + rec_stdt[8:10]
        logger.debug('Recording start date -> {0}'.format(rec_stdt))
        pcm_dir_path = '{0}/{1}'.format(CONFIG['rec_dir_path'], rec_stdt)
        incident_dir_path = '{0}/incident_file'.format(CONFIG['rec_dir_path'])
        if not os.path.exists(pcm_dir_path) and not os.path.exists(incident_dir_path):
            logger.error('pcm directory is not exist -> {0} or {1}'.format(pcm_dir_path, incident_dir_path))
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, before_prgst_cd):
                raise Exception
            del_rcdg_id_list.append(pk)
            continue
        # All pcm file exist check
        rx_file_path = '{0}/{1}.rx.enc'.format(pcm_dir_path, rcdg_id_info_dic[pk]['RCDG_FILE_NM'])
        tx_file_path = '{0}/{1}.tx.enc'.format(pcm_dir_path, rcdg_id_info_dic[pk]['RCDG_FILE_NM'])
        rx_incident_file_path = '{0}/{1}.rx.enc'.format(incident_dir_path, rcdg_id_info_dic[pk]['RCDG_FILE_NM'])
        tx_incident_file_path = '{0}/{1}.tx.enc'.format(incident_dir_path, rcdg_id_info_dic[pk]['RCDG_FILE_NM'])
        rename_rx_pcm_file_path = '{0}/{1}_rx.pcm.enc'.format(TARGET_DIR_PATH, rcdg_id_info_dic[pk]['rcdg_file_name'])
        rename_tx_pcm_file_path = '{0}/{1}_tx.pcm.enc'.format(TARGET_DIR_PATH, rcdg_id_info_dic[pk]['rcdg_file_name'])
        if os.path.exists(rx_file_path) and os.path.exists(tx_file_path):
            logger.debug('All pcm file is exists in rec_server')
            shutil.copy(rx_file_path, rename_rx_pcm_file_path)
            shutil.copy(tx_file_path, rename_tx_pcm_file_path)
        elif os.path.exists(rx_incident_file_path) and os.path.exists(tx_incident_file_path):
            logger.debug('All pcm file is exists in incident_file')
            shutil.copy(rx_incident_file_path, rename_rx_pcm_file_path)
            shutil.copy(tx_incident_file_path, rename_tx_pcm_file_path)
        else:
            logger.debug('pcm file is not exists')
            logger.error(' {0} is not exist in rec_server'.format(rcdg_file_nm))
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, before_prgst_cd):
                raise Exception
            del_rcdg_id_list.append(pk)
            continue
    # Remove information of nonexistent primary key from the dictionary
    for pk in del_rcdg_id_list:
        del rcdg_id_info_dic[pk]
    # Decryption for use of pcm files
    decrypt(TARGET_DIR_PATH)
    return rcdg_id_info_dic


def get_rcdg_info_dic(logger, mysql, pk_list, sftp_set_dir_path):
    """
    Get recording information dictionary for recording id list
    :param      logger:                 Logger
    :param      mysql:                  MySQL
    :param      pk_list:                List of Primary key
    :param      sftp_set_dir_path:      Directory path of sftp set
    :return:                            Information dictionary of recording id
    """
    logger.info('1. Get recording information dictionary')
    logger.info(' load primary key list -> {0}'.format(pk_list))
    # Creating recording file dictionary
    rcdg_id_info_dic = dict()
    for pk in pk_list:
        rcdg_id, rcdg_file_nm, before_prgst_cd = pk.split('####')
        # noinspection PyBroadException
        try:
            row = mysql.select_rcdg_id_info_to_tb_qa_stt_recinfo(logger, rcdg_id, rcdg_file_nm)
            logger.debug('rcdg id [{0}] information'.format(rcdg_id))
            rcdg_id_info_dic[pk] = {
                'RCDG_ID': row.get('RCDG_ID'),
                'RCDG_FILE_NM': row.get('RCDG_FILE_NM'),
                'rcdg_file_name': row.get('RCDG_FILE_NM').replace('.', '_'),
                'CHN_TP_CD': row.get('CHN_TP_CD'),
                'RCDG_TP_CD': row.get('RCDG_TP_CD'),
                'PRGST_CD': row.get('PRGST_CD'),
                'DURATION_HMS': row.get('DURATION_HMS'),
                'REC_STDT': row.get('REC_STDT'),
                'REC_SDTM': row.get('REC_SDTM'),
                'REC_EDTM': row.get('REC_EDTM'),
                'USER_ID': row.get('USER_ID'),
                'EXT_NO': row.get('EXT_NO'),
                'REGP_CD': row.get('REGP_CD'),
                'RGST_PGM_ID': row.get('RGST_PGM_ID'),
                'RGST_DTM': row.get('RGST_DTM'),
                'LST_CHGP_CD': row.get('LST_CHGP_CD'),
                'LST_CHG_PGM_ID': row.get('LST_CHG_PGM_ID'),
                'LST_CHG_DTM': row.get('LST_CHG_DTM')
            }
            logger.debug(rcdg_id_info_dic)
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, '02'):
                raise Exception
        except Exception:
            exc_info = traceback.format_exc()
            logger.error(exc_info)
            logger.error("Can't find '{0}(rcdg_id)' & {1}(rcdg_file_nm) in DB".format(rcdg_id, rcdg_file_nm))
            rcdg_id_info_dic[pk]['PRGST_CD'] = '03'
            rcdg_id_info_dic[pk]['CHN_TP_CD'] = 'None'
            rcdg_id_info_dic[pk]['rcdg_file_name'] = rcdg_file_nm.replace('.', '_')
            # 8-1. Create db_upload txt of TB_QA_STT_CS_INFO table
            make_stt_info_table_txt(logger, pk, rcdg_id_info_dic[pk], sftp_set_dir_path, mysql)
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, '03'):
                raise Exception
            continue
    return rcdg_id_info_dic


def processing(mysql, pk_list):
    """
    CS processing
    :param      mysql:          MySQL
    :param      pk_list:        Primary id list
    """
    global TARGET_DIR_NAME
    global TARGET_DIR_PATH
    global DELETE_FILE_LIST
    cnt = 0
    # Determine temp directory name to be used in script
    while True:
        TARGET_DIR_PATH = "{0}/temp_directory_{1}".format(CONFIG['stt_path'], cnt)
        if not os.path.exists(TARGET_DIR_PATH):
            os.makedirs(TARGET_DIR_PATH)
            TARGET_DIR_NAME = os.path.basename(TARGET_DIR_PATH)
            DELETE_FILE_LIST.append(TARGET_DIR_PATH)
            break
        cnt += 1
    # Determining log_name
    log_name = '{0}_{1}'.format(DT[:8], cnt)
    # Add logging
    logger_args = {
        'base_path': CONFIG['log_dir_path'],
        'log_file_name': log_name,
        'log_level': CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info("-" * 100)
    logger.info('Start CS')
    rcdg_id_info_dic = dict()

    # 만들필요 없음
    sftp_target_dir_path = CONFIG['sftp_dir_path']
    if not os.path.exists(sftp_target_dir_path):
        os.makedirs(sftp_target_dir_path)
    while True:
        ts = time.time()
        dt = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
        # Adding a server-specific unique variable when adding a server
        sftp_set_dir_path = '{0}/{1}_{2}'.format(sftp_target_dir_path, dt, cnt)
        if os.path.exists(sftp_set_dir_path) or os.path.exists(sftp_set_dir_path + '.tmp'):
            logger.info('filename {0} is already exist'.format(sftp_set_dir_path))
            time.sleep(1)
            continue
        else:
            os.makedirs('{0}.tmp'.format(sftp_set_dir_path))
            break
    # noinspection PyBroadException
    try:
        # 1. Get recording information dictionary for recording id list
        rcdg_id_info_dic = get_rcdg_info_dic(logger, mysql, pk_list, sftp_set_dir_path)
        # try exception 처리 걸기!

        # 2. Load pcm file
        rcdg_id_info_dic = load_pcm_file(logger, mysql, rcdg_id_info_dic)
        # 3. Make list file
        thread_cnt = make_list_file(logger)
        # 4. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 5. Execute unseg.exe and do_space.exe
        do_space_dir, rcdg_id_info_dic = execute_unseg_and_do_space(logger, mysql, rcdg_id_info_dic)
        # 6. make output
        make_output(logger, rcdg_id_info_dic, do_space_dir)
        # 7. Set output
        set_output(logger)
        # 8. Make DB upload output
        rcdg_id_info_dic = make_db_upload_output(logger, rcdg_id_info_dic, mysql, sftp_set_dir_path)
        # 9. Move output
        move_output(logger, rcdg_id_info_dic, mysql)
        # 10. Delete garbage list
        delete_garbage_file(logger)
        # 11. Print statistical data
        statistical_data(logger)
        # Update prgst code
        for pk in rcdg_id_info_dic:
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, '05'):
                raise Exception
    except Exception:
        for pk in rcdg_id_info_dic:
            rcdg_id_info_dic[pk]['PRGST_CD'] = '03'
            make_stt_info_table_txt(logger, pk, rcdg_id_info_dic[pk], sftp_set_dir_path, mysql)
            if not mysql.update_prgst_cd_to_tb_qa_stt_recinfo(logger, rcdg_id_info_dic, pk, '03'):
                raise Exception
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error('---------- ERROR ----------')
        logger.info('rename {0}.tmp -> {0}'.format(sftp_set_dir_path))
        os.rename('{0}.tmp'.format(sftp_set_dir_path), sftp_set_dir_path)
        delete_garbage_file(logger)
        sys.exit(1)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(pk_list):
    """
    This is a program that execute CS
    :param      pk_list:    Primary key list
    """
    global DT
    global ST
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    mysql = mysql_connect()
    try:
        processing(mysql, pk_list)
        mysql.disconnect()
    except Exception:
        mysql.disconnect()
        exc_info = traceback.format_exc()
        print exc_info
        sys.exit(1)
