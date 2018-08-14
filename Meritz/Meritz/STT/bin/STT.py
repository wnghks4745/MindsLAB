#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-03-10, modification: 2017-04-20"

###########
# imports #
###########
import os
import sys
import time
import shutil
import socket
import requests
import pymssql
import traceback
import subprocess
import ConfigParser
from operator import itemgetter
from datetime import datetime, timedelta
from distutils.dir_util import copy_tree
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from lib.iLogger import set_logger
from lib.meritz_enc import encrypt

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

#############
# constants #
#############
DT = ""
LOCK = ""
LOGGER = ""
ISP_TMS = ""
DB_CONFIG = ""
LOG_LEVEL = ""
IMG_TABLE = ""
HOST_NAME = ""
TRANS_NO = ""
START_POINT = ""
AGREE_POINT = ""
REC_FILE_PATH = ""
STT_PATH = ""
STT_OUT_PATH = ""
STT_TOOL_PATH = ""
STT_OUT_FILE_PATH = ""
STT_IMG_TABLE_PATH = ""
STT_MERGE_FILE_PATH = ""


#########
# class #
#########


class MySQL(object):
    def __init__(self):
        global DB_CONFIG
        self.conn = pymssql.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            database=DB_CONFIG['database'],
            port=DB_CONFIG['port'],
            charset=DB_CONFIG['charset'],
            login_timeout=5
        )
        self.conn.autocommit(False)
        self.cursor = self.conn.cursor()

    def disconnect(self):
        try:
            self.conn.close()
        except Exception:
            raise Exception(traceback.format_exc())

    def select_stt_targets(self):
        query = """
            WITH RST AS
            (
                SELECT TOP(1)
                    STTACTR.TRANSFER_NUMBER,
                    STTACTR.ISP_TMS
                FROM
                    STTACTRREQ STTACTR WITH(NOLOCK),
                    STTACLLREQ STTACLL WITH(NOLOCK)
                WHERE 1=1
                    AND STTACTR.TRANSFER_NUMBER = STTACLL.TRANSFER_NUMBER
                    AND STTACTR.TMS_FILE_CNT = (
                                                SELECT
                                                    COUNT (*)
                                                FROM
                                                    STTACLLREQ WITH(NOLOCK)
                                                WHERE 1=1
                                                    AND STTACLLREQ.TRANSFER_NUMBER = STTACTR.TRANSFER_NUMBER
                                                    AND STTACLLREQ.ISP_TMS = STTACTR.ISP_TMS
                                                    AND STTACLLREQ.PROG_STAT_CD IN ('21', '22')
                                                )
                    AND STTACTR.ISP_TMS = STTACLL.ISP_TMS
                    AND STTACLL.PROG_STAT_CD = '21'
                    AND STTACTR.HIS_ST_DT <= CURRENT_TIMESTAMP
            )
            SELECT
                STTACLLREQ.TRANSFER_NUMBER,
                STTACLLREQ.RECORD_FILE_NAME,
                STTACLLREQ.ISP_TMS
            FROM
                STTACLLREQ WITH(NOLOCK),
                RST WITH(NOOLOCK)
            WHERE 1=1
                AND STTACLLREQ.TRANSFER_NUMBER = RST.TRANSFER_NUMBER
                AND STTACLLREQ.ISP_TMS = RST.ISP_TMS
                AND STTACLLREQ.PROG_STAT_CD IN ('21')
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is bool:
            return False
        if result:
            return result
        return False

    def insert_data_to_arsl(self, **kwargs):
        global LOGGER
        try:
            query = """
                INSERT INTO STTARSL
                (
                    TRANSFER_NUMBER,
                    DCM_NO,
                    STMT_NO,
                    CHN_ID,
                    STMT_ST,
                    STMT_END,
                    STMT
                )
                VALUES
                (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """
            bind = (
                kwargs.get('trans_no'),
                kwargs.get('dcm_no'),
                kwargs.get('stmt_no'),
                kwargs.get('chn_id'),
                kwargs.get('stmt_st'),
                kwargs.get('stmt_end'),
                kwargs.get('stmt'),
            )
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                # self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error(exc_info)
            self.conn.rollback()

    def update_file_status_ll(self, trans_no, basename, isp, new_state):
        try:
            query = """
                UPDATE
                    STTACLLREQ
                SET
                    PROG_STAT_CD = %s
                WHERE 1=1
                    AND TRANSFER_NUMBER = %s
                    AND RECORD_FILE_NAME = %s
                    AND ISP_TMS = %s
            """
            bind = (new_state, trans_no, basename, isp)
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            self.conn.rollback()
            self.disconnect()
            raise Exception(exc_info)

    def update_file_status_rr(self, trans_no, host_name, isp, new_state):
        try:
            query = """
                UPDATE
                    STTACTRREQ
                SET
                    PROG_STAT_CD = %s,
                    STT_ED_DTM = CONVERT(varchar, GetDate(), 120),
                    STT_HOST_NM = %s,
                WHERE 1=1
                    AND TRANSFER_NUMBER = %s
                    AND ISP_TMS = %s
            """
            bind = (new_state, host_name, trans_no, isp)
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            exc_info = traceback.format_exc()
            self.conn.rollback()
            self.disconnect()
            print e
            raise Exception(exc_info)

    def update_stt_end_time(self, trans_no, host_name, isp):
        try:
            query = """
                UPDATE
                    STTACTRREQ
                SET
                    STT_ED_DTM = CONVERT(varchar, GetDate(), 120),
                    STT_HOST_NM = %s
                WHERE 1=1
                    AND TRANSFER_NUMBER = %s
                    AND ISP_TMS = %s
            """
            bind = (host_name, trans_no, isp)
            self.cursor.execute(query, bind)
            if self.cursor.rowcount > 0:
                self.conn.commit()
                return True
            else:
                self.conn.rollback()
                return False
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            self.conn.rollback()
            self.disconnect()
            raise Exception(exc_info)


#######
# def #
#######
def elapsed_time(sdate):
    """
    elapsed time
    @param      sdate          date object
    @return                    days, hour, minute, sec
    """
    e = datetime.now()
    if not sdate or len(sdate) < 14:
        return 0, 0, 0, 0
    s = datetime(int(sdate[:4]), int(sdate[4:6]), int(sdate[6:8]), int(sdate[8:10]), int(sdate[10:12]),
                 int(sdate[12:14]))
    days = (e - s).days
    sec = (e - s).seconds
    hour, sec = divmod(sec, 3600)
    minute, sec = divmod(sec, 60)
    return days, hour, minute, sec


def load_conf(state, cfg_name):
    """
    Load config file
    :param      state:          state name
    :param      cfg_name:       config name
    :return:                    tate info type is dict
    """
    config = ConfigParser.RawConfigParser()
    script_path = os.path.abspath(os.path.dirname(__file__))
    conf_path = script_path.replace("/bin", "/cfg/{0}.cfg".format(cfg_name))
    config.read(conf_path)
    result = dict(config.items(state))
    return result


def check_file(name_form, file_name):
    """
    Extract need CS file
    :param          name_form:      Check file name form
    :param          file_name:      Input file name
    :return:                        True or False
    """
    return file_name.endswith(name_form)


def del_file(file_name):
    """
    Delete file
    :param      file_name:      Input file name
    """
    if os.path.exists(file_name):
        os.remove(file_name)


def update_targets_status_to_start(mysql, stt_targets_list):
    """
    Update CS target status
    :param      mysql:                  MsSQL
    :param      stt_targets_list:       CS targets [(,,), (,,)]
    """
    global LOGGER
    global LOG_LEVEL
    global STT_PATH

    is_first = True
    for stt_target in stt_targets_list:
        record_name = stt_target[1].strip()
        isp = str(stt_target[2]).strip()
        if is_first:
            # CS 대상 중 처음 한번만 LOGGER 와 STTACTRREQ table 에 status update
            rr_true_or_false = mysql.update_file_status_rr(TRANS_NO, HOST_NAME, isp, 30)
            # Add logging
            args = {
                'base_path': STT_PATH['path'] + "/Meritz",
                'log_file_name': TRANS_NO + "_" + isp,
                'log_level': LOG_LEVEL['level']
            }
            LOGGER = set_logger(args)
            LOGGER.info("Set logger TRANS_NO = {tr}, ISP = {isp}".format(tr=TRANS_NO, isp=isp))
            if not rr_true_or_false:
                LOGGER.error("Failed STTACTRREQ update status --> {0}/{1}".format(TRANS_NO, isp))
            is_first = False
        LOGGER.info("{0} / {1} = Update status 21 to 30".format(TRANS_NO, record_name))
        ll_true_or_false = mysql.update_file_status_ll(TRANS_NO, record_name, isp, 30)
        if not ll_true_or_false:
            LOGGER.error("Failed STTACLLREQ update status --> {0}/{1}".format(TRANS_NO, record_name))
            continue
    mysql.conn.commit()
    LOGGER.info("Success update file state to start 21-30")


def update_file_status_to_error(mysql, stt_targets_list):
    """
    Update CS targets status
    :param          mysql:                   MsSQL
    :param          stt_targets_list:        CS targets
    """
    global LOGGER

    LOGGER.error("CS targets update to error status")
    is_first = True
    for stt_target in stt_targets_list:
        basename = stt_target[1].strip()
        isp = str(stt_target[2]).strip()
        if is_first:
            rr_true_or_false = mysql.update_file_status_rr(TRANS_NO, HOST_NAME, isp, 32)
            if not rr_true_or_false:
                LOGGER.error("Failed STTACLLREQ update state --> {0}/{1}".format(TRANS_NO, isp))
            is_first = False
        LOGGER.info("{0}/{1} = Update state 42".format(TRANS_NO, basename))
        ll_true_or_false = mysql.update_file_status_ll(TRANS_NO, basename, isp, 32)
        if not ll_true_or_false:
            LOGGER.info("Failed STTACLLREQ update state --> {0}/{1}".format(TRANS_NO, basename))
            continue
    mysql.conn.commit()

    if socket.gethostname() == 'vrstt1v':
        requests.get(
            "http://tmdev.meritzfire.com:29600/tmsys/70000/70000_STT_RESULT.jsp?TransferNumber={tr}&SttaRslCd=03".format(
                tr=TRANS_NO))
    elif socket.gethostname() == 'vrta1p' or socket.gethostname() == 'vrta2p':
        requests.get(
            "https://tm.meritzfire.com/tmsys/70000/70000_STT_RESULT.jsp?TransferNumber={tr}&SttaRslCd=03".format(
                tr=TRANS_NO))


def copy_wav_file(mysql, record_file_name, stt_targets_list):
    """
    Copy CS file
    :param          mysql:                      MsSQL
    :param          record_file_name:           wav file name
    :param          stt_targets_list:           CS target list
    """
    global STT_PATH
    global LOGGER

    LOGGER.info("copy wav file -> {0}".format(record_file_name))
    rec_file_path = "{stt}/rec_server/{tr}".format(stt=STT_PATH['path'], tr=TRANS_NO)
    w_ob = os.walk(rec_file_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if len(files) < 1 and dir_path == rec_file_path:
                LOGGER.error("[{fp}] wav file folder is empty".format(fp=file_name))
                update_file_status_to_error(mysql, stt_targets_list)
                if mysql:
                    mysql.disconnect()
                for handler in LOGGER.handlers:
                    handler.close()
                    LOGGER.removeHandler(handler)
                sys.exit(1)
            elif check_file(record_file_name, file_name):
                try:
                    shutil.copy(os.path.join(dir_path, file_name), "{fp}/{tr}_{isp}".
                                format(fp=STT_OUT_FILE_PATH, tr=TRANS_NO, isp=ISP_TMS))
                except Exception as e:
                    print e
                    exc_info = traceback.format_exc()
                    LOGGER.error("Failed wav file copy")
                    LOGGER.error(exc_info)
                    update_file_status_to_error(mysql, stt_targets_list)
                    if mysql:
                        mysql.disconnect()
                    for handler in LOGGER.handlers:
                        handler.close()
                        LOGGER.removeHandler(handler)
                    sys.exit(1)


def make_output_dir():
    """
    Make CS output directory using trans number
    :return:            Folder path
    """
    file_dir_path = "{fp}/{tr}_{isp}".format(fp=STT_OUT_FILE_PATH, tr=TRANS_NO, isp=ISP_TMS)
    if not os.path.exists(file_dir_path):
        os.makedirs(file_dir_path)
    return file_dir_path


def separation_wav_file(mysql, stt_targets_list):
    """
    Separation wav file
    :param          mysql:                   MsSQL
    :param          stt_targets_list:        CS targets list
    :return:                                 Output file dir path
    """
    global LOGGER
    global STT_TOOL_PATH

    LOGGER.info("Do separation wav file")
    file_dir_path = make_output_dir()
    LOGGER.info("CS output file path --> {fp}".format(fp=file_dir_path))

    for tuple_stt_target in stt_targets_list:
        record_file_name = tuple_stt_target[1]
        call_id = record_file_name.replace(".wav", "")
        record_file_path = file_dir_path + "/" + record_file_name
        copy_wav_file(mysql, record_file_name, stt_targets_list)
        os.chdir(file_dir_path)
        # IF rx.wav or tx.wav file is already existed remove file
        try:
            LOGGER.debug("Delete wav file -> {fp}/{cid}_rx.wav".format(fp=file_dir_path, cid=call_id))
            del_file("{fp}/{cid}_rx.wav".format(fp=file_dir_path, cid=call_id))
            LOGGER.debug("Delete wav file -> {fp}/{cid}_tx.wav".format(fp=file_dir_path, cid=call_id))
            del_file("{fp}/{cid}_tx.wav".format(fp=file_dir_path, cid=call_id))
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.debug("Fail delete file -> {0}".format(exc_info))

        LOGGER.info("Execute ffmpeg")
        cmd = '{stt_tool}/ffmpeg -i {rec} -filter_complex "[0:0]pan=1c|c0=c0[left];[0:0]pan=1c|c0=c1[right]" -map "[left]" {cid}_rx.wav -map "[right]" {cid}_tx.wav'.format(
            stt_tool=STT_TOOL_PATH['path'], rec=record_file_path, cid=call_id)
        LOGGER.debug(cmd)
        sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # subprocess 가 끝날때까지 대기
        response_out, response_err = sub_pro.communicate()
        # 더 이상 필요없는 wav file 삭제
        try:
            LOGGER.info("Delete wav file -> {0}".format(record_file_path))
            os.remove(record_file_path)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.debug("Fail delete file -> {0}".format(exc_info))

    LOGGER.info("Success separation wav file")
    return file_dir_path


def symlink_and_unlink(domain, sub_name, link_name):
    """
    Do unlink and symlink
    :param          domain:         Domain
    :param          sub_name:       Sub name
    :param          link_name:      Link name
    """
    global LOGGER
    global STT_IMG_TABLE_PATH

    if domain == 'lg':
        LOGGER.debug("Unlink -> {sp}{ln}".format(sp=STT_IMG_TABLE_PATH['path'], ln=link_name))
        os.unlink(STT_IMG_TABLE_PATH['path'] + link_name)
        LOGGER.debug("Set link -> {sp}/{sn}{do} = {sp}{ln}".
                     format(sp=STT_IMG_TABLE_PATH['path'], sn=sub_name, do=domain, ln=link_name))
        os.symlink(STT_IMG_TABLE_PATH['path'] + "/" + sub_name + domain, STT_IMG_TABLE_PATH['path'] + link_name)
    else:
        LOGGER.debug("Unlink -> {sp}{ln}".format(sp=STT_IMG_TABLE_PATH['path'], ln=link_name))
        os.unlink(STT_IMG_TABLE_PATH['path'] + link_name)
        LOGGER.debug("Set link -> {sp}/{do}{su} = {sp}{ln}".
                     format(sp=STT_IMG_TABLE_PATH['path'], su=sub_name, do=domain, ln=link_name))
        os.symlink(STT_IMG_TABLE_PATH['path'] + "/" + domain + sub_name, STT_IMG_TABLE_PATH['path'] + link_name)


def set_dnn_images_symlink():
    """
    Set DNN images symlink
    """
    global LOGGER
    global IMG_TABLE

    LOGGER.info("Do set DNN images symlink")
    mt_cfg_list = IMG_TABLE['tm'].split(",")
    lm_cfg = mt_cfg_list[0]
    la_cfg = mt_cfg_list[1]
    # ba_cfg = mt_cfg_list[2]

    symlink_and_unlink(lm_cfg, ".sfsm.bin", "/stt_release.sfsm.bin")
    symlink_and_unlink(lm_cfg, "sym.bin", "/stt_release.sym.bin")

    symlink_and_unlink(la_cfg, ".dnn.bin", "/final.dnn.adapt.bin")
    symlink_and_unlink(la_cfg, ".dnn.prior.bin", "/final.dnn.prior.bin")

    # 고객사 전용 학습 이미지 수정
    # symlink_and_unlink(ba_cfg, "final.dnn.lda.bin.", "/final.dnn.lda.bin")
    # symlink_and_unlink(ba_cfg, "stt_release.sam.bin", "/stt_release.sam.bin")

    LOGGER.info("Success set DNN images symlink")


def make_pcm_file_and_list_file(file_dir_path):
    """
    Create PCM file
    :param          file_dir_path:      File directory path
    :return:                            File path list
    """
    global LOGGER
    global STT_PATH
    global STT_TOOL_PATH

    LOGGER.info("Do make PCM file and list file")
    list_file_cnt = 0
    list_file_path_list = list()
    w_ob = os.walk("{fp}/".format(fp=file_dir_path))
    # file_dir_path 하위에 있는 모든 파일 중 .wav 로 끝나는 파일을 대상으로 한다.
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file(".wav", file_name):
                call_id = file_name.replace(".wav", "")
                try:
                    # If PCM file already existed remove file
                    del_file("{fp}/{cid}.pcm".format(fp=file_dir_path, cid=call_id))
                except Exception as e:
                    print e
                    exc_info = traceback.format_exc()
                    LOGGER.error("Can't delete PCM file")
                    LOGGER.error(exc_info)
                cmd = "{stt_tool}/sox -t wav {fp}/{fn} -r 8000 -b 16 -t raw {fp}/{cid}.pcm".format(
                    stt_tool=STT_TOOL_PATH['path'], fp=file_dir_path, fn=file_name, cid=call_id)
                LOGGER.debug(cmd)
                sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                # subprocess 가 끝날때까지 대기
                response_out, response_err = sub_pro.communicate()
                # List 라는 파일에 수행할 PCM 파일명을 입력한다. 한 list 파일에 최대 24개의 파일을 돌리길 권장한다.
                list_file_path = "{fp}/{tr}_{isp}_n{cnt}.list".format(fp=STT_PATH['path'], tr=TRANS_NO, cnt=list_file_cnt, isp=ISP_TMS)
                curr_list_file_path = "{fp}/{tr}_{isp}_n{cnt}_curr.list".format(fp=STT_PATH['path'], tr=TRANS_NO, cnt=list_file_cnt, isp=ISP_TMS)
                list_file_path_list.append(list_file_path)
                list_file_path_list.append(curr_list_file_path)
                output_file_div = open(list_file_path, 'a')
                print >> output_file_div, "{tr}_{isp}/{cid}.pcm".format(tr=TRANS_NO, cid=call_id, isp=ISP_TMS)
                output_file_div.close()
                list_file_cnt += 1
                if list_file_cnt == 24:
                    list_file_cnt = 0

    LOGGER.info("Success make PCM file and list file")
    return list_file_path_list


def error_dnn_process(error_file_list):
    """
    DNN error processing
    :param              error_file_list:            Error lise
    """
    global STT_PATH
    global LOGGER

    stt_error_path = "{stt}/STT_err_{tr}".format(stt=STT_PATH['path'], tr=TRANS_NO)
    stt_path = "{stt}/{tr}".format(stt=STT_PATH['path'], tr=TRANS_NO)
    list_file_cnt = 0
    list_file_path_list = list()
    # STT_err_{tr} 의 폴더를 만들어 오류가 발생한 PCM 파일만 작업하기 위해 list 파일 생성
    for file_name in error_file_list:
        LOGGER.info("Error PCM file = {fn}".format(fn=file_name))
        err_call_id = file_name.split(".")[0]
        wav_file_name = "{cid}.wav".format(cid=err_call_id)
        pcm_file_name = "{cid}.pcm".format(cid=err_call_id)
        list_file_path = "{fp}/STT_err_{tr}_n{cnt}.list".format(fp=STT_PATH['path'], tr=TRANS_NO, cnt=list_file_cnt)
        list_file_path_list.append(list_file_path)
        output_file_div = open(list_file_path, 'a')
        print >> output_file_div, "STT_err_{tr}/{cid}.pcm" .format(tr=TRANS_NO, cid=err_call_id)
        output_file_div.close()
        list_file_cnt += 1
        if list_file_cnt == 24:
            list_file_cnt = 0
        if not os.path.exists(stt_error_path):
            os.makedirs(stt_error_path)
        try:
            shutil.move(os.path.join(stt_path, wav_file_name), stt_error_path)
            shutil.move(os.path.join(stt_path, pcm_file_name), stt_error_path)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error("Can't move err pcm file")
            LOGGER.error(exc_info)

    ori_laser_cfg_file = "{stt}/stt_laser.cfg".format(stt=STT_PATH['path'])
    temp_laser_cfg_file = "{stt}/ori_stt_laser.cfg".format(stt=STT_PATH['path'])
    err_laser_cfg_file = "{stt}/stt_laser_err.cfg".format(stt=STT_PATH['path'])
    # LOCK.acquire()
    try:
        shutil.copy(ori_laser_cfg_file, temp_laser_cfg_file)
        shutil.copy(err_laser_cfg_file, ori_laser_cfg_file)
        os.chdir(STT_PATH['path'])
        dnn_thread = len(error_file_list) if len(error_file_list) < 24 else 24
        cmd = './mt_long_utt_dnn_support.gpu.exe STT_err_{tr} {th} 1 1 1 128 0.8'.format(tr=TRANS_NO, th=dnn_thread)
        LOGGER.debug("Error dnn cmd")
        LOGGER.debug(cmd)
        sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(5)
        shutil.copy(temp_laser_cfg_file, ori_laser_cfg_file)
    except Exception as e:
        print e
        # LOCK.release()
        raise Exception("Can't execute error dnn process")
    # LOCK.release()
    response_out, response_err = sub_pro.communicate()

    # 다시 작업한  Error PCM 파일을 기존의 폴더에 복사
    w_ob = os.walk(stt_error_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            src = "{stt_err}/{fn}".format(stt_err=stt_error_path, fn=file_name)
            dst = "{stt}/{tr}".format(stt=STT_PATH['path'], tr=TRANS_NO)
            dst_fn = "{stt}/{tr}/{fn}".format(stt=STT_PATH['path'], tr=TRANS_NO, fn=file_name)
            if os.path.exists(dst_fn):
                os.remove(dst_fn)
            shutil.move(src, dst)

    # Delete list file
    try:
        LOGGER.info("Delete pcm error temp file = {stt_err}".format(stt_err=stt_error_path))
        shutil.rmtree(stt_error_path)
        for list_file in list_file_path_list:
            try:
                del_file(list_file)
            except Exception as e:
                print e
                exc_info = traceback.format_exc()
                LOGGER.error("Can't delete file")
                LOGGER.error(exc_info)
    except Exception as e:
        print e
        pass


def error_execute_dnn():
    """
    Execute error dnn file
    """
    global LOGGER
    global STT_PATH

    error_file_list = list()
    w_ob = os.walk("{stt}/{tr}_{isp}".format(stt=STT_PATH['path'], tr=TRANS_NO, isp=ISP_TMS))
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file(".err", file_name):
                error_file_list.append(file_name)

    if len(error_file_list) > 0:
        LOGGER.info("Again execute dnn error PCM file")
        error_dnn_process(error_file_list)


def execute_dnn(file_dir_path, stt_targets_list, list_file_path_list):
    """
    Execute DNN (mt_long_utt_dnn_support.gpu.exe)
    :param      file_dir_path:              File directory path
    :param      stt_targets_list:           CS targets list
    :param      list_file_path_list:        Delete file list
    """
    global LOGGER
    global STT_PATH

    LOGGER.info("Execute CS")
    LOGGER.info("Copy dir {fr} to {stt}/{tr}_{isp}".format(fr=file_dir_path, stt=STT_PATH['path'], tr=TRANS_NO, isp=ISP_TMS))
    copy_tree(file_dir_path, "{stt}/{tr}_{isp}".format(stt=STT_PATH['path'], tr=TRANS_NO, isp=ISP_TMS))
    os.chdir(STT_PATH['path'])
    # Thread 는 반드시 target 의 2배로 하고 최대 thread 수는 24 로 한다.
    dnn_thread = len(stt_targets_list) * 2 if len(stt_targets_list) * 2 < 24 else 24
    cmd = "./mt_long_utt_dnn_support.gpu.exe {tr}_{isp} {th} 1 1 1 128 0.8".format(tr=TRANS_NO, isp=ISP_TMS, th=dnn_thread)
    LOGGER.info(cmd)
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날 때까지 대기
    response_out, response_err = sub_pro.communicate()

    # Error PCM check
    # error_execute_dnn()

    # Delete list file
    for list_file in list_file_path_list:
        try:
            del_file(list_file)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error("Can't delete file.")
            LOGGER.error(exc_info)
            continue
    LOGGER.info("Done CS")


def move_mlf_file():
    """
    Move mlf file
    """
    global LOGGER
    global STT_PATH
    global STT_MERGE_FILE_PATH

    LOGGER.info("Move mlf file")
    w_ob = os.walk("{stt}/{tr}_{isp}".format(stt=STT_PATH['path'], tr=TRANS_NO, isp=ISP_TMS))
    STT_MERGE_FILE_PATH = "{fp}/{tr}_{isp}/STT_merge".format(fp=STT_OUT_FILE_PATH, tr=TRANS_NO, isp=ISP_TMS)
    if os.path.exists(STT_MERGE_FILE_PATH):
        try:
            shutil.rmtree(STT_MERGE_FILE_PATH)
        except Exception as e:
            print e
            pass
    if not os.path.exists(STT_MERGE_FILE_PATH):
        os.makedirs(STT_MERGE_FILE_PATH)

    # Move mlf file to STT_merge
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file(".mlf", file_name):
                try:
                    dst_fn = "{stm}/{fn}".format(stm=STT_MERGE_FILE_PATH, fn=file_name)
                    if os.path.exists(dst_fn):
                        os.remove(dst_fn)
                    shutil.move(os.path.join(dir_path, file_name), STT_MERGE_FILE_PATH)
                except Exception as e:
                    print e
                    exc_info = traceback.format_exc()
                    LOGGER.error("Can't move mlf file")
                    LOGGER.error(exc_info)
                    continue

    # Delete garbage file
    try:
        shutil.rmtree("{stt}/{tr}_{isp}".format(stt=STT_PATH['path'], tr=TRANS_NO, isp=ISP_TMS))
    except Exception as e:
        print e
        exc_info = traceback.format_exc()
        LOGGER.error("Can't delete file")
        LOGGER.error(exc_info)
        pass
    LOGGER.info("Done move mlf file")


def merge_time_info(speaker, file_name, output_dict):
    """
    Merge time info
    :param          speaker:            Speaker
    :param          file_name:          File name
    :param          output_dict:        Output dict
    :return:                            Output dict
    """
    global LOGGER
    for line in file_name:
        try:
            line_list = line.split(",")
            if len(line_list) != 3:
                continue
            st = line_list[0].strip()
            et = line_list[1].strip()
            sent = line_list[2].strip()
            modified_st = st.replace("ts=", "").strip()
            if int(modified_st) not in output_dict:
                output_dict[int(modified_st)] = "{0}\t{1}\t{2}\t{3}".format(speaker, st, et, sent)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            LOGGER.error("Error merge_time_info")
            LOGGER.error(line)
            LOGGER.error(exc_info)
            continue
    return output_dict


def execute_unseg():
    """
    Make merged time file
    """
    global LOGGER
    global STT_TOOL_PATH

    LOGGER.info("Make merged time file")
    os.chdir(STT_TOOL_PATH['path'])
    cmd = './unseg.exe -d {mp} {mp} 300'.format(mp=STT_MERGE_FILE_PATH)
    LOGGER.debug(cmd)
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()

    # tx 와 rx 파일을 trx 파일로 merge
    w_ob = os.walk(STT_MERGE_FILE_PATH)
    target_dict = dict()
    # target_dict 에 각 call_id 에 tx, rx 파일 갯수를 저장
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file("_tx.stt", file_name):
                call_id = file_name.replace("_tx.stt", "")
                if call_id not in target_dict:
                    target_dict[call_id] = 1
                else:
                    target_dict[call_id] += 1
            elif check_file("_rx.stt", file_name):
                call_id = file_name.replace("_rx.stt", "")
                if call_id not in target_dict:
                    target_dict[call_id] = 1
                else:
                    target_dict[call_id] += 1

    # 각각의 call_id 에 tx, rx 파일이 2개인 경우만 merge
    for target_call_id, cnt in target_dict.items():
        output_dict = dict()
        if cnt == 2:
            tx_file = open(STT_MERGE_FILE_PATH + "/" + target_call_id + '_tx.stt', 'r')
            rx_file = open(STT_MERGE_FILE_PATH + "/" + target_call_id + '_rx.stt', 'r')
            output_dict = merge_time_info("agent", tx_file, output_dict)
            output_dict = merge_time_info("client", rx_file, output_dict)
            tx_file.close()
            rx_file.close()
        output_dict_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
        output_file = open(STT_MERGE_FILE_PATH + "/" + target_call_id + '_trx.txt', 'w')
        for line_list in output_dict_list:
            print >> output_file, line_list[1]
        output_file.close()
    LOGGER.info("Done make merged time file")


def execute_masking():
    """
    Execute masking
    :return:
    """
    global LOGGER
    global STT_TOOL_PATH

    LOGGER.info("Execute masking")
    os.chdir(STT_TOOL_PATH['path'])
    cmd = './masking {mp} txt euc-kr'.format(mp=STT_MERGE_FILE_PATH)
    LOGGER.debug(cmd)
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # subprocess 가 끝날때까지 대기
    response_out, response_err = sub_pro.communicate()
    LOGGER.info("Done masking")


def make_trx_updated_txt_file(mysql):
    """
    Make trx updated txt file and insert time info to STTARSL
    :param          mysql:          MsSQL
    """
    global LOGGER

    LOGGER.info("Do make trx_updated.txt file")
    # trx.txt 에서 정보를 가져와 format 변경 후 trx_updated.txt 파일 생성과 STTARSL table 에 insert
    w_ob = os.walk(STT_MERGE_FILE_PATH)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file("_trx.txt", file_name):
                call_id = file_name.replace("_trx.txt", "")
                trx_update_output_file = open(STT_MERGE_FILE_PATH + "/" + call_id + "_trx_updated.txt", "w")
                trx_file = open(STT_MERGE_FILE_PATH + "/" + file_name, 'r')
                for idx, line in enumerate(trx_file):
                    line = line.strip()
                    line_list = line.split("\t")
                    if len(line_list) < 3:
                        continue
                    speaker = line_list[0].strip()
                    sent = line_list[3].strip()
                    if speaker == "agent":
                        print >> trx_update_output_file, "[A]{sent}".format(sent=sent)
                    else:
                        print >> trx_update_output_file, "[C]{sent}".format(sent=sent)
                    db_speaker = line_list[0].replace("client", "C").replace("agent", "A").strip()
                    start_time = str(timedelta(seconds=int(line_list[1].replace("ts=", ""))/100))
                    end_time = str(timedelta(seconds=int(line_list[2].replace("te=", "")) / 100))
                    db_sent = line_list[3][:8000]
                    mysql.insert_data_to_arsl(
                        trans_no=TRANS_NO,
                        dcm_no=call_id,
                        stmt_no=idx,
                        chn_id=db_speaker,
                        stmt_st=start_time,
                        stmt_end=end_time,
                        stmt=db_sent
                    )
                trx_update_output_file.close()
                trx_file.close()
    mysql.conn.commit()
    LOGGER.info("Success make trx_updated.txt file")


def update_stt_ed_dtm(mysql):
    """
    Update CS targets status
    :param          mysql:          MySQL
    """
    global LOGGER
    LOGGER.info("Update CS end time and host name TR={tr}, HN={hn}".format(tr=TRANS_NO, hn=HOST_NAME))
    mysql.update_stt_end_time(TRANS_NO, HOST_NAME, ISP_TMS)


def update_file_status_to_end(mysql, stt_targets_list):
    """
    Update TA targets state
    :param          mysql:                      MsSQL
    :param          stt_targets_list:           CS targets
    """
    global LOGGER

    rc = 33 if ISP_TMS != '1' else 31

    if rc == 33:
        # Execute encrypt and delete wav file
        LOGGER.info("ISP is not '1', so execute encrypt and delete wav file")
        target_dir_path = "{fp}/{tr}_{isp}".format(fp=STT_OUT_FILE_PATH, tr=TRANS_NO, isp=ISP_TMS)
        w_ob = os.walk(target_dir_path)
        for dir_path, sub_dirs, files in w_ob:
            for file_name in files:
                if check_file("_trx.txt", file_name) or check_file("_trx_updated.txt", file_name):
                    continue
                else:
                    target_path = os.path.join(dir_path, file_name)
                    del_file(target_path)
        encrypt(target_dir_path)

    LOGGER.info("Update file state to end, MsSQL disconnect and LOGGER close")
    # Update STTACLLREQ table
    for ta_target in stt_targets_list:
        basename = ta_target[1].strip()
        LOGGER.info("{0}/{1} = Update state 30 to {2}".format(TRANS_NO, basename, rc))
        ll_true_or_false = mysql.update_file_status_ll(TRANS_NO, basename, ISP_TMS, rc)
        if not ll_true_or_false:
            LOGGER.error("Failed STTACLLREQ update status --> {0}/{1}".format(TRANS_NO, basename))
            continue

    # Update STTACTRREQ table
    rr_true_or_false = mysql.update_file_status_rr(TRANS_NO, HOST_NAME, ISP_TMS, rc)
    if not rr_true_or_false:
        LOGGER.error("Failed STTACTRREQ update status --> {0}/{1}".format(TRANS_NO, ISP_TMS))

    mysql.conn.commit()


def processing(mysql, stt_targets_list):
    """
    This is function that CS process
    @param      mysql                       MsSQL
    @param      stt_targets_list            CS target list
    """
    global LOGGER
    global ISP_TMS
    global LOG_LEVEL
    global IMG_TABLE
    global HOST_NAME
    global TRANS_NO
    global STT_PATH
    global STT_OUT_PATH
    global STT_TOOL_PATH
    global REC_FILE_PATH
    global STT_OUT_FILE_PATH
    global STT_IMG_TABLE_PATH

    try:
        # Load config info
        STT_PATH = load_conf('STT_PATH', 'CS')
        STT_OUT_PATH = load_conf('STT_OUT_PATH', 'CS')
        STT_TOOL_PATH = load_conf('STT_TOOL_PATH', 'CS')
        REC_FILE_PATH = load_conf('REC_FILE_PATH', 'CS')
        LOG_LEVEL = load_conf('LOG_LEVEL', 'CS')
        STT_IMG_TABLE_PATH = load_conf('STT_IMG_TABLE_PATH', 'CS')
        IMG_TABLE = load_conf('IMG_TABLE', 'CS')
        HOST_NAME = socket.gethostname()
        TRANS_NO = stt_targets_list[0][0]
        ISP_TMS = str(stt_targets_list[0][2]).strip()
        STT_OUT_FILE_PATH = "{fp}/{y}/{m}/{d}".format(fp=STT_OUT_PATH['path'], y=TRANS_NO[:4], m=TRANS_NO[4:6],
                                                      d=TRANS_NO[6:8])
        # Update targets status to start
        update_targets_status_to_start(mysql, stt_targets_list)
    except Exception as e:
        exc_info = traceback.format_exc()
        print e, exc_info
        update_file_status_to_error(mysql, stt_targets_list)
        if mysql:
            mysql.disconnect()
        sys.exit(1)

    try:
        # Separation wav file
        file_dir_path = separation_wav_file(mysql, stt_targets_list)
        # Set DNN images symlink
        # set_dnn_images_symlink()
        # Create pcm file and list file
        list_file_path_list = make_pcm_file_and_list_file(file_dir_path)
        # Execute CS
        execute_dnn(file_dir_path, stt_targets_list, list_file_path_list)
        # Move mlf file
        move_mlf_file()
        # Execute unseg and make merged time file
        execute_unseg()
        # Execute masking
        execute_masking()
        # Update file state to end, MsSQL disconnect and LOGGER close
        update_stt_ed_dtm(mysql)
        update_file_status_to_end(mysql, stt_targets_list)
        # MsSQL disconnect and LOGGER close
        LOGGER.info("MySQL disconnect")
        mysql.disconnect()
        LOGGER.info("Remove logger handler")
        LOGGER.info("END.. The time required = {0}".format(elapsed_time(DT)))
        for handler in LOGGER.handlers:
            handler.close()
            LOGGER.removeHandler(handler)
    except Exception as e:
        print e
        exc_info = traceback.format_exc()
        LOGGER.error(exc_info)
        update_file_status_to_error(mysql, stt_targets_list)
        if mysql:
            mysql.disconnect()
            LOGGER.error("MySQL disconnect")
        LOGGER.error("Remove logger handler")
        for handler in LOGGER.handlers:
            handler.close()
            LOGGER.removeHandler(handler)
        sys.exit(1)


########
# main #
########


def execute_stt():
    """
    This is a program that execute CS
    """
    global DB_CONFIG
    global DT
    global LOCK

    ts = time.time()
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')

    try:
        # Load config info
        LOCK = ""
        DB_CONFIG = load_conf('DB_CONFIG', 'CS')
        # Load MsSQL
        mysql = MySQL()
        # Select TA targets from DB
        stt_targets_list = mysql.select_stt_targets()
        if stt_targets_list == bool or not stt_targets_list:
            print "CS target is none"
            if mysql:
                mysql.disconnect()
        else:
            processing(mysql, stt_targets_list)
    except Exception as e:
        exc_info = traceback.format_exc()
        print e, exc_info
        sys.exit(1)


if __name__ == "__main__":
    execute_stt()

    # vim: set expandtab:
