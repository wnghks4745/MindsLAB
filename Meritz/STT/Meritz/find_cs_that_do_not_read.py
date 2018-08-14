#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MindsLAB"
__date__ = "creation: 2017-11-24, modification: 2017-11-29"


###########
# imports #
###########
import os
import sys
import time
import glob
import shutil
import traceback
import subprocess
from datetime import datetime, timedelta
from operator import itemgetter
from cfg.config import FIND_CONFIG
from bin.lib.iLogger import set_logger


#############
# constants #
#############
ST = ''
DT = ''
TARGET_DIR_NAME = ''
TARGET_DIR_PATH = ''
DELETE_FILE_LIST = list()
PCM_CNT = 0
TOTAL_PCM_TIME = 0


###########
# options #
###########
reload(sys)
sys.setdefaultencoding('utf-8')


#######
# def #
#######
def del_garbage(logger, delete_file_path):
    """
    Delete directory or file
    :param      logger:                 Logger
    :param      delete_file_path:       Input path
    """
    if os.path.exists(delete_file_path):
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


def sub_process(logger, cmd):
    """
    Execute subprocess
    :param      logger:     Logger
    :param      cmd:        Command
    """
    logger.info('Command -> {0}'.format(cmd))
    sub_pro = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Wait for subprocess to finish
    response_out, response_err = sub_pro.communicate()
    if len(response_out) > 0:
        logger.debug(response_out)
    if len(response_err) > 0:
        logger.debug(response_err)
    return response_out


def delete_garbage_file(logger):
    """
    Delete garbage file
    :param      logger:     Logger
    """
    logger.info('8. Delete garbage file')
    for delete_file in DELETE_FILE_LIST:
        try:
            logger.debug('del_garbage : {0}'.format(delete_file))
            del_garbage(logger, delete_file)
        except Exception as e:
            logger.error(e)
            continue


def check_file(name_form, file_name):
    """
    Check file name
    :param      name_form:      Check file name form
    :param      file_name:      Input file name
    :return:                    True or False
    """
    return file_name.endswith(name_form)


def modify_time_info(logger, speaker, file_name, output_dict):
    """
    Modify time info
    :param      logger:             Logger
    :param      speaker:            Speaker
    :param      file_name:          File name
    :param      output_dict:        Output dict
    :return:                        Output dict
    """
    for line in file_name:
        try:
            line_list = line.split(',')
            if len(line_list) != 3:
                continue
            st = line_list[0].strip()
            et = line_list[1].strip()
            start_time = str(timedelta(seconds=float(st.replace('ts=', '')) / 100))
            end_time = str(timedelta(seconds=float(et.replace('te=', '')) / 100))
            sent = line_list[2].strip()
            modified_st = st.replace('ts=', '').strip()
            if int(modified_st) not in output_dict:
                output_dict[int(modified_st)] = '{0}\t{1}\t{2}\t{3}'.format(speaker, start_time, end_time, sent)
        except Exception as e:
            print e
            exc_info = traceback.format_exc()
            logger.error('Error modify_time_info')
            logger.error(line)
            logger.error(exc_info)
            continue
    return output_dict


def make_list(logger, detail_dir, wav_file_path_list):
    """
    Make wait call list:
    :param      logger:                 Logger
    :param      detail_dir:             Directory of detail file
    :param      wav_file_path_list:     List of wav file path
    """
    logger.info('7. Make waiting call list')
    output_txt_dir_path = '{0}/wait_call_result'.format(FIND_CONFIG['stt_path'])
    if not os.path.exists(output_txt_dir_path):
        os.makedirs(output_txt_dir_path)
    output_txt_file_path = '{0}/wait_{1}.txt'.format(output_txt_dir_path, DT[:8])
    detail_file_path_list = glob.glob('{0}/*'.format(detail_dir))
    for detail_file_path in detail_file_path_list:
        file_name = os.path.basename(detail_file_path).replace('_trx.detail', '')
        file_len = ''
        for wav_file_dic in wav_file_path_list:
            dir_name = os.path.dirname(wav_file_dic.keys()[0])
            wav_name = '{0}/{1}.wav'.format(dir_name, file_name)
            logger.debug(wav_name)
            logger.debug(wav_file_dic)
            logger.debug(wav_name in wav_file_dic)
            if wav_name in wav_file_dic:
                file_len = wav_file_dic[wav_name].split(' ')[1]
        detail_file = open(detail_file_path, 'r')
        lines = detail_file.readlines()
        for line in lines:
            find_data = '{0}.wav\t'.format(file_name)
            search_list = FIND_CONFIG['search_list']
            check = False
            for search in search_list:
                if search in line:
                    find_data = '{0}{1}\t'.format(find_data, search)
                    check = True
                    break
            if not check:
                continue
            find_data = '{0}{1}'.format(find_data, file_len)
            output_file = open(output_txt_file_path, 'a')
            print >> output_file, find_data
            output_file.close()
            break


def make_output(logger, wav_file_path_list, unseg_dir_path):
    """
    Make txt file and detail file
    :param      logger:                 Logger
    :param      wav_file_path_list:     List of wav file path
    :param      unseg_dir_path:         Directory path of unseg result
    """
    logger.info('6. Make output [txt and detailed file]')
    # Created directory
    logger.debug('Create directory')
    txt_dir_path = '{0}/txt'.format(TARGET_DIR_PATH)
    detail_dir_path = '{0}/detail'.format(TARGET_DIR_PATH)
    if not os.path.exists(txt_dir_path):
        os.makedirs(txt_dir_path)
    if not os.path.exists(detail_dir_path):
        os.makedirs(detail_dir_path)
    # Create txt & detail file
    logger.debug('Create txt & detail file')
    for wav_file_dic in wav_file_path_list:
        for wav_file_path in wav_file_dic:
            output_dict = dict()
            wav_file_name = os.path.basename(wav_file_path).replace('.wav', '')
            # Check that both stt files exist.
            logger.info('Check that two stt files exist')
            rx_file_path = '{0}/{1}_rx.stt'.format(unseg_dir_path, wav_file_name)
            tx_file_path = '{0}/{1}_tx.stt'.format(unseg_dir_path, wav_file_name)
            if os.path.exists(rx_file_path) and os.path.exists(tx_file_path):
                logger.debug('two stt files are exist -> {0}'.format(wav_file_name))
                # Save the necessary information
                rx_file = open(rx_file_path, 'r')
                tx_file = open(tx_file_path, 'r')
                output_dict = modify_time_info(logger, '[A]', tx_file, output_dict)
                output_dict = modify_time_info(logger, '[C]', rx_file, output_dict)
                tx_file.close()
                rx_file.close()
            else:
                logger.error("{0} don't have tx or rx file.".format(wav_file_name))
                continue
            # txt & detail file creation.
            output_dict_list = sorted(output_dict.iteritems(), key=itemgetter(0), reverse=False)
            txt_output_file = open('{0}/{1}_trx.txt'.format(txt_dir_path, wav_file_name), 'w')
            detail_output_file = open('{0}/{1}_trx.detail'.format(detail_dir_path, wav_file_name), 'w')
            for line_list in output_dict_list:
                detail_line = line_list[1]
                detail_line_list = detail_line.split('\t')
                trx_txt = '{0}{1}'.format(detail_line_list[0], detail_line_list[3])
                print >> txt_output_file, trx_txt
                print >> detail_output_file, detail_line
            txt_output_file.close()
            detail_output_file.close()
    return txt_dir_path, detail_dir_path


def execute_unseg(logger):
    """
    Execute unseg.exe
    :param          logger:     Logger
    :return:        Output:     Directory path
    """
    logger.info('5. Execute unseg.exe and do_space.exe')
    mlf_file_path_list = glob.glob('{0}/*.mlf'.format(TARGET_DIR_PATH))
    mlf_dir_path = '{0}/mlf'.format(TARGET_DIR_PATH)
    unseg_dir_path = '{0}/unseg'.format(TARGET_DIR_PATH)
    os.chdir(FIND_CONFIG['tool_dir_path'])
    # Moving the mlf file
    logger.info('Moving the mlf file')
    if not os.path.exists(mlf_dir_path):
        os.makedirs(mlf_dir_path)
    for mlf_file_path in mlf_file_path_list:
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
    return unseg_dir_path


def execute_dnn(logger, thread_cnt):
    """
    Execute DNN (mt_long_utt_dnn_support.gpu.exe)
    :param      logger:         Logger
    :param      thread_cnt:     Thread count
    """
    logger.info('4. Execute DNN (mt_long_utt_dnn_support.gpu.exe)')
    os.chdir(FIND_CONFIG['stt_path'])
    dnn_thread = thread_cnt if thread_cnt < FIND_CONFIG['thread'] else FIND_CONFIG['thread']
    cmd = './mt_long_utt_dnn_support.gpu.exe {tn} {th} 1 1 {gpu} 128 0.8'.format(
        tn=TARGET_DIR_NAME, th=dnn_thread, gpu=FIND_CONFIG['gpu'])
    sub_process(logger, cmd)


def make_list_file(logger):
    """
    Make list file
    :param      logger:     Logger
    :return:                Thread count
    """
    logger.info('3. Do make list file')
    global DELETE_FILE_LIST
    global PCM_CNT
    global TOTAL_PCM_TIME
    list_file_cnt = 0
    max_list_file_cnt = 0
    check_dict = dict()
    w_ob = os.walk(TARGET_DIR_PATH)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file('.pcm', file_name):
                check_file_name = os.path.splitext(file_name)[0].replace('_rx', '').replace('_tx', '')
                rx_pcm_file_path = '{0}/{1}_rx.pcm'.format(TARGET_DIR_PATH, check_file_name)
                tx_pcm_file_path = '{0}/{1}_tx.pcm'.format(TARGET_DIR_PATH, check_file_name)
                if check_file_name in check_dict:
                    continue
                # Runs if two pcm files exist.
                if os.path.exists(rx_pcm_file_path) and os.path.exists(tx_pcm_file_path):
                    check_dict[check_file_name] = 1
                    logger.info('tx and rx pcm file is exist -> {0}'.format(check_file_name))
                    # Enter the PCM file name in the List file
                    logger.info('Enter the PCM file in the List file')
                    list_file_path = '{0}/{1}_n{2}.list'.format(FIND_CONFIG['stt_path'], TARGET_DIR_NAME, list_file_cnt)
                    curr_list_file_path = '{0}/{1}_n{2}.list'.format(
                        FIND_CONFIG['stt_path'], TARGET_DIR_NAME, list_file_cnt)
                    DELETE_FILE_LIST.append(list_file_path)
                    DELETE_FILE_LIST.append(curr_list_file_path)
                    output_file_div = open(list_file_path, 'a')
                    print >> output_file_div, tx_pcm_file_path
                    print >> output_file_div, rx_pcm_file_path
                    output_file_div.close()
                    # Calculate the result value.
                    logger.info('Calculate the result value')
                    PCM_CNT += 2
                    TOTAL_PCM_TIME += os.stat(tx_pcm_file_path)[6] / 16000.0
                    TOTAL_PCM_TIME += os.stat(rx_pcm_file_path)[6] / 16000.0
                    # Calculate the thread
                    if list_file_cnt > max_list_file_cnt:
                        max_list_file_cnt = list_file_cnt
                    if list_file_cnt + 1 == FIND_CONFIG['thread']:
                        list_file_cnt = 0
                        continue
                    list_file_cnt += 1
    # Last Calculate the thread count
    logger.info('Calculate the thread count')
    if max_list_file_cnt == 0:
        thread_cnt = 1
    else:
        thread_cnt = max_list_file_cnt + 1
    return thread_cnt


def make_pcm_file(logger):
    """
    Create PCM file
    :param      logger:     Logger
    """
    logger.info('2. Do make pcm file')
    w_ob = os.walk(TARGET_DIR_PATH)
    for dir_name, sub_dirs, files in w_ob:
        for file_name in files:
            if check_file('.wav', file_name):
                call_id = file_name.replace('.wav', '')
                wav_file_path = '{0}/{1}'.format(TARGET_DIR_PATH, file_name)
                pcm_file_path = '{0}/{1}.pcm'.format(TARGET_DIR_PATH, call_id)
                if os.path.exists(pcm_file_path):
                    del_garbage(logger, pcm_file_path)
                sox_cmd = 'sox -t wav {0} -r 8000 -b 16 -t raw {1}'.format(wav_file_path, pcm_file_path)
                logger.debug(sox_cmd)
                sub_process(logger, sox_cmd)


def separation_wav_file(logger, wav_file_path_list):
    """
    Separation wav file
    :param      logger:                 Logger
    :param      wav_file_path_list:     List of wav file name
    """
    logger.info('1. Do separation wav file')
    # wav file move to temp directory
    for wav_file_dic in wav_file_path_list:
        for wav_file_path in wav_file_dic:
            logger.info('copy file -> {0} in {1}'.format(wav_file_path, TARGET_DIR_PATH))
            shutil.copy(wav_file_path, TARGET_DIR_PATH)
            # Do separation wav file
            wav_file_name = os.path.basename(wav_file_path)
            target_wav_file_path = '{0}/{1}'.format(TARGET_DIR_PATH, wav_file_name)
            target_wav_call_name = target_wav_file_path.replace('.wav', '')
            rx_wav_file_path = '{0}_rx.wav'.format(target_wav_call_name)
            tx_wav_file_path = '{0}_tx.wav'.format(target_wav_call_name)
            if os.path.exists(rx_wav_file_path):
                del_garbage(logger, rx_wav_file_path)
            if os.path.exists(tx_wav_file_path):
                del_garbage(logger, tx_wav_file_path)
            logger.info('Execute ffmpeg')
            ffmpeg_cmd = '{0}/ffmpeg -i {1} -filter_complex "[0:0]pan=1c|c0=c0[left];[0:0]pan=1c|c0=c1[right]" -map ' \
                         '"[left]" {2} -map "[right]" {3}'.format(
                FIND_CONFIG['tool_dir_path'], target_wav_file_path, rx_wav_file_path, tx_wav_file_path)
            logger.debug(ffmpeg_cmd)
            os.chdir(TARGET_DIR_PATH)
            sub_process(logger, ffmpeg_cmd)
            # Remove copy wav file
            del_garbage(logger, target_wav_file_path)
    logger.info('Success separation wav file')


def processing(wav_file_path_list):
    """
    CS processing
    :param      wav_file_path_list:     Path list of wav file
    """
    global TARGET_DIR_NAME
    global TARGET_DIR_PATH
    global DELETE_FILE_LIST
    cnt = 0
    # Determine temp directory name to be used in script
    while True:
        TARGET_DIR_NAME = '{0}/find_cs_temp_directory_{1}'.format(FIND_CONFIG['stt_path'], cnt)
        if not os.path.exists(TARGET_DIR_PATH):
            os.makedirs(TARGET_DIR_PATH)
            TARGET_DIR_NAME = os.path.basename(TARGET_DIR_PATH)
            DELETE_FILE_LIST.append(TARGET_DIR_PATH)
            break
        cnt += 1
    # Determine log_name
    log_name = '{0}_{1}'.format(FIND_CONFIG['log_name'], cnt)
    # Add logging
    logger_args = {
        'base_path': FIND_CONFIG['log_dir_path'],
        'log_file_name': log_name,
        'log_level': FIND_CONFIG['log_level']
    }
    logger = set_logger(logger_args)
    logger.info('-' * 100)
    logger.info('Start CS')
    try:
        # 1. Separation wav file
        separation_wav_file(logger, wav_file_path_list)
        # 2. Make pcm file
        make_pcm_file(logger)
        # 3. Make list file
        thread_cnt = make_list_file(logger)
        # 4. Execute DNN
        execute_dnn(logger, thread_cnt)
        # 5. Execute unseg.exe
        unseg_dir = execute_unseg(logger)
        # 6. Make output
        txt_dir, detail_dir = make_output(logger, wav_file_path_list, unseg_dir)
        # 7. Make bad_call list file
        make_list(logger, detail_dir, wav_file_path_list)
        # 8. Delete garbage list
        delete_garbage_file(logger)
    except Exception:
        exc_info = traceback.format_exc()
        logger.error(exc_info)
        logger.error('---------- ERROR ----------')
        delete_garbage_file(logger)
        sys.exit(1)
    for handler in logger.handlers:
        handler.close()
        logger.removeHandler(handler)


########
# main #
########
def main(job_list):
    """
    This is a program that find a CS that you do not need
    :param      job_list:       List of job
    """
    global ST
    global DT
    ts = time.time()
    ST = datetime.fromtimestamp(ts).strftime('%Y-%m-%d-%H:%M.%S')
    DT = datetime.fromtimestamp(ts).strftime('%Y%m%d%H%M%S')
    try:
        processing(job_list)
    except Exception:
        exc_info = traceback.format_exc()
        print exc_info
        print '---------- ERROR -----------'
        sys.exit(1)