#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = "MINDsLAB"
__date__ = "creation: 2017-12-05, modification: 0000-00-00"

###########
# imports #
###########
import os
import sys
from datetime import timedelta

###########
# options #
###########
reload(sys)
sys.setdefaultencoding("utf-8")

########
# main #
########


def main(log_dir_path):
    """
    This is a program that monitoring script
    :param          log_dir_path:            Log directory path
    """
    total_wav_cnt = 0
    total_wav_duration = 0
    total_required_time = 0
    if not os.path.exists(log_dir_path) or not os.path.isdir(log_dir_path):
        print "{0} is not log directory path.".format(log_dir_path)
        sys.exit(1)
    w_ob = os.walk(log_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if not file_name.endswith('.log'):
                continue
            log_file_path = os.path.join(dir_path, file_name)
            poli_no = ''
            ctrdt = ''
            chn_tp = ''
            wav_duration = ''
            required_time = ''
            error_yn = False
            log_file = open(log_file_path, 'r')
            for line in log_file:
                line = line.strip()
                if './mt_long_utt_dnn_support.gpu.exe' in line:
                    line_list = line.split('./mt_long_utt_dnn_support.gpu.exe')[1].strip()
                    item = line_list.split()[0]
                    item_list = item.split('_')
                    poli_no = item_list[0]
                    ctrdt = item_list[1]
                if 'CHN_TP = ' in line:
                    idx = line.find('CHN_TP = ') + len('CHN_TP = ')
                    chn_tp = line[idx]
                if chn_tp == 'M' and 'Total WAV duration' in line:
                    wav_duration = line.split('=')[1].strip()
                if chn_tp == 'S' and 'Total WAV average duration' in line:
                    wav_duration = line.split('=')[1].strip()
                if 'TOTAL END' in line:
                    required_time = line.split('The time required = ')[1].strip()
                if 'ERROR' in line:
                    error_yn = True
            if error_yn:
                continue
            print "{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(
                log_file_path, poli_no, ctrdt, chn_tp, wav_duration, required_time)
            total_wav_cnt += 1
            wav_duration_list = wav_duration.split(':')
            wav_hours = float(wav_duration_list[0]) * 3600
            wav_minutes = float(wav_duration_list[1]) * 60
            wav_seconds = float(wav_duration_list[2])
            total_wav_duration += wav_hours + wav_minutes + wav_seconds
            required_time_list = required_time.split(':')
            required_hours = float(required_time_list[0]) * 3600
            required_minutes = float(required_time_list[1]) * 60
            required_seconds = float(required_time_list[2])
            total_required_time += required_hours + required_minutes + required_seconds
            log_file.close()
    output_wav_duration = timedelta(seconds=total_wav_duration)
    output_wav_average_duration = timedelta(seconds=total_wav_duration / float(total_wav_cnt))
    output_required_time = timedelta(seconds=total_required_time)
    output_average_required_time = timedelta(seconds=total_required_time / float(total_wav_cnt))
    print "Total wav count = {0}, duration = {1}, average duration = {2}, required time = {3}, " \
          "average required time = {4}".format(total_wav_cnt, output_wav_duration, output_wav_average_duration,
                                               output_required_time, output_average_required_time)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print "usage : python {0} [Log directory path]".format(sys.argv[0])
        sys.exit(1)