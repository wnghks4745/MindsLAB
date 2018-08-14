#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = 'MindsLAB'
__date__ = 'creation: 2017-12-07, modification: 2017-12-07'


###########
# imports #
###########
import os
import sys
from datetime import timedelta


########
# main #
########
def main(log_dir_path):
    total_process_cnt = 0
    total_wav_duration = 0
    total_required_time = 0
    total_result_count = 0
    w_ob = os.walk(log_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if not file_name.endswith('.log'):
                continue
            log_file_path = os.path.join(dir_path, file_name)
            log_file = open(log_file_path, 'r')
            for line in log_file:
                line = line.strip()
                if 'CS END' in line:
                    total_process_cnt += 1
                    required_time = line.split('=')[2].strip()
                    required_time_list = required_time.split(':')
                    required_hours = float(required_time_list[0]) * 3600
                    required_minutes = float(required_time_list[1]) * 60
                    required_seconds = float(required_time_list[2])
                    total_required_time += (required_hours + required_minutes + required_seconds)
                if 'Total WAV average duration' in line:
                    wav_duration = line.split('=')[1].strip()
                    wav_duration_list = wav_duration.split(':')
                    wav_hours = float(wav_duration_list[0]) * 3600
                    wav_minutes = float(wav_duration_list[1]) * 60
                    wav_seconds = float(wav_duration_list[2])
                    total_wav_duration += (wav_hours + wav_minutes + wav_seconds)
                if 'Result count' in line:
                    result_count = line.split('=')[1].strip()
                    if result_count is not int:
                        result_count = 0
                    total_result_count += int(result_count)
    if total_process_cnt == 0:
        print 'CS Log is not exist'
        sys.exit(0)
    output_wav_average_duration = timedelta(seconds=total_wav_duration / float(total_process_cnt))
    output_average_required_time = timedelta(seconds=total_required_time / float(total_process_cnt))
    print 'Total process count = {0}, average duration = {1}, average required time = {2}, Total wav count = {3}'.format(
        total_process_cnt, output_wav_average_duration, output_average_required_time, total_result_count)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print 'usage : python {0} [Log directory path]'.format(sys.argv[0])
        sys.exit(1)