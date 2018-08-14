#!/usr/bin/python
# -*- coding: euc-kr -*-

"""program"""
__author__ = 'MindsLAB'
__date__ = 'creation: 2017-12-08, modification: 2017-12-08'

###########
# imports #
###########
import os
import sys
import collections
from datetime import timedelta

########
# main #
########
def main(log_dir_path):
    sorted_time_dict = collections.OrderedDict()
    total_process_count = 0
    w_ob = os.walk(log_dir_path)
    for dir_path, sub_dirs, files in w_ob:
        for file_name in files:
            if not file_name.endswith('.log'):
                continue
            log_file_path = os.path.join(dir_path, file_name)
            log_file = open(log_file_path, 'r')
            checking = False
            script_required_hours = 0
            script_required_minutes = 0
            script_required_seconds = 0
            script_process_count = 0
            for line in log_file:
                if 'ERROR' in line and checking:
                    script_process_count -= 1
                    checking = False
                if 'Set logger' in line and not checking:
                    time = line.split(' ')[1].strip()
                    hour = time.split(':')[0].strip()
                    checking = True
                    script_process_count += 1
                if 'The time required = ' in line and checking:
                    required_time = line.split('The time required =')[1].strip()
                    required_time = required_time.replace('(', '').replace(')', '').strip()
                    required_days, required_hours, required_minutes, required_seconds = required_time.split(',')
                    script_required_hours += float(required_hours)
                    script_required_minutes += float(required_minutes)
                    script_required_seconds += float(required_seconds)
                    checking = False
            if not hour in sorted_time_dict:
                sorted_time_dict[hour] = dict()
                sorted_time_dict[hour]['required_hours'] = script_required_hours
                sorted_time_dict[hour]['required_minutes'] = script_required_minutes
                sorted_time_dict[hour]['required_seconds'] = script_required_seconds
                sorted_time_dict[hour]['process_count'] = script_process_count
            else:
                sorted_time_dict[hour]['required_hours'] += script_required_hours
                sorted_time_dict[hour]['required_minutes'] += script_required_minutes
                sorted_time_dict[hour]['required_seconds'] += script_required_seconds
                sorted_time_dict[hour]['process_count'] += script_process_count
    print 'directory path -> {0}'.format(log_dir_path)
    for hour in sorted_time_dict:
        total_required_time = sorted_time_dict[hour]['required_hours'] * 3600 + \
                              sorted_time_dict[hour]['required_minutes'] * 60 + \
                              sorted_time_dict[hour]['required_seconds']
        output_average_required_time = timedelta(seconds=total_required_time / float(sorted_time_dict[hour]['process_count']))
        print 'hour .. {0}  average required time = {1}'.format(hour, output_average_required_time)


if __name__ == '__main__':
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        print 'usage : python {0} [Log directory path]'.format(sys.argv[0])
        sys.exit(1)