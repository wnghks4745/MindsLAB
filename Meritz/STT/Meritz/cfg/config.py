#!/usr/bin/python
# -*- coding: euc-kr -*-

FIND_DAEMON_CONFIG = {
    'log_dir_path': '/data1/MindsVOC/CS/Meritz/logs',
    'pid_file_path': '/data1/MindsVOC/CS/Meritz/find_STT_daemon.pid',
    'job_max_limit': 5,
    'process_max_limit': 4,
    'process_interval': 1,
    'log_file_name': 'find_STT_daemon.log',
    'check_dir_path': '/nasdata/solution/irlink/zirecast',
    'tool_dir_path': '/data1/MindsVOC/CS/Meritz',
    'check_time': ['00:03:00', '00:08:00']
}

FIND_CONFIG = {
    'stt_path': '/data1/MindsVOC/CS',
    'log_dir_path': '/data1/MindsVOC/CS/Meritz',
    'log_name': 'find_cs_that_do_not_read',
    'log_level': 'debug',
    'tool_dir_path': '/data1/MindsVOC/CS/tools',
    'thread': 24,
    'gpu': 1,
    'search_list': [
        '음성 사서함',
        '음성사서함',
        '소리샘',
        '소리 샘'
    ]
}