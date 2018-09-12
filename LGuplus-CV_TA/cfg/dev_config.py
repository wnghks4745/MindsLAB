#!/usr/bin/python
# -*- coding:utf-8 -*-


class CollectorConfig(object):
    logger_name = 'COLLECTOR'
    log_dir_path = '/logs/maum/dev'
    log_file_name = 'collector_process.log'
    backup_count = 8
    log_level = 'debug'
    collector_work_dir_list = [
        '/data/maum/prd/source',
    ]
    processed_dir_path = '/data/maum/dev/processed'
    error_dir_path = '/data/maum/dev/error'


class DaemonConfig(object):
    logger_name = 'DAEMON'
    log_dir_path = '/logs/maum/dev'
    log_file_name = 'daemon_process.log'
    backup_count = 8
    log_level = 'debug'
    process_max_limit = 50
    process_interval = 0.05


class OracleConfig(object):
    host = '172.18.217.146'
    host_list = ['172.18.217.146']
    user = 'STTAADM'
    pd = 'sttasvc!@'
    port = 1525
    sid = 'DUSTTA'
    service_name = 'DUSTTA'
    reconnect_interval = 10


class TAConfig(object):
    logger_name = 'TA'
    log_dir_path = '/logs/maum/dev'
    backup_count = 5
    log_level = 'debug'
    output_file = True
    nlp_engine = 'nlp3'
    upload_tag_dict = {
        'NB': '명사(브랜드명)',
        'NNP': '고유명사',
        'NNG': '일반명사',
        'NNB': '의존명사',
        'NR': '수사',
        'VV': '동사',
        'VA': '형용사',
        'VX': '보조용언',
        'XR': '어근',
        'SL': '외국어',
        'SN': '숫자',
        'MM': '관형사',
        'MAG': '일반부사'
    }
    hmd_cate_delimiter = '!@#$'
    hmd_home_model_name = 'home_hmd_20180713'
    hmd_mobile_model_name = 'mobile_hmd_20180728'
    processed_dir_path = '/data/maum/dev/processed'
    nlp_output_dir_path = '/data/maum/dev/nlp_output'
    hmd_output_dir_path = '/data/maum/dev/hmd_output'
    modified_hmd_output_dir_path = '/data/maum/dev/modified_hmd_output'


class DELConfig(object):
    logger_name = 'DELETE'
    log_dir_path = '/logs/maum/dev'
    log_file_name = 'delete_file.log'
    backup_count = 8
    log_level = 'debug'
    target_directory_list = [
        {
            'dir_path': '/logs/maum/dev',
            'mtn_period': 62
        },
        {
            'dir_path': '/data/maum/dev/processed',
            'mtn_period': 20
        },
        {
            'dir_path': '/data/maum/dev/nlp_output',
            'mtn_period': 20
        },
        {
            'dir_path': '/data/maum/dev/hmd_output',
            'mtn_period': 20
        },
        {
            'dir_path': '/data/maum/dev/modified_hmd_output',
            'mtn_period': 20
        },
        {
            'dir_path': '/logs/maum/kafka',
            'mtn_period': 62
        }
    ]
