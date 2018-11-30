#!/usr/bin/python
# -*- coding:utf-8 -*-


class CollectorConfig(object):
    logger_name = 'COLLECTOR'
    log_dir_path = '/logs/maum/cvta'
    log_file_name = 'collector_process.log'
    backup_count = 8
    log_level = 'info'
    collector_work_dir_list = [
        '/data/maum/source',
    ]
    processed_dir_path = '/data/maum/processed'
    error_dir_path = '/data/maum/error'


class DaemonConfig(object):
    logger_name = 'DAEMON'
    log_dir_path = '/logs/maum/cvta'
    log_file_name = 'daemon_process.log'
    backup_count = 8
    log_level = 'info'
    process_max_limit = 80
    process_interval = 0.05


class OracleConfig(object):
    host = '172.18.200.143'
    host_list = ['172.18.200.143', '172.18.200.144']
    user = 'STTAAPP'
    pd = 'sttasvc!@'
    port = 2525
    sid = 'PUSTTA'
    service_name = 'PUSTTA'
    reconnect_interval = 10


class TAConfig(object):
    logger_name = 'TA'
    log_dir_path = '/logs/maum/cvta'
    log_level = 'info'
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
    hmd_home_model_name = 'HOME'
    hmd_mobile_model_name = 'MOBILE'
    processed_dir_path = '/data/maum/processed'
    nlp_output_dir_path = '/data/maum/nlp_output'
    hmd_output_dir_path = '/data/maum/hmd_output'
    modified_hmd_output_dir_path = '/data/maum/modified_hmd_output'


class DELConfig(object):
    logger_name = 'DELETE'
    log_dir_path = '/logs/maum/cvta'
    log_file_name = 'delete_file.log'
    backup_count = 8
    log_level = 'info'
    target_directory_list = [
        {
            'dir_path': '/logs/maum/cvta',
            'mtn_period': 62
        },
        {
            'dir_path': '/data/maum/processed',
            'mtn_period': 20
        },
        {
            'dir_path': '/data/maum/nlp_output',
            'mtn_period': 20
        },
        {
            'dir_path': '/data/maum/hmd_output',
            'mtn_period': 20
        },
        {
            'dir_path': '/data/maum/modified_hmd_output',
            'mtn_period': 20
        },
        {
            'dir_path': '/logs/maum/kafka',
            'mtn_period': 62
        }
    ]


class TAMNTConfig(object):
    logger_name = 'TAMNT'
    log_dir_path = '/logs/maum/cvta'
    log_file_name = 'ta_monitoring.log'
    backup_count = 8
    log_level = 'info'
