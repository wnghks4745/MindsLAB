#!/usr/bin/python
# -*- coding:utf-8 -*-


class TADaemonConfig(object):
    logger_name = 'DAEMON'
    log_dir_path = '/logs/maum/ta'
    log_file_name = 'daemon_process.log'
    backup_count = 8
    log_level = 'debug'
    process_max_limit = 20
    process_interval = 0.05


class STTDaemonConfig(object):
    logger_name = 'DAEMON'
    log_dir_path = '/logs/maum/stt'
    log_file_name = 'daemon_process.log'
    backup_count = 8
    log_level = 'info'
    process_max_limit = 20
    process_interval = 0.05


class Oracle_mlsta_Config(object):
    # host = '40.242.105.137'
    host_list = ['40.242.105.137', '40.242.105.138']
    user = 'mlsta'
    pd = 'iCatch2018!'
    port = 4521
    sid = 'CATCHALL1'
    service_name = False
    reconnect_interval = 10


class TAConfig(object):
    logger_name = 'TA'
    log_dir_path = '/logs/maum/ta'
    log_level = 'debug'
    nlp_engine = 'nlp3'
    hmd_cate_delimiter = '!@#$'
    processed_dir_path = '/data1/maum/processed'
    nlp_output_dir_path = '/data1/maum/nlp_output'
    hmd_output_dir_path = '/data1/maum/hmd_output'
    hmd_section_output_dir_path = '/data1/maum/section_hmd_output'
    output_file = True
    reply_range = 5
    reply_count = 3


class GETSTTConfig(object):
    logger_name = 'GETSTT'
    log_dir_path = '/logs/maum/stt'
    log_file_name = 'get_stt_rst'
    backup_count = 8
    log_level = 'info'


class UtillConfig(object):
    backup_count = 3
    codec_path = '/data1/maum/code/cfg/codec.cfg'


class DELConfig(object):
    logger_name = 'DELETE'
    log_dir_path = '/logs/maum/delete'
    log_file_name = 'delete_file.log'
    backup_count = 8
    log_level = 'info'
    target_directory_list = [
        # {
        #     'dir_path': '/logs/maum/ta',
        #     'mtn_period': 62
        # },
        # {
        #     'dir_path': '/logs/maum/stt',
        #     'mtn_period': 62
        # },
        # {
        #     'dir_path': '/logs/maum/delete',
        #     'mtn_period': 62
        # },
        # {
        #     'dir_path': '/logs/maum/batch',
        #     'mtn_period': 62
        # },
        {
            'dir_path': '/data1/maum/nlp_output',
            'mtn_period': 31
        },
        {
            'dir_path': '/data1/maum/hmd_output',
            'mtn_period': 31
        },
        {
            'dir_path': '/data1/maum/section_hmd_output',
            'mtn_period': 31
        },
    ]


class DELDaemonConfig(object):
    logger_name = 'DELDaemon'
    log_dir_path = '/logs/maum/delete'
    log_file_name = 'delete_daemon.log'
    backup_count = 8
    log_level = 'debug'


class BatchConfig(object):
    logger_name = 'Etms_Batch'
    log_dir_path = '/logs/maum/batch'
    log_file_name = 'etms_batch'
    log_level = 'debug'
    target_dir_path = '/data1/maum/was/EAI/ETMS'


class SummaryBatchConfig(object):
    logger_name = 'Summary_Batch'
    log_dir_path = '/logs/maum/batch'
    log_file_name = 'summary_batch'
    log_level = 'debug'
