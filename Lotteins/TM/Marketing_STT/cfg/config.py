#!/usr/bin/python
# -*- coding: euc-kr -*-

UPLOAD_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/Marketing_STT',
    'log_name': 'upload_dat.log',
    'log_level': 'info',
    'dat_dir_path': '/app/record/TM/Marketing',
    'dat_output_path': '/app/MindsVOC/TM/Marketing_STT/processed_dat'
}

ORACLE_DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}

START_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/Marketing_STT',
    'log_name': 'marketing_start.log',
    'log_level': 'info',
    'job_max_limit': 5,
    'process_max_limit': 3,
    'stt_script_path': '/app/MindsVOC/TM/Marketing_STT',
    'process_interval': 1
}

STT_CONFIG = {
    'gpu': 2,
    'log_dir_path': '/log/MindsVOC/TM/Marketing_STT',
    'log_level': 'debug',
    'rec_dir_path': '/app/record',
    'stt_path': '/app/MindsVOC/TM/STT',
    'stt_output_path': '/app/MindsVOC/TM/Marketing_STT/STTA_output',
    'stt_tool_path': '/app/MindsVOC/TM/STT/tools',
    'thread': 2,
    'silence_seconds': 5
}

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|�ϰ�|ĥ|����|��|��ȩ|��|��|��|��|��|��|��)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|ĥ|��|��|��|��|��|��|��|��|��|õ|��)\s?){3,}',
#    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:��|��)|��|��|��|��|��|��|ĥ|��|��|��|��|��|��)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:��\s?��\s?��)|(?:��\s?��)|("?:��\s?��)|(?:��\s?��)|(?:��\s?��\s?��\?��)|(?:�Ѹ���)|(?:������)|(?:�����)|(?:�ָ���))',
    'address_rule': r'\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)|\s((.){1,4}ȣ)|((?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
    'name_rule': r'(?:(��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|(?:�Һ�)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|â|ä|ô|õ|��|��|��|��|��|Ź|ź|��|��|��|��|��|��|ǥ|��|��|��|��|��|��|��|��|��|��|��|ȣ|ȫ|ȭ|ȯ|Ȳ|(?:Ȳ��))\s?[(��-��)](\s?[(��-��)]))'
}

OPENSSL_CONFIG = {
    'codec_file_path': '/app/MindsVOC/CS/TA/Ldcc/cfg/codec.cfg'
}