#!/usr/bin/python
# -*- coding: euc-kr -*-
CS_TA = '/app/prd/MindsVOC/CS/TA'
CS_TA_LOG_PATH = '/log/MindsVOC/CS/TA'

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

# ============== DB_CONFIG ==================
DEV_DB_CONFIG = {
    # 'cs_user': 'TELECS_NEW',
    'cs_user': 'ZCS',
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.136)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=kbldev)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

UAT_DB_CONFIG = {
    'cs_user': 'ZCS',
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.138)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=kbluat)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

PRD_DB_CONFIG = {
    'cs_user': 'ZCS',
    'dsn': '''
    (DESCRIPTION=
        (ADDRESS_LIST=
    (ADDRESS=(PROTOCOL=TCP)(HOST=172.226.254.111)(PORT=1561))
    (ADDRESS=(PROTOCOL=TCP)(HOST=172.226.254.113)(PORT=1561))
    )
(CONNECT_DATA=(SERVICE_NAME=KBLAM))
    )
    ''',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

# ============== CS_TA_DAEMON_CONFIG ==================
DEV_DAEMON_CONFIG = {
    'log_dir_path': '/log/MindsVOC/CS_dev/TA',
    'log_file_name': 'TA_daemon.log',
    'pid_dir_path': CS_TA + '/Kbli_dev/bin',
    'pid_file_name': 'TA_daemon.pid',
    'job_max_limit': 2,
    'process_interval': 10,
    'process_max_limit': 1,
    'ta_script_path': CS_TA + '/Kbli_dev',
    'search_date_range': 120,
    'cycle_time': 20,
}

PRD_DAEMON_CONFIG = {
    'log_dir_path': CS_TA_LOG_PATH,
    'log_file_name': 'TA_daemon.log',
    'pid_dir_path': CS_TA + '/Kbli/bin',
    'pid_file_name': 'TA_daemon.pid',
    'job_max_limit': 5,
    'process_interval': 10,
    'process_max_limit': 5,
    'ta_script_path': CS_TA + '/Kbli',
    'search_date_range': 120,
    'cycle_time': 20,
}

# ============== TA_CONFIG ==================
DEV_TA_CONFIG = {
    'hmd_thread': 2,
    'nl_thread': 2,
    'log_level': 'debug',
    'stt_output_path': '/app/prd/MindsVOC/CS/STT/Kbli/STT_output',
    'matrix_file_path':  CS_TA + '/Kbli_dev/matrix/chatbot.matrix',
    'hmd_script_path': CS_TA + '/Kbli_dev/lib',
    'log_dir_path': '/log/MindsVOC/CS_dev/TA',
    'kywd_detect_range': 2,
    'ta_output_path': CS_TA + '/Kbli_dev/TA_output',
    'ta_bin_path': CS_TA + '/LA/bin',
    'ta_data_path': CS_TA + '/data',
    'ta_path': CS_TA
}

PRD_TA_CONFIG = {
    'hmd_thread': 4,
    'nl_thread': 4,
    'log_level': 'info',
    'stt_output_path': '/app/prd/MindsVOC/CS/STT/Kbli/STT_output',
    'matrix_file_path': CS_TA + '/Kbli/matrix/chatbot.matrix',
    'hmd_script_path': CS_TA + '/Kbli/lib',
    'log_dir_path': CS_TA_LOG_PATH,
    'kywd_detect_range': 2,
    'ta_output_path': CS_TA + '/Kbli/TA_output',
    'ta_bin_path': CS_TA + '/LA/bin',
    'ta_data_path': CS_TA + '/data',
    'ta_path': CS_TA
}

OPENSSL_CONFIG = {
    'codec_file_path': CS_TA + '/Kbli/cfg/codec.cfg'
}

TA_CONFIG = {
    'dev': DEV_TA_CONFIG,
    'uat': PRD_TA_CONFIG,
    'prd': PRD_TA_CONFIG
}

DAEMON_CONFIG = {
    'dev': DEV_DAEMON_CONFIG,
    'uat': PRD_DAEMON_CONFIG,
    'prd': PRD_DAEMON_CONFIG
}

DB_CONFIG = {
    'dev': DEV_DB_CONFIG,
    'uat': UAT_DB_CONFIG,
    'prd': PRD_DB_CONFIG
}
