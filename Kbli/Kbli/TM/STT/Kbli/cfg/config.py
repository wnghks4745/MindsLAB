#!/usr/bin/python
# -*- coding: euc-kr -*-
TM = '/app/prd/MindsVOC/TM/STT'
TM_LOG_PATH = '/log/MindsVOC/TM/STT'
REC_SERVER_PATH = '/app/rec_server/prd/kbliTM/b'
CARD_REC_SERVER_PATH = '/app/rec_server/prd/cardTM'

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|�ϰ�|ĥ|����|��|��ȩ|��|��|��|��|��|��|��)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|ĥ|��|��|��|��|��|��|��|��|��|õ|��)\s?){3,}',
    # 'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:��|��)|��|��|��|��|��|��|ĥ|��|��|��|��|��|��)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:��\s?��\s?��)|(?:��\s?��)|("?:��\s?��)|(?:��\s?��)|(?:��\s?��\s?��\?��)|(?:�Ѹ���)|(?:������)|(?:�����)|(?:�ָ���))',
#    'address_rule': r'(��|��|ȣ|(?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
#    'address_rule': r'(����|���)|\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)',
    'address_rule': r'\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)|((?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
    'name_rule': r'(?:(��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|(?:�Һ�)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|â|ä|ô|õ|��|��|��|��|��|Ź|ź|��|��|��|��|��|��|ǥ|��|��|��|��|��|��|��|��|��|��|��|ȣ|ȫ|ȭ|ȯ|Ȳ|(?:Ȳ��))\s?[(��-��)](\s?[(��-��)]))'
}

# ============== DB_CONFIG ==================
DEV_ORACLE_DB_CONFIG = {
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.136)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=kbldev)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

UAT_ORACLE_DB_CONFIG = {
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.138)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=kbluat)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

PRD_ORACLE_DB_CONFIG = {
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

POSTGRESQL_DB_CONFIG = {
    'user': 'sttuser',
    'password': 'sttuser',
    'db': 'ir',
    'host': '172.226.205.241',
    'port': 5432,
    'charset': 'utf8',
    'connect_timeout': 5
}


# ============== DAEMON_CONFIG ==================

DEV_DAEMON_CONFIG = {
    'log_dir_path': TM_LOG_PATH + '_dev',
    'pid_file_path': TM + '/Kbli_dev/bin/STT_daemon.pid',
    'job_max_limit': 1,
    'process_max_limit': 3,
    'process_interval': 1,
    'log_file_name': 'STT_dev_daemon.log',
    'rec_server_path': REC_SERVER_PATH,
    'card_rec_server_path': CARD_REC_SERVER_PATH,
    'stt_script_path': TM,
    'cycle_time': 20,
    'search_date_range': 30
}

PRD_DAEMON_CONFIG = {
    'log_dir_path': TM_LOG_PATH,
    'pid_file_path': TM + '/Kbli/bin/STT_daemon.pid',
    'job_max_limit': 5,
    'process_max_limit': 10,
    'process_interval': 1,
    'log_file_name': 'STT_daemon.log',
    'rec_server_path': REC_SERVER_PATH,
    'card_rec_server_path': CARD_REC_SERVER_PATH,
    'stt_script_path': TM,
    'cycle_time': 20,
    'search_date_range': 30
}


# ============== CONFIG ==================

DEV_CONFIG = {
    'gpu': 1,
    'log_level': 'DEBUG',
    'stt_path': TM,
    'log_dir_path': TM_LOG_PATH + '_dev',
    'rec_dir_path': REC_SERVER_PATH,
    'card_rec_dir_path': CARD_REC_SERVER_PATH,
    'codec_file_path': TM + '/Kbli_dev/cfg/codec.cfg',
    'thread': 1,
    'stt_script_path': TM + '/Kbli_dev',
    'stt_tool_path': TM + '/tools',
    'wav_output_path': '/app/prd/MindsVOC/wav/TM_dev',
    'stt_output_path': TM + '/Kbli_dev/STT_output',
    'silence_seconds': 0,
}

PRD_CONFIG = {
    'gpu': 2,
    'log_level': 'INFO',
    'stt_path': TM,
    'log_dir_path': TM_LOG_PATH,
    'rec_dir_path': REC_SERVER_PATH,
    'card_rec_dir_path': CARD_REC_SERVER_PATH,
    'codec_file_path': TM + '/Kbli/cfg/codec.cfg',
    'thread': 2,
    'stt_script_path': TM + '/Kbli',
    'stt_tool_path': TM + '/tools',
    'wav_output_path': '/app/prd/MindsVOC/wav/TM',
    'stt_output_path': TM + '/Kbli/STT_output',
    'silence_seconds': 0,
}

OPENSSL_CONFIG = {
    'codec_file_path': TM + '/Kbli/cfg/codec.cfg'
}

ORACLE_DB_CONFIG = {
    'dev': DEV_ORACLE_DB_CONFIG,
    'uat': UAT_ORACLE_DB_CONFIG,
    'prd': PRD_ORACLE_DB_CONFIG
}

DAEMON_CONFIG = {
    'dev': DEV_DAEMON_CONFIG,
    'uat': DEV_DAEMON_CONFIG,
    'prd': PRD_DAEMON_CONFIG
}

CONFIG = {
    'dev': DEV_CONFIG,
    'uat': DEV_CONFIG,
    'prd': PRD_CONFIG
}