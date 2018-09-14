#!/usr/bin/python
# -*- coding: euc-kr -*-
CS = '/app/prd/MindsVOC/CS/STT'
CS_LOG_PATH = '/log/MindsVOC/CS/STT'
REC_SERVER_PATH = '/app/rec_server/prd/kbliCS/b'

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|일곱|칠|여덟|팔|아홉|구|공|넷|셋|영|십|백)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십|년|월|백|천|시)\s?){3,}',
    # 'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하|나)|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:골\s?뱅\s?이)|(?:닷\s?컴)|("?:다\s?컴)|(?:닷\s?넷)|(?:닷\s?케\s?이\?알)|(?:한메일)|(?:지메일)|(?:쥐메일)|(?:핫메일))',
#    'address_rule': r'(군|동|호|(?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
#    'address_rule': r'(서울|경기)|\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)',
    'address_rule': r'\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)|((?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
    'name_rule': r'(?:(가|간|갈|감|강|개|견|경|계|고|곡|공|곽|교|구|국|군|궁|궉|권|근|금|기|길|김|나|라|남|(?:남궁)|낭|랑|내|노|로|뇌|누|단|담|당|대|도|(?:독고)|돈|동|(?:동방)|두|라|류|마|망|절|매|맹|먕|모|묘|목|묵|문|미|민|박|반|방|배|백|범|변|복|봉|부|빈|빙|사|(?:사공)|삼|상|서|(?:서문)|석|선|(?:선우)|설|섭|성|소|(?:소봉)|손|송|수|순|숭|시|신|심|십|아|안|애|야|양|량|어|(?:어금)|엄|여|연|염|영|예|오|옥|온|옹|왕|요|용|우|운|원|위|유|육|윤|은|음|이|인|임|림|자|장|전|점|정|제|(?:제갈)|조|종|좌|주|준|즙|지|진|차|창|채|척|천|초|최|추|축|춘|탁|탄|태|판|패|편|평|포|표|퐁|피|필|하|학|한|함|해|허|현|형|호|홍|화|환|황|(?:황보))\s?[(가-힐)](\s?[(가-힐)]))'
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
    'host': '172.226.201.241',
    'port': 5432,
    'charset': 'utf8',
    'connect_timeout': 5
}


# ============== DAEMON_CONFIG ==================

DEV_DAEMON_CONFIG = {
    'log_dir_path': CS_LOG_PATH + '_dev',
    'pid_file_path': CS + '/Kbli_dev/bin/STT_daemon.pid',
    'job_max_limit': 5,
    'process_max_limit': 3,
    'process_interval': 1,
    'log_file_name': 'STT_dev_daemon.log',
    'rec_server_path': REC_SERVER_PATH,
    'stt_script_path': CS,
    'cycle_time': 20,
    'search_date_range': 30
}

PRD_DAEMON_CONFIG = {
    'log_dir_path': CS_LOG_PATH,
    'pid_file_path': CS + '/Kbli/bin/STT_daemon.pid',
    'job_max_limit': 5,
    'process_max_limit': 10,
    'process_interval': 1,
    'log_file_name': 'STT_daemon.log',
    'rec_server_path': REC_SERVER_PATH,
    'stt_script_path': CS,
    'cycle_time': 20,
    'search_date_range': 30
}


# ============== CONFIG ==================

DEV_CONFIG = {
    'gpu': 1,
    'log_level': 'DEBUG',
    'stt_path': CS,
    'log_dir_path': CS_LOG_PATH + '_dev',
    'rec_dir_path': REC_SERVER_PATH,
    'codec_file_path': CS + '/Kbli_dev/cfg/codec.cfg',
    'thread': 1,
    'stt_script_path': CS + '/Kbli_dev',
    'stt_tool_path': CS + '/tools',
    'wav_output_path': '/app/prd/MindsVOC/wav/CS_dev',
    'stt_output_path': CS + '/Kbli_dev/STT_output',
    'silence_seconds': 0,
}

PRD_CONFIG = {
    'gpu': 2,
    'log_level': 'INFO',
    'stt_path': CS,
    'log_dir_path': CS_LOG_PATH,
    'rec_dir_path': REC_SERVER_PATH,
    'codec_file_path': CS + '/Kbli/cfg/codec.cfg',
    'thread': 2,
    'stt_script_path': CS + '/Kbli',
    'stt_tool_path': CS + '/tools',
    'wav_output_path': '/app/prd/MindsVOC/wav/CS',
    'stt_output_path': CS + '/Kbli/STT_output',
    'silence_seconds': 0,
}

OPENSSL_CONFIG = {
    'codec_file_path': CS + '/Kbli/cfg/codec.cfg'
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