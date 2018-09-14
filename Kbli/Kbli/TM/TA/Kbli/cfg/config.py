#!/usr/bin/python
# -*- coding: euc-kr -*-
TM_QA_TA = '/app/prd/MindsVOC/TM/TA'
TM_QA_TA_LOG_PATH = '/log/MindsVOC/TM/TA'

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|일곱|칠|여덟|팔|아홉|구|공|넷|셋|영|십|백)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십|년|월|백|천|시)\s?){3,}',
#    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하|나)|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:골\s?뱅\s?이)|(?:닷\s?컴)|("?:다\s?컴)|(?:닷\s?넷)|(?:닷\s?케\s?이\?알)|(?:한메일)|(?:지메일)|(?:쥐메일)|(?:핫메일))',
    'address_rule': r'\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)|\s((.){1,4}호)|((?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
    'name_rule': r'(?:(가|간|갈|감|강|개|견|경|계|고|곡|공|곽|교|구|국|군|궁|궉|권|근|금|기|길|김|나|라|남|(?:남궁)|낭|랑|내|노|로|뇌|누|단|담|당|대|도|(?:독고)|돈|동|(?:동방)|두|라|류|마|망|절|매|맹|먕|모|묘|목|묵|문|미|민|박|반|방|배|백|범|변|복|봉|부|빈|빙|사|(?:사공)|삼|상|서|(?:서문)|석|선|(?:선우)|설|섭|성|소|(?:소봉)|손|송|수|순|숭|시|신|심|십|아|안|애|야|양|량|어|(?:어금)|엄|여|연|염|영|예|오|옥|온|옹|왕|요|용|우|운|원|위|유|육|윤|은|음|이|인|임|림|자|장|전|점|정|제|(?:제갈)|조|종|좌|주|준|즙|지|진|차|창|채|척|천|초|최|추|축|춘|탁|탄|태|판|패|편|평|포|표|퐁|피|필|하|학|한|함|해|허|현|형|호|홍|화|환|황|(?:황보))\s?[(가-힐)](\s?[(가-힐)]))'
}

# ============== DB_CONFIG ==================
DEV_DB_CONFIG = {
    # 'tm_user': 'TELETM_NEW',
    'tm_user': 'ZDM',
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.136)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=kbldev)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

UAT_DB_CONFIG = {
    'tm_user': 'ZDM',
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.138)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=kbluat)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

PRD_DB_CONFIG = {
    'tm_user': 'ZDM',
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

# ============== TM_QA_TA_DAEMON_CONFIG ==================
DEV_TM_QA_TA_DAEMON_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM_dev/TA',
    'log_file_name': 'TA_daemon_dev.log',
    'pid_dir_path': TM_QA_TA + '/Kbli_dev/bin',
    'pid_file_name': 'TA_daemon.pid',
    'process_interval': 10,
    'process_max_limit': 1,
    'ta_script_path': TM_QA_TA + '/Kbli_dev'
}

PRD_TM_QA_TA_DAEMON_CONFIG = {
    'log_dir_path': TM_QA_TA_LOG_PATH,
    'log_file_name': 'TA_daemon.log',
    'pid_dir_path': TM_QA_TA + '/Kbli/bin',
    'pid_file_name': 'TA_daemon.pid',
    'process_interval': 10,
    'process_max_limit': 5,
    'ta_script_path': TM_QA_TA + '/Kbli'
}

# ============== QA_TA_CONFIG ==================
DEV_QA_TA_CONFIG = {
    'hmd_thread': 2,
    'nl_thread': 2,
    'log_level': 'debug',
    'stt_output_path': '/app/prd/MindsVOC/TM/STT/Kbli/STT_output',
    'hmd_script_path': TM_QA_TA + '/Kbli_dev/lib',
    'log_dir_path': '/log/MindsVOC/TM_dev/TA',
    'kywd_detect_range': 2,
    'ta_output_path': TM_QA_TA + '/Kbli_dev/QA_TA_output',
    'ta_bin_path': TM_QA_TA + '/LA/bin',
    'ta_data_path': TM_QA_TA + '/data',
    'ta_path': TM_QA_TA
}

PRD_QA_TA_CONFIG = {
    'hmd_thread': 4,
    'nl_thread': 4,
    'log_level': 'info',
    'stt_output_path': '/app/prd/MindsVOC/TM/STT/Kbli/STT_output',
    'hmd_script_path': TM_QA_TA + '/Kbli/lib',
    'log_dir_path': TM_QA_TA_LOG_PATH,
    'kywd_detect_range': 2,
    'ta_output_path': TM_QA_TA + '/Kbli/QA_TA_output',
    'ta_bin_path': TM_QA_TA + '/LA/bin',
    'ta_data_path': TM_QA_TA + '/data',
    'ta_path': TM_QA_TA
}

OPENSSL_CONFIG = {
    'codec_file_path': TM_QA_TA + '/Kbli/cfg/codec.cfg'
}

QA_TA_CONFIG = {
    'dev': DEV_QA_TA_CONFIG,
    'uat': PRD_QA_TA_CONFIG,
    'prd': PRD_QA_TA_CONFIG
}

TM_QA_TA_DAEMON_CONFIG = {
    'dev': DEV_TM_QA_TA_DAEMON_CONFIG,
    'uat': PRD_TM_QA_TA_DAEMON_CONFIG,
    'prd': PRD_TM_QA_TA_DAEMON_CONFIG
}

DB_CONFIG = {
    'dev': DEV_DB_CONFIG,
    'uat': UAT_DB_CONFIG,
    'prd': PRD_DB_CONFIG
}
