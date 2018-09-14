#!/usr/bin/python
# -*- coding: euc-kr -*-

CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/TA/INSERT_TB_TM_CNTR_INFO'
    , 'log_file_name': 'insert_tb_tm_cntr_info.log'
}

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

DB_CONFIG = {
    'dev': DEV_DB_CONFIG,
    'uat': UAT_DB_CONFIG,
    'prd': PRD_DB_CONFIG
}

OPENSSL_CONFIG = {
    'codec_file_path': '/app/prd/MindsVOC/TM/TA/Kbli/BATCH/INSERT_TB_TM_CNTR_INFO/cfg/codec.cfg'
}