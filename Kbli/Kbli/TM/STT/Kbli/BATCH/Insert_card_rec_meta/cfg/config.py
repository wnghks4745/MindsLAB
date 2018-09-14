#!/usr/bin/python
# -*- coding: euc-kr -*-

CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/Insert_card_rec_meta',
    'log_file_name': 'insert_card_rec_meta.log',
    'log_level': 'debug',
    'target_dir_path': '/app/prd/MindsVOC/TM/STT/Kbli/BATCH/Sftp_transport_to_card_rec/Output',
    'output_dir_path': '/app/rec_server/prd/cardTM',
}

DEV_DB_CONFIG = {
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.136)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=kbldev)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

UAT_DB_CONFIG = {
    'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.138)(PORT=1541)))(CONNECT_DATA=(SERVICE_NAME=kbluat)))',
    'user': 'zstt',
    'passwd': 'U2FsdGVkX1+5R1cN/se31o0kjdG7nAnpP0fFu6xUiks='
}

PRD_DB_CONFIG = {
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
    'codec_file_path': '/app/prd/MindsVOC/TM/STT/Kbli/BATCH/Insert_card_rec_meta/cfg/codec.cfg'
}
