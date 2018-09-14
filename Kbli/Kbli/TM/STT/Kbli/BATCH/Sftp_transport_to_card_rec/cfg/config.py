#!/usr/bin/python
# -*- coding: euc-kr -*-

SFTP_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/Sftp_transport_to_card_rec',
    'log_file_name': 'sftp.log',
    'log_level': 'debug',
    'host': '172.226.110.69',
    'username': 'sttuser',
    'passwd': 'sttuser',
    #'host': '172.226.205.243',
    #'username': 'minds',
    #'passwd': 'msl1234~'
    'remote_dir_path': '/home/ftpuser06/WAVE_LIST',
    'output_dir_path': '/app/prd/MindsVOC/TM/STT/Kbli/BATCH/Sftp_transport_to_card_rec/Output'
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
    'codec_file_path': '/app/prd/MindsVOC/TM/STT/Kbli/BATCH/Sftp_transport_to_card_rec/cfg/codec.cfg'
}
