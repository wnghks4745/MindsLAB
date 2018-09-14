CONFIG = {
    'log_dir_path': '/log/MindsVOC/get_view_data',
    'log_name': 'get_view_data.log',
    'log_level': 'debug',
    'target_rec_path': {
        'TM': '/app/rec_server/prd/kbliTM/b',
        'CS': '/app/rec_server/prd/kbliCS/b'
    }
}

ORACLE_DB_CONFIG = {
    # 'dsn': '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=172.226.200.136)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=kbldev)))',   # DEV
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

OPENSSL_CONFIG = {
    'codec_file_path': '/app/prd/MindsVOC/get_view_data/cfg/codec.cfg'
}

TM_POSTGRESQL_DB_CONFIG = {
    'user': 'sttuser',
    'password': 'sttuser',
    'db': 'ir',
    'host': '172.226.205.241',
    'port': 5432,
    'charset': 'utf8',
    'connect_timeout': 5
}

CS_POST_GRESQL_DB_CONFIG = {
    'user': 'sttuser',
    'password': 'sttuser',
    'db': 'ir',
    'host': '172.226.201.241',
    'port': 5432,
    'charset': 'utf8',
    'connect_timeout': 5
}
