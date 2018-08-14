CONFIG = {
    'log_dir_path': '/log/MindsVOC/CS/UPLOAD_JSON',
    'log_name': 'upload_json.log',
    'log_level': 'info',
    'rec_dir_path': [
        '/app/record/CS/CN',
    ],
    'json_output_path': '/app/MindsVOC/CS/UPLOAD_JSON/processed_json',
    'rec_move_dir_path': '/app/target'
}

DAEMON_CONFIG = {
    'process_interval': 1,
    'log_dir_path': '/log/MindsVOC/CS/UPLOAD_JSON',
    'log_file_name': 'upload_json_daemon.log',
    'pid_dir_path': '/app/MindsVOC/CS/UPLOAD_JSON/bin',
    'pid_file_name': 'upload_json_daemon.pid',
    'script_path': '/app/MindsVOC/CS/UPLOAD_JSON'
}

ORACLE_DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}

ORACLE_DB_CONFIG_BIZ = {
    'host': '10.151.3.174',
    'user': 'STTAPP',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}

MSSQL_DB_CONFIG = {
    'host': '10.151.3.23',
    'user': 'stt_user',
    'password': 'stt_user',
    'port': 1433,
    'database': 'VoistoreX',
    'charset': 'utf8',
    'login_timeout': 10
}

OPENSSL_CONFIG = {
    'codec_file_path': '/app/MindsVOC/CS/UPLOAD_JSON/cfg/codec.cfg'
}