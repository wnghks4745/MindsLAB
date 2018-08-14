CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/INSERT_QA_RCDG_INFO',
    'log_name': 'insert_qa_rcdg_info.log',
    'log_level': 'debug',
    'http_url': 'http://10.150.9.16:19091/webapp/com/sttStatusUpdate.jsp',
    'requests_timeout': 5,
    'standard_time': -1
}

DAEMON_CONFIG = {
    'process_interval': 1,
    'log_dir_path': '/log/MindsVOC/TM/INSERT_QA_RCDG_INFO',
    'log_file_name': 'insert_qa_rcdg_info_daemon.log',
    'pid_dir_path': '/app/MindsVOC/TM/INSERT_QA_RCDG_INFO/bin',
    'pid_file_name': 'insert_qa_rcdg_info_daemon.pid',
    'script_path': '/app/MindsVOC/TM/INSERT_QA_RCDG_INFO'
}

DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}
