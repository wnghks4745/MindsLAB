CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/UPDATE_QA_STTA_PRGST_CD',
    'log_name': 'update_qa_stta_prgst_cd.log',
    'log_level': 'debug',
    'http_url': 'http://10.150.9.16:19091/webapp/com/sttStatusUpdate.jsp',
    'requests_timeout': 5
}

DAEMON_CONFIG = {
    'process_interval': 1,
    'log_dir_path': '/log/MindsVOC/TM/UPDATE_QA_STTA_PRGST_CD',
    'log_file_name': 'update_qa_stta_prgst_cd_daemon.log',
    'pid_dir_path': '/app/MindsVOC/TM/UPDATE_QA_STTA_PRGST_CD/bin',
    'pid_file_name': 'update_qa_stta_prgst_cd_daemon.pid',
    'script_path': '/app/MindsVOC/TM/UPDATE_QA_STTA_PRGST_CD'
}

DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}
