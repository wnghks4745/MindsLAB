# coding=utf-8
FIND_TA_DAEMON_CONFIG = {
    'log_dir_path': '/data1/MindsVOC/TA/Meritz/logs',
    'pid_file_path': '/data1/MindsVOC/TA/Meritz/find_TA_daemon.pid',
    'chk_dir_path': '/nasdata/solution/stt/completed',
    'job_max_limit': 1,
    'process_max_limit': 2,
    'process_interval': 1,
    'find_script_path': '/data1/MindsVOC/TA/Meritz'
}

FIND_TA_CONFIG = {
    'ta_path': '/data1/MindsVOC/TA',
    'log_name': 'find_ta',
    'log_dir_path': '/data1/MindsVOC/TA/Meritz',
    'log_level': 'info',
    'matrix_file_path': '/data1/MindsVOC/TA/Meritz/badcase_matrix/badcase.matrix',
    'thread': 2,
    'analysis_result_path': '/data1/MindsVOC/TA/analysis_result'
}

DAEMON_CONFIG = {
    'log_dir_path': '/data1/MindsVOC/TA/Meritz/logs',
    'log_file_name': 'TA_daemon.log',
    'ta_path': '/data1/MindsVOC/TA',    # 주의! 변경 불가
    'stt_path': '/data1/MindsVOC/CS',
    'stt_out_path': '/data1/MindsVOC/CS/STT_out/completed',
    'hmd_path': '/data1/MindsVOC/TA/Meritz/cfg/HMD_txt',    # 주의! 변경 불가
    'ta_thread': 2,
    'log_level': 'debug',   # debug, info, warning, error, critical
    'process_max_limit': 10,    # 0:even number, 1: odd number
    'daemon_interval_mod': 0,
    'process_interval': 10,
    'pid_path': '/tmp/Meritz_TA.pid'
}

DB_CONFIG = {
    'user': 'stauser',
    'password': 'stadev123*',
    'database': 'DBSTTS',
    'host': '10.20.10.67',
    'port': 1433,
    'charset': 'utf8'
}