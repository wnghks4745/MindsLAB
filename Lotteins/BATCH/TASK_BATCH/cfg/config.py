CONFIG = {
    'log_dir_path': '/app/maum/logs/TASK_BATCH',
    'log_name': 'task_batch.log',
    'log_level': 'debug',
    'org_file_path_list': {
        'Outbound_Monitoring_Task_TB': '/app/cstwftp/taskinfo/taskinfo.dat'
    },
    'org_output_path': '/app/maum/bin/TASK_BATCH/processed_file',
    'delete_date': 180
}

ORACLE_DB_CONFIG = {
    'host': '10.150.5.115',
    'user': 'STTAPP',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}