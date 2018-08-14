CONFIG = {
    'log_dir_path': '/app/maum/logs/upload_task_batch',
    'log_name': 'task_batch.log',
    'log_level': 'debug',
    'org_file_path_list': {
        'Outbound_Monitoring_Task_TB': '/app/cstwftp/taskinfo/taskinfo.dat'
    },
    'org_output_path': '/app/maum/bin/upload_task_batch/processed_file',
    'delete_date': 180
}

ORACLE_DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'PSTTODBS'
}
