CONFIG = {
    'log_dir_path': '/app/maum/logs/upload_org_batch',
    'log_name': 'org_batch.log',
    'log_level': 'debug',
    'org_file_path_list': {
        'PASU0001': '/app/cstwftp/orginfo/PASU0001.txt',
        'PASU0008': '/app/cstwftp/orginfo/PASU0008.txt'
    },
    'org_output_path': '/app/maum/bin/upload_org_batch/processed_file'
}

ORACLE_DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTAPP',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'PSTTODBS'
}
