CONFIG = {
    'log_dir_path': '/app/maum/logs/upload_poli_batch',
    'log_name': 'poli_batch.log',
    'log_level': 'debug',
    'poli_file_path_list': {
        'CM_TM_POLI_NO_TB': '/app/cstwftp/poliinfo/poliinfo.dat'
    },
    'poli_output_path': '/app/maum/bin/upload_poli_batch/processed_file',
    'delete_date': 180,
    'mapping_date_range': 30
}

ORACLE_DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}
