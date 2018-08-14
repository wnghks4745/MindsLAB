DAEMON_CONFIG = {
    'process_interval': 1,
    'manage_script_path': '/app/prd/MindsVOC/MANAGE_REC',
    'log_level': 'info',
    'log_dir_path': '/log/MindsVOC/MANAGE_REC',
    'log_file_name': 'MANAGE_REC_daemon.log',
    'pid_file_path': '/app/prd/MindsVOC/MANAGE_REC/bin/MANAGE_REC_daemon.pid'
}

CONFIG = {
    'rec_dir_path': '/app/rec_server/prd',
    'incident_dir_path': '/app/rec_server/prd/incident_file',
    'encrypt_rec_dir_path': '/app/rec_server/prd_enc',
    'decompression_dir_path': '/app/rec_server/prd_enc/decompression_file',
    'temp_rollback_dir_path': '/app/prd/MindsVOC/MANAGE_REC',
    'log_level': 'info',
    'log_dir_path': '/log/MindsVOC/MANAGE_REC',
    'codec_file_path': '/app/prd/MindsVOC/MANAGE_REC/cfg/codec.cfg',
    'encrypt_log_file_name': 'encrypt_rec_file.log',
    'compression_log_file_name': 'compression_and_delete_rec_file.log',
    'rollback_log_file_name': 'rollback_rec_file.log',
    'comp_target_directory_list': [
        {
            'directory_path': '/app/rec_server/prd_enc',
            'compression_file_date': 35,
            'delete_file_date': 70,
            'enc': True
        }
    ]
}