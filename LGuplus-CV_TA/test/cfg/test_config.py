class DaemonConfig(object):
    logger_name = 'DAEMON'
    log_dir_path = '/logs/maum/test'
    log_file_name = 'test_daemon_process.log'
    backup_count = 5
    log_level = 'debug'
    target_dir_path = '/data/maum/test'
    processed_dir_path = '/data/maum/test/processed'
    process_max_limit = 50
    process_interval = 1


class TAConfig(object):
    logger_name = 'TA'
    log_dir_path = '/logs/maum'
    backup_count = 5
    log_level = 'debug'
    nlp_engine = 'nlp3'
    hmd_cate_delimiter = '!@#$'
    hmd_model_name = 'home_hmd_20180713'
    processed_dir_path = '/data/maum/test/processed'
    nlp_output_dir_path = '/data/maum/test/nlp_output'
    hmd_output_dir_path = '/data/maum/test/hmd_output'
    modified_hmd_output_dir_path = '/data/maum/test/modified_hmd_output'
