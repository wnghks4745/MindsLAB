#!/usr/bin/python
# -*- coding: euc-kr -*-

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|�ϰ�|ĥ|����|��|��ȩ|��|��|��|��|��|��|��)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|ĥ|��|��|��|��|��|��|��|��|��|õ|��)\s?){3,}',
    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:��|��)|��|��|��|��|��|��|ĥ|��|��|��|��|��|��)\s?){3,}',
    'email_rule': r'(.\s?){4}((?:��\s?��\s?��)|(?:��\s?��)|("?:��\s?��)|(?:��\s?��)|(?:��\s?��\s?��\?��)|(?:�Ѹ���)|(?:������)|(?:�����)|(?:�ָ���))',
#    'address_rule': r'(��|��|ȣ|(?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
#    'address_rule': r'(����|���)|\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)',
    'address_rule': r'\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)|((?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
    'name_rule': r'(?:(��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|(?:�Һ�)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|â|ä|ô|õ|��|��|��|��|��|Ź|ź|��|��|��|��|��|��|ǥ|��|��|��|��|��|��|��|��|��|��|��|ȣ|ȫ|ȭ|ȯ|Ȳ|(?:Ȳ��))\s?[(��-��)](\s?[(��-��)]))'
}

DAEMON_CONFIG = {
    'process_interval': 1,
    'process_max_limit': 10,
    'stt_script_path': '/app/prd/MindsVOC/TM/CS/MiraeAsset',
    'log_dir_path': '/log/MindsVOC/TM',
    'log_file_name': 'STT_daemon.log',
    'pid_file_path': '/app/prd/MindsVOC/TM/CS/MiraeAsset/bin/STT_daemon.pid'
}

STT_CONFIG = {
    'gpu': 2,
    'thread': 3,
    'log_level': 'info',
    'log_dir_path': '/log/MindsVOC/TM',
    'db_upload_path': '/app/prd/MindsVOC/TM/db_upload',
    'stt_path': '/app/prd/MindsVOC/TM/CS',
    'stt_output_path': '/app/prd/MindsVOC/TM/CS/STT_output',
    'stt_tool_path': '/app/prd/MindsVOC/TM/CS/tools',
    'ta_script_path': '/app/prd/MindsVOC/TM/TA/MiraeAsset',
    'rec_dir_path': '/app/rec_server/prd_enc',
    'incident_rec_dir_path': '/app/rec_server/prd_enc/incident_file',
    'wav_output_path': '/app/prd/MindsVOC/TM/CS/wav'
}

TA_CONFIG = {
    'log_level': 'info',
    'log_dir_path': '/log/MindsVOC/TM',
    'nl_thread': 16,
    'hmd_thread': 16,
    'ta_path': '/app/prd/MindsVOC/TM/TA',
    'ta_data_path': '/app/prd/MindsVOC/TM/TA/data',
    'ta_bin_path': '/app/prd/MindsVOC/TM/TA/LA/bin',
    'matrix_path': '/app/prd/MindsVOC/TM/TA/HMD/all_hmd_v3.matrix',
    'hmd_script_path': '/app/prd/MindsVOC/TM/TA/MiraeAsset/lib',
    'stt_output_path': '/app/prd/MindsVOC/TM/CS/STT_output',
    'ta_output_path': '/app/prd/MindsVOC/TM/TA/TA_output'
}

MYSQL_DB_CONFIG = {
    'user': 'msl',
    'passwd': 'Minds12#$',
    'db': 'malife_stta',
    'host': '10.224.51.96',
    'port': 3399,
    'charset': 'utf8',
    'connect_timeout': 5
}

MSSQL_DB_CONFIG = {
    'user': 'sa',
    'password': 'malife!1',
    'database': 'Vc_RecordDB',
    'host': '10.224.80.52',
    'port': 1433,
    'charset': 'utf8',
    'login_timeout': 5
}

WORD2VEC_CONFIG = {
    'cbow': 1,
    'dim': 100,
    'win_size': 8,
    'negative': 25,
    'hs': 0,
    'sample': '1e-4',
    'thread': 16,
    'binary': 1,
    'iteration': 30
}

SFTP_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM',
    'log_file_name': 'sftp',
    'log_level': 'info',
    'host': '10.226.101.72',
    'username': 'sttftp',
    'remote_dir_path': '/stt/upload/data',
    'target_dir_path': '/app/prd/MindsVOC/TM/db_upload'
}

SFTP_DAEMON_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM',
    'pid_file_path': '/app/prd/MindsVOC/TM/CS/MiraeAsset/bin/sftp_daemon.pid',
    'script_path': '/app/prd/MindsVOC/TM/CS/MiraeAsset',
    'log_file_name': 'sftp_daemon.log'
}