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
    'log_dir_path': '/log/MindsVOC/CS',
    'pid_file_path': '/app/prd/MindsVOC/CS/CS/MiraeAsset/bin/STT_daemon.pid',
    'job_max_limit': 5,
    'process_max_limit': 14,
    'process_interval': 1,
    'log_file_name': 'STT_daemon.log',
    'rec_server_path': '/app/rec_server/prd',
    'stt_script_path': '/app/prd/MindsVOC/CS/CS/MiraeAsset',
    'sftp_dir_path': '/app/prd/MindsVOC/CS/db_upload',
    'cycle_time': 300,
    'search_date_range': 30
}

MYSQL_DB_CONFIG = {
    'host': '10.224.51.95',
    'user': 'msl',
    'password': 'Minds12#$',
    'db': 'malife_stta',
    'port': 3399,
    'charset': 'utf8',
    'connect_timeout': 5
}

CONFIG = {
    'gpu': 2,
    'stt_path': '/app/prd/MindsVOC/CS/CS',
    'log_dir_path': '/log/MindsVOC/CS',
    'log_level': 'debug',
    'rec_dir_path': '/app/rec_server/prd',
    'codec_file_path': '/app/prd/MindsVOC/CS/CS/MiraeAsset/cfg/codec.cfg',
    'thread': 2,
    'stt_tool_path': '/app/prd/MindsVOC/CS/CS/tools',
    'sftp_dir_path': '/app/prd/MindsVOC/CS/db_upload',
    'wav_output_path': '/app/prd/MindsVOC/CS/CS/wav',
    'stt_output_path': '/app/prd/MindsVOC/CS/CS/STT_output',
}

SFTP_CONFIG = {
    'log_dir_path': '/log/MindsVOC/CS',
    'log_file_name': 'sftp',
    'log_level': 'debug',
    'host': '10.226.101.71',
    # 'host': '10.223.10.163',      # <- ������ ����
    'username': 'sttftp',
    'remote_dir_path': '/stt/upload/data',
    'target_dir_path': '/app/prd/MindsVOC/CS/db_upload'
}

SFTP_DAEMON_CONFIG = {
    'script_path': '/app/prd/MindsVOC/CS/CS/MiraeAsset',
    'log_dir_path': '/log/MindsVOC/CS',
    'pid_file_path': '/app/prd/MindsVOC/CS/CS/MiraeAsset/bin/sftp_daemon.pid',
    'log_file_name': 'sftp_daemon.log'
}
