#!/usr/bin/python
# -*- coding: euc-kr -*-
import socket

JSON_CONFIG = {
    'log_dir_path': '/log/MindsVOC/CS/UPLOAD_JSON',
    'log_name': 'upload_json.log',
    'log_level': 'debug',
    'json_dir_path': [
        '/app/record/CN'
    ],
    'json_output_path': '/app/MindsVOC/CS/UPLOAD_JSON/processed_json'
}

JSON_DAEMON_CONFIG = {
    'process_interval': 1,
    'log_dir_path': '/log/MindsVOC/CS/UPLOAD_JSON',
    'log_file_name': 'upload_json_daemon.log',
    'pid_dir_path': '/app/MindsVOC/CS/UPLOAD_JSON/bin',
    'pid_file_name': 'upload_json_daemon.pid',
    'script_path': '/app/MindsVOC/CS/UPLOAD_JSON'
}

MYSQL_DB_CONFIG = {
    'host': '192.168.100.23',
    'user': 'minds',
    'passwd': 'ggoggoma',
    'port': 3306,
    'db': 'stt_lotte',
    'charset': 'utf8',
    'connect_timeout': 10
}

OPENSSL_CONFIG = {
    'codec_file_path': '/app/MindsVOC/CS/service/codec.cfg'
}

CS_DAEMON_CONFIG = {
    'job_max_limit': 5,
    'log_dir_path': '/log/MindsVOC/CS/STT',
    'log_file_name': 'CS_daemon.log',
    'pid_dir_path': '/app/MindsVOC/CS/STT/Ldcc/bin',
    'pid_file_name': 'CS_daemon.pid',
    'process_interval': 1,
    'process_max_limit': 15,
    'stt_script_path': '/app/MindsVOC/CS/STT/Ldcc',
}

STT_CONFIG = {
    'gpu': 1,
    'log_dir_path': '/log/MindsVOC/CS/STT',
    'log_level': 'debug',
    'rec_dir_path': '/app/record',
    'stt_path': '/app/MindsVOC/CS/STT',
    'stt_output_path': '/app/MindsVOC/CS/OUTPUT/{0}/STTA_output'.format(str(socket.gethostname())),
    'stt_tool_path': '/app/MindsVOC/CS/STT/tools',
    'thread': 2,
    'crosstalk_ign_len': 3,
    'masking_next_line_cnt': 2
}

MASKING_CONFIG = {
    'minimum_length': 3,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|�ϰ�|ĥ|����|��|��ȩ|��|��|��|��|��|��|��)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|ĥ|��|��|��|��|��|��|��|��|��|õ|��)\s?){3,}',
#    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:��|��)|��|��|��|��|��|��|ĥ|��|��|��|��|��|��)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:��\s?��\s?��)|(?:��\s?��)|("?:��\s?��)|(?:��\s?��)|(?:��\s?��\s?��\?��)|(?:�Ѹ���)|(?:������)|(?:�����)|(?:�ָ���))',
    'address_rule': r'\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)|\s((.){1,4}ȣ)|((?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
    'name_rule': r'(?:(��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|(?:�Һ�)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|â|ä|ô|õ|��|��|��|��|��|Ź|ź|��|��|��|��|��|��|ǥ|��|��|��|��|��|��|��|��|��|��|��|ȣ|ȫ|ȭ|ȯ|Ȳ|(?:Ȳ��))\s?[(��-��)](\s?[(��-��)]))',
    'precent_undetected': [u'��', u'�׳�', u'��', u'��', u'��ø���']
}