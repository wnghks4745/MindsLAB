#!/usr/bin/python
# -*- coding: euc-kr -*-
import socket

TM_DAEMON_CONFIG = {
    'job_max_limit': 5,
    'log_dir_path': '/log/MindsVOC/TM/STT_IE_TA',
    'log_file_name': 'TM_daemon.log',
    'pid_dir_path': '/app/MindsVOC/TM/STT/Ldcc/bin',
    'pid_file_name': 'TM_daemon.pid',
    'pri_process_max_limit': 5,
    'process_interval': 1,
    'process_max_limit': 10,
    'stt_script_path': '/app/MindsVOC/TM/STT/Ldcc',
    'work_sttm': '9',
    'work_endtm': '19',
    'work_process_max_limit': 10
}

STT_CONFIG = {
    'gpu': 2,
    'log_dir_path': '/log/MindsVOC/TM/STT_IE_TA',
    'log_level': 'debug',
    'nl_thread': 1,
    'rec_dir_path': '/app/record',
    'stt_path': '/app/MindsVOC/TM/STT',
    'stt_output_path': '/app/MindsVOC/TM/OUTPUT/{0}/STTA_output'.format(str(socket.gethostname())),
    'stt_tool_path': '/app/MindsVOC/TM/STT/tools',
    'ta_bin_path': '/app/MindsVOC/TM/TA/LA/bin',
    'ta_data_path': '/app/MindsVOC/TM/TA/data',
    'ta_path': '/app/MindsVOC/TM/TA',
    'ta_script_path': '/app/MindsVOC/TM/TA/Ldcc',
    'thread': 2,
    'silence_seconds': 5
}

DB_CONFIG = {
    'host': '10.151.3.174',
#    'host': '10.150.5.115',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}

IE_TA_CONFIG = {
    'hmd_script_path': '/app/MindsVOC/TM/TA/Ldcc/lib',
    'hmd_thread': 1,
    'nl_thread': 1,
    'rec_dir_path': '/app/record',
    'stt_output_path': '/app/MindsVOC/TM/OUTPUT',
    'ta_path': '/app/MindsVOC/TM/TA',
    'ta_data_path': '/app/MindsVOC/TM/TA/data',
    'ta_bin_path': '/app/MindsVOC/TM/TA/LA/bin',
    'ta_output_path': '/app/MindsVOC/TM/OUTPUT/{0}/IE_TA_output'.format(str(socket.gethostname()))
}

WORD2VEC_CONFIG = {
    'cbow': 1,          # �ܾ� ���� ���Ӽ� ����?
    'dim': 100,         # �ܾ� ������ ũ�� ����?
    'win_size': 8,      # �ܾ ��ŵ �ִ� ���� ����
    'negative': 25,     # ������ ����� ��
    'hs': 0,            # ���� �� Softmax ���
    'sample': '1e-4',   # �ܾ� �߻� �Ӱ� �� ����
    'thread': 16,       # ������ �� ����
    'binary': 1,        # ��� ���͸� ���̳ʸ� ���ͷ� ���� ����
    'iteration': 30     # ���� �ݺ� Ƚ�� ����
}

QA_OO_DAEMON_CONFIG = {
#    'http_url': 'http://10.150.5.65/webapp/com/sttStatusUpdate.jsp',
    'http_url': 'http://10.150.9.16:19091/webapp/com/sttStatusUpdate.jsp',
    'requests_timeout': 5,
    'log_dir_path': '/log/MindsVOC/TM/QA_OO_TA',
    'log_file_name': 'TA_daemon.log',
    'oo_job_max_limit': 5,
    'pid_dir_path': '/app/MindsVOC/TM/TA/Ldcc/bin',
    'pid_file_name': 'TA_daemon.pid',
    'pri_process_max_limit': 5,
    'process_interval': 1,
    'process_max_limit': 10,
    'ta_script_path': '/app/MindsVOC/TM/TA/Ldcc'
}

OPENSSL_CONFIG = {
    'codec_file_path': '/app/MindsVOC/TM/STT/Ldcc/cfg/codec.cfg'
}

QA_TA_CONFIG = {
    'hmd_script_path': '/app/MindsVOC/TM/TA/Ldcc/lib',
    'hmd_thread': 16,
#   'http_url': 'http://10.150.5.65/webapp/com/sttStatusUpdate.jsp',
    'http_url': 'http://10.150.9.16:19091/webapp/com/sttStatusUpdate.jsp',
    'requests_timeout': 5,
    'kywd_detect_range': 2,
    'log_dir_path': '/log/MindsVOC/TM/QA_OO_TA',
    'log_level': 'debug',
    'mother_category_txt_path': '/app/MindsVOC/TA/HMD/first_table.txt',
    'nl_thread': 16,
    'stt_output_path': '/app/MindsVOC/TM/OUTPUT',
    'ta_output_path': '/app/MindsVOC/TM/OUTPUT/{0}/QA_TA_output'.format(str(socket.gethostname())),
    'ta_bin_path': '/app/MindsVOC/TM/TA/LA/bin',
    'ta_data_path': '/app/MindsVOC/TM/TA/data',
    'ta_path': '/app/MindsVOC/TM/TA'
}

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|�ϰ�|ĥ|����|��|��ȩ|��|��|��|��|��|��|��)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:�ϳ�)|��|��|��|��|��|��|��|ĥ|��|��|��|��|��|��|��|��|��|õ|��)\s?){3,}',
#    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|��|(?:��|��)|��|��|��|��|��|��|ĥ|��|��|��|��|��|��)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:��\s?��\s?��)|(?:��\s?��)|("?:��\s?��)|(?:��\s?��)|(?:��\s?��\s?��\?��)|(?:�Ѹ���)|(?:������)|(?:�����)|(?:�ָ���))',
    'address_rule': r'\s((.){2}��)|\s((.){2}��)|\s((.){1,4}��)|\s((.){1,4}ȣ)|((?:����)|(?:����Ʈ)|(?:����)|(?:����)|��|��)',
    'name_rule': r'(?:(��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|(?:����)|��|��|(?:����)|��|��|��|��|(?:�Һ�)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:���)|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|��|(?:����)|��|��|��|��|��|��|��|��|��|â|ä|ô|õ|��|��|��|��|��|Ź|ź|��|��|��|��|��|��|ǥ|��|��|��|��|��|��|��|��|��|��|��|ȣ|ȫ|ȭ|ȯ|Ȳ|(?:Ȳ��))\s?[(��-��)](\s?[(��-��)]))'
}
