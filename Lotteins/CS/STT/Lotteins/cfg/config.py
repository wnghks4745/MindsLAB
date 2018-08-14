#!/usr/bin/python
# -*- coding: euc-kr -*-

CS_DAEMON_CONFIG = {
    'job_max_limit': 5,
    'log_dir_path': '/log/MindsVOC/CS/STT_IE_TA',
    'log_file_name': 'CS_daemon.log',
    'pid_dir_path': '/app/MindsVOC/CS/CS/Ldcc/bin',
    'pid_file_name': 'CS_daemon.pid',
    'process_interval': 1,
    'process_max_limit': 15,
    'stt_script_path': '/app/MindsVOC/CS/CS/Ldcc'
}

OPENSSL_CONFIG = {
    'codec_file_path': '/app/MindsVOC/CS/TA/Ldcc/cfg/codec.cfg'
}

STT_CONFIG = {
    'gpu': 2,
    'log_dir_path': '/log/MindsVOC/CS/STT_IE_TA',
    'log_level': 'debug',
    'nl_thread': 16,
    'rec_dir_path': '/app/record',
    'stt_path': '/app/MindsVOC/CS/CS',
    'stt_output_path': '/app/MindsVOC/CS/CS/STTA_output',
    'stt_tool_path': '/app/MindsVOC/CS/CS/tools',
    'ta_bin_path': '/app/MindsVOC/CS/TA/LA/bin',
    'ta_data_path': '/app/MindsVOC/CS/TA/data',
    'ta_path': '/app/MindsVOC/CS/TA',
    'ta_script_path': '/app/MindsVOC/CS/TA/Ldcc',
    'thread': 2,
    'silence_seconds': 5
}

DB_CONFIG = {
    'host': '10.151.3.174',
    'user': 'STTUSR',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'DSTTODBS'
}

IE_TA_CONFIG = {
    'hmd_script_path': '/app/MindsVOC/CS/TA/Ldcc/lib',
    'hmd_thread': 16,
    'nl_thread': 16,
    'rec_dir_path': '/app/record',
    'stt_output_path': '/app/MindsVOC/CS/CS/STTA_output',
    'ta_path': '/app/MindsVOC/CS/TA',
    'ta_data_path': '/app/MindsVOC/CS/TA/data',
    'ta_bin_path': '/app/MindsVOC/CS/TA/LA/bin',
    'ta_output_path': '/app/MindsVOC/CS/TA/IE_TA_output'
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
