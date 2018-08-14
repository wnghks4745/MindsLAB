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
    'cbow': 1,          # 단어 모델의 연속성 설정?
    'dim': 100,         # 단어 백터의 크기 설정?
    'win_size': 8,      # 단어간 스킵 최대 길이 설정
    'negative': 25,     # 부정적 사례의 수
    'hs': 0,            # 계층 적 Softmax 사용
    'sample': '1e-4',   # 단어 발생 임계 값 설정
    'thread': 16,       # 쓰레드 수 설정
    'binary': 1,        # 결과 벡터를 바이너리 벡터로 저장 여부
    'iteration': 30     # 교육 반복 횟수 설정
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
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|일곱|칠|여덟|팔|아홉|구|공|넷|셋|영|십|백)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십|년|월|백|천|시)\s?){3,}',
#    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하|나)|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:골\s?뱅\s?이)|(?:닷\s?컴)|("?:다\s?컴)|(?:닷\s?넷)|(?:닷\s?케\s?이\?알)|(?:한메일)|(?:지메일)|(?:쥐메일)|(?:핫메일))',
    'address_rule': r'\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)|\s((.){1,4}호)|((?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
    'name_rule': r'(?:(가|간|갈|감|강|개|견|경|계|고|곡|공|곽|교|구|국|군|궁|궉|권|근|금|기|길|김|나|라|남|(?:남궁)|낭|랑|내|노|로|뇌|누|단|담|당|대|도|(?:독고)|돈|동|(?:동방)|두|라|류|마|망|절|매|맹|먕|모|묘|목|묵|문|미|민|박|반|방|배|백|범|변|복|봉|부|빈|빙|사|(?:사공)|삼|상|서|(?:서문)|석|선|(?:선우)|설|섭|성|소|(?:소봉)|손|송|수|순|숭|시|신|심|십|아|안|애|야|양|량|어|(?:어금)|엄|여|연|염|영|예|오|옥|온|옹|왕|요|용|우|운|원|위|유|육|윤|은|음|이|인|임|림|자|장|전|점|정|제|(?:제갈)|조|종|좌|주|준|즙|지|진|차|창|채|척|천|초|최|추|축|춘|탁|탄|태|판|패|편|평|포|표|퐁|피|필|하|학|한|함|해|허|현|형|호|홍|화|환|황|(?:황보))\s?[(가-힐)](\s?[(가-힐)]))'
}
