#!/usr/bin/python
# -*- coding: euc-kr -*-

MASKING_CONFIG = {
    'minimum_length': 5,
    'next_line_cnt': 1,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|일곱|칠|여덟|팔|아홉|구|공|넷|셋|영|십|백)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십|년|월|백|천|시)\s?){3,}',
    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하|나)|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십)\s?){3,}',
    'email_rule': r'(.\s?){4}((?:골\s?뱅\s?이)|(?:닷\s?컴)|("?:다\s?컴)|(?:닷\s?넷)|(?:닷\s?케\s?이\?알)|(?:한메일)|(?:지메일)|(?:쥐메일)|(?:핫메일))',
#    'address_rule': r'(군|동|호|(?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
#    'address_rule': r'(서울|경기)|\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)',
    'address_rule': r'\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)|((?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
    'name_rule': r'(?:(가|간|갈|감|강|개|견|경|계|고|곡|공|곽|교|구|국|군|궁|궉|권|근|금|기|길|김|나|라|남|(?:남궁)|낭|랑|내|노|로|뇌|누|단|담|당|대|도|(?:독고)|돈|동|(?:동방)|두|라|류|마|망|절|매|맹|먕|모|묘|목|묵|문|미|민|박|반|방|배|백|범|변|복|봉|부|빈|빙|사|(?:사공)|삼|상|서|(?:서문)|석|선|(?:선우)|설|섭|성|소|(?:소봉)|손|송|수|순|숭|시|신|심|십|아|안|애|야|양|량|어|(?:어금)|엄|여|연|염|영|예|오|옥|온|옹|왕|요|용|우|운|원|위|유|육|윤|은|음|이|인|임|림|자|장|전|점|정|제|(?:제갈)|조|종|좌|주|준|즙|지|진|차|창|채|척|천|초|최|추|축|춘|탁|탄|태|판|패|편|평|포|표|퐁|피|필|하|학|한|함|해|허|현|형|호|홍|화|환|황|(?:황보))\s?[(가-힐)](\s?[(가-힐)]))'
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