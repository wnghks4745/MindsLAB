#!/usr/bin/python
# -*- coding: euc-kr -*-
import socket

# JSON_CONFIG = {
#     'log_dir_path': '/log/MindsVOC/CS/UPLOAD_JSON',
#     'log_name': 'upload_json.log',
#     'log_level': 'debug',
#     'json_dir_path': [
#         '/app/record/CN'
#     ],
#     'json_output_path': '/app/MindsVOC/CS/UPLOAD_JSON/processed_json'
# }
#
# JSON_DAEMON_CONFIG = {
#     'process_interval': 1,
#     'log_dir_path': '/log/MindsVOC/CS/UPLOAD_JSON',
#     'log_file_name': 'upload_json_daemon.log',
#     'pid_dir_path': '/app/MindsVOC/CS/UPLOAD_JSON/bin',
#     'pid_file_name': 'upload_json_daemon.pid',
#     'script_path': '/app/MindsVOC/CS/UPLOAD_JSON'
# }
#
# MYSQL_DB_CONFIG = {
#     'host': '192.168.100.23',
#     'user': 'minds',
#     'passwd': 'ggoggoma',
#     'port': 3306,
#     'db': 'stt_lotte',
#     'charset': 'utf8',
#     'connect_timeout': 10
# }

OPENSSL_CONFIG = {
    'codec_file_path': '/app/MindsVOC/CS/service/codec.cfg'
}

PAST_CS_DAEMON_CONFIG = {
    'job_max_limit': 5,
    'log_dir_path': '/log/MindsVOC/PAST_CS/STT',
    'log_file_name': 'PAST_CS_daemon.log',
    'pid_dir_path': '/app/MindsVOC/PAST_CS/STT/Ldcc/bin',
    'pid_file_name': 'PAST_CS_daemon.pid',
    'process_interval': 1,
    'process_max_limit': 16,
    'stt_script_path': '/app/MindsVOC/PAST_CS/STT/Ldcc',
    'rec_path': [
        '/app/record/CN_MONO'
    ]
}

STT_CONFIG = {
    'gpu': 1,
    'log_dir_path': '/log/MindsVOC/PAST_CS/STT',
    'log_level': 'debug',
    'rec_dir_path': '/app/record',
    'stt_path': '/app/MindsVOC/PAST_CS/STT',
    'stt_output_path': '/app/MindsVOC/PAST_CS/OUTPUT/{0}/STTA_output'.format(str(socket.gethostname())),
    'stt_tool_path': '/app/MindsVOC/PAST_CS/STT/tools',
    'thread': 2,
}

MASKING_CONFIG = {
    'minimum_length': 3,
    'next_line_cnt': 2,
    'number_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|일곱|칠|여덟|팔|아홉|구|공|넷|셋|영|십|백)\s?){3,}',
    'birth_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하나)|이|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십|년|월|백|천|시)\s?){3,}',
#    'etc_rule': r'((0|1|2|3|4|5|6|7|8|9|(?:10)|일|(?:하|나)|둘|삼|사|오|육|륙|칠|팔|구|공|넷|영|십)\s?){3,}',
    'etc_rule': '',
    'email_rule': r'(.\s?){4}((?:골\s?뱅\s?이)|(?:닷\s?컴)|("?:다\s?컴)|(?:닷\s?넷)|(?:닷\s?케\s?이\?알)|(?:한메일)|(?:지메일)|(?:쥐메일)|(?:핫메일))',
    'address_rule': r'\s((.){2}시)|\s((.){2}구)|\s((.){1,4}동)|\s((.){1,4}호)|((?:빌딩)|(?:아파트)|(?:번지)|(?:빌라)|길|읍)',
    'alphabet_rule': r'(?:(에이|비|씨|디|이|에프|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|지)\s?){3,}',
    'name_rule': r'(?:(가|간|갈|감|강|개|견|경|계|고|곡|공|곽|교|구|국|군|궁|궉|권|근|금|기|길|김|나|라|남|(?:남궁)|낭|랑|내|노|로|뇌|누|단|담|당|대|도|(?:독고)|돈|동|(?:동방)|두|라|류|마|망|절|매|맹|먕|모|묘|목|묵|문|미|민|박|반|방|배|백|범|변|복|봉|부|빈|빙|사|(?:사공)|삼|상|서|(?:서문)|석|선|(?:선우)|설|섭|성|소|(?:소봉)|손|송|수|순|숭|시|신|심|십|아|안|애|야|양|량|어|(?:어금)|엄|여|연|염|영|예|오|옥|온|옹|왕|요|용|우|운|원|위|유|육|윤|은|음|이|인|임|림|자|장|전|점|정|제|(?:제갈)|조|종|좌|주|준|즙|지|진|차|창|채|척|천|초|최|추|축|춘|탁|탄|태|판|패|편|평|포|표|퐁|피|필|하|학|한|함|해|허|현|형|호|홍|화|환|황|(?:황보))\s?[(가-힐)](\s?[(가-힐)]))',
    'precent_undetected': [u'네', u'네네', u'네네네', u'아', u'어', u'잠시만요', u'잠깐만요', u'예', u'예예'],
    'non_masking_word': [u'고객님', u'맞', u'본인', u'여권번호', u'감사합', u'확인', u'아니요', u'잠시만요', u'음', u'어', u'위해', u'지금', u'주문', u'말씀', u'여권', u'번호', u'또는', u'사용하시는', u'사용하는', u'아이디', u'십니까', u'성함', u'롯데', u'면세점', u'개인', u'부분', u'포인트', u'소중한', u'개인정보', u'모레', u'아침', u'이메일', u'동의', u'잠시만']
}