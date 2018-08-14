#!/usr/bin/python
# -*- coding: euc-kr -*-

DAEMON_CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM',
    'pid_file_path': '/app/prd/MindsVOC/TM/CS/MiraeAsset/bin/STT_daemon.pid',
    'process_max_limit': 10,
    'process_interval': 1,
    'log_file_name': 'STT_daemon.log',
    'stt_script_path': '/app/prd/MindsVOC/TM/CS/MiraeAsset'
}

STT_CONFIG = {
    'rec_dir_path': '/app/rec_server/prd',
    'incident_rec_dir_path': '/app/rec_server/prd/incident_file',
    'stt_output_path': '/app/prd/MindsVOC/TM/CS/STT_output',
    'db_upload_path': '/app/prd/MindsVOC/TM/db_upload',
    'stt_path': '/app/prd/MindsVOC/TM/CS',
    'log_dir_path': '/log/MindsVOC/TM',
    'log_level': 'info',
    'wav_output_path': '/app/prd/MindsVOC/TM/CS/wav',
    'ta_script_path': '/app/prd/MindsVOC/TM/TA/MiraeAsset'
}

MYSQL_DB_CONFIG = {
    'host': '10.224.51.96',
    'user': 'msl',
    'passwd': 'Minds12#$',
    'db': 'malife_stta',
    'port': 1433,
    'charset': 'utf8',
    'connect_timeout': 10
}

MSSQL_DB_CONFIG = {
    'host': '10.224.80.52',
    'user': 'sa',
    'password': 'malife!1',
    'database': 'VcRecordDB',
    'port': 1433,
    'charset': 'utf8',
    'login_timeout': 10
}

MASKING_CONFIG = {

}