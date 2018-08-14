CONFIG = {
    'log_dir_path': '/app/maum/logs/upload_agent_batch',
    'log_name': 'agent_batch.log',
    'log_level': 'debug',
    'org_file_path_list': {
        'CS_TB_USER': '/app/cstwftp/agentinfo/TELECS_TB_USER.dat',
        'TM_TB_USER': '/app/cstwftp/agentinfo/TB_USER.dat',
        'TB_LUSM0': '/app/cstwftp/agentinfo/LUSM0.dat'
    },
    'org_output_path': '/app/maum/bin/upload_agent_batch/processed_file'
}

ORACLE_DB_CONFIG = {
    'host': '10.150.5.115',
    'user': 'STTAPP',
    'passwd': 'aa12345!',
    'port': 1523,
    'sid': 'PSTTODBS'
}
