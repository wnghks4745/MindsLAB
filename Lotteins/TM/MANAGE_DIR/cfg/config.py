import socket
CONFIG = {
    'log_dir_path': '/log/MindsVOC/TM/MANAGE_DIR',
    'log_file_name': 'delete_file.log',
    'log_level': 'debug',
    'target_directory_list': [
        {
            'directory_path': '/log/MindsVOC/TM/STT_IE_TA',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/QA_OO_TA',
            'delete_file_date':  180
        },
        {
            'directory_path': '/log/MindsVOC/TM/GET_CNTR_INFO',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/INSERT_QA_RCDG_INFO',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/MANAGE_DIR',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/MASKING_CU_NAME',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/UPDATE_QA_STTA_PRGST_CD',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/UPLOAD_JSON',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/CNTR_REMAPPING',
            'delete_file_date': 180
        },
        {
            'directory_path': '/log/MindsVOC/TM/Marketing_STT',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/MindsVOC/TM/OUTPUT/{0}/STTA_output'.format(socket.gethostname()),
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/MindsVOC/TM/OUTPUT/{0}/IE_TA_output'.format(socket.gethostname()),
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/MindsVOC/TM/OUTPUT/{0}/QA_TA_output'.format(socket.gethostname()),
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/MindsVOC/TM/UPLOAD_JSON/processed_json',
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/MindsVOC/TM/Marketing_STT/STTA_output',
            'delete_file_date': 60
        },
    ]
}
