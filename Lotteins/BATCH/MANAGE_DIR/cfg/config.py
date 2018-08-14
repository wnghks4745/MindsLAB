CONFIG = {
    'log_dir_path': '/app/maum/logs/MANAGE_DIR',
    'log_file_name': 'delete_file.log',
    'log_level': 'debug',
    'target_directory_list': [
        {
            'directory_path': '/app/maum/logs/ORG_BATCH',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/logs/MANAGE_DIR',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/logs/AGENT_BATCH',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/logs/TASK_BATCH',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/bin/ORG_BATCH/processed_file',
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/maum/bin/AGENT_BATCH/processed_file',
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/maum/bin/TASK_BATCH/processed_file',
            'delete_file_date': 60
        }
    ]
}
