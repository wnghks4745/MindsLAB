CONFIG = {
    'log_dir_path': '/app/maum/logs/upload_magage_dir',
    'log_file_name': 'delete_file.log',
    'log_level': 'debug',
    'target_directory_list': [
        {
            'directory_path': '/app/maum/logs/upload_org_batch',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/logs/upload_magage_dir',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/logs/upload_agent_batch',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/logs/upload_task_batch',
            'delete_file_date': 180
        },
        {
            'directory_path': '/app/maum/bin/upload_org_batch/processed_file',
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/maum/bin/upload_agent_batch/processed_file',
            'delete_file_date': 60
        },
        {
            'directory_path': '/app/maum/bin/upload_task_batch/processed_file',
            'delete_file_date': 60
        }
    ]
}
