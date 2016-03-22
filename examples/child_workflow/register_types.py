import floto

domain = 'floto_test'

swf = floto.api.Swf()

copy_files = floto.api.ActivityType(name='copyFiles', version='1', domain=domain, 
        default_task_heartbeat_timeout='10')

file_length = floto.api.ActivityType(name='fileLength', version='1', domain=domain, 
        default_task_heartbeat_timeout='10')

swf.register_activity_type(copy_files)
swf.register_activity_type(file_length)


s3_files_workflow_type = floto.api.WorkflowType(domain=domain, name='s3_files_example', version='1',
        default_task_start_to_close_timeout='20')

s3_read_lengths_workflow_type = floto.api.WorkflowType(domain=domain, name='read_file_lengths', 
        version='1', default_task_start_to_close_timeout='20')

swf.register_workflow_type(s3_files_workflow_type)
swf.register_workflow_type(s3_read_lengths_workflow_type)
