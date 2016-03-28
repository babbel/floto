import floto

swf = floto.api.Swf()
domain = 'floto_test'

s3_files_workflow_type = floto.api.WorkflowType(domain=domain, name='s3_files_example', version='1',
        default_task_start_to_close_timeout='20')
swf.register_workflow_type(s3_files_workflow_type)

week_days = floto.api.ActivityType(name='weekDays', version='1', domain=domain,
        default_task_heartbeat_timeout='20')

copy_file = floto.api.ActivityType(name='copyFile', version='1', domain=domain,
        default_task_heartbeat_timeout='20')

file_length = floto.api.ActivityType(name='fileLength', version='1', domain=domain,
        default_task_heartbeat_timeout='20')

sum_length = floto.api.ActivityType(name='sumLength', version='1', domain=domain,
        default_task_heartbeat_timeout='20')

swf.register_activity_type(week_days)
swf.register_activity_type(copy_file)
swf.register_activity_type(file_length)
swf.register_activity_type(sum_length)

