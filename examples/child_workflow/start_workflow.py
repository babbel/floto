import floto

floto.api.Swf().start_workflow_execution(domain='floto_test', 
        workflow_id='s3_files_child_workflow', 
        workflow_type_name='s3_files_example', 
        workflow_type_version='1', 
        task_list='copy_files_task_list',
        input=['file_a.in', 'file_b.in'])

