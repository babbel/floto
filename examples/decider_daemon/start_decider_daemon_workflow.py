import floto.api

floto.api.Swf().start_workflow_execution(domain='floto_test',
                                         workflow_type_name='floto_daemon_type', 
                                         workflow_type_version='v2',
                                         task_list='floto_daemon', 
                                         workflow_id='floto_daemon')
