import floto

floto.api.Swf().start_workflow_execution(domain='floto_test', 
        workflow_id='daemon_test', 
        workflow_type_name='my_workflow_type', 
        workflow_type_version='v1', 
        task_list='simple_decider')

