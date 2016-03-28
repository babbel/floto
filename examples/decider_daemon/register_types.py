import floto

swf = floto.api.Swf()

## Define and register the workflow type for the Daemon.
workflow_type = floto.api.WorkflowType(domain='floto_test', name='floto_daemon_type', version='v2', 
        default_task_start_to_close_timeout='7')

swf.register_workflow_type(workflow_type)
