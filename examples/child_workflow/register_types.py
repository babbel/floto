import floto

swf = floto.api.Swf()

## Define and register a workflow type.
workflow_type = floto.api.WorkflowType(domain='floto_test', name='demo_flow', version='v2', 
        default_task_start_to_close_timeout='7')
swf.register_workflow_type(workflow_type)

child_workflow_type = floto.api.WorkflowType(domain='floto_test', name='demo_flow_child_workflow', 
        version='v1', default_task_start_to_close_timeout='7')
swf.register_workflow_type(child_workflow_type)

## Define and register an activity type
activity_type = floto.api.ActivityType(name='demo_step1', version='v2', domain='floto_test', 
        default_task_heartbeat_timeout='5')
swf.register_activity_type(activity_type)

## Define and register an activity type
activity_type = floto.api.ActivityType(name='demo_step2', version='v2', domain='floto_test', 
        default_task_heartbeat_timeout='5')
swf.register_activity_type(activity_type)

## Define and register an activity type
activity_type = floto.api.ActivityType(name='demo_step3', version='v2', domain='floto_test', 
        default_task_heartbeat_timeout='5')
swf.register_activity_type(activity_type)

activity_type = floto.api.ActivityType(name='demo_step4', version='v1', domain='floto_test', 
        default_task_heartbeat_timeout='5')
swf.register_activity_type(activity_type)

activity_type = floto.api.ActivityType(name='demo_step5', version='v1', domain='floto_test', 
        default_task_heartbeat_timeout='5')
swf.register_activity_type(activity_type)


