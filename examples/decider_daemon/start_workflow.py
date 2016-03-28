import floto

# ---------------------------------- #
# Start the workflow execution
# ---------------------------------- #
workflow_args = {'domain': 'floto_test', 
                 'workflow_type_name': 'demo_flow',
                 'workflow_type_version': 'v2',
                 'task_list': 'demo_step_decisions',
                 'workflow_id': 'demo_flow5',
                 'input': {'start_val': 55}}

response = floto.api.Swf().start_workflow_execution(**workflow_args)

