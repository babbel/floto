import logging

import floto

logger = logging.getLogger(__name__)
# ---------------------------------- #
# Start the workflow execution
# ---------------------------------- #
workflow_args = {'domain': 'floto_test',
                 'workflow_type_name': 'demo_flow',
                 'workflow_type_version': 'v4',
                 'task_list': 'demo_step_decisions',
                 'workflow_id': 'demo_flow-Anne1',
                 'input': {'min_val': 4, 'max_val': 10}}

response = floto.api.Swf().start_workflow_execution(**workflow_args)

logger.debug('Workflow started: {}'.format(response))
