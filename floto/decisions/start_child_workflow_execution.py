from floto.decisions import Decision
import floto.specs

import logging

logger = logging.getLogger(__name__)

class StartChildWorkflowExecution(Decision):
    def __init__(self, **args):
        """
        Parameters
        ----------
        workflow_id: str
        workflow_type: floto.api.WorkflowType
        task_list: str
            The decider task list of the child workflow.
        input: str, dict

        Optional parameters
        -------------------
        startChildWorkflowExecutionDecisionAttributes.taskList
        startChildWorkflowExecutionDecisionAttributes.input
        """

        attributes = {}
        if 'workflow_id' in args:
            attributes['workflowId'] = args['workflow_id']

        if 'workflow_type' in args:
            attributes['workflowType'] = {'name': args['workflow_type'].name,
                                          'version': args['workflow_type'].version}

        if 'task_list' in args:
            attributes['taskList'] = {'name': args['task_list']}

        if 'input' in args:
            attributes['input'] = floto.specs.JSONEncoder.dump_object(args['input'])

        self.startChildWorkflowExecutionDecisionAttributes = attributes

        self.required_fields = ['decisionType',
                                'startChildWorkflowExecutionDecisionAttributes.workflowId',
                                'startChildWorkflowExecutionDecisionAttributes.workflowType.name',
                                'startChildWorkflowExecutionDecisionAttributes.workflowType.version']

    def _get_decision(self):
        d = {'decisionType': 'StartChildWorkflowExecution',
             'startChildWorkflowExecutionDecisionAttributes': self.get_attributes()}
        return d

    def get_attributes(self):
        logger.debug('StartChildWorkflowExecution.get_attributes...')
        return self.startChildWorkflowExecutionDecisionAttributes

    @property
    def workflow_id(self):
        return self._get_attribute('workflowId')

    @workflow_id.setter
    def workflow_id(self, workflow_id):
        self.startChildWorkflowExecutionDecisionAttributes['workflowId'] = workflow_id

    @property
    def task_list(self):
        return self._get_attribute('taskList')

    @task_list.setter
    def task_list(self, task_list):
        self.startChildWorkflowExecutionDecisionAttributes['taskList'] = task_list

    @property
    def workflow_type(self):
        return self._get_attribute('workflowType')

    @workflow_type.setter
    def workflow_type(self, workflow_type):
        workflow_type_dict = {'name': workflow_type.name, 'version': workflow_type.version}
        self.startChildWorkflowExecutionDecisionAttributes['workflowType'] = workflow_type_dict

    def _get_attribute(self, key):
        a = None
        if key in self.startChildWorkflowExecutionDecisionAttributes:
            a = self.startChildWorkflowExecutionDecisionAttributes[key]
        return a
