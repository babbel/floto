from floto.api import SwfType


class WorkflowType(SwfType):
    def __init__(self, domain=None, name=None, version=None, **args):
        super().__init__(domain=domain, name=name, version=version, **args)

        workflow_type_attributes = ['defaultExecutionStartToCloseTimeout',
                                    'defaultChildPolicy',
                                    'defaultLambdaRole']

        default_values = {'defaultChildPolicy': 'TERMINATE',
                          'defaultExecutionStartToCloseTimeout': str(60 * 60 * 24)}

        self._set_attributes(workflow_type_attributes, args, default_values)
        self.attributes += workflow_type_attributes

    @property
    def default_child_policy(self):
        """Specify the default policy to use for the child workflow executions when a workflow
        execution of this type is terminated.

        Parameter
        ---------
        policy: str
            TERMINATE | REQUEST_CANCEL | ABANDON
        """
        return self._default_child_policy

    @property
    def default_execution_start_to_close_timeout(self):
        """Default maximum duration for executions of this workflow type. Default can be overridden
        when starting an execution through the StartWorkflowExecution action or 
        StartChildWorkflowExecution decision.

        Parameter
        ---------
        timeout: str
            Duration in seconds; An integer: 0 <= timeout < 60*60*24*356 (one year)
        """
        return self._default_execution_start_to_close_timeout

    @property
    def default_lambda_role(self):
        """The ARN of the default IAM role to use when a workflow execution of this type invokes 
        AWS Lambda functions.

        Parameter
        ---------
        role: str
        """
        return self._default_lambda_role

    @default_execution_start_to_close_timeout.setter
    def default_execution_start_to_close_timeout(self, timeout):
        self._default_execution_start_to_close_timeout = timeout

    @default_child_policy.setter
    def default_child_policy(self, policy):
        self._default_child_policy = policy

    @default_lambda_role.setter
    def default_lambda_role(self, role):
        self._default_lambda_role = role
