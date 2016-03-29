from floto.api import SwfType


class WorkflowType(SwfType):
    """
    Attributes
    ----------
    default_child_policy : Optional[str]
        Specify the default policy to use for the child workflow executions when a workflow
        execution of this type is terminated.
        Valid values: TERMINATE | REQUEST_CANCEL | ABANDON

        If not assigned, then TERMINATE will be used

    default_execution_start_to_close_timeout : Optional[str]
        Default maximum duration in seconds for executions of this workflow type. Default can be overridden
        when starting an execution through the StartWorkflowExecution action or StartChildWorkflowExecution decision.

        Duration in seconds; An integer: 0 <= timeout < 60*60*24*356 (one year)
        If not assigned, then str(60 * 60 * 24) (one day) will be used

    default_lambda_role : Optional[str]
        The ARN of the default IAM role to use when a workflow execution of this type invokes
        AWS Lambda functions

    """
    def __init__(self, *, domain, name, version,
                 description=None,
                 default_task_list='default',
                 default_task_start_to_close_timeout=None,
                 default_task_priority='0',
                 default_child_policy='TERMINATE',
                 default_execution_start_to_close_timeout=None,
                 default_lambda_role=None
                 ):

        if default_task_start_to_close_timeout is None:
            default_task_start_to_close_timeout = str(60 * 60 * 6)

        super().__init__(domain=domain, name=name, version=version,
                         description=description,
                         default_task_list=default_task_list,
                         default_task_start_to_close_timeout=default_task_start_to_close_timeout,
                         default_task_priority=default_task_priority)

        self.default_child_policy = default_child_policy
        self.default_execution_start_to_close_timeout = default_execution_start_to_close_timeout or str(60 * 60 * 24)
        self.default_lambda_role = default_lambda_role

    @property
    def swf_attributes(self):
        """Class attributes as wanted by the AWS SWF API """
        a = {'domain': self.domain,
             'name': self.name,
             'version': self.version}
        if self.description is not None:
            a['description'] = self.description
        if self.default_task_list is not None:
            a['defaultTaskList'] = {'name': self.default_task_list}
        if self.default_task_start_to_close_timeout is not None:
            a['defaultTaskStartToCloseTimeout'] = self.default_task_start_to_close_timeout
        if self.default_task_priority is not None:
            a['defaultTaskPriority'] = self.default_task_priority

        if self.default_child_policy is not None:
            a['defaultChildPolicy'] = self.default_child_policy
        if self.default_execution_start_to_close_timeout is not None:
            a['defaultExecutionStartToCloseTimeout'] = self.default_execution_start_to_close_timeout
        if self.default_lambda_role is not None:
            a['defaultLambdaRole'] = self.default_lambda_role

        return a
