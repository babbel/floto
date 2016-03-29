class SwfType:
    """Base class for activity and workflow type
    
    Attributes
    ----------
    name : str
        The name of the activity/workflow type within the domain.
    version : str
        The version of the activity/workflow type
    description : Optional[str]
        Textual description of the activity/workflow type
    default_task_list: Optional[str]
        * For workflow types: default task list to use for scheduling decision tasks for executions of
        this workflow type. This default is used only if a task list is not provided when starting
        the execution through the StartWorkflowExecution action or StartChildWorkflowExecution
        decision
        * For activity types: default task list to use for scheduling tasks of this activity type.
        This default task list is used if a task list is not provided when a task is scheduled through
        the ScheduleActivityTask decision.

        If not assigned, then 'default' will be used
    default_task_start_to_close_timeout: Optional[str]
        Default maximum duration (in seconds) of
        * decision tasks for this workflow type
        * activity tasks for this activity type

        This default can be overridden when scheduling an activity task, resp. when starting a workflow
        execution using the StartWorkflowExecution action or the StartChildWorkflowExecution decision.

        An integer >= 0. "NONE" can be used to specify  unlimited duration.

    default_task_priority : Optional[str]
        The default task priority to assign to the activity/workflow type. If not assigned, then "0" will be used.
        Valid values are integers that range from Java's Integer.MIN_VALUE (-2147483648) to
        Integer.MAX_VALUE (2147483647). Higher numbers indicate higher priority.

    """

    def __init__(self, *, domain, name, version, description, default_task_list,
                 default_task_start_to_close_timeout, default_task_priority):

        self.domain = domain
        self.name = name
        self.version = version
        self.description = description
        self.default_task_list = default_task_list
        self.default_task_start_to_close_timeout = default_task_start_to_close_timeout
        self.default_task_priority = default_task_priority
