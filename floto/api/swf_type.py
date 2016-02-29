import re


class SwfType:
    def __init__(self, **args):
        self.attributes = ['domain',
                           'name',
                           'version',
                           'description',
                           'defaultTaskStartToCloseTimeout',
                           'defaultTaskList',
                           'defaultTaskPriority']

        default_values = {'defaultTaskStartToCloseTimeout': str(60 * 60 * 6),
                          'defaultTaskList': 'default',
                          'defaultTaskPriority': '0'}

        self._set_attributes(self.attributes, args, default_values)

    @property
    def domain(self):
        """The name of the domain in which this type is to be registered."""
        return self._domain

    @property
    def name(self):
        """The name of the activity type within the domain."""
        return self._name

    @property
    def version(self):
        """The version of the type."""
        return self._version

    @property
    def description(self):
        """Textual description of the type.

        Parameter
        ---------
        description: str
        """
        return self._description

    @property
    def default_task_start_to_close_timeout(self):
        """Specify the default maximum duration of decision tasks for this workflow type.
        Default can be overridden when starting a workflow execution.
        
        Parameter
        ---------
        timeout: str
             Duration in seconds; An integer >= 0. "NONE" can be used to specify 
             unlimited duration.
        """
        return self._default_task_start_to_close_timeout

    @property
    def default_task_list(self):
        """Specifiy the default task list to use for scheduling decision tasks for executions of
        this workflow type. This default is used only if a task list is not provided when starting 
        the execution through the StartWorkflowExecution action or StartChildWorkflowExecution 
        decision.

        Parameter
        --------
        task_list: str
            name of the task list
        """
        return self._default_task_list

    @property
    def default_task_priority(self):
        """The default task priority to assign to the workflow type. Higher numbers indicate higher 
        priority.

        Parameter
        ---------
        priority: str
            Integer value: -2147483648 <= priority <= 2147483647
        """
        return self._default_task_priority

    @name.setter
    def name(self, name):
        self._name = name

    @domain.setter
    def domain(self, domain):
        self._domain = domain

    @version.setter
    def version(self, version):
        self._version = version

    @default_task_list.setter
    def default_task_list(self, task_list):
        self._default_task_list = task_list

    @default_task_priority.setter
    def default_task_priority(self, priority):
        self._default_task_priority = priority

    @default_task_start_to_close_timeout.setter
    def default_task_start_to_close_timeout(self, timeout):
        self._default_task_start_to_close_timeout = timeout

    def _set_attributes(self, keys, user_defined, defaults):
        for name in keys:
            snake_name = self._camel_case_to_underscore(name)
            attribute = user_defined.get(name, None) or user_defined.get(snake_name, None) \
                        or defaults.get(name, None)
            setattr(self, '_' + snake_name, attribute)

    def _get_properties(self):
        properties = {}
        for a in self.attributes:
            key = self._camel_case_to_underscore(a)
            if hasattr(self, key) and getattr(self, key):
                properties[a] = getattr(self, key)

        if self.default_task_list:
            properties['defaultTaskList'] = {'name': self.default_task_list}
        return properties

    def _camel_case(self, snake):
        parts = snake.split('_')
        camel = parts[0] + ''.join(part.title() for part in parts[1:])
        return camel

    def _camel_case_to_underscore(self, identifier):
        s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', identifier)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()
