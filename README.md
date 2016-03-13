[![Build Status](https://travis-ci.org/babbel/floto.svg?branch=master)](https://travis-ci.com/babbel/floto)

# floto
floto is a task orchestration tool based on AWS SWF (Simple Workflow Service) written in Python. It uses Python 3 and boto3, the AWS SDK for Python.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Introduction](#introduction)
- [Defining the Workflow's Logics](#defining-the-workflows-logics)
  - [Decider](#decider)
  - [Decider Specifications](#decider-specifications)
    - [JSON Representation of Decider Specifications](#json-representation-of-decider-specifications)
  - [Activity Tasks and Timers](#activity-tasks-and-timers)
    - [Retry Strategies of Activity Tasks](#retry-strategies-of-activity-tasks)
    - [Activity Task Inputs](#activity-task-inputs)
    - [Task IDs](#task-ids)
- [Activity Worker](#activity-worker)
  - [Activity Worker Heartbeats](#activity-worker-heartbeats)
- [Inputs and Results](#inputs-and-results)
- [Decider Daemon](#decider-daemon)
  - [Start Decider Daemon](#start-decider-daemon)
  - [Signal Child Workflow Execution](#signal-child-workflow-execution)
- [floto's simple SWF API](#flotos-simple-swf-api)
  - [Interface to SWF](#interface-to-swf)
  - [Start the Workflow](#start-the-workflow)
  - [Register Domains, Workflow Type and Activity Type](#register-domains-workflow-type-and-activity-type)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
## Introduction
The <a href="https://aws.amazon.com/swf/" target="_blank">AWS Simple Workflow Service</a> allows to 
manage distributed applications in a scalable, resilient and fault tolerant way.
It minimizes the complexity by decoupling the execution logics from the application worker. The 
Deciders, which handle the execution logics and the worker are stateless and therefore fault 
tolerant. Whenever something goes wrong the Deciders and worker can be restarted and pick up their 
work where they left of. Furthermore several Deciders and worker of the same kind can be run at 
the same time without interference of the workflow execution or result which again leads to 
higher resilience and scalability. Every step of a workflow execution is recorded by SWF and the 
history of events is provided to the Deciders when they are about to schedule tasks.

The process of implementing a SWF workflow can be somewhat tedious if you want to e.g. 
handle complex execution logics and treat task failures and time-outs.
floto solves this problem by providing a Python package which allows you to easily define the 
execution logics and activity worker.
For the impatient we provide a ["Getting started example"](examples/hello_world.py) of a
simple workflow.

## Defining the Workflow's Logics
The business logic of your distributed application is handled by so called Deciders. Deciders act on events like workflow start, task completion or task failure and schedule tasks that are to be executed. The logic itself is defined by means of "Decider Specifications".
### Decider
The Decider implements the application's business logic. The following code defines the execution logics as depicted in figure 1. In this example ``ActivityA`` and ``ActivityB`` are scheduled after the workflow start. ``ActivityC`` is executed once they are completed.

![alt tag](docs/images/decider_spec_01.png)

```python
from floto.specs import ActivityTask, DeciderSpec
from floto.decider import Decider

activity_task_a = ActivityTask(name='ActivityA', version='v1')
activity_task_b = ActivityTask(name='ActivityB', version='v1')
activity_task_c = ActivityTask(name='ActivityC', version='v1', requires=[activity_task_a, activity_task_b])

decider_spec = DeciderSpec(domain='your_domain',
                           task_list='your_decider_task_list',
                           activity_task_list='your_activity_task_list',
                           activity_tasks=[activity_task_a, activity_task_b, activity_task_c])

Decider(decider_spec=decider_spec).run()
```
### Decider Specifications

As shown above the ```DeciderSpec``` takes the following arguments:

| Argument | Type | Description |
| :---         | :---           | :---          |
| ``domain``   | ``str``        | Your SWF domain.    |
| ``task_list``   | ``str``        | The Decider task list.    |
| ``activity_task_list``   | ``str``        | The task list of the activities.    |
| ``activity_tasks``   | ``list``        | List of ``floto.specs.Task`` objects. See next section.    |
| ``repeat_workflow``   | ``bool``        | When ``True``, the workflow is restarted after successful completion.    |

#### JSON Representation of Decider Specifications 
Decider Specifications have a JSON representation, which alternatively can be passed to a 
``Decider``.

```JSON
{
  "activity_task_list": "your_activity_task_list",
  "activity_tasks": [
    {
      "id_": "ActivityA:v1:-1606196790019401736",
      "input": {
        "task_input": "4"
      },
      "name": "ActivityA",
      "requires": [
        {
          "id_": "ActivityB:v1:-4616425358256355570",
          "name": "ActivityB",
          "type": "floto.specs.ActivityTask",
          "version": "v1"
        }
      ],
      "type": "floto.specs.ActivityTask",
      "version": "v1"
    },
    {
      "id_": "ActivityB:v1:-4616425358256355570",
      "name": "ActivityB",
      "type": "floto.specs.ActivityTask",
      "version": "v1"
    }
  ],
  "domain": "floto_test",
  "repeat_workflow": false,
  "task_list": "your_decider_task_list",
  "type": "floto.specs.DeciderSpec"
}

```

### Activity Tasks and Timers

``floto.specs.ActivityTask`` and ``floto.specs.Timer`` implement ``the floto.specs.Task`` interface. They are the buidling blocks of the execution logics. ``ActivityTask`` objects trigger the execution of the activity worker whereas Timers are used to define time-outs. Time-outs can be used inside the execution graph to delay the execution of a subsequent task (figure 2). Secondly they can be used as independent task in order to delay the execution of a subsequent workflow execution (figure 3).


Example task definitions for the delayed execution of ``ActivityB``:

![alt tag](docs/images/decider_spec_02.png)

```python
activity_task_a = ActivityTask(name='ActivityA', version='v1')
timer_30        = Timer(id_='Timer30', delay_in_seconds=30, requires=[activity_task_a])
activity_task_b = ActivityTask(name='ActivityB', version='v1', requires=[timer_30])
```

Example task definitions for a "repeated workflow execution" delay. In this case the workflow does not complete before the ``timer_3600`` times out after one hour.

![alt tag](docs/images/decider_spec_03.png)

```python
activity_task_a = ActivityTask(name='ActivityA', version='v1')
activity_task_b = ActivityTask(name='ActivityB', version='v1', requires=[activity_task_a])
timer_3600      = Timer(id_='Timer3600', delay_in_seconds=3600)
```

#### Retry Strategies of Activity Tasks
Sometimes activities fail or time out. A retry strategy can be defined for ``ActivityTask`` objects. In case a strategy is defined, the task is rescheduled after an execution failure. The following example shows a task definition which reschedules the task three times before the workflow fails.

```python
from floto.specs.retry_strategy import InstantRetry

retry_strategy = InstantRetry(retries=3)
activity_task = ActivityTask(name='ActivityA', version='v1', retry_strategy=retry_strategy)
```

#### Activity Task Inputs
``ActivityTask`` objects can already be provided with input data at the time of the task 
definition. For more information on inputs and results see section
[Inputs and Results](#inputs-and-results).

```python
activity_task = ActivityTask(name='ActivityA', version='v1', input={'filenames':['a.in', 'b.in']})
```
#### Task IDs
Every task which is used inside the definition of a Decider logic must have a unique task id. In 
case of ``ActivityTask`` objects it can be set by the ``id_`` parameter. If it is not explicitly 
defined it is set to ``<name>:<version>:hash(input)``.
```python
activity_task = ActivityTask(id_='MyUniqueIdForActivityA', name='ActivityA', version='v1')
```

For ``Timer`` objects it has to be set explicitly.

## Activity Worker
The activity worker are the programs which perform the actual work, e.g. data cleansing, database updates or or data processing. In floto ``ActivityWorker`` objects are initiated and started. The worker are triggered by the scheduling of activity tasks by the Deciders. They poll for activity tasks and react with the execution of the corresponding activity. The activities which the worker can handle, react on and run are defined beforehand. The Activities are defined by means of ```@floto.activity``` decorators. ``name`` and ``version`` handed over to the decorator must correspond to the ``ActivityTask`` defined in the Decider logics in order to get executed. The activity itself can have a ``context`` parameter which provides input to the function (See [Inputs and Results](#input-and-results)). The ``task_list`` of the ``ActivityWorker`` must correspond to the ``activity_task_list`` of the Decider definition.

```python
import floto

@floto.activity(name='ActivityA', version='v1')
def activity_a(context):
    print('Running ActivityA')
    print(context)
    return {'your':'result_activity_a'}

@floto.activity(name='ActivityB', version='v1')
def activity_b():
    print('Running ActivityB')
    return {'your':'result_activity_b'}

worker = floto.ActivityWorker(domain='floto_test', task_list='your_activity_task_list')
worker.run()
```
### Activity Worker Heartbeats
By default the activity worker sends a heartbeat to SWF every 120 seconds during the execution of 
the activity. A different timeout can be defined with:
```python
worker = floto.ActivityWorker(domain='floto_test', 
                              task_list='your_activity_task_list',
                              task_heartbeat_in_seconds=20)
```
When ``task_heartbeat_in_seconds`` is set to 0, no heartbeat is sent.

## Inputs and Results
Input data in the context of workflow executions typically consists of context information for the
activities. The information that is sent around is limited in size and consists of simple strings 
or dictionaries. You should not think of it as real input data to a CPU intense process, but 
instead of e.g. paths to this data.

The inputs that activities get access to through the context objects originate from different
sources:

**Workflow start:** When an activity is scheduled after the start of a workflow it can access the
workflow input (See [Start the workflow](#start-the-workflow)) through ``context['workflow']``

**Other activities:** The activities have access to the results of the activities they depend on. 
If ``ActivityB`` requires ``ActivityA`` and ``ActivityA`` has returned a result it can access it 
through ``context['<id of ActivityA>']``

**Task definition:** If an input has been defined at the time of the ``ActivityTask`` definition 
(cf. [Activity Task Inputs](#activity-task-inputs)) it can be accessed by the activity through 
``context['activity_task']``

After the **successful workflow completion** the results of the preceding activities are collected 
and recorded in the ``WorkflowExecutionCompleted`` event.

After a **failed  worfklow execution** the error messages of the failed activities are collected 
and recorded in the ``WorkflowExecutionFailed`` event.

## Decider Daemon
floto is able to run manually defined workflows as shown above. Furthermore it provides a 
"daemonized" service. It is described below how to start a "decider daemon", which acts on signals 
sent to SWF. 

### Start Decider Daemon
```python
import floto.decider

floto.decider.Daemon(domain='floto_test', task_list='floto_daemon').run()
```
Start the "daemon workflow" once:
```python
import floto.api

floto.api.Swf().start_workflow_execution(domain='floto_test', 
        workflow_type_name='floto_daemon_type', workflow_type_version='v1', 
        task_list='floto_daemon', workflow_id='floto_daemon') 
```
### Signal Child Workflow Execution
The Daemon acts on signals and starts child workflows and child deciders as specified in the 
Decider Specification.

```python
import floto.api
from floto.specs import ActivityTask, DeciderSpec    

activity_task_a = ActivityTask(name='ActivityA', version='v1') 

decider_spec = DeciderSpec(activity_tasks=[activity_task_a])
child_workflow_spec = {'decider_spec':decider_spec}

# Send a signal to the daemon and initiate a child workflow
floto.api.Swf().signal_workflow_execution(domain='floto_test', workflow_id='floto_daemon',
                                          signal_name='startChildWorkflowExecution',
                                          input=child_workflow_spec)
```
## floto's simple SWF API
For easier access to the SWF API floto provides functionality throught the ``floto.api`` module.
### Interface to SWF
In order to communicate with SWF create an ``swf`` object:
```python
import floto.api
swf = floto.api.Swf()
```
### Start the Workflow
```python
swf.start_workflow_execution(domain='floto_test',    
                             workflow_type_name=workflow_type.name,    
                             workflow_type_version=workflow_type.version,    
                             task_list='decider_task_list')
```

### Register Domains, Workflow Type and Activity Type
```python

# Register a domain
swf.domains.register_domain('floto_test')

# Define and register a workflow type.
workflow_type = floto.api.WorkflowType(domain='floto_test', name='my_workflow_type', version='v1')
swf.register_workflow_type(workflow_type)

# Define and register an activity type
activity_type = floto.api.ActivityType(domain='floto_test', name='simple_activity', version='v1')
swf.register_activity_type(activity_type)
```
