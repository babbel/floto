import floto
from floto.specs.retry_strategy import InstantRetry
from floto.specs.task import ActivityTask, ChildWorkflow

import datetime as dt

# ---------------------------------- #
# Start the workflow execution
# ---------------------------------- #

rs = InstantRetry(retries=3)

input_copy_files = {'from_date':dt.date(2016,3,6).isoformat(), 
                    'to_date':dt.date(2016,3,11).isoformat()}
copy_files = ActivityTask(name='copyFiles', version='1', retry_strategy=rs, input=input_copy_files)

file_length = ActivityTask(name='fileLength', version='1', retry_strategy=rs)

child_workflow = ChildWorkflow(workflow_type_name='read_file_lengths', 
        workflow_type_version='1', 
        requires=[copy_files], 
        retry_strategy=rs, 
        task_list='s3_files', 
        input={'activity_tasks':[file_length]})

activity_tasks = [copy_files, child_workflow]
workflow_args = {'domain': 'floto_test', 
                 'workflow_type_name': 's3_files_example',
                 'workflow_type_version': '1',
                 'task_list': 's3_files',
                 'workflow_id': 's3_files',
                 'input': {'activity_tasks':activity_tasks}}

response = floto.api.Swf().start_workflow_execution(**workflow_args)
