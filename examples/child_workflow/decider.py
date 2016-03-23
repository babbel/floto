import floto
import logging
from floto.specs.task import ActivityTask, ChildWorkflow
from floto.specs import DeciderSpec
import floto.decider

logger = logging.getLogger(__name__)

rs = floto.specs.retry_strategy.InstantRetry(retries=3)

#Define tasks and decider of the workflow
copy_files = ActivityTask(name='copyFiles', version='1', retry_strategy=rs)

child_workflow = ChildWorkflow(workflow_type_name='s3_files_example', workflow_type_version='1',
        requires=[copy_files], task_list='file_length_task_list', retry_strategy=rs)

decider_spec = DeciderSpec(domain='floto_test',
                           task_list='copy_files_task_list',
                           default_activity_task_list='s3_files_worker',
                           activity_tasks=[copy_files, child_workflow],
                           terminate_decider_after_completion=True)

decider = floto.decider.Decider(decider_spec=decider_spec)
decider.run(separate_process=True)


# Define tasks and decider of the child workflow
file_length = ActivityTask(name='fileLength', version='1', retry_strategy=rs)

child_workflow_spec = DeciderSpec(domain='floto_test',
                           task_list='file_length_task_list',
                           default_activity_task_list='s3_files_worker',
                           activity_tasks=[file_length],
                           terminate_decider_after_completion=True)

child_decider = floto.decider.Decider(decider_spec=child_workflow_spec)
child_decider.run(separate_process=True)

# Wait until workflow has completed
decider._separate_process.join()
