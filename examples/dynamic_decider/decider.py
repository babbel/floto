import floto
import logging
from floto.specs.task import ActivityTask, ChildWorkflow
from floto.specs import DeciderSpec
import floto.decider

logger = logging.getLogger(__name__)

decider_spec = DeciderSpec(domain='floto_test',
                           task_list='s3_files',
                           default_activity_task_list='s3_files_worker',
                           terminate_decider_after_completion=False)

decider = floto.decider.DynamicDecider(decider_spec=decider_spec)
decider.run(separate_process=True)

decider2 = floto.decider.DynamicDecider(decider_spec=decider_spec)
decider2.run(separate_process=True)
