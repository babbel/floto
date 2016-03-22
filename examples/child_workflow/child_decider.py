import floto
import logging
from floto.specs import DeciderSpec
from floto.specs.task import ActivityTask, ChildWorkflow
import floto.decider

logger = logging.getLogger(__name__)

# ---------------------------------- #
# Create Activity Tasks and Decider
# ---------------------------------- #
decider_spec = DeciderSpec(domain='floto_test',
                           task_list='s3_files',
                           default_activity_task_list='s3_files_worker',
                           terminate_decider_after_completion=True)

decider = floto.decider.DynamicDecider(decider_spec=decider_spec)

# ---------------------------------- #
# Start the decider
# ---------------------------------- #
decider.run()
