import floto

import datetime as dt

from_date = dt.date(2016,2,10).isoformat()
to_date = dt.date(2016,3,11).isoformat()

floto.api.Swf().start_workflow_execution(domain='floto_test', workflow_id='s3_files', 
        workflow_type_name='s3_files_example', workflow_type_version='1', task_list='s3_files',
        input={'from_date':from_date, 'to_date':to_date})

