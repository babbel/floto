import sys

import floto.api

if len(sys.argv) > 1:
    workflow_id = sys.argv[1]
else:
    workflow_id = 'my_workflow_type_v1'

swf = floto.api.Swf()
swf.terminate_workflow_execution(domain='floto_test', workflow_id=workflow_id)
