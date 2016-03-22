import floto
import logging
import random
import datetime as dt

logger = logging.getLogger(__name__)

domain = 'floto_test'
activity_task_list = 's3_files_worker'

@floto.activity(name='copyFiles', version='1')
def copy_files(context):
    from_date_iso = context['activity_task']['from_date']
    to_date_iso = context['activity_task']['to_date']
    from_date = dt.datetime.strptime(from_date_iso, '%Y-%m-%d').date()
    to_date = dt.datetime.strptime(to_date_iso, '%Y-%m-%d').date()

    days = [from_date + dt.timedelta(days=n) for n in range(0, (to_date-from_date).days+1)]

    file_list = ['/path/to/data/{}.json'.format(day) for day in days]
    return file_list 

@floto.activity(name='fileLength', version='1')
def file_length(context):
    files = [v for k,v in context['workflow'].items() if 'copyFiles' in k][0]
    print('Calculating the length of the files: {}'.format(files))
    return random.randint(1000, 10000) 

worker_1 = floto.ActivityWorker(domain=domain, task_list=activity_task_list, 
        task_heartbeat_in_seconds=4)
worker_1.run()
