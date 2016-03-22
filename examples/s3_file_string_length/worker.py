import floto
import datetime as dt
import random

from floto.specs.task import ActivityTask

@floto.generator(name='weekDays', version='1')
def week_days(context):
    from_date_iso = context['workflow']['from_date']
    to_date_iso = context['workflow']['to_date']
    from_date = dt.datetime.strptime(from_date_iso, '%Y-%m-%d').date()
    to_date = dt.datetime.strptime(to_date_iso, '%Y-%m-%d').date()
   
    days = [from_date + dt.timedelta(days=n) for n in range(0, (to_date-from_date).days+1)]
    week_days = [day for day in days if day.weekday()<5]
    print('{} weekdays between {} and {}'.format(len(week_days), from_date, to_date))

    def get_tasks(date):
        rs = floto.specs.retry_strategy.InstantRetry(retries=3)
        copy_file = ActivityTask(name='copyFile', version='1', input=date.isoformat(), 
                retry_strategy=rs)
        length = ActivityTask(name='fileLength', version='1', requires=[copy_file], 
                retry_strategy=rs)
        return [copy_file, length]

    tasks = [get_tasks(date) for date in week_days]
    tasks = [t for sublist in tasks for t in sublist] 
    return tasks

@floto.activity(name='copyFile', version='1')
def copy_file(context):
    day = context['activity_task']
    filename = '/path/to/my/data/{}.json'.format(day)
    print('Copying filename: {}'.format(filename))
    return filename

@floto.activity(name='fileLength', version='1')
def file_length(context):
    filename = [v for k,v in context.items() if 'copyFile' in k][0]
    file_length = random.randint(1,1000)
    print('Lenght of {}: {}'.format(filename, file_length))
    return file_length

@floto.activity(name='sumLength', version='1')
def sum_length(context):
    lengths = [v for k,v in context.items() if 'fileLength' in k]
    sum_ = sum(lengths)
    print('Total length of {} files: {}'.format(len(lengths), sum_))
    return sum_

floto.ActivityWorker(task_list='s3_files_worker', domain='floto_test', 
        task_heartbeat_in_seconds=7).run()

