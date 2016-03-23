import floto
import random

@floto.activity(name='copyFiles', version='1')
def copy_files(context):
    files = context['workflow']
    print('Copying files: {}'.format(files))
    return files

@floto.activity(name='fileLength', version='1')
def file_length(context):
    files = [v for k,v in context['workflow'].items() if 'copyFiles' in k][0]
    print('Retrieving length of files: {}'.format(files))
    return {'totalNumberCharacters':random.randint(10000, 100000)}

floto.ActivityWorker(task_list='s3_files_worker', domain='floto_test', 
        task_heartbeat_in_seconds=7).run()
