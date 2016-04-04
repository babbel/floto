import floto
import floto.decider

rs = floto.specs.retry_strategy.InstantRetry(retries=2)
domain = 'floto_test'

workflow_timeout = floto.specs.task.Timer(id_='workflow_timeout', delay_in_seconds=30)

week_days = floto.specs.task.Generator(domain=domain, 
                                       name='weekDays', 
                                       version='1', 
                                       retry_strategy=rs)

sum_length = floto.specs.task.ActivityTask(domain=domain,
                                           name='sumLength', 
                                           version='1', 
                                           requires=[week_days.id_], 
                                           retry_strategy=rs)

spec = floto.specs.DeciderSpec(domain=domain, 
        task_list='s3_files', 
        default_activity_task_list='s3_files_worker',
        terminate_decider_after_completion=False,
        repeat_workflow=False,
        activity_tasks=[workflow_timeout, week_days, sum_length])

decider = floto.decider.Decider(decider_spec=spec)
decider.run(separate_process=True)

decider_2 = floto.decider.Decider(decider_spec=spec)
decider_2.run(separate_process=True)

