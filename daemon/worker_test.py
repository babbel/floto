from daemon import *


class Worker(Daemon):
    @floto.activity(domain=domain, name='activity1', version='v5')
    def simple_activity():
        print('\nSimpleWorker: I\'m working!')
        for i in range(3):
            print('.')
            time.sleep(0.8)

        # Terminate the worker after first execution:
        print('I\'m done.')


    def run(self):
        worker = floto.ActivityWorker(domain=domain, task_list='hello_world_atl')
        worker.run()


def main():
    d = Worker('tmp/worker_stuff.pid')
    sys.exit(d.action())


if __name__ == '__main__':
    main()