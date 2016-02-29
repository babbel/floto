import floto.decider

# Create and start a daemon, which acts on signals
daemon = floto.decider.Daemon(domain='floto_test', task_list='floto_daemon')
daemon.run()
