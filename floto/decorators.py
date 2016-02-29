ACTIVITY_FUNCTIONS = {}


def activity(name, version):
    def function_wrapper(func):
        identifier = '{}:{}'.format(name, version)
        ACTIVITY_FUNCTIONS[identifier] = func

    return function_wrapper
