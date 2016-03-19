from functools import wraps
import floto

ACTIVITY_FUNCTIONS = {}


def activity(name, version):
    def function_wrapper(func):
        identifier = '{}:{}'.format(name, version)
        ACTIVITY_FUNCTIONS[identifier] = func
    return function_wrapper

def check_type_generator_output(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        message = 'Generators must return list(<floto.spec.Task>)'
        if not isinstance(result, list):
            raise ValueError(message)
        if not [isinstance(t, floto.specs.task.Task) for t in result]:
            raise ValueError(message)
        return result
    return wrapper

def generator(name, version):
    def function_wrapper(func):
        identifier = '{}:{}'.format(name, version)
        ACTIVITY_FUNCTIONS[identifier] = check_type_generator_output(func)
    return function_wrapper


