import inspect


def test():

    current_function = inspect.currentframe()

    print(current_function)


test()
