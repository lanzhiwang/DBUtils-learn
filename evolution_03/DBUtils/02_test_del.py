import inspect

class Foo():

    def __init__(self, name):
        self.name = name

    def __del__(self):
        for level in inspect.stack():
            print('{}[{}] -> {}'.format(level.frame.f_code.co_filename, level.lineno,level.code_context[level.index].strip(),))
        print('name: %s is del' % self.name)

foo1 = Foo('name1')

def bar1():
    foo2 = Foo('name2')

bar1()


def bar2():
    return Foo('name4')

foo4 = bar2()

foo3 = Foo('name3')
del foo3

"""
02_test_del.py[9] -> for level in inspect.stack():
02_test_del.py[18] -> bar1()
name: name2 is del
02_test_del.py[9] -> for level in inspect.stack():
02_test_del.py[27] -> del foo3
name: name3 is del
02_test_del.py[9] -> for level in inspect.stack():
name: name1 is del
02_test_del.py[9] -> for level in inspect.stack():
name: name4 is del
"""
