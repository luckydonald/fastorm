import inspect
# from functools import partialmethod

def partialmethod(method, *args, **kw):
    def call(obj, *more_args, **more_kw):
        call_kw = kw.copy()
        call_kw.update(more_kw)
        return getattr(obj, method)(*(args+more_args), **call_kw)
    return call



def borrow_methods(source_type, overwrite=False, exclude=None, include=None,
    uncasted=None):
    '''
    Decorator for borrowing methods from other classes.
    Note: 'include' has priority over 'exclude'.
    '''
    if not exclude:
        exclude = ['__getnewargs__']
    if not include:
        include = ['__repr__', '__format__', '__str__']
    if not uncasted:
        uncasted = ['__int__', '__str__', '__cmp__']

    def invoke_method(self, method_name, *args, **keywords):
        method = getattr(self.value, method_name)
        result = method(*args, **keywords)
        if method_name not in uncasted and type(self) != type(result):
            result = self.__class__(result)
        return result

    def decorator(cls):
        setattr(cls, '_invoke_method', invoke_method)
        for (name, member) in inspect.getmembers(source_type):
            if (not overwrite and hasattr(cls, name)) \
                or (name in exclude and not name in include) \
                or not inspect.ismethoddescriptor(member):
                continue
            setattr(cls, name, partialmethod('_invoke_method', name))
        return cls
    return decorator