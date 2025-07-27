
def fqn(cls: type):
    return f"{cls.__module__}.{cls.__qualname__}"
# end def