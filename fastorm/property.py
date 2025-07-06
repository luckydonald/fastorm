class Property:
    "Emulate PyProperty_Type() in Objects/descrobject.c"

    def __init__(self, fget=None, fset=None, fdel=None, doc=None, freturntype=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self.freturntype = freturntype
        if doc is None and fget is not None:
            doc = fget.__doc__
        self.__doc__ = doc
        self._name = None

    def __set_name__(self, owner, name):
        self._name = name

    @property
    def __name__(self):
        return self._name if self._name is not None else self.fget.__name__

    @__name__.setter
    def __name__(self, value):
        self._name = value

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self
        if self.fget is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no getter'
            )
        return self.fget(obj)

    def __set__(self, obj, value):
        if self.fset is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no setter'
            )
        self.fset(obj, value)

    def __delete__(self, obj):
        if self.fdel is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no deleter'
            )
        self.fdel(obj)

    def getter(self, fget):
        prop = type(self)(fget, self.fset, self.fdel, self.__doc__)
        prop._name = self._name
        return prop

    def setter(self, fset):
        prop = type(self)(self.fget, fset, self.fdel, self.__doc__)
        prop._name = self._name
        return prop

    def deleter(self, fdel):
        prop = type(self)(self.fget, self.fset, fdel, self.__doc__)
        prop._name = self._name
        return prop
# end class

# noinspection PyShadowingBuiltins
property = Property
