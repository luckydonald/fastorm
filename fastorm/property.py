from typing import TypeVar, Any, Type

native_property = property

PropSelf = TypeVar('PropSelf', bound='Property')
ObjectSelf = TypeVar('ObjectSelf', bound=object)
# noinspection SpellCheckingInspection
class Property:
    "Emulate PyProperty_Type() in Objects/descrobject.c"
    # originally from https://github.com/python/cpython/blob/bffed80230f2617de2ee02bd4bdded1024234dab/Doc/howto/descriptor.rst?plain=1#L992-L993

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

    @native_property
    def __name__(self):
        return self._name if self._name is not None else self.fget.__name__

    @__name__.setter
    def __name__(self, value):
        self._name = value

    def __get__(self: PropSelf, obj: ObjectSelf, objtype: Type[ObjectSelf] = None):
        """
        Get the value of the property from the given object.

        ```py
        class Example:
            def __get__(self, obj, objtype=None):
                print(f'__get__ called: self={self}, obj={obj}, objtype={objtype}')
                return 42

        class MyClass:
            value = Example()

        instance = MyClass()
        result = instance.value
        # __get__ called: self=<__main__.Example object at 0x106802240>, obj=<__main__.MyClass object at 0x106802540>, objtype=<class '__main__.MyClass'>
        ```
        """
        if obj is None:
            return self
        # end if
        if self.fget is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no getter'
            )
        # end if
        return self.fget(obj)
    # end def

    def __set__(self: PropSelf, obj: ObjectSelf, value: Any):
        """
        Set the value of the property on the given object.

        ```py
        class Example:
            def __set__(self, obj, value):
                print(f'__set__ called: self={self}, obj={obj}, value={value}')
                obj._stored_value = value

        class MyClass:
            value = Example()

        instance = MyClass()
        instance.value = 123
        # __set__ called: self=<__main__.Example object at 0x106802240>, obj=<__main__.MyClass object at 0x106802540>, value=123
        ```
        """
        if self.fset is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no setter'
            )
        # end if
        self.fset(obj, value)
    # end def

    def __delete__(self: PropSelf, obj: ObjectSelf):
        """
        Delete the property from the given object.

        ```py
        class Example:
            def __delete__(self, obj):
                print(f'__delete__ called: self={self}, obj={obj}')
                obj._deleted = True

        class MyClass:
            value = Example()

        instance = MyClass()
        del instance.value  # Triggers Example.__delete__(self=Example instance, obj=instance)
        # __delete__ called: self=<__main__.Example object at 0x1068026c0>, obj=<__main__.MyClass object at 0x106802690>
        ```
        """
        if self.fdel is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no deleter'
            )
        # end if
        self.fdel(obj)
    # end def

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
