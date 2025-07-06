from typing import TypeVar, Any, Type, Callable

from fastorm.unset import UnsetType, Unset


native_property = property

PropSelf = TypeVar('PropSelf', bound='Property')
ObjectSelf = TypeVar('ObjectSelf', bound=object)

NameType = str | None
DocType = str | None
FGetType = Callable[[ObjectSelf], Any]
FSetType = Callable[[ObjectSelf, Any], None]
FDelType = Callable[[ObjectSelf], None]


# noinspection SpellCheckingInspection
class Property:
    "Emulate PyProperty_Type() in Objects/descrobject.c"
    # originally from https://github.com/python/cpython/blob/bffed80230f2617de2ee02bd4bdded1024234dab/Doc/howto/descriptor.rst?plain=1#L992-L993

    fget: FGetType | None
    fset: FSetType | None
    fdel: FDelType | None
    _doc: DocType
    _name: NameType

    def __init__(
        self,
        fget: FGetType | None = None,
        fset: FSetType | None = None,
        fdel: FDelType | None = None,
        doc: DocType = None,
    ) -> None:
        self.fget = fget
        self.fset = fset
        self.fdel = fdel
        self._doc = doc
        self._name = None

    def __set_name__(self, owner, name: NameType):
        self._name = name

    @native_property
    def __name__(self) -> NameType:
        return self._name if self._name is not None else self.fget.__name__

    @__name__.setter
    def __name__(self, value: NameType) -> None:
        self._name = value
    # end def

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

    @native_property
    def __doc__(self) -> DocType:
        if self._doc:
            return self._doc
        # end if
        if self.fget is not None:
            return self.fget.__doc__
        # end if
        return None
    # end def

    @__doc__.setter
    def __doc__(self: PropSelf, doc: DocType) -> None:
        self._doc = doc
    # end def

    @__doc__.deleter
    def __doc__(self) -> None:
        self._doc = None
    # end def

    def _duplicate(
        self: PropSelf,
        fget: FGetType | None | UnsetType = Unset,
        fset: FSetType | None | UnsetType = Unset,
        fdel: FDelType | None | UnsetType = Unset,
        doc: DocType | UnsetType = Unset,
    ) -> PropSelf:
        """Create a duplicate of this property with the same attributes."""
        prop = type(self)(
            fget=self.fget if fget is Unset else fget,
            fset=self.fset if fset is Unset else fset,
            fdel=self.fdel if fdel is Unset else fdel,
            doc=self._doc if doc is Unset else doc,
        )
        prop._name = self._name
        return prop
    # end def

    def getter(self: PropSelf, fget: FGetType | None) -> PropSelf:
        return self._duplicate(fget=fget)
    # end def

    def setter(self: PropSelf, fset: FSetType | None) -> PropSelf:
        return self._duplicate(fset=fset)
    # end def

    def deleter(self: PropSelf, fdel: FDelType | None) -> PropSelf:
        return self._duplicate(fdel=fdel)
    # end def
# end class


# noinspection PyShadowingBuiltins
property = Property
