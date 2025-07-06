from functools import partialmethod, partial
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
FDocType = Callable[[ObjectSelf], DocType]
FAnnType = Callable[[ObjectSelf], Any]


def log_call[FUNC: Callable | None](name: str, func: FUNC) -> FUNC:
    """Decorator to log function calls."""
    if func is None:
        return None
    # end def
    if hasattr(func, '__name__'):
        name = f"{name} ({func.__name__})"
    # end if
    def wrapper(*args, **kwargs):
        print(f'Calling {name} ({func!r}) with args: {args!r}, kwargs: {kwargs!r}')
        result = func(*args, **kwargs)
        print(f'Called {name} ({func!r}) with result: {result!r}')
        return result
    return wrapper
# end def


# noinspection SpellCheckingInspection
class Property:
    "Emulate PyProperty_Type() in Objects/descrobject.c"
    # originally from https://github.com/python/cpython/blob/bffed80230f2617de2ee02bd4bdded1024234dab/Doc/howto/descriptor.rst?plain=1#L992-L993

    fget: FGetType | None
    fset: FSetType | None
    fdel: FDelType | None
    fdoc: FDocType | None
    fann: FAnnType | None
    _doc: DocType
    _name: NameType

    def __init__(
        self,
        fget: FGetType | None = None,
        fset: FSetType | None = None,
        fdel: FDelType | None = None,
        doc: DocType = None,
        fdoc: FDocType | None = None,
        fann: FDocType | None = None,
    ) -> None:
        self.fget = log_call('fget', fget)
        self.fset = log_call('fset', fset)
        self.fdel = log_call('fdel', fdel)
        self.fdoc = log_call('fdoc', fdoc)
        self.fann = log_call('fann', fann)
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
        print(f'__get__ called: obj={obj}, objtype={objtype}')
        if obj is None:
            return self
        # end if
        if self.fget is None:
            raise AttributeError(
                f'property {self.__name__!r} of {type(obj).__name__!r} '
                'object has no getter'
            )
        # end if
        result = self.fget(obj)
        ResultType = type(result)
        class ProxiedValue(PropertyInstanceProxy, ResultType):
            # A proxy class that allows accessing the property as if it were the instance.
            # This is useful for properties that are not directly accessible on the instance.
            pass
        # end class
        proxied = ProxiedValue(result)
        proxied.prop = self
        proxied.instance = obj
        return proxied
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

    def get_doc(self: PropSelf, obj: ObjectSelf = None) -> DocType:
        if self._doc:
            return self._doc
        # end if
        if obj is None and self.fget is not None and hasattr(self.fget, '__self__'):
            # Get the bound instance from the fget method
            obj = self.fget.__self__
        # end if
        if self.fdoc is not None:
            return self.fdoc(obj)  # TODO: this `self` paramter should be the class we are in, not the property.
        # end if
        if self.fget is not None:
            return self.fget.__doc__
        # end if
        return None
    # end def

    def set_doc(self: PropSelf, doc: DocType, obj: ObjectSelf = None) -> None:
        self._doc = doc
    # end def

    def del_doc(self: PropSelf, obj: ObjectSelf = None) -> None:
        self._doc = None
    # end def

    __doc__ = native_property(get_doc, set_doc, del_doc, "The documentation string for the property.")

    def _duplicate(
        self: PropSelf,
        fget: FGetType | None | UnsetType = Unset,
        fset: FSetType | None | UnsetType = Unset,
        fdel: FDelType | None | UnsetType = Unset,
        doc: DocType | UnsetType = Unset,
        fdoc: FDocType | None | UnsetType = Unset,
        fann: FAnnType | None | UnsetType = Unset,
    ) -> PropSelf:
        """Create a duplicate of this property with the same attributes."""
        prop = type(self)(
            fget=self.fget if fget is Unset else fget,
            fset=self.fset if fset is Unset else fset,
            fdel=self.fdel if fdel is Unset else fdel,
            doc=self._doc if doc is Unset else doc,
            fdoc=self.fdoc if fdoc is Unset else fdoc,
            fann=self.fann if fann is Unset else fann,
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

    def documenter(self: PropSelf, fdoc) -> PropSelf:
        return self._duplicate(fdoc=fdoc, doc=None)
    # end def

    def annotater(self: PropSelf, fann) -> PropSelf:
        return self._duplicate(fann=fann)
    # end def
# end class


class PropertyInstanceProxy:
    prop: PropSelf = None
    instance: ObjectSelf = None

    def __get__(self, obj: ObjectSelf = None) -> PropSelf:
        print(f'Proxy: getting property {self.prop.__name__!r} (instance={self.instance!r})')
        if obj is None:
            return self.prop
        # end if
        return self.prop.fget(self.instance)

    def __getattribute__(self, name: str):
        if name in ('prop', 'instance'):
            return object.__getattribute__(self, name)
        # end if
        print(f'Proxy: forwarding attribute {name!r} (instance={self.instance!r})')
        if name == '__doc__':
            return self.prop.get_doc(self.instance)
        # end if
        return self.prop.fget(self.instance)
    # end def

    def __getattr__(self, name: str):
        """
        Fallback for attributes not found in the property.
        This allows accessing the property as if it were the instance.
        """
        print(f'Proxy: accessing attribute {name!r} (instance={self.instance!r})')
        if hasattr(self.instance, name):
            return getattr(self.instance, name)
        # end if
        raise AttributeError(f"'{type(self.instance).__name__}' object has no attribute '{name}'")
    # end def
# end class


# noinspection PyShadowingBuiltins
property = Property
