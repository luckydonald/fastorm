from typing import TypeVar, Any

from typing import Callable, Any, Type, TypeVar

from fastorm.unset import UnsetType, Unset


native_property = property  # type: ignore[assignment]

PropSelf = TypeVar('PropSelf', bound='Property')
ObjectSelf = TypeVar('ObjectSelf', bound=object)

FDelType = Callable[[ObjectSelf], None]
FDocType = Callable[[ObjectSelf], DocType]
FAnnType = Callable[[ObjectSelf], Any]


def log_call[FUNC: Callable](name: str, func: FUNC) -> FUNC:
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

        self._doc = doc
        self._name = None

    def __set_name__(self, owner, name):
        self.fget = log_call('fget', fget)
        self.fset = log_call('fset', fset)
        self.fdel = log_call('fdel', fdel)
        self.fdoc = log_call('fdoc', fdoc)
        self.fann = log_call('fann', fann)
        self._doc = doc
        self._ssssname = None
        self._prop = PropertyExtensions(self)

        self.__doc__ = __doc__

    def __set_name__(self, owner, name: NameType):
        self._name = name

    @native_property
    @property
    def __name__(self) -> NameType:
        return self._name if self._name is not None else self.fget.__name__

        self.fdel(obj)
    # end def

    def getter(self, fget):
        prop = type(self)(fget, self.fset, self.fdel, self.__doc__)
        prop._name = self._name
        return prop
    def get_doc(self):
        print(f'__doc__ called for {self!r}')
        if self.prop._doc:
            return self._doc
        # end if
        if self.prop.fdoc is not None:
            return self.prop.fdoc(self)
        # end if
        if self.prop.fget is not None:
            return self.prop.fget.__doc__
        # end if
        return None
    # end def

    def set_doc(self, doc):
        self._doc = doc
    # end def

    def setter(self, fset):
        prop = type(self)(self.fget, fset, self.fdel, self.__doc__)
        prop._name = self._name
        return prop
    def del_doc(self) -> None:
        self._doc = None
    # end def

    def deleter(self, fdel):
        prop = type(self)(self.fget, self.fset, fdel, self.__doc__)
    __doc__ = native_property(get_doc, set_doc, del_doc)

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
        prop = self.__class__(
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

    def getter(self: PropSelf, fget) -> PropSelf:
        return self._duplicate(fget=fget)
    # end def

    def setter(self: PropSelf, fset) -> PropSelf:
        return self._duplicate(fset=fset)
    # end def

    def deleter(self: PropSelf, fdel) -> PropSelf:
        return self._duplicate(fdel=fdel)
    # end def

    def documenter(self: PropSelf, fdoc) -> PropSelf:
        return self._duplicate(fdoc=fdoc, doc=None)
    # end def

    def annotate(self: PropSelf, fann) -> PropSelf:
        return self._duplicate(fann=fann)
    # end def
# end class


class PropertyExtensions:
    prop: Property
    def __init__(self, prop: Property):
        self.prop = prop
    # end def

    @native_property
    def __doc__(self) -> DocType:
        if self.prop._doc:
            return self._doc
        # end if
        if self.prop.fdoc is not None:
            return self.prop.fdoc(self)
        # end if
        if self.prop.fget is not None:
            return self.prop.fget.__doc__
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
# end class


# noinspection PyShadowingBuiltins
property = Property
# property = Property
