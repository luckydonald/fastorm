from typing import TypeVar

__all__ = (
    "Undefined",
    "UndefinedType",
)

from fastorm.tools.fully_qualified_class_name import fqn


class Undefined:
    __str__ = lambda self: self.__class__.__name__
    __repr__ = lambda self: fqn(self.__class__)
# end class

UndefinedType = TypeVar("UndefinedType", bound=Undefined)
Undefined = Undefined()
