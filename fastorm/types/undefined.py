from typing import TypeVar

__all__ = (
    "Undefined",
    "UndefinedType",
)

class Undefined:
    pass
    pass
UndefinedType = TypeVar("UndefinedType", bound=Undefined)
Undefined = Undefined()
