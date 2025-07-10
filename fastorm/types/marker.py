from sys import version_info
from typing import TypeAlias, Generic

from .basics import AutoSupportingType, PrimaryKeyDataType, AType

__all__ = (
    'Marker',
    'MarkerClass',
    'MarkerInfo',
    'NotRequiredMarker',
    'ForeignKeyMarker',
    'PKMarker',
    'DefaultMarker',
    'AutoMarker',
)

class Marker:
    """Base class for markers"""
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    # end def

    def __str__(self):
        return f"{self.__class__.__name__}"
    # end def
# end class

MarkerClass = type[Marker]

# Type alias for MarkerInfo, which can be a MarkerClass, UnionType, or a tuple of MarkerClasses
if version_info >= (3, 10):
    from types import UnionType
    MarkerInfo: TypeAlias = MarkerClass | UnionType | tuple[MarkerClass] | tuple['MarkerInfo', ...]
else:
    MarkerInfo: TypeAlias = MarkerClass | tuple[MarkerClass] | tuple['MarkerInfo', ...]
# end if


# Marker classes for Annotated metadata
class NotRequiredMarker(Marker):
    """Marker for Not Required fields."""
    pass
# end class


class ForeignKeyMarker(Marker):
    """Marker for Foreign Key."""
    pass
# end class



class PKMarker(Generic[PrimaryKeyDataType], Marker):
    """Marker for Primary Key."""
    pass
# end class


class DefaultMarker(Generic[AType], Marker):
    def __init__(self, default: AType):
        self.default = default
    # end def


    def __repr__(self):
        return f"{self.__class__.__name__}(default={self.default!r})"
    # end def

    def __str__(self):
        return f"{self.__class__.__name__} with default={self.default!r}"
    # end def
# end class


class AutoMarker(DefaultMarker[AutoSupportingType], Marker):
    """Marker for Auto Primary Key (int or UUID)."""
    def __init__(self, default: AType = None):
        super().__init__(default)
    # end def
# end class