from sys import version_info
from typing import TypeAlias, Generic

from .basics import AutoSupportingType, PrimaryKeyDataType


class Marker:
    """Base class for markers"""
    pass
# end class

MarkerClass = type[Marker]
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


class AutoMarker(Generic[AutoSupportingType], Marker):
    """Marker for Auto Primary Key (int or UUID)."""
    pass
