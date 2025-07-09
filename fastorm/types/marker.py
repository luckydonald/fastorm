from sys import version_info
from types import UnionType
from typing import TypeAlias


class Marker:
    """Base class for markers"""
    pass
# end class

MarkerClass = type[Marker]
if version_info >= (3, 10):
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
