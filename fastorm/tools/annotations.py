import sys
from types import UnionType
from typing import get_origin, Annotated, TypeVar, Type, TypeAlias, Any


AnnotationType = type[Any] | None

def is_annotated(annotated_type: AnnotationType) -> bool:
    return get_origin(annotated_type) is Annotated
# end def


class Marker:
    """Base class for markers"""
    pass
# end class


MarkerClass = Type[Marker]
if sys.version_info >= (3, 10):
    MarkerInfo: TypeAlias = MarkerClass | UnionType | tuple[MarkerClass] | tuple['MarkerInfo', ...]
else:
    MarkerInfo: TypeAlias = MarkerClass | tuple[MarkerClass] | tuple['MarkerInfo', ...]
# end if


def has_marker(annotated_type: AnnotationType, marker: Type[Marker]) -> bool:
    """Check if the annotated type is a valid primary key."""
    # 'Annotated' cannot be used with instance and class checks
    if not is_annotated(annotated_type):
        return False
    # end if
    if not hasattr(annotated_type, "__metadata__"):
        return False
    # end if
    metadata = getattr(annotated_type, '__metadata__', [])
    return any(isinstance(meta, marker) for meta in metadata)
# end def