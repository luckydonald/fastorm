from typing import get_origin, Annotated, Type, Any, get_args, Union

from pydantic.fields import FieldInfo
from sqlalchemy.util.typing import is_optional_union

from ..types.marker import Marker

AnnotationType = type[Any] | None
AnnotatedType = type(Annotated[str, "some metadata"])

def is_annotated(annotated_type: AnnotationType) -> bool:
    return get_origin(annotated_type) is Annotated
# end def

def is_optional(annotated_type: AnnotationType) -> bool:
    """Check if the annotated type is Optional."""
    return is_optional_union(annotated_type)
# end if


def has_marker(annotated_type: AnnotationType | FieldInfo, marker: Type[Marker]) -> bool:
    return get_marker(annotated_type, marker) is not None
# end def


def get_marker(annotated_type: AnnotationType | FieldInfo, marker: Type[Marker]) -> Marker | None:
    """Check if the annotated type is a valid primary key."""
    if is_optional(annotated_type):
        markers = [get_marker(sub, marker) for sub in get_args(annotated_type)]
        markers = [m for m in markers if m is not None]
        assert len(markers) <= 1, f"Optional should only have one marker, but got {len(markers)}: {markers=!r}"
        return markers[0] if markers else None
    # end if
    if isinstance(annotated_type, FieldInfo):
        metadata = annotated_type.metadata
    else:
        if not is_annotated(annotated_type):
            return None
        # end if
        if not hasattr(annotated_type, "__metadata__"):
            return None
        # end if
        metadata = getattr(annotated_type, '__metadata__', [])
    # end if
    markers = [isinstance(meta, marker) for meta in metadata]
    assert len(markers) <= 1, f"Optional should only have one marker, but got {len(markers)}: {markers=!r}"
    return markers[0] if markers else None
# end def