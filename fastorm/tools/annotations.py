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
    """Check if the annotated type is a valid primary key."""
    if is_optional(annotated_type):
        return any(has_marker(sub, marker) for sub in get_args(annotated_type))
    # end if
    if isinstance(annotated_type, FieldInfo):
        metadata = annotated_type.metadata
    else:
        if not is_annotated(annotated_type):
            return False
        # end if
        if not hasattr(annotated_type, "__metadata__"):
            return False
        # end if
        metadata = getattr(annotated_type, '__metadata__', [])
    # end if
    return any(isinstance(meta, marker) for meta in metadata)
# end def