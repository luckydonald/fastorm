from typing import get_origin, Annotated, Type, Any

from pydantic.fields import FieldInfo

from ..types.marker import Marker


AnnotationType = type[Any] | None
AnnotatedType = type(Annotated[str, "some metadata"])

def is_annotated(annotated_type: AnnotationType) -> bool:
    return get_origin(annotated_type) is Annotated
# end def



def has_marker(annotated_type: AnnotationType | FieldInfo, marker: Type[Marker]) -> bool:
    """Check if the annotated type is a valid primary key."""
    # 'Annotated' cannot be used with instance and class checks
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