from typing import get_origin, Annotated, Type, Any, get_args, TypedDict

from pydantic.fields import FieldInfo
from sqlalchemy.util.typing import is_optional_union

from .iterators import must_be_none_or_one
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


def get_actual_type(annotated_type: AnnotationType | FieldInfo) -> AnnotationType:
    """Get the actual type from an annotated type."""
    if is_optional(annotated_type):
        args = get_args(annotated_type)
        if len(args) == 2 and args[1] is type(None):
            return get_actual_type(args[0])
        # end if
        return get_actual_type(must_be_none_or_one(args))
    # end if
    if is_annotated(annotated_type):
        args = get_args(annotated_type)
        if not args:
            return None
        # end if
        if len(args) == 1:
            return get_actual_type(args[0])
        # end if
        # If there are multiple arguments, we assume the first one is the type.
        return get_actual_type(args[0])
    if isinstance(annotated_type, FieldInfo):
        return get_actual_type(annotated_type.annotation)
    # end if
    return annotated_type
# end def


def get_marker(annotated_type: AnnotationType | FieldInfo, marker: Type[Marker]) -> Marker | None:
    """Check if the annotated type is a valid primary key."""
    if is_optional(annotated_type):
        markers = [get_marker(sub, marker) for sub in get_args(annotated_type)]
        markers = [m for m in markers if m is not None]
        return must_be_none_or_one(markers)
    # end if
    if isinstance(annotated_type, FieldInfo):
        metadata = annotated_type.metadata
        if not metadata:
            # In the case of a Union, the metadata is not available directly, instead the annotation is left as is,
            # therefore, we process it as if it were an Annotated type - because it is.
            return get_marker(annotated_type.annotation, marker)
        # end if
    else:
        if not is_annotated(annotated_type):
            return None
        # end if
        if not hasattr(annotated_type, "__metadata__"):
            return None
        # end if
        metadata = getattr(annotated_type, '__metadata__', [])
    # end if
    markers = [meta for meta in metadata if isinstance(meta, marker)]
    return must_be_none_or_one(markers)
# end def


def merge_typeddict_definition(name: str, *classes: type[TypedDict], total: bool = False) -> type[TypedDict]:
    """Dynamically create a new TypedDict merging all fields from given TypedDict classes."""
    merged: dict[str, type] = {}
    for cls in classes:
        merged.update(get_type_hints(cls))
    return typing.TypedDict(name, merged, total=total)
# end def
