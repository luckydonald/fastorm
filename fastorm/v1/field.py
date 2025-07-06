from typing import Any, Callable, Pattern, Literal, Unpack
import logging

from annotated_types import SupportsGt, SupportsGe, SupportsLt, SupportsLe
from pydantic import Field as _Field, AliasPath, AliasChoices, Discriminator

from pydantic.config import JsonDict

from pydantic.fields import PydanticUndefined, _Unset, FieldInfo, Deprecated, _EmptyKwargs

logger = logging.getLogger(__name__)
from fastorm.tools.docs import append_to_docs


# noinspection PyShadowingBuiltins
def reapply_unmemorable_parameter_names(ge, gt, le, lt, max, max_under, min, min_over):
    if min_over is not None:
        gt = min_over
    # end if
    if min is not None:
        ge = min
    # end if
    if max_under is not None:
        lt = max_under
    # end if
    if max is not None:
        le = max
    # end if
    return ge, gt, le, lt
# end def


# noinspection PyIncorrectDocstring,PyPep8Naming,PyShadowingBuiltins
@append_to_docs(_Field.__doc__)
def Field(
    default: Any = PydanticUndefined,
    *,
    default_factory: Callable[[], Any] | None = _Unset,
    alias: str | None = _Unset,
    alias_priority: int | None = _Unset,
    validation_alias: str | AliasPath | AliasChoices | None = _Unset,
    serialization_alias: str | None = _Unset,
    title: str | None = _Unset,
    field_title_generator: Callable[[str, FieldInfo], str] | None = _Unset,
    description: str | None = _Unset,
    examples: list[Any] | None = _Unset,
    exclude: bool | None = _Unset,
    discriminator: str | Discriminator | None = _Unset,
    deprecated: Deprecated | str | bool | None = _Unset,
    json_schema_extra: JsonDict | Callable[[JsonDict], None] | None = _Unset,
    frozen: bool | None = _Unset,
    validate_default: bool | None = _Unset,
    repr: bool = _Unset,
    init: bool | None = _Unset,
    init_var: bool | None = _Unset,
    kw_only: bool | None = _Unset,
    pattern: str | Pattern[str] | None = _Unset,
    strict: bool | None = _Unset,
    coerce_numbers_to_str: bool | None = _Unset,
    min_over: SupportsGt | None = _Unset,  # the better to remember variant of `gt`.
    gt: SupportsGt | None = _Unset,  # kept for compatibility, use `min_over`
    min: SupportsGe | None = _Unset,  # the better to remember variant to `ge`.
    ge: SupportsGe | None = _Unset,  # kept for compatibility, use `min`.
    max_under: SupportsLt | None = _Unset,  # the better to remember variant of `lt`.
    lt: SupportsLt | None = _Unset,  # kept for compatibility, use `max_under`.
    max: SupportsLe | None = _Unset,  # the better to remember variant to `le`
    le: SupportsLe | None = _Unset,  # kept for compatibility, use `max`.
    multiple_of: float | None = _Unset,
    allow_inf_nan: bool | None = _Unset,
    max_digits: int | None = _Unset,
    decimal_places: int | None = _Unset,
    min_length: int | None = _Unset,
    max_length: int | None = _Unset,
    union_mode: Literal['smart', 'left_to_right'] = _Unset,
    fail_fast: bool | None = _Unset,
    **extra: Unpack[_EmptyKwargs],
) -> Any:
    """
    Like Pydantic's Field, but renames the numeric ranges as I don't like strange abbreviations.

    :param min_over: Alias to `gt=…`. Only applies to numbers, requires the field to be "greater than". The schema
      will have an ``exclusiveMinimum`` validation keyword
    :param min: Alias to `ge=…`. Only applies to numbers, requires the field to be "greater than or equal to". The
      schema will have a ``minimum`` validation keyword
    :param max_under: Alias to `lt=…`. Only applies to numbers, requires the field to be "less than". The schema
      will have an ``exclusiveMaximum`` validation keyword
    :param max: Alias to `le=…`. Only applies to numbers, requires the field to be "less than or equal to". The
      schema will have a ``maximum`` validation keyword
    """
    ge, gt, le, lt = reapply_unmemorable_parameter_names(ge, gt, le, lt, max, max_under, min, min_over)
    return _Field(
        default=default,
        default_factory=default_factory,
        alias=alias,
        alias_priority=alias_priority,
        validation_alias=validation_alias,
        serialization_alias=serialization_alias,
        title=title,
        field_title_generator=field_title_generator,
        description=description,
        examples=examples,
        exclude=exclude,
        discriminator=discriminator,
        deprecated=deprecated,
        json_schema_extra=json_schema_extra,
        frozen=frozen,
        validate_default=validate_default,
        repr=repr,
        init=init,
        init_var=init_var,
        kw_only=kw_only,
        pattern=pattern,
        strict=strict,
        coerce_numbers_to_str=coerce_numbers_to_str,
        # min_over=min_over,
        gt=gt,
        # min=min,
        ge=ge,
        # max_under=max_under,
        lt=lt,
        # max=max,
        le=le,
        multiple_of=multiple_of,
        allow_inf_nan=allow_inf_nan,
        max_digits=max_digits,
        decimal_places=decimal_places,
        min_length=min_length,
        max_length=max_length,
        union_mode=union_mode,
        fail_fast=fail_fast,
        **extra,
    )
# end def
