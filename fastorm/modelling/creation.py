from typing import Type, TypeVar, TypedDict, Optional, Union, Any

from pydantic import JsonValue
from pydantic_core import PydanticUndefined
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import declarative_base, DeclarativeMeta, relationship, Mapped, mapped_column
from sqlalchemy.sql.base import NO_ARG
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import (
    BigInteger, Float, Boolean, Text,
    String, DateTime, Date, Time, Interval,
    LargeBinary,
)
import datetime
import uuid

from .mixins import TimestampMixin
from .. import DefaultMarker, ForeignKeyMarker, Undefined
from ..tools.annotations import get_actual_type, get_marker, is_optional
from ..tools.fully_qualified_class_name import fqn
from ..types.models import FastORM
from ..types.sqlalchemy import BaseType

Base: DeclarativeMeta = declarative_base()

COLUMN_TYPE_MAP = {
    int: BigInteger,
    float: Float,
    bool: Boolean,
    str: Text,
    bytes: LargeBinary,
    datetime.datetime: DateTime,
    datetime.date: Date,
    datetime.time: Time,
    datetime.timedelta: Interval,
    uuid.UUID: UUID,
    JsonValue: JSON,
}

# these are used when the type is not supported by the database dialect
COLUMN_TYPE_FALLBACKS = {
    JSON: Text,
    UUID: String(36),
}


FastORMClass = TypeVar("FastORMClass", bound=Type[FastORM])

class TableMeta(TypedDict):
    # noinspection SpellCheckingInspection
    __tablename__: str
# end class

TableDict = dict[str, Union[Mapped[Any], Any]] | TableMeta


def _fastorm_to_sqlalchemy_model_metadata(fastorm_model: FastORMClass, table_name: str = None) -> TableDict:
    """
    Create a SQLAlchemy model class from a Pydantic BaseModel, using modern Mapped types with mapped_column.
    """
    # implementation details:
    # - Sets primary_key=True for fields listed in __primary_keys_names__.
    # - Configures auto-increment for fields with AutoMarker.
    from ..types.marker import AutoMarker
    from ..tools.annotations import has_marker

    attrs = {}
    annotations = {}
    pk_names = set(getattr(fastorm_model, "__primary_keys_names__", ()))
    # due to the way FastORM models are defined in the metaclass,
    # we can assume that we have at least the generated id primary key.
    for name, info in fastorm_model.model_fields.items():
        fk_marker = get_marker(info, ForeignKeyMarker)
        if fk_marker is not None:
            # Get referenced table and PK type
            fk_info = fk_marker.pk_type
            assert fk_info is not Undefined
            field_type = get_actual_type(fk_info)
        else:
            field_type = get_actual_type(info)
        # end if

        # Map to SQLAlchemy type, as a loop as we want to support subclasses
        is_multiple_pk = isinstance(field_type, tuple)
        field_types = field_type if is_multiple_pk else (field_type,)
        column_types = []
        for field_type in field_types:
            try:
                column_type = deduct_sqlalchemy_type(field_type)
                column_types.append(column_type)
            except TypeError as e:
                raise TypeError(
                    f"Error processing field {fqn(fastorm_model)}.{name} (type {field_type!r}, "
                    f"as deducted from {info!r}). Original exception: {e}"
                ) from e
            # end try
        # end for
        if not len(column_types) > 0:
            raise TypeError(
                f"Field {fqn(fastorm_model)}.{name} has no type defined resulting in an empty tuple "
                f"(type {field_type!r}, resulting in {column_types!r}, as deducted from {info!r})"
            )
        # end if
        column_type = column_types[0] if not is_multiple_pk else tuple(column_types)

        # Check if the field has an AutoMarker
        is_auto = has_marker(info, AutoMarker) and fk_marker is None
        is_autoincrement = is_auto and not is_multiple_pk and issubclass(column_type, COLUMN_TYPE_MAP[int])

        default_marker = get_marker(info, DefaultMarker)
        default_value = default_marker.default if default_marker is not None else NO_ARG
        if is_optional(info) and info.default is not PydanticUndefined:
            # TODO: can it have a default value and NOT be Optional?
            if default_value != NO_ARG:
                raise TypeError(
                    f"Field {name} in {fastorm_model.__name__} cannot"
                    f" both be `Optional` with a default value and also have a `DefaultMarker`."
                )
            # end if
            default_value = info.default
        # end if
        is_nullable = default_value is None and not is_auto

        if fk_marker is not None:
            # Create a ForeignKey constraint for foreign key fields
            ref_table = fk_marker.table
            # noinspection SpellCheckingInspection
            ref_table_name = getattr(ref_table, "__tablename__", ref_table.__name__.lower())
            ref_pk_name = getattr(ref_table, "__primary_keys_names__", ["id"])[0]

            # Create mapped_column with ForeignKey for the foreign key field
            mapped_col_kwargs = {
                'primary_key': (name in pk_names),
                'autoincrement': is_autoincrement,
                'nullable': is_nullable,
            }
            if default_value != NO_ARG:
                mapped_col_kwargs['default'] = default_value

            attrs[name] = mapped_column(
                ForeignKey(f"{ref_table_name}.{ref_pk_name}"),
                **mapped_col_kwargs
            )

            # Set up the type annotation for the foreign key field
            if is_nullable:
                annotations[name] = Mapped[Optional[field_type]]
            else:
                annotations[name] = Mapped[field_type]
            # end if
        else:
            # Regular field - create mapped_column
            mapped_col_kwargs = {
                'primary_key': (name in pk_names),
                'autoincrement': is_autoincrement,
                'nullable': is_nullable,
            }
            if default_value != NO_ARG:
                mapped_col_kwargs['default'] = default_value

            # For some column types, we might need to pass the type instance
            if hasattr(column_type, '__call__'):
                # If it's a type constructor like String(30), call it
                try:
                    type_instance = column_type()
                except:
                    type_instance = column_type
            else:
                type_instance = column_type

            attrs[name] = mapped_column(type_instance, **mapped_col_kwargs)

            # Set up the type annotation
            if is_nullable:
                annotations[name] = Mapped[Optional[field_type]]
            else:
                annotations[name] = Mapped[field_type]
            # end if
        # end if
    # end for

    # Add annotations to attrs for proper type hinting
    attrs['__annotations__'] = annotations
    # noinspection SpellCheckingInspection
    attrs['__tablename__'] = table_name or fastorm_model.__name__.lower()
    return attrs


def deduct_sqlalchemy_type(field_type):
    for base_type in COLUMN_TYPE_MAP:
        try:
            if issubclass(field_type, base_type):
                column_type = COLUMN_TYPE_MAP[base_type]
                break
            # end if
        except TypeError as e:
            raise e
        # end try
    else:
        raise TypeError(
            f"Unsupported field type {field_type}. "
            "Please define a custom mapping for this type."  # TODO: Implement custom mapping
        )
    # end for
    return column_type


# end def


def fastorm_to_sqlalchemy_model(fastorm_model: FastORMClass, table_name: str = None) -> type[BaseType] | type[TimestampMixin] | FastORMClass:
    attrs = _fastorm_to_sqlalchemy_model_metadata(fastorm_model, table_name)
    # Create a new SQLAlchemy model class with the attributes from the FastORM model
    return type(
        f"{fastorm_model.__name__}SQLA",
        (TimestampMixin, Base,),
        attrs
    )
# end def