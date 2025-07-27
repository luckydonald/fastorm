from typing import Type, TypeVar, TypedDict

from pydantic import JsonValue
from pydantic_core import PydanticUndefined
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import declarative_base, DeclarativeMeta
from sqlalchemy.sql.base import NO_ARG
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import (
    BigInteger, Float, Boolean, Text,
    String, DateTime, Date, Time, Interval,
    LargeBinary,
)
import datetime
import uuid


from .mixins import TimestampMixin
from .. import DefaultMarker
from ..tools.annotations import get_actual_type, get_marker, is_optional
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

TableDict = dict[str, Column] | TableMeta


def _fastorm_to_sqlalchemy_model_metadata(fastorm_model: FastORMClass, table_name: str = None) -> TableDict:
    """
    Create a SQLAlchemy model class from a Pydantic BaseModel, using the largest reasonable datatypes.
    """
    # implementation details:
    # - Sets primary_key=True for fields listed in __primary_keys_names__.
    # - Configures auto-increment for fields with AutoMarker.
    from ..types.marker import AutoMarker
    from ..tools.annotations import has_marker

    attrs = {}
    pk_names = set(getattr(fastorm_model, "__primary_keys_names__", ()))
    # due to the way FastORM models are defined in the metaclass,
    # we can assume that we have at least the generated id primary key.
    for name, info in fastorm_model.model_fields.items():
        field_type = get_actual_type(info)
        # Map to SQLAlchemy type, as a loop as we want to support subclasses
        for base_type in COLUMN_TYPE_MAP:
            try:
                if issubclass(field_type, base_type):
                    column_type = COLUMN_TYPE_MAP[base_type]
                    break
                # end if
            except TypeError as e:
                raise TypeError(
                    f"Error processing field {name} in {fastorm_model.__name__}: {e}"
                ) from e
            # end try
        else:
            raise TypeError(
                f"Unsupported field type {field_type} for {name} in {fastorm_model.__name__}. "
                "Please define a custom mapping for this type."  # TODO: Implement custom mapping
            )
        # end for

        # Check if the field has an AutoMarker
        is_auto = has_marker(info, AutoMarker)
        is_autoincrement = is_auto and issubclass(column_type, COLUMN_TYPE_MAP[int])

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
        is_nullable = default_value is None

        # Create the column with the appropriate autoincrement setting for AutoMarker fields
        attrs[name] = Column(
            column_type, 
            primary_key=(name in pk_names),
            autoincrement=is_autoincrement,
            default=default_value,
            nullable=is_nullable,
        )
    # end for
    # noinspection SpellCheckingInspection
    attrs['__tablename__'] = table_name or fastorm_model.__name__.lower()
    return attrs
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