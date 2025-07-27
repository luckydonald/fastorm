from typing import Type, TypeVar, TypedDict

from pydantic import JsonValue
from sqlalchemy.orm import declarative_base, DeclarativeMeta
from sqlalchemy import Column, BigInteger, Float, Boolean, DateTime, Date, Time, Text, LargeBinary, Interval, String
from sqlalchemy.dialects.postgresql import JSON
import datetime
import uuid

from .mixins import TimestampMixin
from ..tools.annotations import get_actual_type
from ..types.models import FastORM
from ..types.sqlalchemy import BaseType

from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.dialects.postgresql import JSON

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
    attrs = {}
    pk_names = set(getattr(fastorm_model, "__primary_keys_names__", ()))
    for name, info in fastorm_model.model_fields.items():
        field_type = get_actual_type(info)
        # Map to SQLAlchemy type, default to Text if unknown
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
        else:
            raise TypeError(
                f"Unsupported field type {field_type} for {name} in {fastorm_model.__name__}. "
                "Please define a custom mapping for this type."
            )
        # end try
        attrs[name] = Column(column_type, primary_key=(name in pk_names))
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