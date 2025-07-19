from typing import Type
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Float, Boolean, DateTime, Date, Time, Text, LargeBinary, Interval, String
import datetime
import uuid

from ..tools.annotations import get_actual_type
from ..types.models import FastORM

try:
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID
    HAS_PG_UUID = True
except ImportError:
    HAS_PG_UUID = False

Base = declarative_base()

PYDANTIC_TYPE_MAP = {
    int: BigInteger,
    float: Float,
    bool: Boolean,
    str: Text,
    bytes: LargeBinary,
    datetime.datetime: DateTime,
    datetime.date: Date,
    datetime.time: Time,
    datetime.timedelta: Interval,
    uuid.UUID: PG_UUID if HAS_PG_UUID else String(36),
}

def pydantic_to_sqlalchemy_model(fastorm_model: Type[FastORM], table_name: str = None):
    """
    Create a SQLAlchemy model class from a Pydantic BaseModel, using the largest reasonable datatypes.
    """
    attrs = {}
    for name, info in fastorm_model.__primary_keys_info_dict__.items():
        field_type = get_actual_type(info)
        # Map to SQLAlchemy type, default to Text if unknown
        for base_type in PYDANTIC_TYPE_MAP:
            try:
                if issubclass(field_type, base_type):
                    column_type = PYDANTIC_TYPE_MAP[base_type]
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
            )  # TODO: allow custom mapping, lol
        # end for
        attrs[name] = Column(column_type)
    # Optionally set __tablename__
    attrs['__tablename__'] = table_name or fastorm_model.__name__.lower()
    # Create the model class
    return type(
        f"{fastorm_model.__name__}SQLA",
        (Base,),
        attrs
    )
