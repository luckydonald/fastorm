from typing import Type
from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, Float, Boolean, DateTime, Date, Time, Text, LargeBinary, Interval, String
import datetime
import uuid

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

def pydantic_to_sqlalchemy_model(pydantic_model: Type[BaseModel], table_name: str = None):
    """
    Create a SQLAlchemy model class from a Pydantic BaseModel, using the largest reasonable datatypes.
    """
    attrs = {}
    for name, field in pydantic_model.__fields__.items():
        field_type = field.outer_type_
        # Map to SQLAlchemy type, default to Text if unknown
        column_type = PYDANTIC_TYPE_MAP.get(field_type, Text)
        attrs[name] = Column(column_type)
    # Optionally set __tablename__
    attrs['__tablename__'] = table_name or pydantic_model.__name__.lower()
    # Create the model class
    return type(
        f"{pydantic_model.__name__}SQLA",
        (Base,),
        attrs
    )
