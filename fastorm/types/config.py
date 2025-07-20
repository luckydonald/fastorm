from typing import TypedDict

from pydantic import ConfigDict as PydanticConfigDict, TypeAdapter


class ConfigDict(TypedDict, total=False):
    """A TypedDict for configuring FastORM Model behavior."""
    table_name: str | None
    """
    The title for the database title.
    If omitted or None, the table name will be derived from the model's name (in snake_case).
    If provided, make sure it is a valid table name for SQLAlchemy and your database dialect.
    """
# end class


CombinedConfigDict = merge_typeddict_definition("CombinedConfigDict", ConfigDict, PydanticConfigDict, total=False)
CombinedConfigDict.__doc__ = (
    """
    Custom ConfigDict for FastORM models combined with the data for Pydantic models.
    """
)

ConfigDictAdapter = TypeAdapter[ConfigDict](ConfigDict)
CombinedConfigDictAdapter = TypeAdapter[CombinedConfigDict](CombinedConfigDict)