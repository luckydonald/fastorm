from typing_extensions import TypedDict

from pydantic import ConfigDict as PydanticConfigDict, TypeAdapter

from ..tools.annotations import merge_typeddict_definition
#

class ConfigDict(TypedDict, total=False):
    table_name: str | None
    """
    The title for the database title.
    If omitted or None, the table name will be derived from the model's name (in snake_case).
    If provided, make sure it is a valid table name for SQLAlchemy and your database dialect.
    """
# end class


class CombinedConfigDict(PydanticConfigDict, ConfigDict, total=False):
    pass
    """
    Custom ConfigDict for FastORM models combined with the data for Pydantic models.
    """
# end class


ConfigDictAdapter = TypeAdapter[ConfigDict](ConfigDict)
CombinedConfigDictAdapter = TypeAdapter[CombinedConfigDict](CombinedConfigDict)