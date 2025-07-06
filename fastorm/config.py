from pydantic.config import ConfigDict as PydanticConfigDict, ExtraValues

__all__ = [
    'ConfigDict',
]


class ConfigDict(PydanticConfigDict):
    table: str | None
    """The table name for the generated JSON schema, defaults to the model's name"""

    extra: ExtraValues
    """Defines how to handle extra values in the model, defaults to 'forbid'"""
    extra.__doc__ = PydanticConfigDict.__annotations__['extra'].__doc__.replace("Defaults to `'ignore'`", "Defaults to `'forbid'`")
