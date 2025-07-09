from abc import ABC
from uuid import UUID
from typing import Annotated, Optional, Union, TypeVar

from .marker import NotRequiredMarker, AutoMarker, ForeignKeyMarker, PKMarker
from .basics import AnnotatedType, PrimaryKeyDataType


class PK(ABC):
    def __class_getitem__(cls, item):
        """Allows PK to be used as a generic type."""
        if not isinstance(item, type):
            if isinstance(item, TypeVar):
                return Annotated[item, PKMarker()]
            # end if
            raise TypeError(f"Expected a type, got {item!r}")
        # end if
        return Annotated[item, PKMarker()]
    # end def
# end class


class ForeignKey(ABC):
    def __class_getitem__(cls, table: type['BaseModelWithPK']) -> AnnotatedType:
        from . import BaseModelWithPK
        """Allows PK to be used as a generic type."""
        if not isinstance(table, type):
            if isinstance(table, TypeVar):
                return Annotated[table, ForeignKeyMarker()]
            # end if
            raise TypeError(f"Expected a type, got {table!r}")
        # end if
        if not issubclass(table, BaseModelWithPK):
            raise TypeError(f"Expected a BaseModelWithPK, got {table!r}")
        # end if
        pk_type = table.__primary_keys_type__
        print(f"1. Getting foreign key type for {table.__name__}: {pk_type=!r}, {table=!r}")
        return Annotated[Union[table, *pk_type], ForeignKeyMarker()]
    # end def

    def __new__(cls, table: type['BaseModelWithPK']) -> AnnotatedType:
        return cls.__class_getitem__(table)
    # end def
# end class


AutoPK = Annotated[Optional[PK[PrimaryKeyDataType]], AutoMarker(), NotRequiredMarker()]
type NotRequired[type] = Annotated[Optional[type], NotRequiredMarker()]

AutoIncrement = AutoPK[int]
AutoUUID = AutoPK[UUID]

