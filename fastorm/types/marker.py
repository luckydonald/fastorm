from sys import version_info
from typing import TypeAlias, Generic, TYPE_CHECKING

from .basics import AutoSupportingType, PrimaryKeyDataType, AType
from .undefined import Undefined, UndefinedType

if TYPE_CHECKING:
    from .models import FastORM as FastOrmTable
else:
    type FastOrmTable = 'FastORM'
# end if


__all__ = (
    'Marker',
    'MarkerClass',
    'MarkerInfo',
    'ForeignKeyMarker',
    'PKMarker',
    'DefaultMarker',
    'AutoMarker',
)

class Marker:
    """Base class for markers"""
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    # end def

    def __str__(self):
        return f"{self.__class__.__name__}"
    # end def
# end class

MarkerClass = type[Marker]

# Type alias for MarkerInfo, which can be a MarkerClass, UnionType, or a tuple of MarkerClasses
if version_info >= (3, 10):
    from types import UnionType
    MarkerInfo: TypeAlias = MarkerClass | UnionType | tuple[MarkerClass] | tuple['MarkerInfo', ...]
else:
    MarkerInfo: TypeAlias = MarkerClass | tuple[MarkerClass] | tuple['MarkerInfo', ...]
# end if


class ForeignKeyMarker(Marker):
    """Marker for Foreign KeyForeignKeyMarker."""
    table: type[FastOrmTable]
    """
    The orm object, i.e. the referenced table
    """

    pk_type: PrimaryKeyDataType | UndefinedType
    """
    primary key type stays `Undefined` if it's used in a non-generic way.
    """

    def __init__(
        self,
        table: type[FastOrmTable],
        pk_type: PrimaryKeyDataType | UndefinedType = Undefined,
    ):
        self.table = table
        self.pk_type = pk_type
    # end def

    def __repr__(self):
        return f"{self.__class__.__name__}(table={self.table.__name__!s}, pk_type={self.pk_type!r})"
    # end def

    def __str__(self):
        return (
            f"{self.__class__.__name__} with table={self.table.__name__!r}"
            + ("" if self.pk_type is Undefined else f" and pk_type={self.pk_type!r}")
        )
    # end def
# end class



class PKMarker(Generic[PrimaryKeyDataType], Marker):
    """Marker for Primary Key."""
    pass
# end class


class DefaultMarker(Generic[AType], Marker):
    default: AType

    def __init__(self, default: AType):
        self.default = default
    # end def


    def __repr__(self):
        return f"{self.__class__.__name__}(default={self.default!r})"
    # end def

    def __str__(self):
        return f"{self.__class__.__name__} with default={self.default!r}"
    # end def
# end class


class AutoMarker(DefaultMarker[AutoSupportingType], Marker):
    """Marker for Auto Primary Key (int or UUID)."""
    def __init__(self, default: AType = None):
        super().__init__(default)
    # end def
# end class