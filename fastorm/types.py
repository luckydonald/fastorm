from abc import ABC
from typing import TypeVar, Generic, Annotated, Optional, Type, Union, ClassVar, Iterable, Any
from uuid import UUID
from pydantic import BaseModel
from pydantic.fields import FieldInfo

from fastorm.property import Property, ClassProperty
from fastorm.tools.annotations import Marker, has_marker, AnnotationType, AnnotatedType

PrimaryKeyDataType = TypeVar("PrimaryKeyDataType")
PrimaryKeyDataTypeArg = PrimaryKeyDataType | tuple[PrimaryKeyDataType, ...]
PrimaryKeyDataTypeArgType = Type[PrimaryKeyDataType] | tuple[Type[PrimaryKeyDataType], ...]

type MaybeTuple[T] = T | tuple[T, ...]
type MaybeTypeType[T] = MaybeTuple[type[T]]

class UseDefault:
    pass
UseDefaultType = TypeVar("UseDefaultType", bound=UseDefault)
UseDefault = UseDefault()

def unpack_single[t](many: tuple[t]) -> MaybeTuple[t]:
    """Unpack a single-element tuple to its element."""
    if len(tuple) == 1:
        return tuple[0]
    # end if
    return tuple
# end def


type _Namespace = dict[str, object]  # Class attributes/methods


# noinspection PyMethodParameters
class FastOrmMeta(type(BaseModel)):
    """
    Meta class for FastORM models.
    This class is used to store metadata about the model, such as the primary key fields and their types.
    Also it creates a `id: AutoIncrement` field if no primary key is defined.
    """

    @staticmethod
    def _write_variable_to_namespace(
        namespace: _Namespace, key: str,
        *,
        annotation: Any = UseDefault,
        default: Any = UseDefault,
    ) -> None:
        """
        Writes the variable to the namespace.
        This is a workaround for Python 3.8 and earlier, where `__annotations__` is not writable.
        """
        namespace['__annotations__'] = dict(namespace.get('__annotations__', {}))

        if default is not UseDefault:
            namespace[key] = default
        # end if
        if annotation is not UseDefault:
            namespace['__annotations__'][key] = annotation
        # end if
    # end def

    def __new__(
        mcs: type,  # The metaclass itself
        name: str,  # Name of the class being created
        bases: tuple[type, ...],  # Base classes of the new class
        namespace: _Namespace, # Class attributes/methods
    ) -> type["BaseModelWithPK"]:
        # Check if the model has a primary key defined.
        has_primary_key = any(
            has_marker(field, PKMarker)
            for field in namespace.values()
            if isinstance(field, FieldInfo)
        )
        if not has_primary_key:
            # If no primary key is defined, add an `id: AutoIncrement` field.
            print(f"Adding implicit primary key to {name}: id: AutoIncrement")
            assert 'id' not in namespace, f"Implicit primary key 'id' already exists in {name}, but you didn't provide any PK yourself."
            FastOrmMeta._write_variable_to_namespace(
                namespace,
                key='id',
                default=None,
                annotation=AutoIncrement,
            )
        # end if

        # Create the class
        return super().__new__(mcs, name, bases, namespace)
    @property
    def __primary_keys_fields__(cls: BaseModel) -> dict[str, FieldInfo]:
        """Returns the primary key of the model."""
        # iterate over the fields to find the primary key (Annotated with PKMarker)
        fields: dict[str, FieldInfo] = {}  # key: field_name, value: Annotation
        for field_name, field in cls.model_fields.items():
            if not has_marker(field, PKMarker):
                continue
            # end if
            fields[field_name] = field
        # end for
        return fields
    # end def

    @property
    def __primary_keys_field_info__(cls) -> tuple[FieldInfo]:
        return tuple(cls.__primary_keys_fields__.values())
    # end def

    @property
    def __primary_keys_type__(cls) -> tuple[AnnotationType]:
        infos = cls.__primary_keys_field_info__
        print(f"Getting primary key type for {cls.__name__}: {infos=!r}")
        return tuple(field.annotation for field in infos)
    # end def

    @property
    def __primary_keys_name__(cls) -> tuple[str]:
        infos = cls.__primary_keys_fields__.keys()
        print(f"Getting primary key type for {cls.__name__}: {infos=!r}")
        return tuple(infos)
    # end def
# end class


class BaseModelWithPK(BaseModel, Generic[PrimaryKeyDataType], metaclass=FastOrmMeta):
    """Base model class with a primary key."""

    __primary_keys_fields__: ClassVar[dict[str, FieldInfo]]
    __primary_keys_field_info__: ClassVar[tuple[FieldInfo]]
    __primary_keys_type__: ClassVar[tuple[str]]

    @Property
    def pk(self) -> PrimaryKeyDataTypeArg:
        """Returns the primary key of the model."""
        return tuple(
            getattr(self, field_name)
            for field_name in
            self.__class__.__primary_keys_fields__.keys()
        )


    @pk.annotater
    def pk(self) -> PrimaryKeyDataTypeArgType:
        return self.__class__.__primary_keys_fields__
    # end def

    def __init__(self, **kwargs):
        # Ensure that the primary key is set if it is required.
        for field_name, field in self.__class__.__primary_keys_fields__:
            if (
                has_marker(field, PKMarker)
                and has_marker(field.annotation, NotRequiredMarker)
                and field_name not in kwargs
            ):
                kwargs[field_name] = None
            # end if
        # end for

        super().__init__(**kwargs)
    # end def
# end class


AutoSupporting = int | UUID
AutoSupportingType = TypeVar("AutoSupportingType", bound=AutoSupporting)


# Marker classes for Annotated metadata
class NotRequiredMarker(Marker):
    """Marker for Not Required fields."""
    pass
# end class

class PKMarker(Generic[PrimaryKeyDataType], Marker):
    """Marker for Primary Key."""
    pass

class ForeignKeyMarker(Marker):
    """Marker for Foreign Key."""
    pass

class AutoMarker(Generic[AutoSupportingType], Marker):
    """Marker for Auto Primary Key (int or UUID)."""
    pass


# Annotated types for user-facing API
TYPE = TypeVar("TYPE")


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
    def __class_getitem__(cls, table: Type[BaseModelWithPK]) -> AnnotatedType:
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

    def __new__(cls, table: Type[BaseModelWithPK]) -> AnnotatedType:
        return cls.__class_getitem__(table)
    # end def
# end class


AutoPK = Annotated[Optional[PK[PrimaryKeyDataType]], AutoMarker(), NotRequiredMarker()]
type NotRequired[type] = Annotated[Optional[type], NotRequiredMarker()]

AutoIncrement = AutoPK[int]
AutoUUID = AutoPK[UUID]


class ExampleTableWithAutoincrement(BaseModelWithPK):
    id: AutoIncrement
    name: str
    description: str

class ExampleTableWithIntPK(BaseModelWithPK):
    id: PK[int]
    name: str
    description: str

class ExampleTableWithStrPK(BaseModelWithPK):
    id: PK[str]
    name: str
    description: str

class ExampleTableWithUUIDPK(BaseModelWithPK):
    id: AutoPK[UUID]
    name: str
    description: str

class ExampleTableWithTwoPKs(BaseModelWithPK):
    id1: PK[str]
    id2: PK[int]
    name: str
    description: str

class ExampleTableWithImplicitPK(BaseModelWithPK):
    # Implicit primary key, so `id: AutoIncrementPK`
    name: str
    description: str

class ExampleTableWithFK(BaseModelWithPK):
    name: str
    description: str
    foreign_key_int: ForeignKey[ExampleTableWithIntPK]
    foreign_key_str: ForeignKey[ExampleTableWithStrPK]
    foreign_key_uuid: ForeignKey[ExampleTableWithUUIDPK]
    foreign_key_two: ForeignKey[ExampleTableWithTwoPKs]
    foreign_key_nullable: ForeignKey[ExampleTableWithIntPK] | None


def test_insert_row_manually() -> None:
    auto_auto = ExampleTableWithAutoincrement(
        name="Example Name",
        description="This is an example description."
    )
    auto_set = ExampleTableWithAutoincrement(
        id=2,  # We can set the ID manually if needed
        name="Example Name",
        description="This is an example description."
    )
    int_set = ExampleTableWithIntPK(
        id=123,
        name="Example Name",
        description="This is an example description."
    )
    str_set = ExampleTableWithStrPK(
        id="example_id",
        name="Example Name",
        description="This is an example description."
    )
    uuid_auto = ExampleTableWithUUIDPK(
        name="Example Name",
        description="This is an example description."
    )
    uuid_set = ExampleTableWithUUIDPK(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        name="Example Name",
        description="This is an example description."
    )
    two_pk_set = ExampleTableWithTwoPKs(
        id1="part1",
        id2=456,
        name="Example Name",
        description="This is an example description."
    )
    implicit_pk_set = ExampleTableWithImplicitPK(
        name="Implicit PK Name",
        description="This is an implicit primary key example."
    )
    fk_set_1 = ExampleTableWithFK(
        name="Example Name",
        description="This is an example description.",
        foreign_key_int=int_set,
        foreign_key_str=str_set,
        foreign_key_uuid=uuid_auto,
        foreign_key_two=two_pk_set,
        foreign_key_nullable=None,
    )
    fk_set_2 = ExampleTableWithFK(
        name="Example Name",
        description="This is an example description.",
        foreign_key_int=123,
        foreign_key_str=str_set,
        foreign_key_uuid="example_id",
        foreign_key_two=("part1", 456),
        foreign_key_nullable=123,
    )
# end def

if __name__ == "__main__":
    test_insert_row_manually()