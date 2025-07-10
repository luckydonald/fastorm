from abc import ABC
from typing import TypeVar, Generic, Annotated, Optional, Type, Union, ClassVar, Iterable, Any
from uuid import UUID

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from ..property import Property
from ..tools.annotations import Marker, has_marker, AnnotationType, get_marker

from .marker import Marker, NotRequiredMarker, ForeignKeyMarker, PKMarker, AutoMarker, DefaultMarker
from .fields import AutoPK, AutoIncrement, PrimaryKeyDataType, PK, ForeignKey
from .basics import MaybeTuple, AutoSupporting, PrimaryKeyDataTypeArg, PrimaryKeyDataTypeArgType


class UseDefault:
    pass
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


class FastOrmModelTypehints(ABC):
    __primary_keys_field_info__: ClassVar[tuple[FieldInfo]]
    __primary_keys_info_dict__: ClassVar[dict[str, FieldInfo]]
    __primary_keys_names__: ClassVar[tuple[str,]]
    __primary_keys_type__: ClassVar[tuple[str]]
# end class


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
    ) -> _Namespace:
        """
        Writes the variable to the namespace.
        This is a workaround for Python 3.8 and earlier, where `__annotations__` is not writable.
        """
        if not '__annotations__' in namespace and not isinstance(annotation, dict):
            # only destroy possible references if we are not using a dict
            namespace['__annotations__'] = dict(namespace.get('__annotations__', {}))
        # end if

        if default is not UseDefault:
            namespace[key] = default
        # end if
        if annotation is not UseDefault:
            namespace['__annotations__'][key] = annotation
        # end if
        return namespace
    # end def

    def __new__(
        mcs: type,  # The metaclass itself
        name: str,  # Name of the class being created
        bases: tuple[type, ...],  # Base classes of the new class
        namespace: _Namespace, # Class attributes/methods
    ) -> type["BaseModelWithPK"]:
        # TODO: figure out way to not hardcode that string:
        if name == 'BaseModelWithPK' and bases in (
                (BaseModel,),
                (BaseModel, Generic),
                (BaseModel, Generic, FastOrmModelTypehints),
        ):
            # go directly to start, don't draw 200 bits
            # skip the root class itself.
            return super().__new__(mcs, name, bases, namespace)
        # end if
        __annotations__: dict[str, Any] = namespace.get('__annotations__', {})
        namespace['__annotations__'] = __annotations__
        # Check if the model has a primary key defined.
        has_primary_key = any(
            has_marker(field, PKMarker)
            for field in list(__annotations__.values())
        )
        # (<class 'pydantic.main.BaseModel'>, <class 'typing.Generic'>)
        # (<class 'fastorm.types.BaseModelWithPK'>,)
        if not has_primary_key:
            # If no primary key is defined, add an `id: AutoIncrement` field.
            print(f"Adding implicit primary key to {name}: id: AutoIncrement")
            assert 'id' not in namespace, f"Implicit primary key 'id' already exists in {name}, but you didn't provide any PK yourself."
            namespace = FastOrmMeta._write_variable_to_namespace(
                namespace,
                key='id',
                annotation=AutoIncrement,
            )

        # end if

        # set `default = None` for all AutoMarker
        for field_name, field in __annotations__.items():
            marker: DefaultMarker | None = get_marker(field, DefaultMarker)
            if not marker:
                continue
            # end if
            marker_name = marker.__class__.__name__
            if type(marker) != DefaultMarker:
                # it's a subclass of AutoMarker
                marker_name = f"{marker_name}, a DefaultMarker"
            # end if
            print(f" Setting default=None for {name}.{field_name} ({marker_name})")
            namespace = FastOrmMeta._write_variable_to_namespace(
                namespace,
                key=field_name,
                default=marker.default,
            )
            # end if
        # end for

        # Fill in the `__primary_keys_*` properties
        # merge __annotations__ with FastOrmModelTypehints.__annotations__
        namespace['__annotations__'] |= FastOrmModelTypehints.__annotations__

        # Create the class
        return super().__new__(mcs, name, bases, namespace)
    # end def

    @property
    def __primary_keys_info_dict__(cls: BaseModel) -> dict[str, FieldInfo]:
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
    def __primary_keys_field_info__(cls) -> tuple[FieldInfo, ...]:
        return tuple(cls.__primary_keys_info_dict__.values())
    # end def


    @property
    def __primary_keys_names__(cls) -> tuple[str, ...]:
        return tuple(cls.__primary_keys_info_dict__.keys())
    # end def

    @property
    def __primary_keys_type__(cls) -> tuple[AnnotationType, ...]:
        infos = cls.__primary_keys_field_info__
        print(f"Getting primary key type for {cls.__name__}: {infos=!r}")
        return tuple(field.annotation for field in infos)
    # end def

    @property
    def __primary_keys_name__(cls) -> tuple[str]:
        infos = cls.__primary_keys_names__
        print(f"Getting primary key type for {cls.__name__}: {infos=!r}")
        return tuple(infos)
    # end def
# end class


class BaseModelWithPK(BaseModel, Generic[PrimaryKeyDataType], FastOrmModelTypehints, metaclass=FastOrmMeta):
    """Base model class with a primary key."""

    @Property
    def pk(self) -> tuple[PrimaryKeyDataType, ...]:
        """Returns the primary key of the model."""
        return tuple(
            getattr(self, field_name)
            for field_name in
            self.__class__.__primary_keys_names__
        )
    # end def

    @pk.annotater
    def pk(self) -> PrimaryKeyDataTypeArgType:
        if not isinstance(self.pk, BaseModelWithPK):  # if it's called statically on the class itself, not an instance
            return self.__primary_keys_type__
        # end if
        return self.__class__.__primary_keys_type__
    # end def
# end class


# Annotated types for user-facing API
TYPE = TypeVar("TYPE")




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

class ExampleTableWithOneFK(BaseModelWithPK):
    name: str
    description: str
    foreign_key: ForeignKey[ExampleTableWithAutoincrement]


class ExampleTableWithFK(BaseModelWithPK):
    name: str
    description: str
    foreign_key_int: ForeignKey[ExampleTableWithIntPK]
    foreign_key_str: ForeignKey[ExampleTableWithStrPK]
    foreign_key_uuid: ForeignKey[ExampleTableWithUUIDPK]
    foreign_key_two: ForeignKey[ExampleTableWithTwoPKs]
    foreign_key_nullable: ForeignKey[ExampleTableWithIntPK] | None


def test_instance_creation() -> None:
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
    auto_auto.id = 1  # Set the ID manually for the autoincrement table
    fk_auto = ExampleTableWithOneFK(
        name="Example Name",
        description="This is an example description.",
        foreign_key=auto_auto,
    )
    fk_set = ExampleTableWithOneFK(
        name="Example Name",
        description="This is an example description.",
        foreign_key=1,
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
        foreign_key_uuid="550e8400-e29b-11d4-a716-446655440000",  # the one from wikipedia.
        foreign_key_two=("part1", 456),
        foreign_key_nullable=123,
    )
    pass
# end def

if __name__ == "__main__":
    test_instance_creation()
# end if
