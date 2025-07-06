from typing import NewType, TypeVar, Union, Generic, TYPE_CHECKING, NotRequired, Annotated, Optional
from uuid import UUID
from pydantic import BaseModel
from pydantic.fields import FieldInfo

from fastorm.property import Property
from fastorm.tools.annotations import Marker, has_marker, AnnotationType

PrimaryKeyDataType = TypeVar("PrimaryKeyDataType")


class BaseModelWithPK(BaseModel, Generic[PrimaryKeyDataType]):
    """Base model class with a primary key."""
    @property
    def __primary_keys__(self) -> dict[str, FieldInfo]:
        """Returns the primary key of the model."""
        # iterate over the fields to find the primary key (Annotated with PKMarker)
        fields: dict[str, FieldInfo] = {}  # key: field_name, value: Annotation
        for field_name, field in self.model_fields.items():
            if not has_marker(field.annotation, PKMarker):
                continue
            # end if
            fields[field_name] = field
        # end for
        return fields
    # end def

    @Property
    def pk(self) -> PrimaryKeyDataType:
        """Returns the primary key of the model."""
        raise NotImplementedError("Subclasses must implement the pk property.")
    # end def
    @pk.annotater
    def pk(self):
        return tuple(field.annotation for field in self.__primary_keys__.values())
    # end def
# end class


type OtherTableDataType[PrimaryKeyDataType] = BaseModelWithPK[PrimaryKeyDataType]
AutoSupporting = int | UUID
AutoSupportingType = TypeVar("AutoSupportingType", bound=AutoSupporting)


# Marker classes for Annotated metadata
class PKMarker(Generic[PrimaryKeyDataType], Marker):
    """Marker for Primary Key."""
    pass

class ForeignKeyMarker[PrimaryKeyDataType](OtherTableDataType[PrimaryKeyDataType], Marker):
    """Marker for Foreign Key."""
    pass

class AutoMarker(Generic[AutoSupportingType], Marker):
    """Marker for Auto Primary Key (int or UUID)."""
    pass

# Annotated types for user-facing API
PK = Annotated[PrimaryKeyDataType, PKMarker[PrimaryKeyDataType]]
ForeignKey = Annotated[OtherTableDataType | PrimaryKeyDataType | tuple[PrimaryKeyDataType], ForeignKeyMarker[OtherTableDataType]]
AutoPK = Optional[Annotated[PK[PrimaryKeyDataType], AutoMarker[PrimaryKeyDataType]]]

AutoIncrement = AutoPK[int]
AutoUUID = AutoPK[UUID]


class ExampleTableWithAutoincrement(BaseModel):
    id: AutoIncrement
    name: str
    description: str

class ExampleTableWithIntPK(BaseModel):
    id: PK[int]
    name: str
    description: str

class ExampleTableWithStrPK(BaseModel):
    id: PK[str]
    name: str
    description: str

class ExampleTableWithUUIDPK(BaseModel):
    id: AutoPK[UUID]
    name: str
    description: str

class ExampleTableWithTwoPKs(BaseModel):
    id1: PK[str]
    id2: PK[int]
    name: str
    description: str

class ExampleTableWithImplicitPK(BaseModel):
    # Implicit primary key, so `id: AutoIncrementPK`
    name: str
    description: str

class ExampleTableWithFK(BaseModel):
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