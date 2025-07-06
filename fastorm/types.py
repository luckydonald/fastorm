from typing import NewType, TypeVar, Union, Generic
from uuid import UUID

from pydantic import BaseModel

PrimaryKeyDataType = TypeVar("PrimaryKeyDataType")
OtherTableDataType = TypeVar("OtherTableDataType", bound=BaseModel)
AutoSupporting = int | UUID
AutoSupportingType = TypeVar("AutoSupportingType", bound=AutoSupporting)


class PK(Generic[PrimaryKeyDataType]):
    """Type marker for a Primary Key of type PrimaryKeyDataType."""
    pass

class ForeignKey(Generic[OtherTableDataType]):
    """Type marker for a Foreign Key referencing type OtherTableDataType."""
    pass

class AutoPK(PK[AutoSupportingType]):
    """Type marker for a Primary Key that can be auto-incrementing or automatic UUID."""
    pass

class AutoIncrementPK(AutoPK[int]):
    """Type marker for an auto-incrementing integer Primary Key."""
    pass

class AutoUUID(AutoPK[UUID]):
    """Type marker for an auto-incrementing integer Primary Key."""
    pass

class ExampleTableWithAutoincrement(BaseModel):
    id: AutoIncrementPK
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