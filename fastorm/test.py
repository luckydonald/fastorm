from uuid import UUID

from fastorm.types import BaseModelWithPK, AutoIncrement, PK, AutoPK, ForeignKey


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
