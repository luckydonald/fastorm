from uuid import UUID
import unittest

from fastorm import FastORM, AutoIncrement, PK, AutoPK, ForeignKey
from fastorm.modelling.creation import fastorm_to_sqlalchemy_model
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class ExampleTableWithAutoincrement(FastORM):
    id: AutoIncrement
    name: str
    description: str

class ExampleTableWithIntPK(FastORM):
    id: PK[int]
    name: str
    description: str

class ExampleTableWithStrPK(FastORM):
    id: PK[str]
    name: str
    description: str

class ExampleTableWithUUIDPK(FastORM):
    id: AutoPK[UUID]
    name: str
    description: str

class ExampleTableWithTwoPKs(FastORM):
    id1: PK[str]
    id2: PK[int]
    name: str
    description: str

class ExampleTableWithImplicitPK(FastORM):
    # Implicit primary key, so `id: AutoIncrementPK`
    name: str
    description: str

class ExampleTableWithOneFK(FastORM):
    name: str
    description: str
    foreign_key: ForeignKey[ExampleTableWithAutoincrement]


class ExampleTableWithFK(FastORM):
    name: str
    description: str
    foreign_key_int: ForeignKey[ExampleTableWithIntPK]
    foreign_key_str: ForeignKey[ExampleTableWithStrPK]
    foreign_key_uuid: ForeignKey[ExampleTableWithUUIDPK]
    foreign_key_two: ForeignKey[ExampleTableWithTwoPKs]
    foreign_key_nullable: ForeignKey[ExampleTableWithIntPK] | None


class TestInstanceCreation(unittest.TestCase):
    def test_autoincrement_auto(self):
        auto_auto = ExampleTableWithAutoincrement(
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(auto_auto.name, "Example Name")
        self.assertEqual(auto_auto.description, "This is an example description.")
    # end def

    def test_autoincrement_set(self):
        auto_set = ExampleTableWithAutoincrement(
            id=2,
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(auto_set.id, 2)
    # end def

    def test_int_pk_set(self):
        int_set = ExampleTableWithIntPK(
            id=123,
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(int_set.id, 123)
    # end def

    def test_str_pk_set(self):
        str_set = ExampleTableWithStrPK(
            id="example_id",
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(str_set.id, "example_id")
    # end def

    def test_uuid_pk_auto(self):
        uuid_auto = ExampleTableWithUUIDPK(
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(uuid_auto.name, "Example Name")
    # end def

    def test_uuid_pk_set(self):
        uuid_val = UUID("12345678-1234-5678-1234-567812345678")
        uuid_set = ExampleTableWithUUIDPK(
            id=uuid_val,
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(uuid_set.id, uuid_val)
    # end def

    def test_two_pk_set(self):
        two_pk_set = ExampleTableWithTwoPKs(
            id1="part1",
            id2=456,
            name="Example Name",
            description="This is an example description."
        )
        self.assertEqual(two_pk_set.id1, "part1")
        self.assertEqual(two_pk_set.id2, 456)
    # end def

    def test_implicit_pk_set(self):
        implicit_pk_set = ExampleTableWithImplicitPK(
            name="Implicit PK Name",
            description="This is an implicit primary key example."
        )
        self.assertEqual(implicit_pk_set.name, "Implicit PK Name")
    # end def

    def test_fk_auto(self):
        auto_auto = ExampleTableWithAutoincrement(
            name="Example Name",
            description="This is an example description."
        )
        auto_auto.id = 1
        fk_auto = ExampleTableWithOneFK(
            name="Example Name",
            description="This is an example description.",
            foreign_key=auto_auto,
        )
        self.assertEqual(fk_auto.foreign_key, auto_auto)
    # end def

    def test_fk_set(self):
        fk_set = ExampleTableWithOneFK(
            name="Example Name",
            description="This is an example description.",
            foreign_key=1,
        )
        self.assertEqual(fk_set.foreign_key, 1)
    # end def

    def test_fk_set_1(self):
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
        two_pk_set = ExampleTableWithTwoPKs(
            id1="part1",
            id2=456,
            name="Example Name",
            description="This is an example description."
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
        self.assertIsNone(fk_set_1.foreign_key_nullable)
    # end def

    def test_fk_set_2(self):
        str_set = ExampleTableWithStrPK(
            id="example_id",
            name="Example Name",
            description="This is an example description."
        )
        fk_set_2 = ExampleTableWithFK(
            name="Example Name",
            description="This is an example description.",
            foreign_key_int=123,
            foreign_key_str=str_set,
            foreign_key_uuid="550e8400-e29b-11d4-a716-446655440000",
            foreign_key_two=("part1", 456),
            foreign_key_nullable=123,
        )
        self.assertEqual(fk_set_2.foreign_key_int, 123)
        self.assertEqual(fk_set_2.foreign_key_nullable, 123)
    # end def
# end class


class TestSqlalchemyCreation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Setup in-memory SQLite DB and session
        cls.engine = create_engine("sqlite:///:memory:")
        cls.Session = sessionmaker(bind=cls.engine)
    # end def

    def setUp(self):
        self.session = self.Session()
    # end def

    def tearDown(self):
        self.session.close()
    # end def

    # Example: override one test to check SQLAlchemy model creation
    def test_sqlalchemy_model_creation(self):
        # Create all tables for all test models
        sqlalchemy_models = {}
        # Pick one model to test
        for model in [
            ExampleTableWithAutoincrement,
            ExampleTableWithIntPK,
            ExampleTableWithStrPK,
            ExampleTableWithUUIDPK,
            ExampleTableWithTwoPKs,
            ExampleTableWithImplicitPK,
            ExampleTableWithOneFK,
            ExampleTableWithFK,
        ]:
            with self.subTest(model.__name__):
                sqla_model = fastorm_to_sqlalchemy_model(model)
                sqla_model.metadata.create_all(self.engine)
                # Create an instance and add to session
                obj = sqla_model(name="SQLA Name", description="SQLAlchemy test row")
                self.session.add(obj)
                try:
                    self.session.commit()
                except Exception as e:
                    self.session.rollback()
                    raise e
                # Query back
                result = self.session.query(sqla_model).filter_by(name="SQLA Name").first()
                self.assertIsNotNone(result)
                self.assertEqual(result.name, "SQLA Name")
                self.assertEqual(result.description, "SQLAlchemy test row")
            # end with
        # end for
    # end def

    # You can add more tests for other models as needed, or call super().test_* if you want to reuse logic.
# end class

if __name__ == '__main__':
    unittest.main()
# end if