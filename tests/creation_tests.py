from operator import index
from unittest import TestCase
from uuid import UUID
import unittest

from sqlalchemy.util.compat import inspect_getfullargspec, FullArgSpec

from fastorm import FastORM, AutoIncrement, PK, AutoPK, ForeignKey, Undefined
from fastorm.modelling.creation import fastorm_to_sqlalchemy_model, _fastorm_to_sqlalchemy_model_metadata
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


def constructor_based_equality_check(self: TestCase, a, b):
    """Check if two objects are equal based on their constructor arguments."""
    type_a = type(a)
    type_b = type(b)
    if type_a is not type_b:
        self.fail(f"Objects are of different types: {type_a} and {type_b}")
    # end if

    try:
        spec_a: FullArgSpec = inspect_getfullargspec(a.__init__)
    except TypeError as e:
        spec_a = e
    # end try
    try:
        spec_b: FullArgSpec = inspect_getfullargspec(b.__init__)
    except TypeError as e:
        spec_b = e
    # end try

    if isinstance(spec_a, TypeError) ^ isinstance(spec_b, TypeError):  # only one is TypeError
        self.fail(
            f"One of the objects has a TypeError when loading the constructor spec, the other not:\n"
            f"{type_a} ({a=!r}, {spec_a=!r}) vs. {type_b} ({b=!r}, {spec_b=!r})"
        )
    # end if
    if isinstance(spec_a, TypeError) and isinstance(spec_b, TypeError):
        # type error usually is because the __init__ method is not defined or not callable - i.e. no extra arguments needed.
        # E.g. `TypeError: <method-wrapper '__init__' of BigInteger object at 0x107a010d0> is not a Python function`
        self.assertEqual(str(a), str(b), msg=f"TypeError for both objects, but they are not even str() equal. {str(a)=!r}, {str(b)=!r}")
        self.assertEqual(repr(a), repr(b), msg=f"TypeError for both objects, but they are not even repr() equal. {repr(a)=!s}, {repr(b)=!s}")
        return  # we survived the TypeError, good enough, so we can return early.
    # end if

    self.assertEqual(spec_a, spec_b, msg=f"Constructor arguments do not match for {type_a} ({a=!r}, {b=!r})")
    fields = spec_a.args + spec_a.kwonlyargs
    for field in fields:
        value_a = getattr(a, field, Undefined)
        value_b = getattr(b, field, Undefined)
        if value_a != value_b:
            self.fail(f"Field '{field}' of type {type_a} does not match: {value_a!r} != {value_b!r} ({a=!r}, {b=!r})")
        # end if
    # end for
# end def


class TestSqlalchemyCreation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Setup in-memory SQLite DB and session
        cls.engine = create_engine("sqlite:///tests.sqlite", echo="debug")
        cls.Session = sessionmaker(bind=cls.engine)
    # end def

    def setUp(self):
        self.session = self.Session()
    # end def

    def tearDown(self):
        self.session.close()
    # end def

    constructor_based_equality_check = constructor_based_equality_check

    # Example: override one test to check SQLAlchemy model creation
    def test_sqlalchemy_meta_generation(self):
        # Create all tables for all test models
        sqlalchemy_models = {}
        # Pick one model to test
        from sqlalchemy import Column
        from sqlalchemy import Text, BigInteger, UUID

        # The `Column` type has the attributes:
        # type_, key, primary_key, nullable, index, unique, system, doc, autoincrement, constraints, foreign_keys,
        # comment, computed, identity, default, onupdate, server_default, insert_default, server_onupdate, info,
        # quote, insert_sentinel
        # we wanna check the following ones:
        INTERESTING_COLUMN_ATTRIBUTES = (
            "type",
            "primary_key",
            "nullable",
            "autoincrement",
            "unique",
            "index",
            "foreign_keys",
        )
        for model, expected_column_meta in {
            ExampleTableWithAutoincrement: {
                '__tablename__': 'exampletablewithautoincrement',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                 'id': dict(
                     type=BigInteger(),
                     primary_key=True,
                     nullable=False,
                     autoincrement=True,
                     unique=None,
                     index=None,
                     foreign_keys=set(),
                 ),
                 'name': dict(
                     type=Text(),
                     primary_key=False,
                     nullable=False,
                     autoincrement=False,
                     unique=None,
                     index=None,
                     foreign_keys=set(),
                 ),
            },
            ExampleTableWithIntPK: {
                '__tablename__': 'exampletablewithintpk',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'id': dict(
                    type=BigInteger(),
                    primary_key=True,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'name': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
            },
            ExampleTableWithStrPK: {
                '__tablename__': 'exampletablewithstrpk',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'id': dict(
                    type=Text(),
                    primary_key=True,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'name': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
            },
            ExampleTableWithUUIDPK: {
                '__tablename__': 'exampletablewithuuidpk',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'id': dict(
                    type=Text(),  # UUID is stored as a string in SQLite
                    primary_key=True,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'name': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
            },
            ExampleTableWithTwoPKs: {
                '__tablename__': 'exampletablewithtwopks',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'id1': dict(
                    type=Text(),
                    primary_key=True,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'id2': dict(
                    type=BigInteger(),
                    primary_key=True,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'name': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
            },
            ExampleTableWithImplicitPK: {
                '__tablename__': 'exampletablewithimplicitpk',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'name': dict(
                    type=Text(),
                    primary_key=True,  # Implicit PK
                    nullable=False,
                    autoincrement=True,  # AutoIncrementPK
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
            },
            ExampleTableWithOneFK: {
                '__tablename__': 'exampletablewithonefk',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'foreign_key': dict(
                    type=BigInteger(),  # ForeignKey to ExampleTableWithAutoincrement
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys={'yes'}, # ExampleTableWithAutoincrement.__tablename__
                ),
                'name': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
            },
            ExampleTableWithFK: {
                '__tablename__': 'exampletablewithfk',
                'description': dict(
                    type=Text(),
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys=set(),
                ),
                'foreign_key_int': dict(
                    type=BigInteger(),  # ForeignKey to ExampleTableWithIntPK
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys={'yes'}, # ExampleTableWithIntPK.__tablename__
                ),
                'foreign_key_str': dict(
                    type=Text(),  # ForeignKey to ExampleTableWithStrPK
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys={'yes'},  # ExampleTableWithStrPK.__tablename__
                ),
                'foreign_key_uuid': dict(
                    type=UUID(),  # ForeignKey to ExampleTableWithUUIDPK
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys={'yes'},  # ExampleTableWithUUIDPK.__tablename__
                ),
                'foreign_key_two': dict(
                    type=Text(),  # ForeignKey to ExampleTableWithTwoPKs
                    primary_key=False,
                    nullable=False,
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys={'yes'},  # ExampleTableWithTwoPKs.__tablename__
                ),
                'foreign_key_nullable': dict(
                    type=BigInteger(),  # ForeignKey to ExampleTableWithIntPK, can be None
                    primary_key=False,
                    nullable=True,  # Nullable ForeignKey
                    autoincrement=False,
                    unique=None,
                    index=None,
                    foreign_keys={'yes'},  # ExampleTableWithIntPK.__tablename__
                ),
                'name': dict(
                     type=Text(),
                     primary_key=False,
                     nullable=False,
                     autoincrement=False,
                     unique=None,
                     index=None,
                     foreign_keys=set(),
                 ),
            },
        }.items():
            with self.subTest(model.__name__):
                got_meta = _fastorm_to_sqlalchemy_model_metadata(model)
                print(got_meta)

                self.assertEqual(
                    set(expected_column_meta.keys()),
                    set(got_meta.keys()),
                    msg=f"The returned metadata dictionary for {model.__name__} in the test does not contain all expected keys.",
                )

                for column, expected_column_definition in expected_column_meta.items():
                    if column == '__tablename__':
                        self.assertEqual(expected_column_definition, got_meta[column], msg=f"Expected table name for {model.__name__} does not match.")
                        continue
                    # end if
                    with self.subTest(f"{model.__name__} > {column}"):
                        self.assertEqual(
                            set(INTERESTING_COLUMN_ATTRIBUTES),
                            set(expected_column_definition.keys()),
                            msg=f"The expected metadata defined for {model.__name__}.{column} in the test does not contain all required attributes.",
                        )

                        got_column_definition = got_meta[column]
                        for attr, expected_value in expected_column_definition.items():
                            with self.subTest(f"{model.__name__} > {column} > {attr}"):
                                got_value = getattr(got_column_definition, attr)
                                if attr == "type":
                                    # Those things don't implement __eq__, so we need to compare their string representations.
                                    self.constructor_based_equality_check(expected_value, got_value)
                                    continue
                                # end if
                                self.assertEqual(
                                    expected_value,
                                    got_value,
                                    msg=f"Column definition mismatch for {model.__name__}.{column}.{attr}: expected {expected_value}, got {got_value}",
                                )
                            # end with
                        # end for
                    # end with
                # end for
            # end with
        # end for
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
                print(sqla_model.__qualname__)
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