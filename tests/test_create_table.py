import unittest
from datetime import datetime
from typing import Optional, Union, Any, Type, List, Tuple, Dict, ForwardRef
from pydantic import dataclasses, BaseModel
from pydantic.fields import ModelField, Undefined, Field

from fastorm import FastORM, Autoincrement, FieldInfo
from fastorm.compat import get_type_hints_with_annotations
from tests.tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring, VerboseTestCase


@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    default: Any
# end class


ExpectedResult: Type[Any]


SystemUnderTest = ForwardRef("SystemUnderTest")


class TableWithForwardRef(FastORM):
    """
        CREATE TABLE "table_with_forwardref" (
          "id" BIGSERIAL NOT NULL PRIMARY KEY,
          "future_table_1__t0_id" BIGINT NOT NULL,
          "future_table_2__t0_id" BIGINT,
          "future_table_3__t0_id" BIGINT NOT NULL,
          "future_table_4__t0_id" BIGINT,
          "future_table_5__t0_id" BIGINT
        );
        -- and now the references --
        CREATE INDEX "idx_table_with_forwardref___future_table_1__t0_id" ON "table_with_forwardref" ("future_table_1__t0_id");
        CREATE INDEX "idx_table_with_forwardref___future_table_2__t0_id" ON "table_with_forwardref" ("future_table_2__t0_id");
        CREATE INDEX "idx_table_with_forwardref___future_table_3__t0_id" ON "table_with_forwardref" ("future_table_3__t0_id");
        CREATE INDEX "idx_table_with_forwardref___future_table_4__t0_id" ON "table_with_forwardref" ("future_table_4__t0_id");
        CREATE INDEX "idx_table_with_forwardref___future_table_5__t0_id" ON "table_with_forwardref" ("future_table_5__t0_id");
        ALTER TABLE "table_with_forwardref" ADD CONSTRAINT "fk_table_with_forwardref___future_table_1__t0_id" FOREIGN KEY ("future_table_1__t0_id") REFERENCES "cool_table_yo" ("t0_id") ON DELETE CASCADE;
        ALTER TABLE "table_with_forwardref" ADD CONSTRAINT "fk_table_with_forwardref___future_table_2__t0_id" FOREIGN KEY ("future_table_2__t0_id") REFERENCES "cool_table_yo" ("t0_id") ON DELETE CASCADE;
        ALTER TABLE "table_with_forwardref" ADD CONSTRAINT "fk_table_with_forwardref___future_table_3__t0_id" FOREIGN KEY ("future_table_3__t0_id") REFERENCES "cool_table_yo" ("t0_id") ON DELETE CASCADE;
        ALTER TABLE "table_with_forwardref" ADD CONSTRAINT "fk_table_with_forwardref___future_table_4__t0_id" FOREIGN KEY ("future_table_4__t0_id") REFERENCES "cool_table_yo" ("t0_id") ON DELETE CASCADE;
        ALTER TABLE "table_with_forwardref" ADD CONSTRAINT "fk_table_with_forwardref___future_table_5__t0_id" FOREIGN KEY ("future_table_5__t0_id") REFERENCES "cool_table_yo" ("t0_id") ON DELETE CASCADE;
    """
    _table_name = 'table_with_forwardref'
    _primary_keys = ['id']
    _automatic_fields = ['id']
    _ignored_fields = []

    id: Optional[int]
    future_table_1: SystemUnderTest
    future_table_2: Optional[SystemUnderTest]
    future_table_3: Union[SystemUnderTest, int]
    future_table_4: Optional[Union[SystemUnderTest, int]]
    future_table_5: Union[SystemUnderTest, int, None]
# end class


class OtherTable(FastORM):
    """
        CREATE TABLE "other_table" (
          "id_part_1" BIGINT NOT NULL,
          "id_part_2" TEXT NOT NULL,
          PRIMARY KEY ("id_part_1", "id_part_2")
        );
        -- and now the references --
        SELECT 1;
    """
    _table_name = 'other_table'
    _primary_keys = ['id_part_1', 'id_part_2']
    _ignored_fields = []

    id_part_1: int
    id_part_2: str
# end class


class TheReferenceHasBeenDoubled(FastORM):
    """
        CREATE TABLE "double_reference" (
          "another_reference__id_part_1" BIGINT NOT NULL,
          "another_reference__id_part_2" TEXT NOT NULL,
          PRIMARY KEY ("another_reference__id_part_1", "another_reference__id_part_2")
        );
        -- and now the references --
        CREATE INDEX "idx_double_reference___another_reference__id_part_1" ON "double_reference" ("another_reference__id_part_1");
        CREATE INDEX "idx_double_reference___another_reference__id_part_2" ON "double_reference" ("another_reference__id_part_2");
        ALTER TABLE "double_reference" ADD CONSTRAINT "fk_double_reference___another_reference" FOREIGN KEY ("another_reference__id_part_1", "another_reference__id_part_2") REFERENCES "other_table" ("id_part_1", "id_part_2") ON DELETE CASCADE;
    """
    _table_name = 'double_reference'
    _primary_keys = ['another_reference']
    _ignored_fields = []

    another_reference: OtherTable
# end class


# noinspection PyRedeclaration
class SystemUnderTest(FastORM):
    """
        CREATE TABLE "cool_table_yo" (
          "t0_id" BIGSERIAL NOT NULL PRIMARY KEY,
          "t1_1" TEXT NOT NULL,
          "t1_2" TEXT NOT NULL,
          "t1_3" TEXT NOT NULL,
          "t1_4" TEXT NOT NULL,
          "t2_1" TEXT,
          # "t2_2" TEXT,
          # "t2_3" TEXT,
          "t2_4" TEXT,
          "t2_5" TEXT,
          "t2_6" TEXT,
          # "t2_7" TEXT,
          # "t2_8" TEXT,
          # "t2_9" TEXT,
          "t3_1" TEXT NOT NULL,
          "t3_2" BIGINT NOT NULL,
          "t3_3" DOUBLE PRECISION NOT NULL,
          "t3_4" BOOLEAN NOT NULL,
          "t5_1" TIMESTAMP NOT NULL,
          "t6_1" TEXT NOT NULL DEFAULT 'test',
          "t6_2" TEXT DEFAULT NULL,
          "t6_3" BIGINT NOT NULL DEFAULT 69,
          "t6_4" TEXT NOT NULL DEFAULT 'this test will proof if "something" ain''t escaped properly. ^^''',
          "t7_1__id_part_1" BIGINT NOT NULL,
          "t7_1__id_part_2" TEXT NOT NULL,
          "t8_1" JSONB NOT NULL,
          "t8_2" BIGINT[] NOT NULL,
          "t8_3" BIGINT[][] NOT NULL,
          "t8_4" JSONB NOT NULL,
          "t9_1" BIGINT[] NOT NULL,
          "t9_2" JSONB NOT NULL
        );
        -- and now the references --
        CREATE INDEX "idx_cool_table_yo___t7_1__id_part_1" ON "cool_table_yo" ("t7_1__id_part_1");
        CREATE INDEX "idx_cool_table_yo___t7_1__id_part_2" ON "cool_table_yo" ("t7_1__id_part_2");
        ALTER TABLE "cool_table_yo" ADD CONSTRAINT "fk_cool_table_yo___t7_1" FOREIGN KEY ("t7_1__id_part_1", "t7_1__id_part_2") REFERENCES "other_table" ("id_part_1", "id_part_2") ON DELETE CASCADE;
        """
    _table_name = 'cool_table_yo'
    _primary_keys = ['t0_id']
    _automatic_fields = ['t0_id']
    _ignored_fields = []

    #
    # Required str
    #

    t0_id: int = Field(default_factory=Autoincrement)
    __result__t0_id = ExpectedResult(is_optional=True, sql_type="BIGINT", default=Undefined)

    t1_1: str
    __result__t1_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=Undefined)

    t1_2: Union[str]
    __result__t1_2 = ExpectedResult(is_optional=False, sql_type="TEXT", default=Undefined)

    t1_3: Union[Union[str]]
    __result__t1_3 = ExpectedResult(is_optional=False, sql_type="TEXT", default=Undefined)

    t1_4: Union[str, str]
    __result__t1_4 = ExpectedResult(is_optional=False, sql_type="TEXT", default=Undefined)


    #
    # Optional str
    #

    t2_1: Union[str, None]
    __result__t2_1 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    # t2_2: str | None
    __result__t2_2 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    # t2_3: None | str
    __result__t2_3 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    t2_4: Optional[str]
    __result__t2_4 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    t2_5: Union[Union[str, None], None]
    __result__t2_5 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    t2_6: Union[Optional[str], None]
    __result__t2_6 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    # t2_7: Union[str | None, None]
    __result__t2_7 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    # t2_8: Union[str | None] | None
    __result__t2_8 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    # t2_9: Union[str | None] | Optional[None] | None
    __result__t2_9 = ExpectedResult(is_optional=True, sql_type="TEXT", default=Undefined)

    #
    # base types
    #

    t3_1: str
    __result__t3_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=Undefined)

    t3_2: int
    __result__t3_2 = ExpectedResult(is_optional=False, sql_type="BIGINT", default=Undefined)

    t3_3: float
    __result__t3_3 = ExpectedResult(is_optional=False, sql_type="DOUBLE PRECISION", default=Undefined)

    t3_4: bool
    __result__t3_4 = ExpectedResult(is_optional=False, sql_type="BOOLEAN", default=Undefined)

    #
    # more uncommon types
    #

    t5_1: datetime
    __result__t5_1 = ExpectedResult(is_optional=False, sql_type="TIMESTAMP", default=Undefined)

    #
    # defaults
    #

    t6_1: str = "test"
    __result__t6_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default="test")

    t6_2: Optional[str] = None
    __result__t6_2 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    t6_3: int = 69
    __result__t6_3 = ExpectedResult(is_optional=False, sql_type="BIGINT", default=69)

    t6_4: str = "this test will proof if \"something\" ain't escaped properly. ^^'"
    __result__t6_4 = ExpectedResult(is_optional=False, sql_type="TEXT", default="this test will proof if \"something\" ain't escaped properly. ^^'")

    #
    # references
    #

    t7_1: OtherTable
    # For the single field FastORM.match_type(…) this results as JSONB (because BaseModel)
    # For the table view FastORM.build_sql_create() this results as two keys with the respective types.
    __result__t7_1 = ExpectedResult(is_optional=False, sql_type="JSONB", default=Undefined)

    #
    # special cases for lists
    #

    t8_1: list
    __result__t8_1 = ExpectedResult(is_optional=False, sql_type="JSONB", default=Undefined)

    t8_2: List[int]
    __result__t8_2 = ExpectedResult(is_optional=False, sql_type="BIGINT[]", default=Undefined)

    t8_3: List[List[int]]
    __result__t8_3 = ExpectedResult(is_optional=False, sql_type="BIGINT[][]", default=Undefined)

    t8_4: List[List[Union[str, int]]]
    __result__t8_4 = ExpectedResult(is_optional=False, sql_type="JSONB", default=Undefined)

    #
    # special cases for tuples
    #

    t9_1: Tuple[int, int, int]
    __result__t9_1 = ExpectedResult(is_optional=False, sql_type="BIGINT[]", default=Undefined)

    t9_2: Tuple[int, str, float]
    __result__t9_2 = ExpectedResult(is_optional=False, sql_type="JSONB", default=Undefined)

# end class


SystemUnderTest.update_forward_refs()
TableWithForwardRef.update_forward_refs()


class WrongStuff(BaseModel):
    #
    # wrong stuff
    #
    t4_1: Union[int, str]
    __result__t4_1 = TypeError

    t4_2: Optional[Union[int, str]]
    __result__t4_2 = TypeError

    t4_3: Union[int, str]
    __result__t4_3 = TypeError

    t4_4: Union[int, str, None]
    __result__t4_4 = TypeError

    # t4_5: str = None
    __result__t4_5 = ValueError
# end class


# noinspection DuplicatedCode
class CreateTableTestCase(VerboseTestCase):
    def test_type_detection_typing(self):
        type_hints: Dict[str, any] = get_type_hints_with_annotations(SystemUnderTest)
        for key, type_hint in type_hints.items():
            if key.startswith('_'):
                continue
            # end if
            expected_result: ExpectedResult | Type[Exception]
            expected_result = getattr(SystemUnderTest, f'_{SystemUnderTest.__name__}__result__{key}')
            with self.subTest(msg=key):
                print(key, ",", type_hint, ",", expected_result)
                if isinstance(expected_result, ExpectedResult):
                    is_optional, sql_type = FastORM.match_type(type_hint=type_hint, key=key)
                    self.assertEqual(expected_result.sql_type, sql_type, msg=f'sql_type of {key}')
                    self.assertEqual(expected_result.is_optional, is_optional, msg=f'is_optional of {key}: {expected_result}')
                else:
                    with self.assertRaises(expected_result):
                        is_optional, sql_type = FastORM.match_type(type_hint=type_hint)
                        print("failed assertion!", key, is_optional, sql_type)
                    # end with
            # end which
        # end for
    # end def

    def test_type_detection_pydantic(self):
        type_hints: Dict[str, FieldInfo[ModelField]] = SystemUnderTest.get_fields_typehints()
        for key, type_hint in type_hints.items():
            expected_result: ExpectedResult | Type[Exception]
            expected_result = getattr(SystemUnderTest, f'_{SystemUnderTest.__name__}__result__{key}')
            with self.subTest(msg=key):
                print(key, ",", type_hint, ",", expected_result)
                if isinstance(expected_result, ExpectedResult):
                    is_optional, sql_type = FastORM.match_type(type_hint=type_hint.types[0].type_, key=key)
                    self.assertEqual(expected_result.sql_type, sql_type, msg=f'{key} sql_type')
                    self.assertEqual(expected_result.is_optional, is_optional, msg=f'{key} is_optional')
                else:
                    with self.assertRaises(expected_result):
                        is_optional, sql_type = FastORM.match_type(type_hint=type_hint)
                        print("failed assertion!", key, is_optional, sql_type)
                    # end with
            # end which
        # end for
    # end def

    def test_sql_text_create(self):
        self.maxDiff = None
        for table_cls in (OtherTable, TheReferenceHasBeenDoubled, SystemUnderTest,):
            with self.subTest(msg=f'class {table_cls.__name__}'):
                expected_sql = extract_create_and_reference_sql_from_docstring(table_cls).create
                actual_sql, *actual_params = table_cls.build_sql_create()
                self.assertEqual(expected_sql, actual_sql, msg="create")
                self.assertListEqual([], actual_params, "create")
            # end with
        # end for
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        for table_cls in (OtherTable, TheReferenceHasBeenDoubled, SystemUnderTest,):
            with self.subTest(msg=f'class {table_cls.__name__}'):
                expected_sql = extract_create_and_reference_sql_from_docstring(table_cls).references
                actual_sql, *actual_params = table_cls.build_sql_references()
                self.assertEqual(expected_sql, actual_sql, msg="references")
                self.assertListEqual([], actual_params, "references")
            # end with
        # end for
    # end def

    def test_sql_text_forwardref_create(self):
        self.maxDiff = None
        for table_cls in (TableWithForwardRef,):
            with self.subTest(msg=f'class {table_cls.__name__}'):
                expected_sql = extract_create_and_reference_sql_from_docstring(table_cls).create
                actual_sql, *actual_params = table_cls.build_sql_create()
                self.assertEqual(expected_sql, actual_sql, msg="create")
                self.assertListEqual([], actual_params, "create")
            # end with
        # end for
    # end def

    def test_sql_text_forwardref_references(self):
        self.maxDiff = None
        for table_cls in (TableWithForwardRef,):
            with self.subTest(msg=f'class {table_cls.__name__}'):
                expected_sql = extract_create_and_reference_sql_from_docstring(table_cls).references
                actual_sql, *actual_params = table_cls.build_sql_references()
                self.assertEqual(expected_sql, actual_sql, msg="references")
                self.assertListEqual([], actual_params, "references")
            # end with
        # end for
    # end def

    def test_TYPES_mapping_subclass_shadowing(self):
        """
        if you have `class A(object): pass` and `class B(A): pass`
        if you would list `A` first, it will never reach `B` in processing as `A` already matches all `B` objects..
        :return:
        """

        LISTS_TO_CHECK = {
            "COLUMN_TYPES": FastORM._COLUMN_TYPES,
            "COLUMS_AUTO_TYPES": FastORM._COLUMN_AUTO_TYPES
        }
        for list_name, list_to_check in LISTS_TO_CHECK.items():
            classes = list(list_to_check.keys())
            with self.subTest(msg=f'{FastORM.__name__}.{list_name}'):
                for first_class_index, first_class in enumerate(classes):
                    # for all classes which are after first_class(_index)
                    for second_class_index, second_class in enumerate(classes[first_class_index + 1:], start=first_class_index + 1):
                        # the second may not be a subclass of the first, that would mean the fist would shadow the second.
                        self.assertFalse(
                            issubclass(second_class, first_class),
                            msg=f'The broader class {first_class!r} (at index {first_class_index}) is shadowing the later class {second_class!r} (at index {second_class_index}).')
                    # end for
                # end for
            # end with
        # end for
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
