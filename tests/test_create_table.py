import unittest
from datetime import datetime
from typing import get_type_hints
from typing import Optional, Union, Any, Type
from fastorm import FastORM
from pydantic import dataclasses, BaseModel


@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    default: Any
# end class

ExpectedResult: Type[Any]


class OtherTable(FastORM):
    _table_name = 'other_table'
    _primary_keys = ['id_part_1', 'id_part_2']

    id_part_1: int
    id_part_2: str
# end class


class SystemUnderTest(FastORM):
    """
        CREATE TABLE "cool_table_yo" (
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
          "t2_7" TEXT,
          # "t2_8" TEXT,
          # "t2_9" TEXT,
          "t3_1" TEXT NOT NULL,
          "t3_2" BIGINT NOT NULL,
          "t3_3" DOUBLE PRECISION NOT NULL,
          "t3_4" BOOLEAN NOT NULL,
          "t5_1" TIMESTAMP NOT NULL,
          "t6_1" TEXT NOT NULL DEFAULT '%1'
        )
        """
    _table_name = 'cool_table_yo'
    _automatic_fields = []
    _ignored_fields = []

    #
    # Required str
    #

    t1_1: str
    __result__t1_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)

    t1_2: Union[str]
    __result__t1_2 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)

    t1_3: Union[Union[str]]
    __result__t1_3 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)

    t1_4: Union[str, str]
    __result__t1_4 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)


    #
    # Optional str
    #

    t2_1: Union[str, None]
    __result__t2_1 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    # t2_2: str | None
    __result__t2_2 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    # t2_3: None | str
    __result__t2_3 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    t2_4: Optional[str]
    __result__t2_4 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    t2_5: Union[Union[str, None], None]
    __result__t2_5 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    t2_6: Union[Optional[str], None]
    __result__t2_6 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    t2_7: Union[str | None, None]
    __result__t2_7 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    # t2_8: Union[str | None] | None
    __result__t2_8 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    # t2_9: Union[str | None] | Optional[None] | None
    __result__t2_9 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    #
    # base types
    #

    t3_1: str
    __result__t3_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)

    t3_2: int
    __result__t3_2 = ExpectedResult(is_optional=False, sql_type="BIGINT", default=None)

    t3_3: float
    __result__t3_3 = ExpectedResult(is_optional=False, sql_type="DOUBLE PRECISION", default=None)

    t3_4: bool
    __result__t3_4 = ExpectedResult(is_optional=False, sql_type="BOOLEAN", default=None)

    #
    # more uncommon types
    #

    t5_1: datetime
    __result__t5_1 = ExpectedResult(is_optional=False, sql_type="TIMESTAMP", default=None)

    #
    # defaults
    #

    t6_1: str = "test"
    __result__t6_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default="test")

    t6_2: str = None
    __result__t6_2 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)

    t6_3: Optional[str] = None
    __result__t6_3 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    #
    # references
    #

    # t7_1: OtherTable
    __result__t7_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None)

    #
    # special cases for lists
    #

    t8_1: list
    __result__t8_1 = ExpectedResult(is_optional=False, sql_type="JSONB", default=None)

    t8_2: list[int]
    __result__t8_2 = ExpectedResult(is_optional=False, sql_type="BIGINT[]", default=None)

    t8_3: list[list[int]]
    __result__t8_3 = ExpectedResult(is_optional=False, sql_type="BIGINT[][]", default=None)

    t8_4: list[list[Union[str, int]]]
    __result__t8_4 = ExpectedResult(is_optional=False, sql_type="JSONB", default=None)

    t8_3: tuple[int, int, int]
    __result__t8_3 = ExpectedResult(is_optional=False, sql_type="BIGINT[]", default=None)

    #
    # special cases for tuples
    #

    t9_1: tuple[int, int, int]
    __result__t9_1 = ExpectedResult(is_optional=False, sql_type="BIGINT[]", default=None)

    t9_2: tuple[int, str, float]
    __result__t9_2 = ExpectedResult(is_optional=False, sql_type="JSONB", default=None)

# end class


class WrongStuff(BaseModel):
    #
    # wrong stuff
    #
    t4_1: Union[int, str]

    t4_2: Optional[Union[int, str]]
# end def


class CreateTableTestCase(unittest.TestCase):
    def test_type_detection(self):
        type_hints: dict[str, any] = get_type_hints(SystemUnderTest)
        for key, type_hint in type_hints.items():
            if key.startswith('_'):
                continue
            # end if
            expected_result: ExpectedResult | None = getattr(SystemUnderTest, f'_{SystemUnderTest.__name__}__result__{key}')
            with self.subTest(msg=key):
                print(key, ",", type_hint, ",", expected_result)
                if expected_result is not None:
                    is_optional, sql_type = FastORM.match_type(type_hint=type_hint)
                    self.assertEqual(expected_result.sql_type, sql_type)
                    self.assertEqual(expected_result.is_optional, is_optional)
                else:
                    with self.assertRaises(TypeError):
                        is_optional, sql_type = FastORM.match_type(type_hint=type_hint)
                        print("failed assertion!", key, is_optional, sql_type)
                    # end with
            # end which
        # end for
    # end def

    def test_sql_text(self):
        expected_sql = "\n".join(line.removeprefix('    ') for line in SystemUnderTest.__doc__.splitlines() if not line.strip().startswith('#'))
        actual_sql, *params  = SystemUnderTest.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_TYPES_mapping_subclass_shadowing(self):
        """
        if you have `class A(object): pass` and `class B(A): pass`
        if you would list `A` first, it will never reach `B` in processing as `A` already matches all `B` objects..
        :return:
        """

        LISTS_TO_CHECK = {
            "COLUMN_TYPES": FastORM._COLUMN_TYPES,
            "COLUMS_AUTO_TYPES": FastORM._COLUMS_AUTO_TYPES
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
# end class


if __name__ == '__main__':
    unittest.main()
# end if
