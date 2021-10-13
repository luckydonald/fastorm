import dataclasses
import unittest
from typing import get_type_hints
from typing import Optional, Union, Any, Type
from fastorm import FastORM


@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    default: Any
    sql: str
# end class

ExpectedResult: Type[Any]


class SystemUnderTest(object):

    #
    # Required str
    #

    t1_1: str; __result__t1_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None, sql="TEXT NOT NULL")

    t1_2: Union[str]
    __result__t1_2 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None, sql="TEXT NOT NULL")

    t1_3: Union[Union[str]]
    __result__t1_3 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None, sql="TEXT NOT NULL")

    t1_4: Union[str, str]
    __result__t1_4 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None, sql="TEXT NOT NULL")


    #
    # Optional str
    #

    t2_1: Union[str, None]
    __result__t2_1 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_2: str | None
    __result__t2_2 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_3: None | str
    __result__t2_3 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_4: Optional[str]
    __result__t2_4 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_5: Union[Union[str, None], None]
    __result__t2_5 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_6: Union[Optional[str], None]
    __result__t2_6 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_7: Union[str | None, None]
    __result__t2_7 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_8: Union[str | None] | None
    __result__t2_8 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    t2_9: Union[str | None] | Optional[None] | None
    __result__t2_9 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None, sql="TEXT")

    #
    # base types
    #

    t3_1: str
    __result__t3_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default=None, sql="TEXT NOT NULL")

    t3_2: int
    __result__t3_2 = ExpectedResult(is_optional=False, sql_type="BIGINT", default=None, sql="BIGINT NOT NULL")

    t3_3: float
    __result__t3_3 = ExpectedResult(is_optional=False, sql_type="DOUBLE PRECISION", default=None, sql="DOUBLE PRECISION NOT NULL")

    t3_4: bool
    __result__t3_4 = ExpectedResult(is_optional=False, sql_type="BOOLEAN", default=None, sql="BOOLEAN NOT NULL")

    #
    # wrong stuff
    #
    t4_1: Union[int, str]
    __result__t4_1 = None

    #
    # defaults
    #
    t5_1: str = "test"
    __result__t5_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default="test", sql="TEXT NOT NULL DEFAULT '%2'")


# end class


class CreateTableTestCase(unittest.TestCase):
    def test_type_detection(self):
        type_hints: dict[str, any] = get_type_hints(SystemUnderTest)
        for key, type_hint in type_hints.items():
            expected_result: ExpectedResult = getattr(SystemUnderTest, key)
            with self.subTest(msg=key):
                print(key, ",", type_hint, ",", expected_result)
                is_optional, sql_type = FastORM.match_type(type_hint=type_hint)
                self.assertEqual(sql_type, expected_result.sql_type)
                self.assertEqual(is_optional, expected_result.is_optional)
            # end which
        # end for
        self.assertEqual(True, False)  # add assertion here
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
