import dataclasses
import unittest
from typing import get_type_hints
from typing import Optional, Union, Any, Type
from cheap_orm import CheapORM

@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    sql: str
# end class
ExpectedResult: Type[Any]

class SystemUnderTest(object):
    t1_1: str = ExpectedResult(is_optional=False, sql_type="TEXT", sql="TEXT NOT NULL")
    t1_2: Union[str] = ExpectedResult(is_optional=False, sql_type="TEXT", sql="TEXT NOT NULL")
    t1_3: Union[Union[str]] = ExpectedResult(is_optional=False, sql_type="TEXT", sql="TEXT NOT NULL")

    t2_1: Union[str, None] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_2: str | None = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_3: None | str = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_4: Optional[str] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_5: Union[Union[str, None], None] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_6: Union[Optional[str], None] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_7: Union[str | None, None] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_8: Union[str | None] | None = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_9: Union[str | None] | Optional[None] | None = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")

    t3_1: str = ExpectedResult(is_optional=False, sql_type="TEXT", sql="TEXT NOT NULL")
    t3_2: int = ExpectedResult(is_optional=False, sql_type="BIGINT", sql="BIGINT NOT NULL")
    t3_3: float = ExpectedResult(is_optional=False, sql_type="DOUBLE PRECISION", sql="DOUBLE PRECISION NOT NULL")
# end class


class MyTestCase(unittest.TestCase):
    def test_something(self):
        type_hints: dict[str, any] = get_type_hints(SystemUnderTest)
        for key, type_hint in type_hints.items():
            expected_result: ExpectedResult = getattr(SystemUnderTest, key)
            with self.subTest(msg=key):
                print(key, ",", type_hint, ",", expected_result)
                is_optional, sql_type = CheapORM.match_type(type_hint=type_hint)
                self.assertEqual(sql_type, expected_result.sql_type)
                self.assertEqual(is_optional, expected_result.is_optional)
            # end which
        # end for
        self.assertEqual(True, False)  # add assertion here
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
