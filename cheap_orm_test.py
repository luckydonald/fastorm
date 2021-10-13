import dataclasses
import unittest
from typing import Optional, Union, get_type_hints, Any, Type
from cheap_orm import CheapORM

@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    sql: str
# end class
ExpectedResult: Type[Any]

class SystemUnderTest(object):
    t1: str = ExpectedResult(is_optional=False, sql_type="TEXT", sql="TEXT NOT NULL")
    t2_1: Union[str, None] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_2: str | None = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
    t2_3: Optional[str] = ExpectedResult(is_optional=True, sql_type="TEXT", sql="TEXT")
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
