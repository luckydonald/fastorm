import os
import unittest
from datetime import datetime
from typing import Optional, Any, Type
from pydantic import dataclasses
from pydantic.fields import Undefined, Field

from fastorm import FastORM, Autoincrement
from tests.tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring

POSTGRES_DSN_URL = os.getenv('POSTGRES_DSN_URL', 'postgres://')  # default is to try localhost
assert POSTGRES_DSN_URL is not None


try:
    from unittest import IsolatedAsyncioTestCase
    has_easy_unittest = True
    unittest_cls = IsolatedAsyncioTestCase
except ImportError:
    has_easy_unittest = False
    unittest_cls = unittest.TestCase
# end if


@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    default: Any
# end class

ExpectedResult: Type[Any]


DATETIME_NOW = datetime(4458, 12, 24, 0, 6, 9)

class SystemUnderTest(FastORM):
    """
        CREATE TABLE "cool_table_yo" (
          "t0_id" BIGSERIAL NOT NULL PRIMARY KEY,
          "t1_1" TEXT NOT NULL DEFAULT 'test',
          "t1_2" TEXT DEFAULT NULL,
          "t1_3" BIGINT NOT NULL DEFAULT 4458,
          "t1_4" TEXT NOT NULL DEFAULT 'this test will proof if "something" ain''t escaped properly. ^^''',
          "t2_1" TEXT NOT NULL DEFAULT 'test ööö',
          "t3_1" TIMESTAMP NOT NULL DEFAULT '4458-12-24T00:06:09'::timestamp
        );
        -- and now the references --
        SELECT 1;
    """
    _table_name = 'cool_table_yo'
    _primary_keys = ['t0_id']
    _automatic_fields = ['t0_id']
    _ignored_fields = []

    t0_id: int = Field(default_factory=Autoincrement)
    __result__t0_id = ExpectedResult(is_optional=False, sql_type="BIGINT", default=Undefined)

    #
    # defaults
    #

    t1_1: str = "test"
    __result__t1_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default="test")

    t1_2: Optional[str] = None
    __result__t1_2 = ExpectedResult(is_optional=True, sql_type="TEXT", default=None)

    t1_3: int = 4458
    __result__t1_3 = ExpectedResult(is_optional=False, sql_type="BIGINT", default=4458)

    t1_4: str = "this test will proof if \"something\" ain't escaped properly. ^^'"
    __result__t1_4 = ExpectedResult(is_optional=False, sql_type="TEXT", default="this test will proof if \"something\" ain't escaped properly. ^^'")

    #
    # Non-Ascii str
    #


    t2_1: str = 'test ööö'
    __result__t2_1 = ExpectedResult(is_optional=False, sql_type="TEXT", default='test ööö')

    #
    # other types
    #

    t3_1: datetime = DATETIME_NOW
    __result__t3_1 = ExpectedResult(is_optional=True, sql_type="DATETIME", default=DATETIME_NOW)
# end class


class CreateTableOnlineTestCase(unittest_cls):
    def test_sql_text_connection_missing(self):
        with self.assertRaises(ValueError) as e:
            SystemUnderTest.build_sql_create()
        # end with
        self.assertEqual(str(e.exception), 'For using complex default values (everything other than None, bool, int, and pure ascii strings) a psycopg2 connection or cursor needs to be provided as the psycopg2_conn parameter.')
    # end def

    def test_sql_text_connection_typeerror(self):
        with self.assertRaises(TypeError) as e:
            SystemUnderTest.build_sql_create(psycopg2_conn=127834)
        # end with
        self.assertEqual(str(e.exception), 'For using complex default values (everything other than None, bool, int, and pure ascii strings) a psycopg2 connection or cursor needs to be provided as the psycopg2_conn parameter.')
    # end def

    def test_sql_text_connection_valid_psycop2(self):
        expected_sql = extract_create_and_reference_sql_from_docstring(SystemUnderTest).create
        import psycopg2
        connection = psycopg2.connect(POSTGRES_DSN_URL)
        actual_sql, *actual_params = SystemUnderTest.build_sql_create(psycopg2_conn=connection)
        self.assertEqual(expected_sql, actual_sql)
        self.assertListEqual([], actual_params)
    # end def

    async def test_sql_text_connection_valid_asyncpg(self):
        expected_sql = extract_create_and_reference_sql_from_docstring(SystemUnderTest).create

        import asyncpg
        connection = await asyncpg.connect(POSTGRES_DSN_URL)
        actual_sql, *actual_params = SystemUnderTest.build_sql_create(psycopg2_conn=connection)
        self.assertEqual(expected_sql, actual_sql)
        self.assertListEqual([], actual_params)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
