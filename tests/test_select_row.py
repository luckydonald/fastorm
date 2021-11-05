import unittest
from datetime import datetime
from typing import get_type_hints
from typing import Optional, Union, Any, Type, List, Tuple, Dict
from pydantic import dataclasses, BaseModel
from pydantic.fields import ModelField, Undefined, Field

from fastorm import FastORM
from fastorm.compat import get_type_hints_with_annotations
from tests.tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring


class OtherTable(FastORM):
    """
        CREATE TABLE "other_table" (
          "id_part_1" TEXT NOT NULL,
          "id_part_2" BIGINT NOT NULL,
          PRIMARY KEY ("id_part_1", "id_part_2")
        );
    """
    _table_name = 'cool_table_name_yoooo'
    _primary_keys = ['id_part_1', 'id_part_2']
    _ignored_fields = []

    id_part_1: int
    id_part_2: str
    foo: float
    bar: str
# end class


class SelectRowTestCase(unittest.TestCase):
    def test_simple_select_single_parameter_no_pk(self):
        expected_foo = 123.2
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected_sql = """
        SELECT "id_part_1","id_part_2","foo","bar" FROM "cool_table_name_yoooo" WHERE "foo" = $1
        """.strip()
        sql, *where_values = OtherTable.build_sql_select(foo=expected_foo)
        self.assertEqual(expected_sql, sql)
        self.assertEqual([expected_foo], where_values)
    # end def

    def test_simple_select_multiple_parameters_no_pk(self):
        expected_foo = 123.2
        expected_bar = "Läääl"
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected_sql = """
        SELECT "id_part_1","id_part_2","foo","bar" FROM "cool_table_name_yoooo" WHERE "foo" = $1 AND "bar" = $2
        """.strip()
        sql, *where_values = OtherTable.build_sql_select(foo=expected_foo, bar=expected_bar)
        self.assertEqual(expected_sql, sql)
        self.assertEqual([expected_foo, expected_bar], where_values)
    # end def

    def test_simple_select_no_parameters_no_pk(self):
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected_sql = """
        SELECT "id_part_1","id_part_2","foo","bar" FROM "cool_table_name_yoooo"
        """.strip()
        sql, *where_values = OtherTable.build_sql_select()
        self.assertEqual(expected_sql, sql)
        self.assertEqual([], where_values)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
