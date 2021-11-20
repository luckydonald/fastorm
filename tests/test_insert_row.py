__author__ = 'luckydonald'

import unittest
from textwrap import dedent
from typing import Optional, Union, Any, Type, List, Tuple, Dict

from fastorm import FastORM


class Table1(FastORM):
    _table_name = 'table1'
    _automatic_fields = ['id']
    _primary_keys = ['id']

    id: Union[int, None]
    name: str
    number: int
# end class


# noinspection SqlNoDataSourceInspection,SqlResolve
class InsertRowSimpleTestCase(unittest.TestCase):
    def test_insert(self):
        expected_sql = dedent(
            """
            INSERT INTO "table1" ("name","number")
             VALUES ($1,$2)
             RETURNING "id"
            ;
            """
        ).strip()
        expected_params = ["Sample Text", 123]
        row = Table1(
            name=expected_params[0],
            number=expected_params[1]
        )
        actual_sql, *actual_params = row.build_sql_insert()
        self.assertEqual(expected_sql, actual_sql)
        self.assertEqual(expected_params, actual_params)
    # end def
# end class


class TableDoubleKey(FastORM):
    _table_name = 'table_double_key'
    _automatic_fields = ['id_a']
    _primary_keys = ['id_a', 'id_b']

    id_a: Union[int, None]
    id_b: Union[int, None]
    name: str
    number: int
# end class


class RefToDoubleKey(FastORM):
    _table_name = 'ref_to_double_key'
    _automatic_fields = []
    _primary_keys = ['ref']

    ref: Union[None, TableDoubleKey]
    text: str
# end class


# noinspection SqlNoDataSourceInspection,SqlResolve
class InsertRowRefTestCase(unittest.TestCase):
    FastORM.update_forward_refs()
    def test_insert_tuple(self):
        # also has no return
        expected_sql = dedent(
            """
            INSERT INTO "ref_to_double_key" ("ref__id_a","ref__id_b","text")
             VALUES ($1,$2,$3)
            ;
            """
        ).strip()
        expected_params = [4458, 69, 'littlepip']
        row = RefToDoubleKey(
            ref=(4458, 69),
            text="littlepip"
        )
        actual_sql, *actual_params = row.build_sql_insert()
        self.assertEqual(expected_sql, actual_sql)
        self.assertEqual(expected_params, actual_params)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if

