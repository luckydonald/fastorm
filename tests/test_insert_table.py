__author__ = 'luckydonald'

import unittest
from datetime import datetime
from typing import get_type_hints
from typing import Optional, Union, Any, Type, List, Tuple, Dict

from fastorm import FastORM
from tests.test_create_table import remove_prefix


class Table1(FastORM):
    """
        INSERT INTO "table1" ("name","number")
         VALUES ($1,$2)
         RETURNING "id"
        ;
    """
    _table_name = 'table1'
    _automatic_fields = ['id']
    _primary_keys = ['id']

    id: Union[int, None]
    name: str
    number: int
# end class


def extract_sql_from_docstring(cls):
    return "\n".join(remove_prefix(line, '        ') for line in cls.__doc__.strip().splitlines() if not line.strip().startswith('#'))
# end def


# ----------------------------------------------------


class InsertTableTestCase(unittest.TestCase):
    def test_insert(self):
        expected_sql = extract_sql_from_docstring(Table1)
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


if __name__ == '__main__':
    unittest.main()
# end if

