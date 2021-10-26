import unittest
from datetime import datetime
from typing import get_type_hints
from typing import Optional, Union, Any, Type, List, Tuple, Dict
from pydantic import dataclasses, BaseModel
from pydantic.fields import ModelField, Undefined, Field

from fastorm import FastORM, Autoincrement
from fastorm.compat import get_type_hints_with_annotations
from tests.test_create_table import remove_prefix


@dataclasses.dataclass
class ExpectedResult(object):
    is_optional: bool
    sql_type: str
    default: Any
# end class

ExpectedResult: Type[Any]


class Table1(FastORM):
    _table_name = 'table1'
    _automatic_fields = ['id']
    _primary_keys = ['id']

    id: Union[int, None]
# end class


class Table2(FastORM):
    _table_name = 'table2'
    _automatic_fields = ['id']
    _primary_keys = ['id']

    id: Union[int, None]
# end class


class Table1HavingTable2VersionOptionalSingleReferences(FastORM):
    """
        CREATE TABLE "table1_having_table2" (
          "table1__id" BIGINT,
          "table2__id" BIGINT
        )
    """
    _table_name = 'table1_having_table2'
    _primary_keys = ['table1', 'table2']
    _automatic_fields = ['table1', 'table2']

    table1: Union[Table1, int]
    table2: Union[Table2, int]
# end class


class Table1HavingTable2VersionFixedSingleReferences(FastORM):
    """
        CREATE TABLE "table1_having_table2" (
          "table1__id" BIGINT,
          "table2__id" BIGINT
        )
    """
    _table_name = 'table1_having_table2'
    _primary_keys = ['table1', 'table2']
    _automatic_fields = ['table1', 'table2']

    table1: Table1
    table2: Table2
# end class


class CreateTableTestCase(unittest.TestCase):
    def test_working_table_optional_single_references(self):
        cls = Table1HavingTable2VersionOptionalSingleReferences
        expected_sql = "\n".join(remove_prefix(line, '        ') for line in cls.__doc__.strip().splitlines() if not line.strip().startswith('#'))
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_fixed_single_references(self):
        cls = Table1HavingTable2VersionFixedSingleReferences
        expected_sql = "\n".join(remove_prefix(line, '        ') for line in cls.__doc__.strip().splitlines() if
                                 not line.strip().startswith('#'))
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
