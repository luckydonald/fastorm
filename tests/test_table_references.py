import unittest
from typing import Optional, Union, Any, Type, List, Tuple, Dict

from fastorm import FastORM
from tests.tools_for_the_tests_of_fastorm import extract_sql_from_docstring


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


class Table1HavingTable2VersionSingleReferencesOptional(FastORM):
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


class Table1HavingTable2VersionSingleReferencesMandatory(FastORM):
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


class DoublePrimaryKeyTable(FastORM):
    """
        CREATE TABLE "table1_having_table2" (
          "table1__id" BIGINT NOT NULL,
          "table2__id" BIGINT
        )
    """
    _table_name = 'double_primary_key'
    _primary_keys = ['id_part1', 'id_part2']
    _automatic_fields = []

    id_part1: int
    id_part2: Optional[int]
# end class


class ReferencingDoublePrimaryKeyTableVersionMultiReferencesMandatory(FastORM):
    """
        CREATE TABLE "double_primary_key" (
          "double_trouble__id_part1" BIGINT NOT NULL,
          "double_trouble__id_part2" BIGINT
        )
    """
    _table_name = 'double_primary_key'
    _primary_keys = ['double_trouble']
    _automatic_fields = []

    double_trouble: DoublePrimaryKeyTable
# end class


class ReferencingDoublePrimaryKeyTableVersionMultiReferencesOptional(FastORM):
    """
        CREATE TABLE "double_primary_key" (
          "double_trouble__id_part1" BIGINT NOT NULL,
          "double_trouble__id_part2" BIGINT
        )
    """
    _table_name = 'double_primary_key'
    _primary_keys = ['double_trouble']
    _automatic_fields = []

    double_trouble: Union[DoublePrimaryKeyTable, Tuple[int, int]]
# end class


# ----------------------------------------------------


class CreateTableTestCase(unittest.TestCase):
    def test_working_table_single_references_mandatory(self):
        cls = Table1HavingTable2VersionSingleReferencesMandatory
        expected_sql = extract_sql_from_docstring(cls)
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_single_references_optional(self):
        cls = Table1HavingTable2VersionSingleReferencesOptional
        expected_sql = extract_sql_from_docstring(cls)
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_multi_references_mandatory(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesMandatory
        expected_sql = extract_sql_from_docstring(cls)
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_multi_references_optional(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesOptional
        expected_sql = extract_sql_from_docstring(cls)
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
