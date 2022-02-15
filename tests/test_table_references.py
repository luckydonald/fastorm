import unittest
from typing import Optional, Union, Any, Type, List, Tuple, Dict

from pydantic import BaseConfig
from pydantic.fields import ModelField

from fastorm import FastORM, FieldInfo, FieldItem
from tests.tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring, VerboseTestCase


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
          "table2__id" BIGINT,
          PRIMARY KEY ("table1__id", "table2__id")
        );
        -- and now the references --
        CREATE INDEX "idx_table1_having_table2___table1__id" ON "table1_having_table2" ("table1__id");
        CREATE INDEX "idx_table1_having_table2___table2__id" ON "table1_having_table2" ("table2__id");
        ALTER TABLE "table1_having_table2" ADD CONSTRAINT "fk_table1_having_table2___table1__id" FOREIGN KEY ("table1__id") REFERENCES "table1" ("id") ON DELETE CASCADE;
        ALTER TABLE "table1_having_table2" ADD CONSTRAINT "fk_table1_having_table2___table2__id" FOREIGN KEY ("table2__id") REFERENCES "table2" ("id") ON DELETE CASCADE;
    """
    _table_name = 'table1_having_table2'
    _primary_keys = ['table1', 'table2']
    _automatic_fields = []

    table1: Union[Table1, int]
    table2: Union[Table2, int]
# end class


class Table1HavingTable2VersionSingleReferencesMandatory(FastORM):
    """
        CREATE TABLE "table1_having_table2" (
          "table1__id" BIGINT,
          "table2__id" BIGINT,
          PRIMARY KEY ("table1__id", "table2__id")
        );
        -- and now the references --
        CREATE INDEX "idx_table1_having_table2___table1__id" ON "table1_having_table2" ("table1__id");
        CREATE INDEX "idx_table1_having_table2___table2__id" ON "table1_having_table2" ("table2__id");
        ALTER TABLE "table1_having_table2" ADD CONSTRAINT "fk_table1_having_table2___table1__id" FOREIGN KEY ("table1__id") REFERENCES "table1" ("id") ON DELETE CASCADE;
        ALTER TABLE "table1_having_table2" ADD CONSTRAINT "fk_table1_having_table2___table2__id" FOREIGN KEY ("table2__id") REFERENCES "table2" ("id") ON DELETE CASCADE;
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
          "table2__id" BIGINT,
          PRIMARY KEY ("id_part1", "id_part2")
        );
        -- and now the references --
        SELECT 1;
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
          "double_trouble__id_part2" BIGINT,
          PRIMARY KEY ("double_trouble__id_part1", "double_trouble__id_part2")
        );
        -- and now the references --
        CREATE INDEX "idx_double_primary_key___double_trouble__id_part1" ON "double_primary_key" ("double_trouble__id_part1");
        CREATE INDEX "idx_double_primary_key___double_trouble__id_part2" ON "double_primary_key" ("double_trouble__id_part2");
        ALTER TABLE "double_primary_key" ADD CONSTRAINT "fk_double_primary_key___double_trouble" FOREIGN KEY ("double_trouble__id_part1", "double_trouble__id_part2") REFERENCES "double_primary_key" ("id_part1", "id_part2") ON DELETE CASCADE;
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
          "double_trouble__id_part2" BIGINT,
          PRIMARY KEY ("double_trouble__id_part1", "double_trouble__id_part2")
        );
        -- and now the references --
        CREATE INDEX "idx_double_primary_key___double_trouble__id_part1" ON "double_primary_key" ("double_trouble__id_part1");
        CREATE INDEX "idx_double_primary_key___double_trouble__id_part2" ON "double_primary_key" ("double_trouble__id_part2");
        ALTER TABLE "double_primary_key" ADD CONSTRAINT "fk_double_primary_key___double_trouble" FOREIGN KEY ("double_trouble__id_part1", "double_trouble__id_part2") REFERENCES "double_primary_key" ("id_part1", "id_part2") ON DELETE CASCADE;
    """
    _table_name = 'double_primary_key'
    _primary_keys = ['double_trouble']
    _automatic_fields = []

    double_trouble: Union[DoublePrimaryKeyTable, Tuple[int, Optional[int]]]
# end class


# ----------------------------------------------------


class CreateTableTestCase(VerboseTestCase):
    def test_working_table_single_references_mandatory_create(self):
        cls = Table1HavingTable2VersionSingleReferencesMandatory
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).create
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_single_references_mandatory_references(self):
        cls = Table1HavingTable2VersionSingleReferencesMandatory
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).references
        actual_sql, *actual_params = cls.build_sql_references()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_single_references_optional_create(self):
        cls = Table1HavingTable2VersionSingleReferencesOptional
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).create
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_single_references_optional_references(self):
        cls = Table1HavingTable2VersionSingleReferencesOptional
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).references
        actual_sql, *actual_params = cls.build_sql_references()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_multi_references_mandatory_create(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesMandatory
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).create
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_multi_references_mandatory_references(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesMandatory
        print(extract_create_and_reference_sql_from_docstring(cls))
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).references
        actual_sql, *actual_params = cls.build_sql_references()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_multi_references_optional_create(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesOptional
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).create
        actual_sql, *actual_params = cls.build_sql_create()
        self.assertEqual(expected_sql, actual_sql)
    # end def

    def test_working_table_multi_references_optional_get_fields_typehints(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesOptional
        actual = cls.get_fields_typehints(flatten_table_references=True)
        expected = {
            'double_trouble__id_part1': FieldInfo(
                is_primary_key=True,
                types=[
                    FieldItem(field='double_trouble', type_=ModelField(name='double_trouble', type_=DoublePrimaryKeyTable, required=True, class_validators={}, model_config=BaseConfig)),
                    FieldItem(field='id_part1', type_=ModelField(name='id_part1', type_=Tuple[int, Optional[int]], required=True, class_validators={}, model_config=BaseConfig)),
                ],
            ),
            'double_trouble__id_part2': FieldInfo(
                is_primary_key=True,
                types=[
                    FieldItem(field='double_trouble', type_=ModelField(name='double_trouble', type_=DoublePrimaryKeyTable, required=True, class_validators={}, model_config=BaseConfig)),
                    FieldItem(field='id_part2', type_=ModelField(name='id_part2', type_=Tuple[int, Optional[int]], required=True, class_validators={}, model_config=BaseConfig)),
                ],
            ),
        }
        self.assertEqual(str(expected), str(actual))
    # end def

    def test_working_table_multi_references_optional_get_fields_references(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesOptional
        actual = cls.get_fields_references(recursive=True)
        expected = {
            'double_trouble__id_part1': FieldInfo(
                is_primary_key=True,
                types=[
                    FieldItem(field='double_trouble', type_=DoublePrimaryKeyTable),
                    FieldItem(field='id_part1', type_=int),
                ],
            ),
            'double_trouble__id_part2': FieldInfo(
                is_primary_key=True,
                types=[
                    FieldItem(field='double_trouble', type_=DoublePrimaryKeyTable),
                    FieldItem(field='id_part2', type_=Optional[int]),
                ],
            ),
        }
        self.assertEqual(str(expected), str(actual))
    # end def

    def test_working_table_multi_references_optional_references(self):
        cls = ReferencingDoublePrimaryKeyTableVersionMultiReferencesOptional
        expected_sql = extract_create_and_reference_sql_from_docstring(cls).references
        actual_sql, *actual_params = cls.build_sql_references()
        self.assertEqual(expected_sql, actual_sql)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
