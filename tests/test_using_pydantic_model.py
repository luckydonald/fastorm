import unittest
from pydantic import BaseModel
from fastorm import FastORM
from tests.tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring, VerboseTestCase


class TableHowItUsedToBe(FastORM):
    _table_name = 'table_how_it_used_to_be'
    _primary_keys = ['id']
    _ignored_fields = []

    id: int
    name: str
    field_with_default: str = 'mangoes are the best'

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "table_how_it_used_to_be" (
      "id" BIGINT NOT NULL PRIMARY KEY,
      "name" TEXT NOT NULL,
      "field_with_default" TEXT NOT NULL DEFAULT 'mangoes are the best'
    );
    -- and now the references --
    SELECT 1;
    """
# end class


class TableModel(BaseModel):
    id: int
    name: str
    field_with_default: str = 'mangoes are the best'
# end class


class TableWithModelFirst(TableModel, FastORM):
    _table_name = 'table_with_model_first'
    _primary_keys = ['id']
    _ignored_fields = []

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "table_with_model_first" (
      "id" BIGINT NOT NULL PRIMARY KEY,
      "name" TEXT NOT NULL,
      "field_with_default" TEXT NOT NULL DEFAULT 'mangoes are the best'
    );
    -- and now the references --
    SELECT 1;
    """
# end def


class TableWithModelLast(TableModel, FastORM):
    _table_name = 'table_with_model_last'
    _primary_keys = ['id']
    _ignored_fields = []

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "table_with_model_last" (
      "id" BIGINT NOT NULL PRIMARY KEY,
      "name" TEXT NOT NULL,
      "field_with_default" TEXT NOT NULL DEFAULT 'mangoes are the best'
    );
    -- and now the references --
    SELECT 1;
    """
# end def


class MyTestCase(VerboseTestCase):
    def test_create(self):
        for test_cls in (TableHowItUsedToBe, TableWithModelFirst, TableWithModelLast):
            print(f'testing {test_cls.__name__}')
            with self.subTest(test_cls.__name__):
                expected_sql = extract_create_and_reference_sql_from_docstring(test_cls).create
                actual_sql, *actual_params = test_cls.build_sql_create()
                self.assertEqual(expected_sql, actual_sql)
                self.assertListEqual([], actual_params)
            # end with
        # end for
    # end def

    def test_references(self):
        for test_cls in (TableHowItUsedToBe, TableWithModelFirst, TableWithModelLast):
            print(f'testing {test_cls.__name__}')
            with self.subTest(test_cls.__name__):
                expected_sql = extract_create_and_reference_sql_from_docstring(test_cls).references
                actual_sql, *actual_params = test_cls.build_sql_references()
                self.assertEqual(expected_sql, actual_sql)
                self.assertListEqual([], actual_params)
            # end with
        # end for
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
