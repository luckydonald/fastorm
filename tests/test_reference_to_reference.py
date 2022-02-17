import textwrap
import unittest
from textwrap import dedent
from typing import Optional, List

from pydantic import BaseConfig
from pydantic.fields import ModelField

from fastorm import FastORM, In, SqlFieldMeta, FieldInfo, FieldItem
from tools_for_the_tests_of_fastorm import VerboseTestCase, extract_create_and_reference_sql_from_docstring


class UserTable(FastORM):
    _table_name = 'user_table'
    _automatic_fields = ['id']
    _primary_keys = ['id']

    id: int
    text: str

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "user_table" (
      "id" BIGSERIAL NOT NULL PRIMARY KEY,
      "text" TEXT NOT NULL
    );
    -- and now the references --
    SELECT 1;
    """
# end class


class ItemTable(FastORM):
    _table_name = 'item_table'
    _automatic_fields = ['id']
    _primary_keys = ['id']

    id: int
    name: str
    description: str

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "item_table" (
      "id" BIGSERIAL NOT NULL PRIMARY KEY,
      "name" TEXT NOT NULL,
      "description" TEXT NOT NULL
    );
    -- and now the references --
    SELECT 1;
    """
# end class


class ItemQualityTable(FastORM):
    _table_name = 'item_quality_table'
    _primary_keys = ['item', 'quality']

    item: ItemTable
    quality: int

    description: str

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "item_quality_table" (
      "item__id" BIGINT NOT NULL,
      "quality" BIGINT NOT NULL,
      "description" TEXT NOT NULL,
      PRIMARY KEY ("item__id", "quality")
    );
    -- and now the references --
    CREATE INDEX "idx_item_quality_table___item__id" ON "item_quality_table" ("item__id");
    # CREATE INDEX "idx_item_quality_table___pk" ON "item_quality_table" ("item__id", "quality");
    ALTER TABLE "item_quality_table" ADD CONSTRAINT "fk_item_quality_table___item__id" FOREIGN KEY ("item__id") REFERENCES "item_table" ("id") ON DELETE CASCADE;
    """
# end class


class UserHasItemWithQualityTable(FastORM):
    _table_name = 'user_has_item_with_quality_table'
    _primary_keys = ['user', 'item_quality']

    user: UserTable
    item_quality: ItemQualityTable

    # noinspection SqlNoDataSourceInspection
    __doc__ = """
    CREATE TABLE "user_has_item_with_quality_table" (
      "user__id" BIGINT NOT NULL,
      "item_quality__item__id" BIGINT NOT NULL,
      "item_quality__quality" BIGINT NOT NULL,
      PRIMARY KEY ("user__id", "item_quality__item__id", "item_quality__quality")
    );
    -- and now the references --
    CREATE INDEX "idx_user_has_item_with_quality_table___user__id" ON "user_has_item_with_quality_table" ("user__id");
    CREATE INDEX "idx_user_has_item_with_quality_table___item_quality__item__id" ON "user_has_item_with_quality_table" ("item_quality__item__id");
    CREATE INDEX "idx_user_has_item_with_quality_table___item_quality__quality" ON "user_has_item_with_quality_table" ("item_quality__quality");
    ALTER TABLE "user_has_item_with_quality_table" ADD CONSTRAINT "fk_user_has_item_with_quality_table___user__id" FOREIGN KEY ("user__id") REFERENCES "user_table" ("id") ON DELETE CASCADE;
    ALTER TABLE "user_has_item_with_quality_table" ADD CONSTRAINT "fk_user_has_item_with_quality_table___item_quality" FOREIGN KEY ("item_quality__item__id", "item_quality__quality") REFERENCES "item_quality_table" ("item__id", "quality") ON DELETE CASCADE;
    """
# end class


class ReferenceToReferenceTestCase(VerboseTestCase):
    def test_sql_text_create_(self):
        self.maxDiff = None
        for table_cls in (UserTable, ItemTable, ItemQualityTable, UserHasItemWithQualityTable):
            with self.subTest(msg=f'class {table_cls.__name__}'):
                expected_sql = extract_create_and_reference_sql_from_docstring(table_cls).create
                actual_sql, *actual_params = table_cls.build_sql_create()
                self.assertEqual(expected_sql, actual_sql, msg=f"CREATE class {table_cls.__name__}")
                self.assertListEqual([], actual_params, f"CREATE class {table_cls.__name__}")
            # end with
        # end for
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        for table_cls in (UserTable, ItemTable, ItemQualityTable, UserHasItemWithQualityTable):
            with self.subTest(msg=f'class {table_cls.__name__}'):
                expected_sql = extract_create_and_reference_sql_from_docstring(table_cls).references
                actual_sql, *actual_params = table_cls.build_sql_references()
                self.assertEqual(expected_sql, actual_sql, msg=f"REFERENCE of class {table_cls.__name__}")
                self.assertListEqual([], actual_params, f"REFERENCE of class {table_cls.__name__}")
            # end with
        # end for
    # end def
# end class
