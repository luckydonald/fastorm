import textwrap
import unittest
from textwrap import dedent
from typing import Optional, List

from pydantic import BaseConfig
from pydantic.fields import ModelField

from fastorm import FastORM, In, SqlFieldMeta, FieldInfo, FieldItem
from tools_for_the_tests_of_fastorm import VerboseTestCase


class DoublePrimaryKeyTable(FastORM):
    _table_name = 'double_primary_key_table'
    _primary_keys = ['id_a', 'id_b']

    id_a: int
    id_b: int
    banana: str
# end class


class SinglePrimaryKeyTable(FastORM):
    _table_name = 'normal_table'
    _primary_keys = ['id']

    id: int
# end class


class ReferencingTable(FastORM):
    _table_name = 'referencing_table'
    _primary_keys = ['double', 'single']

    double: DoublePrimaryKeyTable
    single: SinglePrimaryKeyTable
    has_mango: bool
# end class


class ReferencingTableTestCase(VerboseTestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "referencing_table" (
              "double__id_a" BIGINT NOT NULL,
              "double__id_b" BIGINT NOT NULL,
              "single__id" BIGINT NOT NULL,
              "has_mango" BOOLEAN NOT NULL,
              PRIMARY KEY ("double__id_a", "double__id_b", "single__id")
            );
            """
        ).strip()
        actual_sql, *actual_params = ReferencingTable.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = textwrap.dedent("""
        CREATE INDEX "idx_referencing_table___double__id_a" ON "referencing_table" ("double__id_a");
        CREATE INDEX "idx_referencing_table___double__id_b" ON "referencing_table" ("double__id_b");
        CREATE INDEX "idx_referencing_table___single__id" ON "referencing_table" ("single__id");
        ALTER TABLE "referencing_table" ADD CONSTRAINT "fk_referencing_table___double" FOREIGN KEY ("double__id_a", "double__id_b") REFERENCES "double_primary_key_table" ("id_a", "id_b") ON DELETE CASCADE;
        ALTER TABLE "referencing_table" ADD CONSTRAINT "fk_referencing_table___single__id" FOREIGN KEY ("single__id") REFERENCES "normal_table" ("id") ON DELETE CASCADE;
        """).strip()
        actual_sql, *actual_params = ReferencingTable.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def
# end class
