import unittest
from textwrap import dedent

from fastorm import FastORM
from tests.tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring


class SimpleTable(FastORM):
    _table_name = 'simple_table'
    _primary_keys = ['id']

    id: int
    text: str
# end class


class NonPrimaryKeyReferenceTo(FastORM):
    _table_name = 'non_pk_reference_to'
    _primary_keys = ['id']

    id: int
    simple_table_ref: SimpleTable
# end class


class PrimaryKeyReferenceTo(FastORM):
    _table_name = 'pk_reference_to'
    _primary_keys = ['pk_is_simple_table']

    pk_is_simple_table: SimpleTable
    some_number: float
# end class


class NormalTableTestCase(unittest.TestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "simple_table" (
              "id" BIGINT NOT NULL PRIMARY KEY,
              "text" TEXT NOT NULL
            );
            """
        ).strip()
        actual_sql, *actual_params = SimpleTable.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = "SELECT 1;"
        actual_sql, *actual_params = SimpleTable.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test_normal_select_single_field_pk(self):
        actual = SimpleTable.build_sql_select(id=69)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","text" FROM "simple_table" WHERE "id" = $1', 69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_normal_select_single_field_non_pk(self):
        actual = SimpleTable.build_sql_select(text="foobar")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","text" FROM "simple_table" WHERE "text" = $1', "foobar"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_normal_select_multi_field(self):
        actual = SimpleTable.build_sql_select(id=4458, text="littlepip")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","text" FROM "simple_table" WHERE "id" = $1 AND "text" = $2', 4458, "littlepip"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def
# end class


class NonPrimaryKeyReferenceTableTestCase(unittest.TestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "non_pk_reference_to" (
              "id" BIGINT NOT NULL PRIMARY KEY,
              "simple_table_ref__id" BIGINT NOT NULL
            );
            """
        ).strip()
        actual_sql, *actual_params = NonPrimaryKeyReferenceTo.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE INDEX "idx_non_pk_reference_to___simple_table_ref__id" ON "non_pk_reference_to" ("simple_table_ref__id");
            ALTER TABLE "non_pk_reference_to" ADD CONSTRAINT "fk_non_pk_reference_to___simple_table_ref__id" FOREIGN KEY ("simple_table_ref__id") REFERENCES "simple_table" ("id") ON DELETE CASCADE;
            """
        ).strip()
        actual_sql, *actual_params = NonPrimaryKeyReferenceTo.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test_non_pk_reference_select_single_field_pk(self):
        actual = NonPrimaryKeyReferenceTo.build_sql_select(id=69)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","simple_table_ref__id" FROM "non_pk_reference_to" WHERE "id" = $1', 69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_non_pk(self):
        actual = NonPrimaryKeyReferenceTo.build_sql_select(simple_table_ref="foobar")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","simple_table_ref__id" FROM "non_pk_reference_to" WHERE "simple_table_ref__id" = $1', "foobar"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_multi_field(self):
        actual = NonPrimaryKeyReferenceTo.build_sql_select(id=4458, simple_table_ref="littlepip")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","simple_table_ref__id" FROM "non_pk_reference_to" WHERE "id" = $1 AND "simple_table_ref__id" = $2', 4458, "littlepip"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def
# end class


class PrimaryKeyReferenceTableTestCase(unittest.TestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "pk_reference_to" (
              "pk_is_simple_table__id" BIGINT NOT NULL PRIMARY KEY,
              "some_number" DOUBLE PRECISION NOT NULL
            );
            """
        ).strip()
        actual_sql, *actual_params = PrimaryKeyReferenceTo.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE INDEX "idx_pk_reference_to___pk_is_simple_table__id" ON "pk_reference_to" ("pk_is_simple_table__id");
            ALTER TABLE "pk_reference_to" ADD CONSTRAINT "fk_pk_reference_to___pk_is_simple_table__id" FOREIGN KEY ("pk_is_simple_table__id") REFERENCES "simple_table" ("id") ON DELETE CASCADE;
            """
        ).strip()
        actual_sql, *actual_params = PrimaryKeyReferenceTo.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test_non_pk_reference_select_single_field_pk(self):
        actual = PrimaryKeyReferenceTo.build_sql_select(id=69)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","simple_table_ref__id" FROM "pk_reference_to" WHERE "id" = $1', 69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_non_pk(self):
        actual = PrimaryKeyReferenceTo.build_sql_select(simple_table_ref="foobar")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","simple_table_ref__id" FROM "pk_reference_to" WHERE "simple_table_ref__id" = $1', "foobar"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_multi_field(self):
        actual = PrimaryKeyReferenceTo.build_sql_select(id=4458, simple_table_ref="littlepip")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","simple_table_ref__id" FROM "pk_reference_to" WHERE "id" = $1 AND "simple_table_ref__id" = $2', 4458, "littlepip"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def
# end class



if __name__ == '__main__':
    unittest.main()
