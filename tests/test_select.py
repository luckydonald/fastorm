import unittest
from textwrap import dedent
from typing import Optional, List

from fastorm import FastORM, In


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

    def test__prepare_kwargs_single(self):
        expected = [{'id': 12}]
        actual = SimpleTable._prepare_kwargs(id=12, _allow_in=True)
        self.assertEqual(expected, actual)
    # end def

    def test__prepare_kwargs_double(self):
        expected = [{'id': 42}, {'text': 'sample text'}]
        actual = SimpleTable._prepare_kwargs(id=42, text="sample text", _allow_in=True)
        self.assertEqual(expected, actual)
    # end def

    def test__prepare_kwargs_double_with_union(self):
        expected = [In[{'id': 42}, {'id': 69}], {'text': 'sample text'}]
        actual = SimpleTable._prepare_kwargs(id=In[42, 69], text="sample text", _allow_in=True)
        self.assertEqual(expected, actual)
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
        actual = PrimaryKeyReferenceTo.build_sql_select(some_number=69.42)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "pk_is_simple_table__id","some_number" FROM "pk_reference_to" WHERE "some_number" = $1', 69.42

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_non_pk(self):
        actual = PrimaryKeyReferenceTo.build_sql_select(pk_is_simple_table=4458)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "pk_is_simple_table__id","some_number" FROM "pk_reference_to" WHERE "pk_is_simple_table__id" = $1', 4458

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_multi_field(self):
        actual = PrimaryKeyReferenceTo.build_sql_select(pk_is_simple_table=4458, some_number=42.69)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "pk_is_simple_table__id","some_number" FROM "pk_reference_to" WHERE "pk_is_simple_table__id" = $1 AND "some_number" = $2', 4458, 42.69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def
# end class


class DoublePrimaryKeyTable(FastORM):
    _table_name = 'double_pk_table'
    _primary_keys = ['id_part_1', 'id_part_2']

    id_part_1: int
    id_part_2: float
# end class


class DoublePrimaryKeyTableTestCase(unittest.TestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "double_pk_table" (
              "id_part_1" BIGINT NOT NULL,
              "id_part_2" DOUBLE PRECISION NOT NULL,
              PRIMARY KEY ("id_part_1", "id_part_2")
            );
            """
        ).strip()
        actual_sql, *actual_params = DoublePrimaryKeyTable.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            SELECT 1;
            """
        ).strip()
        actual_sql, *actual_params = DoublePrimaryKeyTable.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test_non_pk_reference_select_single_field_pk(self):
        actual = DoublePrimaryKeyTable.build_sql_select(id_part_1=4458)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_part_1","id_part_2" FROM "double_pk_table" WHERE "id_part_1" = $1', 4458

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_non_pk(self):
        actual = DoublePrimaryKeyTable.build_sql_select(id_part_2=69.042)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_part_1","id_part_2" FROM "double_pk_table" WHERE "id_part_2" = $1', 69.042

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_multi_field(self):
        actual = DoublePrimaryKeyTable.build_sql_select(id_part_1=4458, id_part_2=42.69)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_part_1","id_part_2" FROM "double_pk_table" WHERE "id_part_1" = $1 AND "id_part_2" = $2', 4458, 42.69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def
# end class


class ReferencingDoubleKey(FastORM):
    _table_name = "ref_to_double_key_table"
    _primary_keys = ['id_ref_part', 'id_part_3']

    id_ref_part: DoublePrimaryKeyTable
    id_part_3: str
    other_field: str
# end class


class ReferencingDoubleKeyTestCase(unittest.TestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "ref_to_double_key_table" (
              "id_ref_part__id_part_1" BIGINT NOT NULL,
              "id_ref_part__id_part_2" DOUBLE PRECISION NOT NULL,
              "id_part_3" TEXT NOT NULL,
              "other_field" TEXT NOT NULL,
              PRIMARY KEY ("id_ref_part__id_part_1", "id_ref_part__id_part_2", "id_part_3")
            );
            """
        ).strip()
        actual_sql, *actual_params = ReferencingDoubleKey.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE INDEX "idx_ref_to_double_key_table___id_ref_part__id_part_1" ON "ref_to_double_key_table" ("id_ref_part__id_part_1");
            CREATE INDEX "idx_ref_to_double_key_table___id_ref_part__id_part_2" ON "ref_to_double_key_table" ("id_ref_part__id_part_2");
            ALTER TABLE "ref_to_double_key_table" ADD CONSTRAINT "fk_ref_to_double_key_table___id_ref_part__id_part_1" FOREIGN KEY ("id_ref_part__id_part_1") REFERENCES "double_pk_table" ("id_part_1") ON DELETE CASCADE;
            ALTER TABLE "ref_to_double_key_table" ADD CONSTRAINT "fk_ref_to_double_key_table___id_ref_part__id_part_2" FOREIGN KEY ("id_ref_part__id_part_2") REFERENCES "double_pk_table" ("id_part_2") ON DELETE CASCADE;
            """
        ).strip()
        actual_sql, *actual_params = ReferencingDoubleKey.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test__prepare_kwargs_double_with_union_of_object_and_tuple(self):
        expected = [In[{'id_ref_part__id_part_1': 101, 'id_ref_part__id_part_2': 1.11}, {'id_ref_part__id_part_1': 202, 'id_ref_part__id_part_2': 2.22}], {'other_field': 'sample text'}]
        actual = ReferencingDoubleKey._prepare_kwargs(id_ref_part=In[DoublePrimaryKeyTable(id_part_1=101, id_part_2=1.11), (202, 2.22)], other_field="sample text", _allow_in=True)
        self.assertEqual(expected, actual)
    # end def


    def test_non_pk_reference_select_single_field_non_pk(self):
        actual = ReferencingDoubleKey.build_sql_select(other_field="do you like -mmmh- bananas?")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE "other_field" = $1', "do you like -mmmh- bananas?"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_pk_non_ref(self):
        actual = ReferencingDoubleKey.build_sql_select(id_part_3="Sample Text")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE "id_part_3" = $1', "Sample Text"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_pk_with_ref_object(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=DoublePrimaryKeyTable(id_part_1=12, id_part_2=34.56))
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") = ($1, $2)', 12, 34.56

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_single_field_pk_with_ref_tuple(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=(12, 34.56))
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") = ($1, $2)', 12, 34.56

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_non_pk_reference_select_multi_field(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=DoublePrimaryKeyTable(id_part_1=123, id_part_2=456.789), id_part_3="littlepip is best pony")
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") = ($1, $2) AND "id_part_3" = $3', 123, 456.789, "littlepip is best pony"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')

        # test that order is not relevant
        actual = ReferencingDoubleKey.build_sql_select(id_part_3="littlepip is best pony", id_ref_part=DoublePrimaryKeyTable(id_part_1=123, id_part_2=456.789))
        self.assertEqual(expected[0], actual[0], 'sql (should ignore order)')
        self.assertEqual(expected[1:], actual[1:], 'variables (should ignore order)')
    # end def

    def test_in_clause_non_pk_single_old_list_syntax(self):
        actual = ReferencingDoubleKey.build_sql_select(id_part_3=["littlepip is best pony"])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE "id_part_3" = $1', ["littlepip is best pony"]

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_in_clause_non_pk_single(self):
        actual = ReferencingDoubleKey.build_sql_select(id_part_3=In["littlepip is best pony"])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE "id_part_3" = $1', "littlepip is best pony"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_in_clause_non_pk_multiple(self):
        actual = ReferencingDoubleKey.build_sql_select(id_part_3=In["littlepip is best pony", "littlepip is my waifu"])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE "id_part_3" IN ($1, $2)', "littlepip is best pony", "littlepip is my waifu"

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_in_clause_non_pk_reference_tuple_multiple(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=In[(12, 34.56), (69, 4458.0)])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") IN (($1, $2), ($3, $4))', 12, 34.56, 69, 4458.0

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_in_clause_non_pk_reference_tuple_single(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=In[(69, 4458.0)])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") = ($1, $2)', 69, 4458.0

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test__prepare_kwargs_in_clause_non_pk_reference_tuple_single(self):
        actual = ReferencingDoubleKey._prepare_kwargs(id_ref_part=In[(69, 4458.0)], _allow_in=True)
        expected = [{"id_ref_part__id_part_1": 69, "id_ref_part__id_part_2": 4458.0}]
        self.assertListEqual(expected, actual)
    # end def

    def test_list_in(self):
        actual = list(In[(69, 4458.0)])
        expected = [(69, 4458.0),]
        self.assertListEqual(expected, actual)
    # end def

    def test_in_clause_non_pk_reference_FastORM_multiple(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=In[DoublePrimaryKeyTable(id_part_1=123456, id_part_2=456.789), DoublePrimaryKeyTable(id_part_1=69, id_part_2=4458.69)])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") IN (($1, $2), ($3, $4))', 123456, 456.789, 69, 4458.69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_in_clause_non_pk_reference_FastORM_single(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=In[DoublePrimaryKeyTable(id_part_1=69, id_part_2=4458.69)])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") = ($1, $2)', 69, 4458.69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_in_clause_non_pk_reference_mixed_multiple(self):
        actual = ReferencingDoubleKey.build_sql_select(id_ref_part=In[DoublePrimaryKeyTable(id_part_1=123456, id_part_2=543.21), (4458, 987.654)])
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id_ref_part__id_part_1","id_ref_part__id_part_2","id_part_3","other_field" FROM "ref_to_double_key_table" WHERE ("id_ref_part__id_part_1", "id_ref_part__id_part_2") IN (($1, $2), ($3, $4))', 123456, 543.21, 4458, 987.654

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def
# end class


class TableWithWayTooManyReferences(FastORM):
    _table_name = 'too_many_refs'
    _automatic_fields = ['id_01']
    _primary_keys = ['id_01', 'id_02', 'id_03', 'id_04', 'id_05', 'id_06', 'id_07', 'id_08', 'id_09', 'id_10']

    id_01: Optional[int]
    id_02: int
    id_03: Optional[int]
    id_04: float
    id_05: str
    id_06: dict
    id_07: List[int]
    id_08: Optional[List[int]]
    id_09: List
    id_10: List
# end class


class UhOhReferencingTableWithWayTooManyReferences(FastORM):
    _table_name = 'uh_oh'
    _primary_keys = ['ref_1']

    ref_1: Optional[TableWithWayTooManyReferences]
    ref_2: TableWithWayTooManyReferences
# end class


class TableWithWayTooManyReferencesTestCase(unittest.TestCase):
    def test_sql_text_create_a(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "too_many_refs" (
              "id_01" BIGSERIAL NOT NULL,
              "id_02" BIGINT NOT NULL,
              "id_03" BIGINT,
              "id_04" DOUBLE PRECISION NOT NULL,
              "id_05" TEXT NOT NULL,
              "id_06" JSONB NOT NULL,
              "id_07" BIGINT[] NOT NULL,
              "id_08" BIGINT[],
              "id_09" JSONB NOT NULL,
              "id_10" JSONB NOT NULL,
              PRIMARY KEY ("id_01", "id_02", "id_03", "id_04", "id_05", "id_06", "id_07", "id_08", "id_09", "id_10")
            );
            """
        ).strip()
        actual_sql, *actual_params = TableWithWayTooManyReferences.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_create_b(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "uh_oh" (
              "ref_1__id_01" BIGINT,
              "ref_1__id_02" BIGINT,
              "ref_1__id_03" BIGINT,
              "ref_1__id_04" DOUBLE PRECISION,
              "ref_1__id_05" TEXT,
              "ref_1__id_06" JSONB,
              "ref_1__id_07" BIGINT[],
              "ref_1__id_08" BIGINT[],
              "ref_1__id_09" JSONB,
              "ref_1__id_10" JSONB,
              "ref_2__id_01" BIGINT,"""
            # "ref_2__id_01" BIGINT NOT NULL  # TODO
            """
              "ref_2__id_02" BIGINT NOT NULL,
              "ref_2__id_03" BIGINT,
              "ref_2__id_04" DOUBLE PRECISION NOT NULL,
              "ref_2__id_05" TEXT NOT NULL,
              "ref_2__id_06" JSONB NOT NULL,
              "ref_2__id_07" BIGINT[] NOT NULL,
              "ref_2__id_08" BIGINT[],
              "ref_2__id_09" JSONB NOT NULL,
              "ref_2__id_10" JSONB NOT NULL,
              PRIMARY KEY ("ref_1__id_01", "ref_1__id_02", "ref_1__id_03", "ref_1__id_04", "ref_1__id_05", "ref_1__id_06", "ref_1__id_07", "ref_1__id_08", "ref_1__id_09", "ref_1__id_10")
            );
            """
        ).strip()
        actual_sql, *actual_params = UhOhReferencingTableWithWayTooManyReferences.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references_a(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            SELECT 1;
            """
        ).strip()
        actual_sql, *actual_params = TableWithWayTooManyReferences.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test_sql_text_references_b(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE INDEX "idx_too_many_refs___ref_1__id_01" ON "too_many_refs" ("ref_1__id_01");
            CREATE INDEX "idx_too_many_refs___ref_1__id_02" ON "too_many_refs" ("ref_1__id_02");
            CREATE INDEX "idx_too_many_refs___ref_1__id_03" ON "too_many_refs" ("ref_1__id_03");
            CREATE INDEX "idx_too_many_refs___ref_1__id_04" ON "too_many_refs" ("ref_1__id_04");
            CREATE INDEX "idx_too_many_refs___ref_1__id_05" ON "too_many_refs" ("ref_1__id_05");
            CREATE INDEX "idx_too_many_refs___ref_1__id_06" ON "too_many_refs" ("ref_1__id_06");
            CREATE INDEX "idx_too_many_refs___ref_1__id_07" ON "too_many_refs" ("ref_1__id_07");
            CREATE INDEX "idx_too_many_refs___ref_1__id_08" ON "too_many_refs" ("ref_1__id_08");
            CREATE INDEX "idx_too_many_refs___ref_1__id_09" ON "too_many_refs" ("ref_1__id_09");
            CREATE INDEX "idx_too_many_refs___ref_1__id_10" ON "too_many_refs" ("ref_1__id_10");
            CREATE INDEX "idx_too_many_refs___ref_2__id_01" ON "too_many_refs" ("ref_2__id_01");
            CREATE INDEX "idx_too_many_refs___ref_2__id_02" ON "too_many_refs" ("ref_2__id_02");
            CREATE INDEX "idx_too_many_refs___ref_2__id_03" ON "too_many_refs" ("ref_2__id_03");
            CREATE INDEX "idx_too_many_refs___ref_2__id_04" ON "too_many_refs" ("ref_2__id_04");
            CREATE INDEX "idx_too_many_refs___ref_2__id_05" ON "too_many_refs" ("ref_2__id_05");
            CREATE INDEX "idx_too_many_refs___ref_2__id_06" ON "too_many_refs" ("ref_2__id_06");
            CREATE INDEX "idx_too_many_refs___ref_2__id_07" ON "too_many_refs" ("ref_2__id_07");
            CREATE INDEX "idx_too_many_refs___ref_2__id_08" ON "too_many_refs" ("ref_2__id_08");
            CREATE INDEX "idx_too_many_refs___ref_2__id_09" ON "too_many_refs" ("ref_2__id_09");
            CREATE INDEX "idx_too_many_refs___ref_2__id_10" ON "too_many_refs" ("ref_2__id_10");
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_01" FOREIGN KEY ("ref_1__id_01") REFERENCES "too_many_refs" ("id_01") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_02" FOREIGN KEY ("ref_1__id_02") REFERENCES "too_many_refs" ("id_02") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_03" FOREIGN KEY ("ref_1__id_03") REFERENCES "too_many_refs" ("id_03") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_04" FOREIGN KEY ("ref_1__id_04") REFERENCES "too_many_refs" ("id_04") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_05" FOREIGN KEY ("ref_1__id_05") REFERENCES "too_many_refs" ("id_05") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_06" FOREIGN KEY ("ref_1__id_06") REFERENCES "too_many_refs" ("id_06") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_07" FOREIGN KEY ("ref_1__id_07") REFERENCES "too_many_refs" ("id_07") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_08" FOREIGN KEY ("ref_1__id_08") REFERENCES "too_many_refs" ("id_08") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_09" FOREIGN KEY ("ref_1__id_09") REFERENCES "too_many_refs" ("id_09") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_1__id_10" FOREIGN KEY ("ref_1__id_10") REFERENCES "too_many_refs" ("id_10") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_01" FOREIGN KEY ("ref_2__id_01") REFERENCES "too_many_refs" ("id_01") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_02" FOREIGN KEY ("ref_2__id_02") REFERENCES "too_many_refs" ("id_02") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_03" FOREIGN KEY ("ref_2__id_03") REFERENCES "too_many_refs" ("id_03") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_04" FOREIGN KEY ("ref_2__id_04") REFERENCES "too_many_refs" ("id_04") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_05" FOREIGN KEY ("ref_2__id_05") REFERENCES "too_many_refs" ("id_05") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_06" FOREIGN KEY ("ref_2__id_06") REFERENCES "too_many_refs" ("id_06") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_07" FOREIGN KEY ("ref_2__id_07") REFERENCES "too_many_refs" ("id_07") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_08" FOREIGN KEY ("ref_2__id_08") REFERENCES "too_many_refs" ("id_08") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_09" FOREIGN KEY ("ref_2__id_09") REFERENCES "too_many_refs" ("id_09") ON DELETE CASCADE;
            ALTER TABLE "uh_oh" ADD CONSTRAINT "fk_tuh_oh___ref_2__id_10" FOREIGN KEY ("ref_2__id_10") REFERENCES "too_many_refs" ("id_10") ON DELETE CASCADE;
            """
        ).strip()
        actual_sql, *actual_params = UhOhReferencingTableWithWayTooManyReferences.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
