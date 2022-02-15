import textwrap
import unittest
from textwrap import dedent
from typing import Optional, List

from pydantic import BaseConfig
from pydantic.fields import ModelField

from fastorm import FastORM, In, SqlFieldMeta, FieldInfo, FieldItem
from tools_for_the_tests_of_fastorm import VerboseTestCase


class SelfReferencingTable(FastORM):
    _table_name = 'self_referencing_table'
    _primary_keys = ['id']

    id: int
    self_ref_optional: Optional['SelfReferencingTable']
# end class


SelfReferencingTable.update_forward_refs()


class SelfReferencingTableTestCase(VerboseTestCase):
    def test_sql_text_create(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = dedent(
            """
            CREATE TABLE "self_referencing_table" (
              "id" BIGINT NOT NULL PRIMARY KEY,
              "self_ref_optional__id" BIGINT
            );
            """
        ).strip()
        actual_sql, *actual_params = SelfReferencingTable.build_sql_create()
        self.assertEqual(expected_sql, actual_sql, msg="create")
        self.assertListEqual([], actual_params, "create")
    # end def

    def test_sql_text_references(self):
        self.maxDiff = None
        # noinspection SqlNoDataSourceInspection,SqlResolve
        expected_sql = textwrap.dedent("""
        CREATE INDEX "idx_self_referencing_table___self_ref_optional__id" ON "self_referencing_table" ("self_ref_optional__id");
        ALTER TABLE "self_referencing_table" ADD CONSTRAINT "fk_self_referencing_table___self_ref_optional__id" FOREIGN KEY ("self_ref_optional__id") REFERENCES "self_referencing_table" ("id") ON DELETE CASCADE;
        """).strip()
        actual_sql, *actual_params = SelfReferencingTable.build_sql_references()
        self.assertEqual(expected_sql, actual_sql, msg="references")
        self.assertListEqual([], actual_params, "references")
    # end def

    def test__prepare_kwargs_single(self):
        actual = SelfReferencingTable._prepare_kwargs(id=1, _allow_in=True)

        expected = [
            {
                'id': SqlFieldMeta(
                    sql_name='id',
                    field_name='id',
                    type_=FieldInfo(is_primary_key=True, types=[FieldItem(field='id', type_=int)]),
                    field=FieldInfo(is_primary_key=True, types=[FieldItem(field='id', type_=ModelField(name='id', type_=int, required=True, class_validators={}, model_config=BaseConfig))]),
                    value=1
                )
            }
        ]

        self.assertEqual(str(expected), str(actual))
    # end def

    def test__prepare_kwargs_double(self):
        actual = SelfReferencingTable._prepare_kwargs(id=2, self_ref_optional=2, _allow_in=True)
        self.assertIsInstance(actual, list)
        self.assertEqual(2, len(actual))

        self.assertIsInstance(actual[0], dict)
        self.assertEqual(['id'], list(actual[0].keys()))
        self.assertIsInstance(actual[0]['id'], SqlFieldMeta)
        self.assertEqual('id', actual[0]['id'].field_name)
        self.assertEqual('id', actual[0]['id'].sql_name)
        self.assertIsInstance(actual[0]['id'].type_, FieldInfo)
        self.assertEqual(int, actual[0]['id'].type_.referenced_type)
        self.assertEqual(int, actual[0]['id'].type_.resulting_type)
        self.assertEqual(2, actual[0]['id'].value)

        self.assertIsInstance(actual[1], dict)
        self.assertEqual(['self_ref_optional__id'], list(actual[1].keys()))
        self.assertIsInstance(actual[1]['self_ref_optional__id'], SqlFieldMeta)
        self.assertEqual('self_ref_optional', actual[1]['self_ref_optional__id'].field_name)
        self.assertEqual('self_ref_optional__id', actual[1]['self_ref_optional__id'].sql_name)
        self.assertIsInstance(actual[1]['self_ref_optional__id'].type_, FieldInfo)
        self.assertEqual(SelfReferencingTable, actual[1]['self_ref_optional__id'].type_.referenced_type)
        self.assertEqual(int, actual[1]['self_ref_optional__id'].type_.resulting_type)
        self.assertEqual(2, actual[1]['self_ref_optional__id'].value)
    # end def

    def test__prepare_kwargs_double_with_union_on_pk(self):
        actual = SelfReferencingTable._prepare_kwargs(id=In[42, 69], self_ref_optional=4458, _allow_in=True)
        id_type = FieldInfo(is_primary_key=True, types=[FieldItem(field='id', type_=int)])
        id_field = FieldInfo(is_primary_key=True, types=[FieldItem(field='id', type_=ModelField(name='id', type_=int, required=True, class_validators={}, model_config=BaseConfig))])

        expected = [
            In[
                {
                    'id': SqlFieldMeta(
                        value=42,
                        sql_name='id', field_name='id',
                        field=id_field,
                        type_=id_type,
                    ),
                },
                {
                    'id': SqlFieldMeta(
                        value=69,
                        sql_name='id', field_name='id',
                        field=id_field,
                        type_=id_type
                    ),
                }
            ],
            {
                'self_ref_optional__id': SqlFieldMeta(
                    value=4458,
                    sql_name='self_ref_optional__id', field_name='self_ref_optional',
                    field=FieldInfo(is_primary_key=False, types=[FieldItem(field='self_ref_optional', type_=ModelField(name='self_ref_optional', type_=SelfReferencingTable, required=False, class_validators={}, model_config=BaseConfig)), FieldItem(field='id', type_=ModelField(name='id', type_=int, required=True, class_validators={}, model_config=BaseConfig))]),
                    type_=FieldInfo(is_primary_key=False, types=[FieldItem(field='self_ref_optional', type_=SelfReferencingTable), FieldItem(field='id', type_=int)]),
                ),
            }
        ]
        prep_result = lambda value: str(value).replace(",", ",\n")
        self.assertEqual(prep_result(expected), prep_result(actual))
    # end def

    def test_normal_select_single_field_pk(self):
        actual = SelfReferencingTable.build_sql_select(id=69)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","self_ref_optional__id" FROM "self_referencing_table" WHERE "id" = $1', 69

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_normal_select_single_field_non_pk(self):
        actual = SelfReferencingTable.build_sql_select(self_ref_optional=12)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","self_ref_optional__id" FROM "self_referencing_table" WHERE "self_ref_optional__id" = $1', 12

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def

    def test_normal_select_multi_field(self):
        actual = SelfReferencingTable.build_sql_select(id=1337, self_ref_optional=4458)
        # noinspection SqlResolve,SqlNoDataSourceInspection
        expected = 'SELECT "id","self_ref_optional__id" FROM "self_referencing_table" WHERE "id" = $1 AND "self_ref_optional__id" = $2', 1337, 4458,

        self.assertEqual(expected[0], actual[0], 'sql')
        self.assertEqual(expected[1:], actual[1:], 'variables')
    # end def# end class


# Loop with two tables


class TwoTableLoopA(FastORM):
    _table_name = 'two_table_loop_a'
    _primary_keys = ['id']

    id: int
    other_table_ref_optional: Optional['TwoTableLoopB']
# end class


if __name__ == '__main__':
    unittest.main()
# end if
