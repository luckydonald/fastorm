import unittest
from textwrap import dedent

from fastorm import FastORM


class OtherTable(FastORM):
    _table_name = 'cool_table_name_yoooo'
    _primary_keys = ['id_part_1', 'id_part_2']
    _ignored_fields = []

    id_part_1: int
    id_part_2: str
    foo: float
    bar: str
# end class


class UpdateRowTestCase(unittest.TestCase):
    def test_foo(self):
        object = OtherTable(
            id_part_1=12,
            id_part_2='banana',
            foo=2.35,
            bar="kiwi",
        )

        sql, *where_values = object.build_sql_delete()
        # noinspection SqlResolve,SqlNoDataSourceInspection
        self.assertEqual(
            dedent("""
                DELETE FROM "cool_table_name_yoooo"
                 WHERE "id_part_1" = $1 AND "id_part_2" = $2
                ;
            """).strip(),
            sql,
            "the produced update SQL is correct"
        )
    # end def
# end class


class PrimaryKeyIsReferenceTable(FastORM):
    _table_name = 'primary_key_is_reference'
    _primary_keys = ['that_other_table']
    _ignored_fields = []

    that_other_table: OtherTable
    fooz: float
    barz: str
# end class


class PrimaryKeyIsReferenceTestCase(unittest.TestCase):
    def test_foo(self):
        object = PrimaryKeyIsReferenceTable(
            that_other_table=(12, 'text'),
            fooz=2.35,
            barz="kiwi",
        )

        sql, *where_values = object.build_sql_delete()
        # noinspection SqlResolve,SqlNoDataSourceInspection
        self.assertEqual(
            dedent("""
                DELETE FROM "primary_key_is_reference"
                 WHERE "that_other_table__id_part_1" = $1 AND "that_other_table__id_part_2" = $2
                ;
            """).strip(),
            sql,
            "the produced update SQL is correct"
        )
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
