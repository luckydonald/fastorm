import unittest
from textwrap import dedent

from fastorm import FastORM


class OtherTable(FastORM):
    """
        CREATE TABLE "other_table" (
          "id_part_1" TEXT NOT NULL,
          "id_part_2" BIGINT NOT NULL,
          PRIMARY KEY ("id_part_1", "id_part_2")
        );
    """
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
        self.assertDictEqual(
            {},
            object._database_cache,
            "database cache starts empty"
        )

        self.assertDictEqual(
            {'id_part_1': 12, 'id_part_2': 'banana', 'foo': 2.35, 'bar': 'kiwi'},
            object.get_changes(),
            "get_changes() identifies initial changes correctly"
        )

        object._database_cache_overwrite_with_current()

        self.assertDictEqual(
            {},
            object.get_changes(),
            "get_changes() identifies no changes after they were synced"
        )

        self.assertDictEqual(
            {'id_part_1': 12, 'id_part_2': 'banana', 'foo': 2.35, 'bar': 'kiwi'},
            object._database_cache,
            "database cache synced"
        )

        self.assertEqual(
            2.35,
            object.foo,
            "check that object.foo is correct before the change"
        )

        object.foo = 69.42

        self.assertEqual(
            69.42,
            object.foo,
            "check that object.foo is correct after the change"
        )

        self.assertDictEqual(
            {'id_part_1': 12, 'id_part_2': 'banana', 'foo': 2.35, 'bar': 'kiwi'},
            object._database_cache,
            "database cache still unchanged after property value change"
        )

        self.assertDictEqual(
            {"foo": 69.42},
            object.get_changes(),
            "get_changes() identifies the change in the foo attribute correctly"
        )


        sql, *where_values = object.build_sql_update()
        # noinspection SqlResolve,SqlNoDataSourceInspection
        self.assertEqual(
            dedent("""
                UPDATE "cool_table_name_yoooo"
                 SET "foo" = $1
                 WHERE "id_part_1" = $2 AND "id_part_2" = $3
                ;
            """).strip(),
            sql,
            "the produced update SQL is correct"
        )

        self.assertEqual(
            [   # the value to change:
                69.42,
                # and the primary key(s):
                12, "banana",
            ],
            where_values,
            "make sure that the update's actual values are correct."
        )
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
