import unittest
from textwrap import dedent

from fastorm import FastORM


class OtherTable(FastORM):
    """
        CREATE TABLE "other_table" (
          "id_part_1" TEXT NOT NULL,
          "id_part_2" BIGINT NOT NULL,
          PRIMARY KEY ("id_part_1", "id_part_2")
        )
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


if __name__ == '__main__':
    unittest.main()
# end if
