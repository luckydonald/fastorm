import unittest

from pydantic import BaseConfig
from pydantic.fields import ModelField

from fastorm import FastORM, FieldInfo, FieldItem
from tools_for_the_tests_of_fastorm import VerboseTestCase


class FirstTable(FastORM):
    _table_name = 'first_table'
    _primary_keys = ['id_part_1', 'id_part_2']
    _ignored_fields = []

    id_part_1: int
    id_part_2: str
    boring_non_id_field: int
# end class


class SecondTable(FastORM):
    _table_name = 'second_table'
    _primary_keys = ['cool_reference', 'additional_id']
    _ignored_fields = []
    cool_reference: FirstTable
    additional_id: int
    third_and_boring_field: int
# end class


class ThirdTable(FastORM):
    _table_name = 'third_table'
    _primary_keys = ['the_reference_has_been_doubled']
    _ignored_fields = []
    the_reference_has_been_doubled: SecondTable
    and_for_good_measure_here_is_another_field: FirstTable
# end class


class MyTestCase(VerboseTestCase):
    def test_first_table(self):
        expected = ['id_part_1', 'id_part_2', 'boring_non_id_field']
        result = FirstTable.get_sql_fields()
        self.assertListEqual(expected, result)
    # end def

    def test_second_table(self):
        expected = ['cool_reference__id_part_1', 'cool_reference__id_part_2', 'additional_id', 'third_and_boring_field']
        result = SecondTable.get_sql_fields()
        self.assertListEqual(expected, result)
    # end def

    def test_third_table(self):
        expected = [
            'the_reference_has_been_doubled__cool_reference__id_part_1',
            'the_reference_has_been_doubled__cool_reference__id_part_2',
            'the_reference_has_been_doubled__additional_id',
            'and_for_good_measure_here_is_another_field__id_part_1',
            'and_for_good_measure_here_is_another_field__id_part_2',
        ]
        result = ThirdTable.get_sql_fields()
        self.assertListEqual(expected, result)
    # end def
# end class


if __name__ == '__main__':
    unittest.main()
# end if
