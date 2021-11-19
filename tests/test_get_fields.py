import unittest
from fastorm import FastORM


class OtherTable(FastORM):
    _table_name = 'other_table'
    _primary_keys = ['id_part_1', 'id_part_2']
    _ignored_fields = []

    id_part_1: int
    id_part_2: str
# end class


class ActualTable(FastORM):
    _table_name = 'actual_table'
    _primary_keys = ['cool_reference']
    _ignored_fields = []
    cool_reference: OtherTable
# end class


class ThirdTable(FastORM):
    _table_name = 'third_table'
    _primary_keys = ['the_reference_has_been_doubled']
    _ignored_fields = []
    the_reference_has_been_doubled: ActualTable
# end class


class MyTestCase(unittest.TestCase):
    def test_flattened(self):
        type_hints = ActualTable.get_fields_typehints(flatten_table_references=True)
        """
        {
            'cool_reference__id_part_1': ModelField(name='id_part_1', type=int, required=True),
            'cool_reference__id_part_2': ModelField(name='id_part_2', type=str, required=True)
        }
        """
        print(type_hints)
        self.assertListEqual(list(type_hints.keys()), ['cool_reference__id_part_1', 'cool_reference__id_part_2'])
        self.assertEqual(int, type_hints['cool_reference__id_part_1'].types[-1].type_.type_)
        self.assertEqual(str, type_hints['cool_reference__id_part_2'].types[-1].type_.type_)
    # end def

    def test_not_fattened(self):
        type_hints = ActualTable.get_fields_typehints(flatten_table_references=False)
        """
        {
            'cool_reference': ModelField(name='cool_reference', type=OtherTable, required=True)
        }
        """
        self.assertListEqual(['cool_reference'], list(type_hints.keys()))
        self.assertEqual(OtherTable, type_hints['cool_reference'].types[0].type_.type_)
    # end def

    @unittest.skip('Currently out of scope.')
    def test_flattened_double_level(self):
        type_hints = ThirdTable.get_fields_typehints(flatten_table_references=True)
        """
        {
            'the_reference_has_been_doubled__cool_reference__id_part_1': ModelField(name='id_part_1', type=int, required=True),
            'the_reference_has_been_doubled__cool_reference__id_part_2': ModelField(name='id_part_2', type=str, required=True)
        }
        """
        print(type_hints)
        self.assertListEqual(['the_reference_has_been_doubled__cool_reference__id_part_1', 'the_reference_has_been_doubled__cool_reference__id_part_2'], list(type_hints.keys()), )
        self.assertEqual(int, type_hints['the_reference_has_been_doubled__cool_reference__id_part_1'].type_)
        self.assertEqual(str, type_hints['the_reference_has_been_doubled__cool_reference__id_part_2'].type_)
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
# end if
