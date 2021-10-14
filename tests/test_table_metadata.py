import unittest

from test_create_table import SystemUnderTest, OtherTable, TheReferenceHasBeenDoubled


class TableMetadataTestCase(unittest.TestCase):
    def test_table_quoted_name(self):
        tables = {
            SystemUnderTest: '"cool_table_yo"',
            OtherTable: '"other_table"',
            TheReferenceHasBeenDoubled: '"double_reference"',
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_table())
            # end with
        # end for
    # end def

    def test_table_name(self):
        tables = {
            SystemUnderTest: 'cool_table_yo',
            OtherTable: 'other_table',
            TheReferenceHasBeenDoubled: 'double_reference',
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_name())
            # end with
        # end for
    # end def

    def test_primary_keys(self):
        tables = {
            SystemUnderTest: ['t0_id'],
            OtherTable: ['id_part_1', 'id_part_2'],
            TheReferenceHasBeenDoubled: ['another_reference'],
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_primary_keys_keys())
            # end with
        # end for
    # end def

    def test_ignored_fields(self):
        tables = {
            SystemUnderTest: ['_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache', '__selectable_fields', f'_{SystemUnderTest.__name__!s}__selectable_fields', '__slots__'],
            OtherTable: ['_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache', '__selectable_fields', f'_{OtherTable.__name__!s}__selectable_fields', '__slots__'],
            TheReferenceHasBeenDoubled: ['_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache', '__selectable_fields', f'_{TheReferenceHasBeenDoubled.__name__!s}__selectable_fields', '__slots__'],
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_ignored_fields())
            # end with
        # end for
    # end def

    def test_automatic_fields(self):
        tables = {
            SystemUnderTest: ['_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache', '__selectable_fields', f'_{SystemUnderTest.__name__!s}__selectable_fields', '__slots__'],
            OtherTable: ['_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache', '__selectable_fields', f'_{OtherTable.__name__!s}__selectable_fields', '__slots__'],
            TheReferenceHasBeenDoubled: ['_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache', '__selectable_fields', f'_{TheReferenceHasBeenDoubled.__name__!s}__selectable_fields', '__slots__'],
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_ignored_fields())
            # end with
        # end for
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
