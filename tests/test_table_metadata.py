import unittest

from test_create_table import SystemUnderTest, OtherTable, TheReferenceHasBeenDoubled
from tools_for_the_tests_of_fastorm import VerboseTestCase


class TableMetadataTestCase(VerboseTestCase):
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
        standard_ignored_fields = lambda cls: [
            '_table_name', '_ignored_fields', '_automatic_fields', '_primary_keys', '_database_cache',
            '__selectable_fields', '__fields_typehints', '__fields_references',
            f'_{cls.__name__!s}__selectable_fields', f'_{cls.__name__!s}__fields_typehints', f'_{cls.__name__!s}__fields_references',
            '__slots__'
        ]
        tables = {
            SystemUnderTest: [] + standard_ignored_fields(SystemUnderTest) + [],
            OtherTable: [] + standard_ignored_fields(OtherTable) + [],
            TheReferenceHasBeenDoubled: [] + standard_ignored_fields(TheReferenceHasBeenDoubled) + [],
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_ignored_fields())
            # end with
        # end for
    # end def

    def test_automatic_fields(self):
        tables = {
            SystemUnderTest: ['t0_id'],
            OtherTable: [],
            TheReferenceHasBeenDoubled: [],
        }
        for table, expected_name in tables.items():
            with self.subTest(msg=table.__name__):
                self.assertEqual(expected_name, table.get_automatic_fields())
            # end with
        # end for
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
