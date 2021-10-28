import unittest

from fastorm import FastORM


class VerboseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None
    # end def
# end def


class GetFieldsReferencesSimpleTest(VerboseTestCase):
    def test_simple_double_key_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        refs = Test1.get_fields_references(recursive=False)
        expected = {'id_part_1': (True, int, None), 'id_part_2': (True, str, None)}
        self.assertEqual(expected, refs)
    # end def

    def test_simple_single_key_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        refs = Test1.get_fields_references(recursive=False)
        expected = {'id': (True, int, None), 'text': (False, str, None)}
        self.assertEqual(expected, refs)
    # end def

    def test_simple_double_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        refs = Test1.get_fields_references(recursive=True)
        expected = {'id_part_1': (True, int, None), 'id_part_2': (True, str, None)}
        self.assertEqual(expected, refs)
    # end def

    def test_simple_single_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        refs = Test1.get_fields_references(recursive=True)
        expected = {'id': (True, int, None), 'text': (False, str, None)}
        self.assertEqual(expected, refs)
    # end def
# end def


class GetFieldsReferencesSingleReferenceTest(VerboseTestCase):
    def test_single_reference_double_key_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['id']

            id: int
            test_one: Test1
        # end class

        refs = Test2.get_fields_references(recursive=False)
        expected = {'id': (True, int, None), 'test_one': (False, Test1, {'id_part_1': 'test_one__id_part_1', 'id_part_2': 'test_one__id_part_2'})}
        self.assertEqual(expected, refs)
    # end def

    def test_single_reference_no_pk_single_key_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['id']

            id: int
            test_one: Test1
        # end class

        refs = Test2.get_fields_references(recursive=False)
        expected = {'id': (True, int, None), 'test_one': (False, Test1, {'id': 'test_one__id'})}
        self.assertEqual(expected, refs)
    # end def

    def test_single_reference_no_pk_double_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['id']

            id: int
            test_one: Test1
        # end class

        refs = Test2.get_fields_references(recursive=True)
        expected = {'id': (True, int, None), 'test_one': (False, Test1, {'id_part_1': 'test_one__id_part_1', 'id_part_2': 'test_one__id_part_2'})}
        self.assertEqual(expected, refs)
    # end def

    def test_single_reference_no_pk_single_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['id']

            id: int
            test_one: Test1
        # end class

        refs = Test2.get_fields_references(recursive=True)
        expected = {'id': (True, int, None), 'test_one': (False, Test1, {'id': 'test_one__id'})}
        self.assertEqual(expected, refs)
    # end def

    def test_single_reference_stacked_pk_key_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['test_one']

            id: int
            test_one: Test1
        # end class

        refs = Test2.get_fields_references(recursive=False)
        expected = {'id': (False, int, None), 'test_one': (True, Test1, {'id_part_1': 'test_one__id_part_1', 'id_part_2': 'test_one__id_part_2'})}
        self.assertEqual(expected, refs)
    # end def

    def test_single_reference_single_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['test_one']

            id: int
            test_one: Test1
        # end class

        refs = Test2.get_fields_references(recursive=True)
        expected = {'id': (False, int, None), 'test_one': (True, Test1, {'id': 'test_one__id'})}
        self.assertEqual(expected, refs)
    # end def
# end class


class GetFieldsReferencesMultiLayerReferenceTest(VerboseTestCase):
    def test_two_level_on_double_reference_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['test_one']

            some_number: int
            test_one: Test1
        # end class

        class Test3(FastORM):
            _table_name = 'test3'
            _primary_keys = ['test_two']

            test_two: Test2
            title: str
        # end class

        refs = Test3.get_fields_references(recursive=False)
        expected = {'test_two': (True, Test2, {'test_one': 'test_two__test_one'}), 'title': (False, str, None)}
        # expected = {'test_two': (True, Test2, {'id_part_1': 'test_one__id_part_1', 'id_part_2': 'test_one__id_part_2'}), 'title': (False, str, None)}
        self.assertEqual(expected, refs)
    # end def

if __name__ == '__main__':
    unittest.main()
