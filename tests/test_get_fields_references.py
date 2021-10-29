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

        with self.subTest(msg='recursive=False'):
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id_part_1': (True, [('id_part_1', int)]),
                'id_part_2': (True, [('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id_part_1': (True, [('id_part_1', int)]),
                'id_part_2': (True, [('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
    # end def

    def test_simple_single_key_no_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        with self.subTest(msg='recursive=False'):
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id': (True, [('id', int)]),
                'text': (False, [('text', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'text': (False, [('text', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
    # end def

    def test_simple_double_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class
        with self.subTest(msg='recursive=False'):
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id_part_1': (True, [('id_part_1', int)]),
                'id_part_2': (True, [('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id_part_1': (True, [('id_part_1', int)]),
                'id_part_2': (True, [('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
    # end def

    def test_simple_single_key_recursive(self):
        class Test1(FastORM):
            _table_name = 'test1'
            _primary_keys = ['id']

            id: int
            text: str
        # end class

        with self.subTest(msg='recursive=False'):
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'text': (False, [('text', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'text': (False, [('text', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
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

        with self.subTest(msg='recursive=False'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'id': (True, [('id', int)]),
                'test_one': (False, [('test_one', Test1)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with
        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'test_one__id_part_1': (False, [('test_one', Test1), ('id_part_1', str)]),
                'test_one__id_part_2': (False, [('test_one', Test1), ('id_part_2', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=True')
        # end with
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

        with self.subTest(msg='recursive=False'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'id': (True, [('id', int)]),
                'test_one': (False, [('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end if

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'test_one__id_part_1': (False, [('test_one', Test1), ('id_part_1', str)]),
                'test_one__id_part_2': (False, [('test_one', Test1), ('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end if
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

        with self.subTest(msg='recursive=False'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'id': (True, [('id', int)]),
                'test_one': (False, [('test_one', Test1)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'test_one__id_part_1': (False, [('test_one', Test1), ('id_part_1', str)]),
                'test_one__id_part_2': (False, [('test_one', Test1), ('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
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

        with self.subTest(msg='recursive=False'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'id': (True, [('id', int)]),
                'test_one': (False, [('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': (True, [('id', int)]),
                'test_one__id_part_1': (False, [('test_one', Test1), ('id_part_1', str)]),
                'test_one__id_part_2': (False, [('test_one', Test1), ('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
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

        with self.subTest(msg='recursive=False'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'id': (False, [('id', int)]),
                'test_one': (True, [('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': (False, [('id', int)]),
                'test_one__id_part_1': (True, [('test_one', Test1), ('id_part_1', int)]),
                'test_one__id_part_2': (True, [('test_one', Test1), ('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with
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

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'id': (False, [('id', int)]),
                'test_one': (True, [('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': (False, [('id', int)]),
                'test_one__id': (True, [('test_one', Test1), ('id', int)]),
            }
            self.assertEqual(expected, refs)
        # end with
    # end def
# end class


class GetFieldsReferencesMultiLayerReferenceTest(VerboseTestCase):
    def test_two_level_on_double_reference_1(self):
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

        with self.subTest(msg='recursive=False'):
            refs = Test3.get_fields_references(recursive=False)
            expected = {
                'test_two': (True, [('test_two', Test2)]),
                'title': (False, [('title', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test3.get_fields_references(recursive=True)
            expected = {
                'test_two__test_one__id_part_1': (True, [('test_two', Test2), ('test_one', Test1), ('id_part_1', int)]),
                'test_two__test_one__id_part_2': (True, [('test_two', Test2), ('test_one', Test1), ('id_part_2', str)]),
                'title': (False, [('title', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=True')
        # end with
    # end def

    def test_two_level_on_double_reference_2(self):
        class Test1A(FastORM):
            _table_name = 'test1a'
            _primary_keys = ['id_part_1', 'id_part_2']

            id_part_1: int
            id_part_2: str
        # end class

        class Test1B(FastORM):
            _table_name = 'test2b'
            _primary_keys = ['id']

            id: int
        # end class

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['test_one_a', 'test_one_b']

            some_number: int
            test_one_a: Test1A
            test_one_b: Test1B
        # end class

        class Test3(FastORM):
            _table_name = 'test3'
            _primary_keys = ['test_two', 'test_one_b']

            test_two: Test2
            test_one_b: Test1B
            title: str
        # end class

        with self.subTest(msg='recursive=False'):
            refs = Test3.get_fields_references(recursive=False)
            expected = {
                'test_two': (True, [('test_two', Test2)]),
                'test_one_b': (True, [('test_one_b', Test1B)]),
                'title': (False, [('title', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test3.get_fields_references(recursive=True)
            expected = {
                'test_two__test_one_a__id_part_1': (True, [('test_two', Test2), ('test_one_a', Test1A), ('id_part_1', int)]),
                'test_two__test_one_a__id_part_2': (True, [('test_two', Test2), ('test_one_a', Test1A), ('id_part_2', str)]),
                'test_one_b__id': (True, [('test_one_b', Test1B), ('id', int)]),
                'title': (False, [('title', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=True')
        # end with
    # end def

if __name__ == '__main__':
    unittest.main()
