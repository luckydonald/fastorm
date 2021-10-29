import contextlib
import unittest.case

from fastorm import FastORM

FieldReference = FastORM.FieldReference
Item = FastORM.FieldReferenceItem


# noinspection PyUnresolvedReferences
_subtest_msg_sentinel = unittest.case._subtest_msg_sentinel



class VerboseTestCase(unittest.TestCase):
    show_real_diffs_in_pycharm_instead_of_having_subtests = True

    def subTest(self, msg=_subtest_msg_sentinel, **params):
        if not VerboseTestCase.show_real_diffs_in_pycharm_instead_of_having_subtests:
            return super().subTest(msg=msg, **params)
        else:
            @contextlib.contextmanager
            def subTestNoOP(msg=msg, **params):
                yield
            # end def
            return subTestNoOP(msg=msg, **params)
        # end if
    # end def

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

        with self.subTest():
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id_part_1': FieldReference(True, [Item('id_part_1', int)]),
                'id_part_2': FieldReference(True, [Item('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest():
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id_part_1': FieldReference(True, [Item('id_part_1', int)]),
                'id_part_2': FieldReference(True, [Item('id_part_2', str)]),
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

        with self.subTest():
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'text': FieldReference(False, [Item('text', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest():
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'text': FieldReference(False, [Item('text', str)]),
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
        with self.subTest():
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id_part_1': FieldReference(True, [Item('id_part_1', int)]),
                'id_part_2': FieldReference(True, [Item('id_part_2', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest():
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id_part_1': FieldReference(True, [Item('id_part_1', int)]),
                'id_part_2': FieldReference(True, [Item('id_part_2', str)]),
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

        with self.subTest():
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'text': FieldReference(False, [Item('text', str)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest():
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'text': FieldReference(False, [Item('text', str)]),
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
                'id': FieldReference(True, [Item('id', int)]),
                'test_one': FieldReference(False, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with
        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'test_one__id_part_1': FieldReference(False, [Item('test_one', Test1), Item('id_part_1', str)]),
                'test_one__id_part_2': FieldReference(False, [Item('test_one', Test1), Item('id_part_2', str)]),
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
                'id': FieldReference(True, [Item('id', int)]),
                'test_one': FieldReference(False, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end if

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'test_one__id_part_1': FieldReference(False, [Item('test_one', Test1), Item('id_part_1', str)]),
                'test_one__id_part_2': FieldReference(False, [Item('test_one', Test1), Item('id_part_2', str)]),
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
                'id': FieldReference(True, [Item('id', int)]),
                'test_one': FieldReference(False, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'test_one__id_part_1': FieldReference(False, [Item('test_one', Test1), Item('id_part_1', str)]),
                'test_one__id_part_2': FieldReference(False, [Item('test_one', Test1), Item('id_part_2', str)]),
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
                'id': FieldReference(True, [Item('id', int)]),
                'test_one': FieldReference(False, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(True, [Item('id', int)]),
                'test_one__id_part_1': FieldReference(False, [Item('test_one', Test1), Item('id_part_1', str)]),
                'test_one__id_part_2': FieldReference(False, [Item('test_one', Test1), Item('id_part_2', str)]),
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
                'id': FieldReference(False, [Item('id', int)]),
                'test_one': FieldReference(True, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(False, [Item('id', int)]),
                'test_one__id_part_1': FieldReference(True, [Item('test_one', Test1), Item('id_part_1', int)]),
                'test_one__id_part_2': FieldReference(True, [Item('test_one', Test1), Item('id_part_2', str)]),
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
                'id': FieldReference(False, [Item('id', int)]),
                'test_one': FieldReference(True, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs)
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'id': FieldReference(False, [Item('id', int)]),
                'test_one__id': FieldReference(True, [Item('test_one', Test1), Item('id', int)]),
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

        with self.subTest(msg='Test1, recursive=False'):
            refs = Test1.get_fields_references(recursive=False)
            expected = {
                'id_part_1': FieldReference(True, [Item('id_part_1', int)]),
                'id_part_2': FieldReference(True, [Item('id_part_2', str)]),
            }
            self.assertEqual(expected, refs, msg='Test1, recursive=False')
        # end with

        with self.subTest(msg='Test1, recursive=True'):
            refs = Test1.get_fields_references(recursive=True)
            expected = {
                'id_part_1': FieldReference(True, [Item('id_part_1', int)]),
                'id_part_2': FieldReference(True, [Item('id_part_2', str)]),
            }
            self.assertEqual(expected, refs, msg='Test1, recursive=True')
        # end with

        class Test2(FastORM):
            _table_name = 'test2'
            _primary_keys = ['test_one']

            some_number: int
            test_one: Test1
        # end class

        with self.subTest(msg='Test2, recursive=False'):
            refs = Test2.get_fields_references(recursive=False)
            expected = {
                'some_number': FieldReference(False, [Item('some_number', int)]),
                'test_one': FieldReference(True, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs, msg='Test2, recursive=False')
        # end with

        with self.subTest(msg='Test2, recursive=True'):
            refs = Test2.get_fields_references(recursive=True)
            expected = {
                'some_number': FieldReference(False, [Item('some_number', int)]),
                'test_one__id_part_1': FieldReference(True, [Item('test_one', Test1)]),
                'test_one__id_part_2': FieldReference(True, [Item('test_one', Test1)]),
            }
            self.assertEqual(expected, refs, msg='Test2, recursive=True')
        # end with

        class Test3(FastORM):
            _table_name = 'test3'
            _primary_keys = ['test_two']

            test_two: Test2
            title: str
        # end class

        with self.subTest(msg='Test3, recursive=False'):
            refs = Test3.get_fields_references(recursive=False)
            expected = {
                'test_two': FieldReference(True, [Item('test_two', Test2)]),
                'title': FieldReference(False, [Item('title', str)]),
            }
            self.assertEqual(expected, refs, msg='Test3, recursive=False')
        # end with

        with self.subTest(msg='Test3, recursive=True'):
            refs = Test3.get_fields_references(recursive=True)
            expected = {
                'test_two__test_one__id_part_1': FieldReference(True, [Item('test_two', Test2), Item('test_one', Test1), Item('id_part_1', int)]),
                'test_two__test_one__id_part_2': FieldReference(True, [Item('test_two', Test2), Item('test_one', Test1), Item('id_part_2', str)]),
                'title': FieldReference(False, [Item('title', str)]),
            }
            self.assertEqual(expected, refs, msg='Test3, recursive=True')
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
                'test_two': FieldReference(True, [Item('test_two', Test2)]),
                'test_one_b': FieldReference(True, [Item('test_one_b', Test1B)]),
                'title': FieldReference(False, [Item('title', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=False')
        # end with

        with self.subTest(msg='recursive=True'):
            refs = Test3.get_fields_references(recursive=True)
            expected = {
                'test_two__test_one_a__id_part_1': FieldReference(True, [Item('test_two', Test2), Item('test_one_a', Test1A), Item('id_part_1', int)]),
                'test_two__test_one_a__id_part_2': FieldReference(True, [Item('test_two', Test2), Item('test_one_a', Test1A), Item('id_part_2', str)]),
                'test_one_b__id': FieldReference(True, [Item('test_one_b', Test1B), Item('id', int)]),
                'title': FieldReference(False, [Item('title', str)]),
            }
            self.assertEqual(expected, refs, msg='recursive=True')
        # end with
    # end def

if __name__ == '__main__':
    unittest.main()
