import typing
import unittest
from typing import Union

from fastorm import FastORM, IS_PYTHON_3_9, ModelMetaclassFastORM


class MyTestCase(unittest.TestCase):
    def test_unchanged(self):
        class TableDoubleKey(FastORM):
            _table_name = 'table_double_key'
            _automatic_fields = []
            _primary_keys = ['id_a', 'id_b']

            id_a: int
            id_b: Union[int, None]
            name: str
            number: int
        # end class

        expected_old_annotations = {'id_a': int, 'id_b': typing.Optional[int], 'name': str, 'number': int}
        expected_new_annotations = expected_old_annotations

        self.assertEqual(expected_old_annotations, TableDoubleKey.__original__annotations__)
        self.assertEqual(expected_new_annotations, TableDoubleKey.__annotations__)
    # end def

    def test_automatic_value(self):
        class Table(FastORM):
            _table_name = 'table'
            _automatic_fields = ['id']
            _primary_keys = []

            id: int
            number: int
        # end class

        expected_old_annotations = {'id': int, 'number': int}
        expected_new_annotations = {'id': typing.Optional[int], 'number': int}
        self.assertEqual(expected_old_annotations, Table.__original__annotations__)
        self.assertEqual(expected_new_annotations, Table.__annotations__)
    # end def

    def test_automatic_value_with_optional(self):
        class Table(FastORM):
            _table_name = 'table'
            _automatic_fields = ['id']
            _primary_keys = []

            id: typing.Optional[int]
            number: int
        # end class

        expected_old_annotations = {'id': typing.Optional[int], 'number': int}
        expected_new_annotations = {'id': typing.Optional[int], 'number': int}
        self.assertEqual(expected_old_annotations, Table.__original__annotations__)
        self.assertEqual(expected_new_annotations, Table.__annotations__)
    # end def

    def test_ref(self):
        class Table(FastORM):
            _table_name = 'table'
            _automatic_fields = []
            _primary_keys = ['id']

            id: int
        # end class

        class Reference(FastORM):
            ref: Table
        # end class

        expected_old_annotations = {'ref': Table}
        expected_new_annotations = {'ref': typing.Union[Table, int]}
        self.assertEqual(expected_old_annotations, Reference.__original__annotations__)
        self.assertEqual(expected_new_annotations, Reference.__annotations__)
    # end def

    def test_ref_with_union(self):
        class Table(FastORM):
            _table_name = 'table'
            _automatic_fields = []
            _primary_keys = ['id']

            id: int
        # end class

        class Reference(FastORM):
            ref: Union[Table, int]
        # end class

        expected_old_annotations = {'ref': typing.Union[Table, int]}
        expected_new_annotations = {'ref': typing.Union[Table, int]}
        self.assertEqual(expected_old_annotations, Reference.__original__annotations__)
        self.assertEqual(expected_new_annotations, Reference.__annotations__)
    # end def

    def test_ref_tuple(self):
        class Table(FastORM):
            _table_name = 'table'
            _automatic_fields = []
            _primary_keys = ['id_1', 'id_2']

            id_1: int
            id_2: str
        # end class

        class Reference(FastORM):
            ref: Table
        # end class

        expected_old_annotations = {'ref': Table}
        expected_new_annotations = {'ref': typing.Union[Table, typing.Tuple[int, str]]}

        with self.subTest('old'):
            self.assertEqual(expected_old_annotations, Reference.__original__annotations__)
        # end with
        with self.subTest('new'):
            self.assertEqual(expected_new_annotations, Reference.__annotations__)
        # end with
    # end def

    def test_ref_tuple_with_union(self):
        class Table(FastORM):
            _table_name = 'table'
            _automatic_fields = []
            _primary_keys = ['id_1', 'id_2']

            id_1: int
            id_2: str
        # end class

        class Reference(FastORM):
            ref: Union[Table, typing.Tuple[int,str]]
        # end class

        expected_old_annotations = {'ref': typing.Union[Table, typing.Tuple[int, str]]}
        expected_new_annotations = {'ref': typing.Union[Table, typing.Tuple[int, str]]}

        with self.subTest('old'):
            self.assertEqual(expected_old_annotations, Reference.__original__annotations__)
        # end with
        with self.subTest('new'):
            self.assertEqual(expected_new_annotations, Reference.__annotations__)
        # end with
    # end def

    @unittest.skipIf(not IS_PYTHON_3_9, 'skip if low python version')
    def test_type_deduplication_py39(self):
        similar_stuffsies = {
            "tuple": [tuple[str, int], typing.Tuple[str, int]],
            "tuple with elipsis": [tuple[str, ...], typing.Tuple[str, ...], typing.Tuple[str, Ellipsis], tuple[str, Ellipsis]],
            "dict": [dict[str, int], typing.Dict[str, int]],
            "list": [list[str], typing.List[str]],
        }
        for key, similar_stuff in similar_stuffsies.items():
            with self.subTest(msg=key):
                result = ModelMetaclassFastORM.deduplicate_types(similar_stuff)
                self.assertEqual(1, len(result), msg=key)
            # end with
        # end for
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
