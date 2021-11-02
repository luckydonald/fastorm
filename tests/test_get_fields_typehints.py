import contextlib
import unittest.case
from textwrap import dedent

from pydantic import BaseConfig
from pydantic.fields import ModelField

from fastorm import FastORM
from fastorm.classes import FieldInfo, FieldItem

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

        class OtherTable(FastORM):
            _table_name = 'other_table'
            _primary_keys = ['id_part_1', 'id_part_2']
            id_part_1: int
            id_part_2: str

        class ActualTable(FastORM):
            _table_name = 'actual_table'
            _primary_keys = ['cool_reference']
            cool_reference: OtherTable


        with self.subTest():
            expected = dedent("""
            {'cool_reference': FieldInfo(is_primary_key=True, types=[FieldItem(field='cool_reference', type_=ModelField(name='cool_reference', type=OtherTable, required=True))])}
            """).strip()
            refs = ActualTable.get_fields_typehints(flatten_table_references=False)
            self.assertEqual(expected, repr(refs))
        # end with
        with self.subTest():
            expected = dedent("""
            {'cool_reference__id_part_1': FieldInfo(is_primary_key=True, types=[FieldItem(field='cool_reference', type_=ModelField(name='cool_reference', type=OtherTable, required=True)), FieldItem(field='id_part_1', type_=ModelField(name='id_part_1', type=int, required=True))]), 'cool_reference__id_part_2': FieldInfo(is_primary_key=True, types=[FieldItem(field='cool_reference', type_=ModelField(name='cool_reference', type=OtherTable, required=True)), FieldItem(field='id_part_2', type_=ModelField(name='id_part_2', type=str, required=True))])}            """).strip()
            refs = ActualTable.get_fields_typehints(flatten_table_references=True)
            self.assertEqual(expected, repr(refs))
        # end with
    # end def
# end class

if __name__ == '__main__':
    unittest.main()
