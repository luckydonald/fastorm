from fastorm import FastORM
from tools_for_the_tests_of_fastorm import VerboseTestCase


class SimpleTable(FastORM):
    _table_name = 'simple_table'
    _primary_keys = ['id']

    id: int
    banana: str
# end class


class FastApiTestCase(VerboseTestCase):
    def test_dict_no_namespace(self):
        row = {'id': 12, 'banana': 'yellow'}

        result = SimpleTable.from_row(row)

        self.assertIsInstance(result, SimpleTable)
        self.assertEquals(12, result.id)
        self.assertEquals('yellow', result.banana)
    # end def

    def test_dict_with_namespace(self):
        row = {'simple_table id': 12, 'simple_table banana': 'yellow'}

        result = SimpleTable.from_row(row)

        self.assertIsInstance(result, SimpleTable)
        self.assertEquals(12, result.id)
        self.assertEquals('yellow', result.banana)
    # end def

    def test_list(self):
        row = [13, 'green']

        result = SimpleTable.from_row(row)

        self.assertIsInstance(result, SimpleTable)
        self.assertEquals(13, result.id)
        self.assertEquals('green', result.banana)
    # end def

    def test_tuple(self):
        row = [14, 'yellow-green']

        result = SimpleTable.from_row(row)

        self.assertIsInstance(result, SimpleTable)
        self.assertEquals(14, result.id)
        self.assertEquals('yellow-green', result.banana)
    # end def
# end class
