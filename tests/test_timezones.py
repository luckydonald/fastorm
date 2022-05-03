import datetime
import unittest
import pytz

from textwrap import dedent
from typing import Optional, Union, Any, Type, List, Tuple, Dict
from tools_for_the_tests_of_fastorm import extract_create_and_reference_sql_from_docstring, VerboseTestCase

from fastorm import FastORM


class TestTable(FastORM):
    some_datetime: datetime.datetime
# end class


# noinspection DuplicatedCode,PyShadowingBuiltins
class MyTestCase(VerboseTestCase):
    def test_datetime_with_local_timezone(self):
        timezone = datetime.timezone(offset=datetime.timedelta(hours=2,), name='plus two hour test timezone')

        local = datetime.datetime(year=2022, month=5, day=3, hour=13, minute=30, second=45, tzinfo=timezone)
        expected = datetime.datetime(year=2022, month=5, day=3, hour=11, minute=30, second=45)

        input = dict(some_datetime=local)
        output = TestTable._prepare_kwargs(**input, _allow_in=False)
        result = output[0]['some_datetime'].value

        self.assertEquals(expected, result)
    # end def

    def test_native_datetime(self):
        local = datetime.datetime.now()
        utc = datetime.datetime.utcnow()

        input = dict(some_datetime=local)
        output = TestTable._prepare_kwargs(**input, _allow_in=False)
        result = output[0]['some_datetime'].value

        if utc > result:
            diff: datetime.timedelta = (utc - result)
        else:
            diff: datetime.timedelta = (result - utc)
        # end if
        diff.total_seconds()

        self.assertLess(diff.total_seconds(), 10.0)
    # end def

    def test_datetime_with_utc_timezone(self):
        utc = datetime.datetime(year=2022, month=5, day=3, hour=11, minute=30, second=45, tzinfo=datetime.timezone.utc)
        expected = datetime.datetime(year=2022, month=5, day=3, hour=11, minute=30, second=45)

        input = dict(some_datetime=utc)
        output = TestTable._prepare_kwargs(**input, _allow_in=False)
        result = output[0]['some_datetime'].value

        self.assertEquals(expected, result)
    # end def
# end def


if __name__ == '__main__':
    unittest.main()
# end def
