#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Yes, this has a very long name, but we need to import it absolutely so better make sure it's unique.
"""
import contextlib
import unittest.case

from luckydonaldUtils.logger import logging

__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if


def remove_prefix(line, prefix):
    try:
        return line.removeprefix(prefix)
    except AttributeError:
        if line.startswith(prefix):
            return line[len(prefix):]
        # end if
        return line
    # end try
# end def


def extract_sql_from_docstring(cls):
    return "\n".join(remove_prefix(line, '        ') for line in cls.__doc__.strip().splitlines() if not line.strip().startswith('#'))
# end def


# noinspection PyUnresolvedReferences,PyProtectedMember
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
