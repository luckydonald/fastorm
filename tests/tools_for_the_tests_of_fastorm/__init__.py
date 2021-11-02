#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Yes, this has a very long name, but we need to import it absolutely so better make sure it's unique.
"""


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
