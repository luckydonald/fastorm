#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'luckydonald'

from typing import Any




# noinspection PyUnusedLocal
def check_is_union_type(variable: Any) -> bool:
    # as there's no UnionType, we can't have an instance of it.
    return False
# end def

try:
    from types import UnionType

    def check_is_union_type(variable: Any) -> bool:
        return isinstance(variable, UnionType)
    # end def

except ImportError:
    pass
# end try
