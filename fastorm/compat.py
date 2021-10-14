#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'luckydonald'

from types import GenericAlias
from typing import Any, Union
from pydantic.fields import ModelField


TYPEHINT_TYPE = Union[GenericAlias, type, ModelField]


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

    TYPEHINT_TYPE = Union[TYPEHINT_TYPE, UnionType]
except ImportError:
    pass
# end try
