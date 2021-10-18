#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'luckydonald'

from typing import Any, Union
from pydantic.fields import ModelField


TYPEHINT_TYPE = Union[type, ModelField]


# noinspection PyUnusedLocal
def check_is_union_type(variable: Any) -> bool:
    # as there's no UnionType, we can't have an instance of it.
    return False
# end def


def check_is_generic_alias(variable: Any) -> bool:
    # as there's no GenericAlias, we can't have an instance of it.
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


try:
    from types import GenericAlias

    def check_is_generic_alias(variable: Any) -> bool:
        return isinstance(variable, GenericAlias)
    # end def

    TYPEHINT_TYPE = Union[TYPEHINT_TYPE, GenericAlias]
except ImportError:
    pass
# end try
