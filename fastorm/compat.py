#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'luckydonald'

from types import GenericAlias
from typing import Any, Union, Type
from pydantic.fields import ModelField


TYPEHINT_TYPE = Union[GenericAlias, type, ModelField]


# noinspection PyUnusedLocal
def check_is_union_type(variable: Any) -> bool:
    # as there's no UnionType, we can't have an instance of it.
    return False
# end def
try:
    from typing import Annotated

    class AnnotatedType:
        __origin__: type
        __metadata__: tuple
    # end class
except ImportError:
    class AnnotatedType(object):
        def __getitem__(self, item):
            if isinstance(item, tuple):
                return item[0]
            # end if
            return item
        # end def
    # end class
    Annotated = AnnotatedType()
# end def

AnnotatedType: Type[AnnotatedType] = type(Annotated[str, str])



try:
    from types import UnionType

    def check_is_union_type(variable: Any) -> bool:
        return isinstance(variable, UnionType)
    # end def

    TYPEHINT_TYPE = Union[TYPEHINT_TYPE, UnionType]
except ImportError:
    pass
# end try
