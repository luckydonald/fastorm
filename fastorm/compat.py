#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'luckydonald'

import sys
from typing import Any, Union, Dict
from pydantic.fields import ModelField


TYPEHINT_TYPE = Union[type, ModelField]

IS_PYTHON_3_7 = sys.version_info[:3] >= (3, 7, 0)
IS_PYTHON_3_10 = sys.version_info[:3] >= (3, 10, 0)


# noinspection PyUnusedLocal
def check_is_union_type(variable: Any) -> bool:
    # as there's no UnionType, we can't have an instance of it.
    return False
# end def


def check_is_generic_alias(variable: Any) -> bool:
    # as there's no GenericAlias, we can't have an instance of it.
    return False
# end def


def check_is_annotated_type(variable: Any) -> bool:
    # as there's no Annotated, we can't have an instance of it.
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
    from typing import List

    GenericAliasOld = type(List[int])

    def check_is_generic_alias(variable: Any) -> bool:
        return (
            isinstance(variable, GenericAlias) or  # type(list[int])
            isinstance(variable, GenericAliasOld)  # type(List[int])
        )
    # end def

    TYPEHINT_TYPE = Union[TYPEHINT_TYPE, GenericAlias, GenericAliasOld]
except ImportError:
    pass
# end try

Annotated = None

try:
    try:
        from typing import Annotated  # 3.10
    except ImportError:
        from typing_extensions import Annotated  # 3.7
    # end try

    AnnotatedType = type(Annotated)


    def check_is_annotated_type(variable: Any) -> bool:
        # as there's no Annotated, we can't have an instance of it.
        return isinstance(variable, AnnotatedType)
    # end def
except ImportError:
    pass
# end try

try:
    from types import NoneType
except:
    NoneType = type(None)
# end try

if IS_PYTHON_3_10:
    from typing import get_type_hints

    def get_type_hints_with_annotations(cls) -> Dict[str, any]:
        return get_type_hints(cls, include_extras=True)
    # end def
elif not IS_PYTHON_3_7:
    from typing import get_type_hints

    def get_type_hints_with_annotations(cls) -> Dict[str, any]:
         return get_type_hints(cls, include_extras=True)
    # end def
else:  # pre 3.7
    from typing import get_type_hints

    def get_type_hints_with_annotations(cls) -> Dict[str, any]:
         return get_type_hints(cls)
    # end def
# end def

