# based on SQLAlchemy's util/typing.py
# https://github.com/sqlalchemy/sqlalchemy/blob/rel_2_0_36/lib/sqlalchemy/util/typing.py
#
# Which is licensed under the MIT License, including the following notice:
#
# Copyright (C) 2022-2024 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: https://www.opensource.org/licenses/mit-license.php
#
# My own modifications to this file are also released under the MIT License,
# Copyright (C) 2025, luckydonald, FastORM authors and FastORM contributors.
#
# mypy: allow-untyped-defs, allow-untyped-calls
from types import NoneType
from typing import Any, TypeGuard, Optional, Tuple, Protocol, ForwardRef, Union, NewType, TypeAliasType, Type, TypeVar, \
    get_args
from typing import get_origin as typing_get_origin
from sys import version_info


# noinspection PyPep8Naming
class compat:
    # calculate if we are running on Python 3.10 or later

    py310  = (3, 10) <=  tuple(version_info[:2])
    py36 = (3, 6) <= tuple(version_info[:2])
# end class


_T = TypeVar("_T", bound=Any)


_AnnotationScanType = Union[
    Type[Any], str, ForwardRef, NewType, TypeAliasType, "GenericProtocol[Any]"
]

class GenericProtocol(Protocol[_T]):
    """protocol for generic types.

    this since Python.typing _GenericAlias is private

    """

    __args__: Tuple[_AnnotationScanType, ...]
    __origin__: Type[_T]

    # Python's builtin _GenericAlias has this method, however builtins like
    # list, dict, etc. do not, even though they have ``__origin__`` and
    # ``__args__``
    #
    # def copy_with(self, params: Tuple[_AnnotationScanType, ...]) -> Type[_T]:
    #     ...



class ArgsTypeProcotol(Protocol):
    """protocol for types that have ``__args__``

    there's no public interface for this AFAIK

    """

    __args__: Tuple[_AnnotationScanType, ...]


def _get_type_name(type_: Type[Any]) -> str:
    if compat.py310:
        return type_.__name__
    else:
        typ_name = getattr(type_, "__name__", None)
        if typ_name is None:
            typ_name = getattr(type_, "_name", None)

        return typ_name  # type: ignore


def is_origin_of(
    type_: Any, *names: str, module: Optional[str] = None
) -> bool:
    """return True if the given type has an __origin__ with the given name
    and optional module."""

    origin = typing_get_origin(type_)
    if origin is None:
        return False

    return _get_type_name(origin) in names and (
        module is None or origin.__module__.startswith(module)
    )

def is_optional(type_: Any) -> TypeGuard[ArgsTypeProcotol]:
    return is_origin_of(
        type_,
        "Optional",
        "Union",
        "UnionType",
    )


def is_optional_union(type_: Any) -> bool:
    return is_optional(type_) and NoneType in get_args(type_)