#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dataclasses
import typing

import pydantic.fields
from luckydonaldUtils.logger import logging

__author__ = 'luckydonald'

from pydantic.dataclasses import dataclass

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if


def __getitem__(self, key):
    return getattr(self, key)
# end def


def __iter__(self):
    return iter(dataclasses.astuple(self))
# end def


FIELD_REFERENCE_TYPE = typing.TypeVar("FIELD_REFERENCE_TYPE")

FIELD_REFERENCE_ITEM_WITH_TYPE = typing.Union[type | typing.Type['FastORM']]   # the FIELD_REFERENCE_TYPE of TYPE type()
FIELD_REFERENCE_ITEM_WITH_TYPE_HINT = pydantic.fields.FieldInfo  # the FIELD_REFERENCE_TYPE of TYPE pydantic.typehint


@dataclass
class Item(typing.Generic[FIELD_REFERENCE_TYPE]):
    field: str
    type_: FIELD_REFERENCE_TYPE

    __getitem__ = __getitem__  # reuse, as it's the same function basically
    __iter__ = __iter__  # reuse, as it's the same function basically
# end class


@dataclass
class FieldReference(typing.Generic[FIELD_REFERENCE_TYPE]):
    is_primary_key: bool
    types: typing.List[Item[FIELD_REFERENCE_TYPE]]

    Item: typing.Type[Item] = Item
    __getitem__ = __getitem__  # reuse, as it's the same function basically
    __iter__ = __iter__  # reuse, as it's the same function basically
# end class
