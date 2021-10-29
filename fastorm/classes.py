#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dataclasses
import typing

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


@dataclass
class Item(object):
    field: str
    type_: typing.Union[type | typing.Type['FastORM']]

    __getitem__ = __getitem__  # reuse, as it's the same function basically
    __iter__ = __iter__  # reuse, as it's the same function basically
# end class


@dataclass
class FieldReference(object):
    is_primary_key: bool
    types: typing.List[Item]

    Item = Item
    __getitem__ = __getitem__  # reuse, as it's the same function basically
    __iter__ = __iter__  # reuse, as it's the same function basically
# end class
