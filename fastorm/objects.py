#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Any, TypeVar

import dataclasses

from luckydonaldUtils.exceptions import assert_type_or_raise
from luckydonaldUtils.logger import logging
from luckydonaldUtils.typing import JSONType


__author__ = 'luckydonald'

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if



CLS_TYPE = TypeVar("CLS_TYPE")


class HelpfulDataclassDatabaseMixin(object):
    _table_name: str
    _ignored_fields: List[str]
    _automatic_fields: List[str]
    _primary_keys: List[str]

    def as_dict(self) -> Dict[str, JSONType]:
        return dataclasses.asdict(self)
    # end def

    def build_sql_insert(self, *, ignore_automatic_fields: bool) -> Tuple[str, Any]:
        own_values = self.as_dict().items()
        _table_name = getattr(self, '_table_name')
        _ignored_fields = getattr(self, '_ignored_fields')
        _automatic_fields = getattr(self, '_automatic_fields')
        assert_type_or_raise(_table_name, str, parameter_name='self._table_name')
        assert_type_or_raise(_ignored_fields, list, parameter_name='self._ignored_fields')
        assert_type_or_raise(_automatic_fields, list, parameter_name='self._automatic_fields')
        _ignored_fields += ['_table_name', '_ignored_fields']

        placeholder = []
        values: List[JSONType] = []
        keys = []
        i = 0
        primary_key_index = 0
        for key, value in own_values:
            if key in _ignored_fields:
                continue
            # end if
            if ignore_automatic_fields and key in _automatic_fields:
                continue
            # end if
            i += 1
            placeholder.append(f'${i}')
            if isinstance(value, dict):
                pass
                # value = json.dumps(value)
            # end if
            if isinstance(value, HelpfulDataclassDatabaseMixin):
                # we have a different table in this table, so we probably want to go for it's `id` or whatever the primary key is.
                # if you got more than one of those PKs, simply specify them twice for both fields.
                value = value.get_primary_keys_values()[primary_key_index]
                primary_key_index += 1
            # end if
            values.append(value)
            keys.append(f'"{key}"')
        # end if
        return (f'INSERT INTO "{_table_name}" ({",".join(keys)}) VALUES ({",".join(placeholder)}) RETURNING "id";', *values)
    # end def

    def clone(self: CLS_TYPE) -> CLS_TYPE:
        return self.__class__(**self.as_dict())
    # end if

    def get_primary_keys(self) -> Dict[str, Any]:
        return {k: v for k, v in self.as_dict().items()}
    # end def

    def get_primary_keys_values(self):
        return list(self.get_primary_keys().values())
    # end def
# end if
