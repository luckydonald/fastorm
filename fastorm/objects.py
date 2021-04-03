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

    def build_sql_insert(
        self, *, ignore_automatic_fields: bool, on_conflict_upsert_field_list: Optional[List[str]]
    ) -> Tuple[str, Any]:
        own_keys = [f.name for f in dataclasses.fields(self)]
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
        upsert_fields = {}  # key is field name, values is the matching placeholder_index.
        placeholder_index = 0
        primary_key_index = 0
        for key in own_keys:
            if key in _ignored_fields:
                continue
            # end if
            is_automatic_field = None
            if ignore_automatic_fields or upsert:
                is_automatic_field = key in _automatic_fields
            if ignore_automatic_fields and is_automatic_field:
                continue
            # end if
            value = getattr(self, key)
            placeholder_index += 1
            placeholder.append(f'${placeholder_index}')
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

            if on_conflict_upsert_field_list and not is_automatic_field:
                upsert_fields[key] = placeholder_index
            # end if
        # end if

        sql = f'INSERT INTO "{_table_name}" ({",".join(keys)})\n VALUES ({",".join(placeholder)})'
        if on_conflict_upsert_field_list and upsert_fields:
            upsert_sql = ', '.join([f'"{key}" = ${placeholder_index}' for key, placeholder_index in upsert_fields.items()])
            upsert_fields_sql = ', '.join([f'"{field}"' for field in on_conflict_upsert_field_list])
            sql += f'\n ON CONFLICT ({upsert_fields_sql}) DO UPDATE SET {upsert_sql}'
        # end if
        if _automatic_fields:
            automatic_fields_sql = ', '.join([f'"{key}"' for key in _automatic_fields])
            sql += f'\n RETURNING {automatic_fields_sql}'
        # end if
        sql += '\n;'
        return (sql, *values)
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
