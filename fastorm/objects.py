#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
from abc import abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Any, TypeVar
from asyncpg import Connection


import dataclasses

from luckydonaldUtils.exceptions import assert_type_or_raise
from luckydonaldUtils.logger import logging
from luckydonaldUtils.typing import JSONType
from pytgbot.api_types.receivable.updates import Message


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

    @classmethod
    def get_fields(cls) -> List[str]:
        return [f.name for f in dataclasses.fields(cls)]
    # end if

    def build_sql_insert(
        self, *, ignore_setting_automatic_fields: bool, on_conflict_upsert_field_list: Optional[List[str]]
    ) -> Tuple[str, Any]:
        own_keys = self.get_fields()
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
            if ignore_setting_automatic_fields or on_conflict_upsert_field_list:
                is_automatic_field = key in _automatic_fields
            if ignore_setting_automatic_fields and is_automatic_field:
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

    @classmethod
    async def get(cls: CLS_TYPE, conn: Connection, **kwargs) -> Optional[CLS_TYPE]:
        """
        Like `.select(…)` but returns `None` for no matches, the match itself or an error if it's more than one row.

        :param conn:
        :param kwargs:
        :return:
        """
        rows = await cls.select(conn=conn, **kwargs)
        if len(rows) == 0:
            return None
        # end if
        assert len(rows) <= 1
        return rows[0]
    # end def

    @classmethod
    async def select(cls: CLS_TYPE, conn: Connection, **kwargs) -> List[CLS_TYPE]:
        """
        Get's multiple ones.
        :param conn:
        :param kwargs:
        :return:
        """
        fetch_params = await cls.build_sql_select(**kwargs)
        logger.debug(f'SQL: {fetch_params[0]!r} with values {fetch_params[1:]}')
        rows = await conn.fetch(*fetch_params)
        return [cls.from_row(row) for row in rows]
    # end def

    @classmethod
    async def build_sql_select(cls, **kwargs):
        _ignored_fields = getattr(cls, '_ignored_fields')
        fields = ','.join([
            f'"{field}"'
            for field in cls.get_fields()
            if not field.startswith('_') and field not in _ignored_fields
        ])
        where_index = 0
        where_parts = []
        where_values = []
        # noinspection PyUnusedLocal
        where_wolf = None
        for key, value in kwargs.items():
            assert not isinstance(value, HelpfulDataclassDatabaseMixin)
            # if isinstance(value, HelpfulDataclassDatabaseMixin):
            #     # we have a different table in this table, so we probably want to go for it's `id` or whatever the primary key is.
            #     # if you got more than one of those PKs, simply specify them twice for both fields.
            #     value = value.get_primary_keys_values()[primary_key_index]
            #     primary_key_index += 1
            # # end if
            where_index += 1
            where_parts.append(f'"{key}" = ${where_index}')
            where_values.append(value)
        # end if

        sql = f'SELECT {fields} FROM "{cls._table_name}" WHERE {" AND ".join(where_parts)}'
        return (sql, *where_values)
    # end def

    async def insert(
        self, conn: Connection, ignore_setting_automatic_fields: bool,
        on_conflict_upsert_field_list: Optional[List[str]],
        write_back_automatic_fields: bool,
    ):
        """

        :param conn:
        :param ignore_setting_automatic_fields:
        :param on_conflict_upsert_field_list:
        :param write_back_automatic_fields: Apply the automatic fields back to ourself.
                                            Ignored if `ignore_setting_automatic_fields` is False.
        :return:
        """
        artist_sql = self.build_sql_insert(
            ignore_setting_automatic_fields=ignore_setting_automatic_fields,
            on_conflict_upsert_field_list=on_conflict_upsert_field_list,
        )
        _automatic_fields = getattr(self, '_automatic_fields')
        logger.debug(f'Insert query for {self.__class__.__name__}: {artist_sql!r}')
        updated_automatic_values_rows = await conn.fetch(*artist_sql)
        logger.debug(f'Inserted {self.__class__.__name__}: {updated_automatic_values_rows} for {self}')
        assert len(updated_automatic_values_rows) == 1
        updated_automatic_values = updated_automatic_values_rows[0]
        if not ignore_setting_automatic_fields and write_back_automatic_fields:
            for field in _automatic_fields:
                assert field in updated_automatic_values
                setattr(self, field, updated_automatic_values[field])
            # end for
        # end if
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

    @classmethod
    def from_row(cls, row):
        # noinspection PyArgumentList
        return cls(*row)
    # end def

    @staticmethod
    def dataclass(other_cls):
        """
        :param other_cls:
        :return:
        """
        body = """

        """
        _primary_keys = getattr(other_cls, '_primary_keys')
        fields = [
             f for f in dataclasses.fields(other_cls)
             if f.init and f.name in _primary_keys
        ]
        func_args = ','.join([dataclasses._init_param(f) for f in fields])
        call_args = ','.join([f'{f.name}={f.name}' for f in fields])
        locals = {f'_type_{f.name}': f.type for f in fields}
        locals[other_cls.__name__] = other_cls
        import builtins
        globals = {}
        globals['__builtins__'] = builtins
        # Compute the text of the entire function.
        txt = f'async def get(self, {func_args}) -> {other_cls.__name__}:\n await self.get({call_args})'
        logger.debug(f'setting up `get`: {txt!r}')
        function = _create_func('get', txt, globals, locals)
        setattr(other_cls, 'get', function)
        return other_cls
    # end def
# end if
