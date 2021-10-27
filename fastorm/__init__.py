#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""FastORM framework, easy to learn, fast to code"""
__author__ = 'luckydonald'
__version__ = "0.0.6"
__all__ = ['__author__', '__version__', 'FastORM', 'Autoincrement']

import ipaddress
import builtins
import datetime
import asyncpg
import decimal
import typing
import types
import uuid
from typing import List, Dict, Any, Optional, Tuple, Type, get_type_hints, Union, TypeVar, Callable
from asyncpg import Connection
from luckydonaldUtils.exceptions import assert_type_or_raise
from luckydonaldUtils.logger import logging
from luckydonaldUtils.typing import JSONType

from pydantic import BaseModel
from pydantic.fields import ModelField, UndefinedType, Undefined, Field, PrivateAttr
from pydantic.typing import NoArgAnyCallable
from typeguard import check_type

from .compat import check_is_union_type, TYPEHINT_TYPE, check_is_generic_alias, check_is_annotated_type
from .compat import Annotated, NoneType

VERBOSE_SQL_LOG = True
CLS_TYPE = TypeVar("CLS_TYPE")


logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if


class FastORM(BaseModel):
    _table_name: str  # database table name we run queries against
    _ignored_fields: List[str]  # fields which never are intended for the database and will be excluded in every operation. (So are all fields starting with an underscore)
    _automatic_fields: List[str]  # fields the database fills in, so we will ignore them on INSERT.
    _primary_keys: List[str]  # this is how we identify ourself.
    _database_cache: Dict[str, JSONType] = PrivateAttr()  # stores the last known retrieval, so we can run UPDATES after you changed parameters.
    __selectable_fields: List[str] = PrivateAttr()  # cache for `cls.get_sql_fields()`

    def __init__(self, **data: Any):
        super().__init__(**data)
        self._database_cache: Dict[str, Any] = {}
    # end def

    def _database_cache_overwrite_with_current(self):
        """
        Resets the database cache from the current existing fields.
        This is used just after something is loaded from a database row.
        :return:
        """
        self._database_cache = {}
        for field in self.get_fields():
            self._database_cache[field] = getattr(self, field)
        # end if

    def _database_cache_remove(self):
        """
        Removes the database cache completely.
        This is used after deleting an entry.
        :return:
        """
        self._database_cache = {}
    # end def

    def as_dict(self) -> Dict[str, JSONType]:
        return self.dict()
    # end def

    @classmethod
    def get_fields_typehints(cls, *, flatten_table_references: bool = False) -> Dict[str, ModelField]:
        """
        Get's all fields which have type hints and thus we consider as fields for the database.
        Filters out constants (all upper case, like `CAPSLOCK_VARIABLE`) and hidden fields (starting with `_`).

        :param flatten_table_references:
                True if we should flatten the references to other table's primary key in the format of `f"{original_key}__{other_table_key}`.
                False to not resolve those fields, and instead return the type hint for the other FastORM class.
        :return: the dictionary with pydantic's ModelField descriptions.

        Example:

            >>> class OtherTable(FastORM):
            ...     _table_name = 'other_table'
            ...     _primary_keys = ['id_part_1', 'id_part_2']
            ...
            ...     id_part_1: int
            ...     id_part_2: str
            ... # end class
            ...

            >>> class ActualTable(FastORM):
            ...     _table_name = 'actual_table'
            ...     _ignored_fields = []
            ...     cool_reference: OtherTable
            ...

            >>> ActualTable.get_fields_typehints(flatten_table_references=False)
            {'cool_reference': ModelField(name='cool_reference', type=OtherTable, required=True)}

            >>> ActualTable.get_fields_typehints(flatten_table_references=True)
            {'cool_reference__id_part_1': ModelField(name='id_part_1', type=int, required=True), 'cool_reference__id_part_2': ModelField(name='id_part_2', type=str, required=True)}

        """
        _ignored_fields = cls.get_ignored_fields()
        # copy the type hints as we might add more type hints for the primary key fields of referenced models, and we wanna filter.
        type_hints = {
            key: value for key, value in cls.__fields__.items()
            if (
                not key.startswith('_')
                and not key.isupper()
                and not key in _ignored_fields
            )
        }
        if flatten_table_references is False:
            return type_hints
        # end if
        flattened_type_hints = {}
        for key, value in type_hints.items():
            type_hint = type_hints[key]
            inner_type = type_hint.type_
            other_class: Union[Type[FastORM], None]
            if (
                check_is_generic_alias(inner_type) and
                hasattr(inner_type, '__origin__') and
                type_hint.type_.__origin__ == typing.Union
            ):  # Union
                # it's a Union
                union_params = type_hint.type_.__args__[:]
                first_union_type = union_params[0]
                if issubclass(first_union_type, FastORM):
                    # we can have a reference to another Table, so it could be that
                    # the table ist the first entry and the actual field type is the second.
                    # Union[Table, int]
                    # Union[Table, Tuple[int, int]]
                    pk_keys = first_union_type.get_primary_keys_keys()
                    typehints = first_union_type.get_fields_typehints()
                    key_types = [typehints[key] for key in pk_keys]
                    if not len(union_params) == 2:
                        raise TypeError(
                            f'Union with other table type must have it\'s primary key(s) as second argument: Union{union_params!r}'
                        )
                    # end if
                    implied_other_class_pk_types = union_params[1]
                    if (
                        check_is_generic_alias(implied_other_class_pk_types) and
                        hasattr(implied_other_class_pk_types, '__origin__') and
                        implied_other_class_pk_types.__origin__ == tuple and
                        hasattr(implied_other_class_pk_types, '__args__') and
                        implied_other_class_pk_types.__args__
                    ):
                        implied_other_class_pk_types = list(implied_other_class_pk_types.__args__)
                    else:
                        implied_other_class_pk_types = [implied_other_class_pk_types]
                    # end if
                    typehint_union_types = [key_type.type_ for key_type in key_types]
                    if implied_other_class_pk_types == typehint_union_types:
                        # so basically the we know Table has _id = ['id'],
                        # and Table.id is of type int,
                        # and now our given type is Union[Table, int], matching that.
                        other_class = first_union_type
                    else:
                        other_class = None
                    # end if
                else:
                    other_class = None
                # end if
            else:
                other_class = None
            # end if
            if not other_class:
                try:
                    if issubclass(type_hint.type_, FastORM):
                        other_class: Type[FastORM] = type_hint.type_
                    else:
                        other_class = None
                except TypeError:
                    other_class = None
                # end try
            # end if
            other_class: Union[Type[FastORM], None]
            if not other_class:
                # is a regular key, just keep it as is
                flattened_type_hints[key] = type_hint  # TODO: make a copy?
                # and then let's do the next key
                continue
            # end if

            # now it's another FastORM table definition.
            assert issubclass(other_class, FastORM)
            other_class_type_hints: Dict[str, ModelField] = other_class.get_fields_typehints(flatten_table_references=True)
            for other_class_primary_key in other_class.get_primary_keys_keys():
                new_key = f'{key}__{other_class_primary_key}'
                if new_key in type_hints:
                    raise ValueError(
                        f'The constructed reference key {new_key!r}, for field {cls.__name__}.{key} pointing to '
                        f'table {other_class.__name__}.{other_class_primary_key}, would overwrite an already existing key.'
                    )
                # end if
                other_class_type_hint = other_class_type_hints[other_class_primary_key]
                flattened_type_hints[new_key] = other_class_type_hint  # TODO: make a copy?
            # end for
        # end for
        return flattened_type_hints
    # end def

    @classmethod
    def get_fields(cls, flatten_table_references: bool = False) -> List[str]:
        """
        Get's all fields which have type hints and thus we consider as fields for the database.
        Filters out constants (all upper case, like `CAPSLOCK_VARIABLE`) and hidden fields (starting with `_`).
        :return: a list with the keys.
        """
        return list(cls.get_fields_typehints(flatten_table_references=flatten_table_references).keys())
    # end def

    @classmethod
    def get_automatic_fields(cls) -> List[str]:
        _automatic_fields = getattr(cls, '_automatic_fields', [])[:]
        return _automatic_fields
    # end def

    @classmethod
    def get_ignored_fields(cls) -> List[str]:
        _ignored_fields = getattr(cls, '_ignored_fields', [])
        if isinstance(_ignored_fields, types.MemberDescriptorType):
            # basically that means it couldn't find any actually existing field
            _ignored_fields = []
        # end if
        _ignored_fields = [*_ignored_fields]  # make copy
        assert_type_or_raise(_ignored_fields, list, parameter_name=f'{cls.__name__}._ignored_fields')
        _ignored_fields += [
            '_table_name',
            '_ignored_fields',
            '_automatic_fields',
            '_primary_keys',
            '_database_cache',
            '__selectable_fields',
            f'_{cls.__name__!s}__selectable_fields',
            '__slots__'
        ]
        return _ignored_fields
    # end def

    @classmethod
    def get_sql_fields(cls) -> List[str]:
        key = f'_{cls.__name__!s}__selectable_fields'
        if getattr(cls, key, None) is None:
            setattr(cls, key, [field for field in cls.get_fields() if not field.startswith('_')])
        # end if
        return getattr(cls, key)
    # end if

    @classmethod
    def get_select_fields(cls, *, namespace=None) -> str:
        if namespace:
            return ', '.join([f'"{namespace}"."{field}" AS "{namespace} {field}"' for field in cls.get_sql_fields()])
        # end if
        return ', '.join([f'"{field}"' for field in cls.get_sql_fields()])
    # end def

    @classmethod
    def get_select_fields_len(cls) -> int:
        return len(cls.get_sql_fields())
    # end if

    @classmethod
    def get_table(cls) -> str:
        """
        Provides the name of the table in a already quoted way, ready to use in SQL queries.
        Note, that is naive quoting, and can be easily broken out of (possible SQL INJECTION), however the table name
        should never be user input anyway.

            >>> class Test(FastORM):
            ...   _table_name = 'sample'
            >>> Test.get_name()
            'sample'
            >>> Test.get_table()
            '"sample"'
            >>> print(Test.get_name())
            sample
            >>> print(Test.get_table())
            "sample"

        :return: The quoted table name
        """
        _table_name = cls.get_name()
        return f'"{_table_name}"'
    # end def

    @classmethod
    def get_name(cls) -> str:
        """
        The name of the table, as used with the database.
        :return: `table`
        """
        _table_name = getattr(cls, '_table_name')
        return _table_name
    # end def

    def build_sql_insert(
        self, *,
        ignore_setting_automatic_fields: Optional[bool] = None,
        on_conflict_upsert_field_list: Optional[List[str]] = None,
    ) -> Tuple[str, Any]:
        own_keys = self.get_fields()
        _ignored_fields = self.get_ignored_fields()
        _automatic_fields = self.get_automatic_fields()
        assert_type_or_raise(_ignored_fields, list, parameter_name='self._ignored_fields')
        assert_type_or_raise(_automatic_fields, list, parameter_name='self._automatic_fields')

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
            if ignore_setting_automatic_fields is None and value is None:
                continue
            # end if
            placeholder_index += 1
            placeholder.append(f'${placeholder_index}')
            if isinstance(value, dict):
                pass
                # value = json.dumps(value)
            # end if
            if isinstance(value, FastORM):
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

        # noinspection SqlNoDataSourceInspection,SqlResolve
        sql = f'INSERT INTO {self.get_table()} ({",".join(keys)})\n VALUES ({",".join(placeholder)})'
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
        # noinspection PyRedundantParentheses
        return (sql, *values)
    # end def

    @classmethod
    async def get(cls: Type[CLS_TYPE], conn: Connection, **kwargs) -> Optional[CLS_TYPE]:
        """
        Retrieves a single Database element. Error if there are more matching ones.
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
    async def select(cls: Type[CLS_TYPE], conn: Connection, **kwargs) -> List[CLS_TYPE]:
        """
        Get's multiple ones.
        :param conn:
        :param kwargs:
        :return:
        """
        fetch_params = cls.build_sql_select(**kwargs)
        logger.debug(f'SELECT query for {cls.__name__}: {fetch_params[0]!r} with values {fetch_params[1:]}')
        rows = await conn.fetch(*fetch_params)
        return [cls.from_row(row) for row in rows]
    # end def

    @classmethod
    def build_sql_select(cls, **kwargs):
        _ignored_fields = cls.get_ignored_fields()
        typehints: Dict[str, Any] = get_type_hints(cls)
        non_ignored_fields = [field for field in cls.get_fields() if field not in _ignored_fields]
        fields = ','.join([
            f'"{field}"'
            for field in non_ignored_fields
            if not field.startswith('_')
        ])
        where_index = 0
        where_parts = []
        where_values = []
        # noinspection PyUnusedLocal
        where_wolf = None
        for key, value in kwargs.items():
            if key not in non_ignored_fields:
                raise ValueError(f'key {key!r} is not a non-ignored field!')
            # end if
            assert not isinstance(value, FastORM)
            # if isinstance(value, HelpfulDataclassDatabaseMixin):
            #     # we have a different table in this table, so we probably want to go for it's `id` or whatever the primary key is.
            #     # if you got more than one of those PKs, simply specify them twice for both fields.
            #     value = value.get_primary_keys_values()[primary_key_index]
            #     primary_key_index += 1
            # # end if
            where_index += 1
            is_in_list_clause = cls._param_is_list_of_multiple_values(key, value, typehints[key])
            if is_in_list_clause:
                assert isinstance(value, (list, tuple))
                assert len(value) >= 1
                if len(value) == 1:
                    # single element list -> normal where is fine -> so we go that route with it.
                    value = value[0]
                    is_in_list_clause = False
                # end if
            # end if
            if not is_in_list_clause:  # no else-if as is_in_list_clause could be set False again.
                where_parts.append(f'"{key}" = ${where_index}')
                where_values.append(value)
            else:  # is_in_list_clause is True
                where_part = ''
                for actual_value in value:
                    where_part += f'${where_index},'
                    where_values.append(actual_value)
                    where_index += 1
                # end for
                where_index -= 1  # we end up with incrementing once too much
                where_parts.append(f'"{key}" IN ({where_part.rstrip(",")})')
            # end if
        # end if

        # noinspection SqlResolve,SqlNoDataSourceInspection
        sql = f'SELECT {fields} FROM "{cls._table_name}" WHERE {" AND ".join(where_parts)}'
        # noinspection PyRedundantParentheses
        return (sql, *where_values)
    # end def

    async def insert(
        self, conn: Connection, *,
        ignore_setting_automatic_fields: Optional[bool] = None,
        on_conflict_upsert_field_list: Optional[List[str]] = None,
        write_back_automatic_fields: bool = True,
    ):
        """
        :param conn: Database connection to run at.
        :param ignore_setting_automatic_fields:
            Skip setting fields marked as automatic, even if you provided.
            For example if the id field is marked automatic, as it's an autoincrement int.
            If `True`, setting `id=123` (commonly `id=None`) would be ignored, and instead the database assigns that value.
            If `False`, the value there will be written to the database.
            If `None`, it will be ignored as long as the value actually is None, but set if it is non-None.
            The default setting is `None`.
        :param on_conflict_upsert_field_list: List of fields which are expected to cause an duplicate conflict, and thus will instead be overwritten.
        :param write_back_automatic_fields: Apply the automatic fields back to this object.
                                            Ignored if `ignore_setting_automatic_fields` is False.
        :return:
        """
        fetch_params = self.build_sql_insert(
            ignore_setting_automatic_fields=ignore_setting_automatic_fields,
            on_conflict_upsert_field_list=on_conflict_upsert_field_list,
        )
        self._database_cache_overwrite_with_current()
        _automatic_fields = self.get_automatic_fields()
        if VERBOSE_SQL_LOG:
            fetch_params_debug = "\n".join([f"${i}={param!r}" for i, param in enumerate(fetch_params)][1:])
            logger.debug(f'INSERT query for {self.__class__.__name__}\nQuery:\n{fetch_params[0]}\nParams:\n{fetch_params_debug!s}')
        else:
            logger.debug(f'INSERT query for {self.__class__.__name__}: {fetch_params!r}')
        # end if
        updated_automatic_values_rows = await conn.fetch(*fetch_params)
        logger.debug(f'INSERT for {self.__class__.__name__}: {updated_automatic_values_rows} for {self}')
        assert len(updated_automatic_values_rows) == 1
        updated_automatic_values = updated_automatic_values_rows[0]
        if ignore_setting_automatic_fields and write_back_automatic_fields:
            for field in _automatic_fields:
                assert field in updated_automatic_values
                setattr(self, field, updated_automatic_values[field])
                self._database_cache[field] = updated_automatic_values[field]
            # end for
        # end if
    # end def

    def build_sql_update(self):
        """
        Builds a prepared SQL statement for update.
        Only fields with changed values will be updated in the database.
        However this one doesn't resets the cache for those, see `FastORM.update(…)` for that.

        :return: The SQL string followed by positional parameters for the `conn.execute(…)` method.
        """
        _table_name = getattr(self, '_table_name')
        _primary_keys = getattr(self, '_primary_keys')
        _database_cache = getattr(self, '_database_cache')
        _automatic_fields = self.get_automatic_fields()
        assert_type_or_raise(_table_name, str, parameter_name='self._table_name')
        assert_type_or_raise(_primary_keys, list, parameter_name='self._primary_keys')
        assert_type_or_raise(_database_cache, dict, parameter_name='self._database_cache')
        assert_type_or_raise(_automatic_fields, list, parameter_name='self._automatic_fields')

        # SET ...
        update_values = self.get_changes()

        # UPDATE ... SET ... WHERE ...
        placeholder_index = 0
        other_primary_key_index = 0
        values: List[Any] = []
        update_keys: List[str] = []  # "foo" = $1
        for key, value in update_values.items():
            value = getattr(self, key)
            placeholder_index += 1
            if isinstance(value, FastORM):
                # we have a different table in this table, so we probably want to go for it's `id` or whatever the primary key is.
                # if you got more than one of those PKs, simply specify them twice for both fields.
                value = value.get_primary_keys_values()[other_primary_key_index]
                other_primary_key_index += 1
            # end if

            values.append(value)
            update_keys.append(f'"{key}" = ${placeholder_index}')
        # end if

        # WHERE pk...
        primary_key_where: List[str] = []  # "foo" = $1
        for primary_key in _primary_keys:
            if primary_key in _database_cache:
                value = _database_cache[primary_key]
            else:
                value = getattr(self, primary_key)
            # end if
            placeholder_index += 1
            primary_key_where.append(f'"{primary_key}" = ${placeholder_index}')
            values.append(value)
        # end if
        logger.debug(f'Fields to UPDATE for selector {primary_key_where!r}: {update_values!r}')

        assert update_keys
        sql = f'UPDATE "{_table_name}"\n'
        sql += f' SET {",".join(update_keys)}\n'
        sql += f' WHERE {" AND ".join(primary_key_where)}\n'
        sql += ';'
        # noinspection PyRedundantParentheses
        return (sql, *values)
    # end def

    def get_changes(self) -> Dict:
        """
        Returns all values which got changed and are now different to the last downloaded database version.
        """
        own_keys = self.get_fields()
        _database_cache = self._database_cache
        _ignored_fields = self.get_ignored_fields()
        assert_type_or_raise(own_keys, list, parameter_name='own_keys')
        assert_type_or_raise(_database_cache, dict, parameter_name='self._database_cache')
        assert_type_or_raise(_ignored_fields, list, parameter_name='self._ignored_fields')

        update_values: Dict[str, Any] = {}
        for key in own_keys:
            if key.startswith('_') or key in _ignored_fields:
                continue
            # end if
            value = getattr(self, key)
            if key not in _database_cache:
                update_values[key] = value
            elif _database_cache[key] != value:
                update_values[key] = value
            # end if
        # end if
        return update_values
    # end def

    def has_changes(self) -> bool:
        """
        :returns: if we have unsaved changes.
        """
        return bool(self.get_changes())
    # end if

    async def update(self, conn: Connection) -> None:
        """
        Update the made changes to the database.
        Only fields with changed values will be updated in the database.

        :uses: FastORM.build_sql_update()
        """
        if not getattr(self, '_database_cache', None):
            return  # nothing to do.
        # end if
        fetch_params = self.build_sql_update()
        logger.debug(f'UPDATE query for {self.__class__.__name__}: {fetch_params!r}')
        update_status = await conn.execute(*fetch_params)
        logger.debug(f'UPDATE for {self.__class__.__name__}: {update_status} for {self}')
        self._database_cache_overwrite_with_current()
    # end if

    def build_sql_delete(self):
        _primary_keys = self.get_primary_keys_keys()
        _ignored_fields = self.get_ignored_fields()
        _database_cache = self._database_cache
        assert_type_or_raise(_ignored_fields, list, parameter_name='self._ignored_fields')
        assert_type_or_raise(_primary_keys, list, parameter_name='self._primary_keys')
        assert_type_or_raise(_database_cache, dict, parameter_name='self._database_cache')

        # DELETE FROM "name" WHERE pk...
        where_values = []
        placeholder_index = 0
        primary_key_parts: List[str] = []  # "foo" = $1
        for primary_key in _primary_keys:
            if primary_key in _database_cache:
                value = _database_cache[primary_key]
            else:
                value = getattr(self, primary_key)
            # end if
            placeholder_index += 1
            primary_key_parts.append(f'"{primary_key}" = ${placeholder_index}')
            where_values.append(value)
        # end if
        logger.debug(f'Fields to DELETE for selector {primary_key_parts!r}: {where_values!r}')

        # noinspection SqlWithoutWhere,SqlResolve,SqlNoDataSourceInspection
        sql = f'DELETE FROM {self.get_table()}\n'
        sql += f' WHERE {" AND ".join(primary_key_parts)}'
        sql += '\n;'
        # noinspection PyRedundantParentheses
        return (sql, *where_values)
    # end def

    async def delete(self, conn: Connection):
        fetch_params = self.build_sql_delete()
        logger.debug(f'DELETE query for {self.__class__.__name__}: {fetch_params!r}')
        delete_status = await conn.execute(*fetch_params)
        logger.debug(f'DELETE for {self.__class__.__name__}: {delete_status} for {self}')
        self._database_cache_remove()
    # end if

    def clone(self: CLS_TYPE) -> CLS_TYPE:
        return self.__class__(**self.as_dict())
    # end if

    @classmethod
    def get_primary_keys_keys(cls) -> List[str]:
        return cls._primary_keys
    # end def

    def get_primary_keys(self) -> Dict[str, Any]:
        _primary_keys = self.get_primary_keys_keys()
        return {k: v for k, v in self.as_dict().items() if k in _primary_keys}
    # end def

    def get_primary_keys_values(self):
        return list(self.get_primary_keys().values())
    # end def

    @classmethod
    def from_row(cls, row):
        # noinspection PyArgumentList
        instance = cls(*row)
        instance._database_cache_overwrite_with_current()
        return instance
    # end def

    _COLUMN_AUTO_TYPES: Dict[type, str] = {
        int: "BIGSERIAL",
    }

    _COLUMN_TYPES: Dict[type, str] = {
        bool: "BOOLEAN",
        bytes: "BYTEA",
        bytearray: "BYTEA",
        str: "TEXT",
        # Python Type
        # PostgreSQL Type
        # Source: https://magicstack.github.io/asyncpg/current/usage.html#type-conversion

        # anyenum
        # str

        # anyrange
        # asyncpg.Range

        # record
        # asyncpg.Record, tuple, Mapping

        # bit, varbit
        # asyncpg.BitString

        asyncpg.Box: "BOX",

        # cidr
        # ipaddress.IPv4Network, ipaddress.IPv6Network
        ipaddress.IPv4Network: "CIDR",
        ipaddress.IPv6Network: "CIDR",

        # inet
        # ipaddress.IPv4Interface, ipaddress.IPv6Interface, ipaddress.IPv4Address, ipaddress.IPv6Address
        ipaddress.IPv4Interface: "INET",
        ipaddress.IPv6Interface: "INET",
        ipaddress.IPv4Address: "INET",
        ipaddress.IPv6Address: "INET",

        # macaddr
        # str

        # time
        # offset-naïve datetime.time

        # time with time zone
        # offset-aware datetime.time
        datetime.time: "TIME",

        # timestamp
        # offset-naïve datetime.datetime
        #  +
        # timestamp with time zone
        # offset-aware datetime.datetime
        datetime.datetime: "TIMESTAMP",

        datetime.date: "DATE",  # must come after datetime.datetime as datetime is a subclass of this

        datetime.timedelta: "INTERVAL",

        # float, double precision
        # float [2]
        # Inexact single-precision float values may have a different representation when decoded into a Python float. This is inherent to the implementation of limited-precision floating point types. If you need the decimal representation to match, cast the expression to double or numeric in your query.
        float: "DOUBLE PRECISION",

        # smallint, integer, bigint
        # int
        int: "BIGINT",

        # numeric
        # Decimal
        decimal.Decimal: "NUMERIC",

        # json, jsonb
        # str
        dict: "JSONB",
        list: "JSONB",  # the special cases like INT[] will be processed beforehand.

        # line
        # asyncpg.Line
        asyncpg.Line: "LINE",

        # lseg
        # asyncpg.LineSegment
        asyncpg.LineSegment: "LSEG",

        # money
        # str

        asyncpg.Circle: "CIRCLE",

        # point
        # asyncpg.Point
        asyncpg.Point: "POINT",

        # polygon
        # asyncpg.Polygon
        asyncpg.Polygon: "POLYGON",

        # path
        # asyncpg.Path
        asyncpg.Path: "PATH",

        # uuid
        # uuid.UUID
        uuid.UUID: "UUID",

        BaseModel: "JSONB",
    }

    _COLUMN_TYPES_SPECIAL: Dict[Callable[[type], bool], str] = {
        lambda cls: hasattr(cls, 'to_dict'): _COLUMN_TYPES[dict],
        lambda cls: hasattr(cls, 'to_array'): _COLUMN_TYPES[dict],  # pytgbot object uses to_array
    }

    _COLUMN_AUTO_TYPES_SPECIAL: Dict[Callable[[type], bool], str] = {
    }

    @classmethod
    def _match_type(cls, python_type: type, *, automatic: bool) -> str:
        try:
            issubclass(python_type, object)
        except TypeError:  # issubclass() arg 1 must be a class
            raise TypeError(f'Could not process type {python_type} as a python type. Probably a typing annotation?.')
        if automatic:
            for sql_py_type, sql_type in cls._COLUMN_AUTO_TYPES.items():
                if issubclass(python_type, sql_py_type):
                    return sql_type
                # end if
            # end for
            for check_function, sql_type in cls._COLUMN_AUTO_TYPES_SPECIAL.items():
                if check_function(python_type):
                    return sql_type
                # end if
            # end for
        # end if
        for sql_py_type, sql_type in cls._COLUMN_TYPES.items():
            if issubclass(python_type, sql_py_type):
                return sql_type
            # end if
        # end for
        for check_function, sql_type in cls._COLUMN_TYPES_SPECIAL.items():
            if check_function(python_type):
                return sql_type
            # end if
        # end for
        raise TypeError(f'Could not process type {python_type} as database type.')
    # end def

    @classmethod
    async def create_table(cls, conn: Connection, if_not_exists: bool = False):
        create_params = cls.build_sql_create(if_not_exists=if_not_exists)
        logger.debug(f'CREATE query for {cls.__name__}: {create_params!r}')
        crate_status = await conn.execute(*create_params)
        logger.debug(f'CREATEed {cls.__name__}: {crate_status}')
    # end if

    @classmethod
    def build_sql_create(
        cls,
        if_not_exists: bool = False
    ) -> Tuple[str, Any]:
        assert issubclass(cls, BaseModel)  # because we no longer use typing.get_type_hints, but pydantic's `cls.__fields__`
        _table_name = getattr(cls, '_table_name')
        _automatic_fields = cls.get_automatic_fields()
        assert_type_or_raise(_table_name, str, parameter_name='cls._table_name')
        assert_type_or_raise(_automatic_fields, list, parameter_name='cls._automatic_fields')
        _ignored_fields = cls.get_ignored_fields()

        type_hints: Dict[str, ModelField] = cls.get_fields_typehints(flatten_table_references=True)

        # .required tells us if we have a default value set or not.
        # .allow_none tells us if None is supported
        # .default tells us what default (or None)

        placeholder_index = 0
        placeholder_values = []
        type_definitions = []
        for key, type_hint in type_hints.items():
            is_automatic_field = key in _automatic_fields

            is_optional, sql_type = cls.match_type(type_hint=type_hint, is_automatic_field=is_automatic_field, key=key)
            # if is_automatic_field:
            #     is_optional = False
            # # end if

            # Now let's build that column's sql part

            # column_name, data_type:
            type_definition_parts = [f'\n  "{key}"', sql_type]

            # column_constraints:
            if not is_optional:
                type_definition_parts.append("NOT NULL")
            # end if

            # has it a default value?
            if not isinstance(type_hint.field_info.default, UndefinedType):
                placeholder_index += 1
                type_definition_parts.append(f'DEFAULT ${placeholder_index}')
                placeholder_values.append(type_hint.field_info.default)
            # end if
            type_definitions.append(" ".join(type_definition_parts))
        # end for
        sql = ",".join(
            type_definitions
        ).join(
            [
                f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{cls.get_table()} (",
                # <joined type_definitions>
                "\n)"
            ]
        )

        # noinspection PyRedundantParentheses
        return (sql, *placeholder_values)
    # end def

    @classmethod
    def match_type(
        cls,
        type_hint: TYPEHINT_TYPE,
        *,
        is_automatic_field: Optional[bool] = None,
        key: Optional[str] = None,
        is_outer_call: bool = True
    ) -> Tuple[bool, str]:
        """
        Processes a type hint to produce a CREATE TABLE sql segment of the type of that type hint and if it's optional..

            >>> class Example(FastORM):
            ...   foo: Optional[int]
            ...

            >>> type_hints = typing.get_type_hints(Example)
            >>> Example.match_type(type_hints['foo'], is_automatic_field=False)
            (True, 'BIGINT')

        """
        is_union_type = check_is_union_type(type_hint)
        if hasattr(type_hint, '__origin__') or is_union_type:
            if check_is_annotated_type(type_hint):
                # https://stackoverflow.com/q/68275615/3423324#what-is-the-right-way-to-check-if-a-type-hint-is-annotated
                actual_type = type_hint.__origin__  # str or wherever was the first parameter.
                metadata = type_hint.__metadata__
                if not isinstance(metadata, (tuple, list)):
                    metadata = (metadata,)
                # end if
                if AutoincrementType in metadata:
                    is_automatic_field = True
                # end if
                is_optional, sql_type = cls.match_type(
                    actual_type, is_automatic_field=is_automatic_field, is_outer_call=False
                )
                return is_optional, sql_type
            # end if

            origin = type_hint.__origin__ if hasattr(type_hint, '__origin__') else type(type_hint)
            is_union_type = check_is_union_type(origin)
            if is_union_type or origin in (typing.Optional, typing.Union):  # Optional is an special union, too
                union_params = type_hint.__args__[:]  # this was __union_params__ in python3.5, but __args__ in 3.6+
                if not isinstance(union_params, (list, tuple)):
                    raise TypeError(
                        f'Union type for key {key} has unparsable params.', union_params,
                    )
                # end if
                if NoneType in union_params:
                    is_optional = True
                    union_params = [param for param in union_params if not issubclass(param, NoneType)]
                else:
                    is_optional = False
                # end if
                if len(union_params) == 0:
                    raise TypeError(
                        f'Union with no (non-None) type(s) at key {key}.', type_hint.__args__,
                    )
                # end if
                first_union_type = union_params[0]
                if not all(first_union_type == x for x in union_params[1:]):
                    raise TypeError(
                        f'Union with more than one type at key {key}.', union_params,
                    )
                # end if
                additional_is_optional, sql_type = cls.match_type(
                    first_union_type, is_automatic_field=is_automatic_field, is_outer_call=False
                )
                if additional_is_optional:
                    is_optional = True
                # end if
            elif isinstance(origin, typing.List) or issubclass(origin, list):
                list_params = type_hint.__args__
                if len(list_params) != 1:  # list has one type
                    raise TypeError(
                        'List with more than one type parameter.', type_hint, list_params
                    )
                # end if
                the_type = list_params[0]
                try:
                    # we will now recursively go into that list.
                    # if it is like `list[list[list[int]]] it will succeed as INT,
                    # and for those 3 lists the [] will be added 3 times, resulting in INT[][][]
                    # If any of those inner lists aren't a compatible type (TypeError),
                    # e.g. list[list[Union[str, int]]], we have to use a json dict instead.
                    _, sql_type = cls.match_type(
                        the_type, is_automatic_field=is_automatic_field, is_outer_call=False
                    )
                    sql_type = "".join((sql_type, "[]"))  # append '[]' to the sql_type
                    return False, sql_type  # the list itself can't be optional, that has to be done by an outer Optional[].
                except TypeError as e:
                    if not is_outer_call:
                        # make sure we don't end up with JSONB[] for list[list[Union[str, int]]],
                        # only the outer one should migrate to json.
                        raise e
                    # end if
                    logger.debug('Could not parse as a single type list (e.g. INT[][]), now will be a json field.', exc_info=True)
                    return False, cls._COLUMN_TYPES[dict]
                # end try
            elif isinstance(origin, typing.Tuple) or issubclass(origin, builtins.tuple):
                tuple_params = type_hint.__args__
                if len(tuple_params) == 0:  # list has one type
                    raise TypeError(
                        'Tuple has no parameters.', type_hint, tuple_params
                    )
                # end if

                # check if all types of the tuple are the same, so we can use a list
                first_type = tuple_params[0]
                if all(first_type == x for x in tuple_params[1:]):
                    # we hope this will give us something like  INT, TEXT, FLOAT, etc.
                    _, sql_type = cls.match_type(
                        first_type, is_automatic_field=is_automatic_field, is_outer_call=False
                    )
                    sql_type = "".join((sql_type, "[]"))  # append '[]' to the sql_type
                    return False, sql_type  # the tuple itself can't be optional, that has to be done by an outer Optional[].
                # end if

                # so the types are all over the place, so we will have to fallback to json.
                sql_type = cls._COLUMN_TYPES[dict]
                return False, sql_type  # the list itself can't be optional, that has to be done by an outer Optional[].
            else:
                raise ValueError('Enclosed by an unknown type', origin, f'key={key!r}')
            # end case
        elif isinstance(type_hint, ModelField):
            is_optional = type_hint.allow_none
            # is_optional = type_hint.allow_none and (type_hint.shape != SHAPE_SINGLETON or not type_hint.sub_fields)
            try:
                subtype_is_optional, sql_type = cls.match_type(
                    type_hint=type_hint.type_, is_automatic_field=is_automatic_field, key=key, is_outer_call=False,
                )
            except TypeError as e:
                if not is_outer_call:
                    # make sure we don't end up with JSONB[] for list[list[Union[str, int]]],
                    # only the outer one should migrate to json.
                    raise e
                # end if
                logger.debug(
                    'Could not parse as a single type list (e.g. INT[][]), now will be a json field.', exc_info=True
                )
                return False, cls._COLUMN_TYPES[dict]
            # end try

            # pydantic makes t6_2: str = None  to be  t6_2: Optional[str] = None, couldn't find a way to detect only the first variant.
            # if is_optional and type_hint.field_info.default is None:
            #     # e.g. t6_2: str = None
            #     assert type_hint.field_info.default is None
            #     assert not isinstance(type_hint.field_info.default, UndefinedType)
            #     raise ValueError("You can't have an non-optional type default to None")
            # # end if

            if type_hint.outer_type_ == type_hint.type_:
                # e.g. for str,  typehint.outer_type_ is str
                #      for str,  typehint.type_       is str
                # but also for Optional[str] it is  both  str!
                return is_optional, sql_type
            if check_is_generic_alias(type_hint.outer_type_) and hasattr(type_hint.outer_type_, '__origin__'):
                # e.g. for list[int],  typehint.outer_type_ is List[int], thus having a .__origin__ == list
                #      for list[int],  typehint.type_       is int
                # isinstance(list[int], GenericAlias) == True

                wrapper_class = type_hint.outer_type_.__origin__  # e.g. list if we had List[int].
                if issubclass(wrapper_class, list):
                    sql_type = "".join([sql_type, "[]"])
                # end if
            elif check_is_annotated_type(type_hint.outer_type_):
                return cls.match_type(
                    type_hint.outer_type_, is_automatic_field=is_automatic_field, is_outer_call=False
                )
            # end if
        else:
            is_optional = False
            sql_type = cls._match_type(type_hint, automatic=is_automatic_field)  # fails anyway if not in the list above
        # end case
        return is_optional, sql_type
    # end def

    @classmethod
    def _param_is_list_of_multiple_values(cls, key: str, value: Any, typehint: Any):
        """
        If a value is multiple times of what was defined.
        :param key:
        :param value:
        :param typehint:
        :return: True if the `value` is a list of tuple of arguments satisfying the `typehint`.
        """
        if not isinstance(value, (list, tuple)):
            # we don't have a list -> can't be multiple values
            # this is a cheap check preventing most of the values.
            return False
        # end if

        original_type_fits = False
        listable_type_fits = False
        try:
            check_type(argname=key, value=value, expected_type=typehint)
            original_type_fits = True  # the original was already compatible
        except TypeError:
            pass
        # end if
        try:
            check_type(argname=key, value=value, expected_type=Union[Tuple[typehint], List[typehint]])
            listable_type_fits = True  # the original was already compatible
        except TypeError:
            pass
        # end if

        logger.debug(f'type fitting analysis: original: {original_type_fits}, tuple/list: {listable_type_fits}')
        if not listable_type_fits:
            # easy one,
            # so our list/tuple check doesn't match,
            # so it isn't compatible.
            return False
        # end if
        if not original_type_fits:
            # tuple/list one fits
            # but the original one does not.
            # pretty confident we have a list/tuple of a normal attribute and can do a `WHERE {key} IN ({",".join(value)})`
            return True
        # end if

        # else -> listable_type_fits == True, original_type_fits == True
        # ooof, so that one would have already be accepted by the original type...
        # That's a tough one...
        # we err to the side of caution
        logger.warn(f'Not quite sure if it fits or not, erring to the side of caution and assuming single parameter.')
        return False
    # end def

    _CLASS_SERIALIZERS = {
        # # class: callable(data) -> json
        # # E.g.:
        # TgBotApiObject: lambda obj: return obj._raw if hasattr(obj, '_raw') and obj._raw else obj.to_array()
    }

    @classmethod
    async def get_connection(cls, database_url) -> Connection:
        # https://magicstack.github.io/asyncpg/current/usage.html#example-automatic-json-conversion
        conn = await asyncpg.connect(database_url)
        return await cls._set_up_connection(conn=conn)
    # end def

    @classmethod
    async def _set_up_connection(cls, conn: Connection):
        """
        Sets up a connection to properly do datetime and json decoding.

        Prepares writing datetimes (as ISO format) and class instances as json if they have a `.to_dict()`, `.to_array()` function.
        An easy way to add your is by having a `.to_json()` function like above or
        appending your class to `_CLASS_SERIALIZERS` like so:
        ```py
        # anywhere in your code, to be run once
        FastORM._CLASS_SERIALIZERS[SomeClass] = lambda obj: obj.do_something()
        ```
        :param conn:
        :return:
        """
        import json

        def decoder_with_empty(text):
            if text.strip() == '':
                return None
            # end if
            return json.loads(text)
        # end def

        def json_dumps(obj):
            logger.debug(f'Encoding to JSON: {obj!r}')

            def my_converter(o):
                if isinstance(o, datetime.datetime):
                    return o.isoformat()
                # end def
                if hasattr(o, 'to_array'):
                    return o.to_array()  # TgBotApiObject from pytgbot
                # end def
                if hasattr(o, 'to_dict'):
                    return o.to_dict()
                # end def

                # check _CLASS_SERIALIZERS,
                # a easy way to add your own by writing FastORM._CLASS_SERIALIZERS[Class] = lambda obj: obj
                for type_to_check, callable_function in cls._CLASS_SERIALIZERS.items():
                    if isinstance(o, type_to_check):
                        return callable_function(o)
                    # end if
                # end for
            # end def
            return json.dumps(obj, default=my_converter)
        # end def

        for sql_type in ('json', 'jsonb'):
            await conn.set_type_codec(
                sql_type,
                encoder=json_dumps,
                decoder=decoder_with_empty,
                schema='pg_catalog'
            )
        # end for
        return conn
    # end def
# end class


class AutoincrementType(object):
    """
    Below there will be a singleton called Autoincrement.
    It can be used in the following two ways, they are the same:

        >>. Autoincrement = AutoincrementType()
        >>. class Foo(FastORM):
        ...   id_a: int = Field(default_factory=Autoincrement, other_field_parameters=...)
        ...   id_b: int = Autoincrement(other_field_parameters=...)

    In other words, the following two are doing the same:

        >>. example_1 = Autoincrement(default=None)
        >>. example_2 = Field(default_factory=Autoincrement)

        >>. from pydantic.fields import FieldInfo
        >>. isinstance(example_1, FieldInfo)
        True
        >>. isinstance(example_2, FieldInfo)
        True

        >>. Autoincrement().__dir__() == Field(default_factory=Autoincrement).__dir__()
        True

    """

    def __init__(self):
        self.__name__ = "fastorm.Autoincrement"  # some internals of Field want to know that.
    # end def

    @typing.overload
    def __call__(
        self,
        default: Any = Undefined,
        *,
        default_factory: Optional[NoArgAnyCallable] = None,
        alias: str = None,
        title: str = None,
        description: str = None,
        const: bool = None,
        gt: float = None,
        ge: float = None,
        lt: float = None,
        le: float = None,
        multiple_of: float = None,
        min_items: int = None,
        max_items: int = None,
        min_length: int = None,
        max_length: int = None,
        allow_mutation: bool = True,
        regex: str = None,
        **extra: Any,
    ) -> Any:
        pass
    # end def

    def __call__(self, *args, **kwargs) -> None:
        if not args and not kwargs:
            # For the use in `Field(default_factory=Autoincrement)`, we will be called without parameters.
            # so we return the default value this object should then have, that is `None`.
            # It would be cooler, but we can't return Autoincrement as that would be incompatible with the field's type.
            return None
        # end if

        # so it's not the `Field(default_factory=Autoincrement)` calling us with zero parameters
        assert 'default_factory' not in kwargs
        kwargs['default_factory'] = Autoincrement
        return Field(*args, **kwargs)
    # end def
# end class


Autoincrement = AutoincrementType()
