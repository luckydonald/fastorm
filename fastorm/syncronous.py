#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime
from typing import List, Dict, Any, Type, Optional, Union

from asyncpg import Record
from luckydonaldUtils.exceptions import assert_type_or_raise
from luckydonaldUtils.logger import logging

__author__ = 'luckydonald'

from luckydonaldUtils.typing import JSONType
from pydantic import PrivateAttr
from pydantic.fields import ModelField

logger = logging.getLogger(__name__)
if __name__ == '__main__':
    logging.add_colored_handler(level=logging.DEBUG)
# end if

from psycopg2.extensions import connection as Connection
from psycopg2.extensions import cursor as Cursor

from fastorm import _BaseFastORM, ModelMetaclassFastORM, FieldInfo, CLS_TYPE, VERBOSE_SQL_LOG, SQL_DO_NOTHING


class SyncFastORM(_BaseFastORM, metaclass=ModelMetaclassFastORM):
    _table_name: str  # database table name we run queries against
    _ignored_fields: List[str]  # fields which never are intended for the database and will be excluded in every operation. (So are all fields starting with an underscore)
    _automatic_fields: List[str]  # fields the database fills in, so we will ignore them on INSERT.
    _primary_keys: List[str]  # this is how we identify ourself.
    _database_cache: Dict[str, JSONType] = PrivateAttr()  # stores the last known retrieval, so we can run UPDATES after you changed parameters.
    __selectable_fields: List[str] = PrivateAttr()  # cache for `cls.get_sql_fields()`
    __fields_typehints: Dict[bool, Dict[str, FieldInfo[ModelField]]] = PrivateAttr()  # cache for `cls.get_fields_typehint()`
    __fields_references: Dict[bool, Dict[str, FieldInfo[ModelField]]] = PrivateAttr()  # cache for `cls.get_fields_typehint()`
    __original__annotations__: Dict[str, Any]  # filled by the metaclass, before we do modify the __annotations__
    __original__fields__: Dict[str, ModelField]  # filled by the metaclass, before we do modify the __fields__

    @classmethod
    def sql_compatibility_layer(cls, fetch_params):
        sql_query, *params = [*fetch_params]
        max_param = len(params)


    @classmethod
    def get(cls: Type[CLS_TYPE], conn: Connection, **kwargs) -> Optional[CLS_TYPE]:
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

    async def insert(
        self: Union[CLS_TYPE, "_BaseFastORM"], conn: Connection, *,
        ignore_setting_automatic_fields: Optional[bool] = None,
        upsert_on_conflict: Union[List[str], bool] = False,
        write_back_automatic_fields: bool = True,
        on_conflict_upsert_field_list: None = None,  # deprecated! Use `upsert_on_conflict=…` instead!
    ) -> Union[CLS_TYPE, "_BaseFastORM"]:
        """
        :param conn: Database connection to run at.
        :param ignore_setting_automatic_fields:
            Skip setting fields marked as automatic, even if you provided.
            For example if the id field is marked automatic, as it's an autoincrement int.
            If `True`, setting `id=123` (commonly `id=None`) would be ignored, and instead the database assigns that value.
            If `False`, the value there will be written to the database.
            If `None`, it will be ignored as long as the value actually is None, but set if it is non-None.
            The default setting is `None`.
        :param upsert_on_conflict:
            List of fields which are expected to cause a duplicate conflict, and thus all the other fields will be overwritten.
            Either a boolean to set the automatic mode, or a list of fields.
            If `True`: Will automatically use the primary key field(s) as conflict source.
            If `False`: Will not upsert (update) but simply fail.
            If `List[str]`: Will use those given conflicting fields.
        :param on_conflict_upsert_field_list: Deprecated and will be removed in the future. Use `upsert_on_conflict` instead.
        :param write_back_automatic_fields: Apply the automatic fields back to this object.
                                            Ignored if `ignore_setting_automatic_fields` is False.
        :return: self
        """
        fetch_params = self._insert_preparation(
            ignore_setting_automatic_fields=ignore_setting_automatic_fields,
            upsert_on_conflict=upsert_on_conflict,
            on_conflict_upsert_field_list=on_conflict_upsert_field_list,
        )
        updated_automatic_values_rows: List[Record] = await conn.fetch(*fetch_params)
        logger.debug(f'INSERT for {self.__class__.__name__}: {updated_automatic_values_rows} for {self}')
        self._insert_postprocess(
            updated_automatic_values_rows=updated_automatic_values_rows,
            ignore_setting_automatic_fields=ignore_setting_automatic_fields,
            write_back_automatic_fields=write_back_automatic_fields,
        )
        return self

    # end def

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

    async def delete(self, conn: Connection):
        fetch_params = self.build_sql_delete()
        logger.debug(f'DELETE query for {self.__class__.__name__}: {fetch_params!r}')
        delete_status = await conn.execute(*fetch_params)
        logger.debug(f'DELETE for {self.__class__.__name__}: {delete_status} for {self}')
        self._database_cache_remove()
    # end if

    @classmethod
    async def create_table(
        cls,
        conn: Connection,
        if_not_exists: bool = False,
        psycopg2_conn: Union['psycopg2.extensions.connection', 'psycopg2.extensions.cursor', None] = None,
    ):
        """
        Builds and executes a CREATE TABLE statement.

        :param conn: the `asyncpg` database connection to execute this with.

        :param if_not_exists:
            If the table definition should include IF NOT EXISTS, thus not producing an error if it does, but instead being silently ignored.

        :param psycopg2_conn:
            If you have complex default types for your fields (everything other than None, bool, int, and pure ascii strings),
            The psycopg2 library is used to build a injection safe SQL string.
            Therefore then psycopg2 has to be installed (pip install psycopg2-binary),
            and a connection (or cursor) to the database must be provided (psycopg2_conn = psycopg2.connect(…)).
        :return:
        """
        create_params = cls.build_sql_create(if_not_exists=if_not_exists,
                                             psycopg2_conn=psycopg2_conn if psycopg2_conn else conn)
        logger.debug(f'CREATE query for {cls.__name__}: {create_params!r}')
        create_status = await conn.execute(*create_params)
        logger.debug(f'CREATEed {cls.__name__}: {create_status}')
    # end if

    @classmethod
    async def create_table_references(
        cls,
        conn: Connection,
    ):
        """
        Builds and executes a ALTER TABLE and CREATE TABLE statement.

        :param conn: the `asyncpg` database connection to execute this with.
        :return:
        """
        reference_params = cls.build_sql_references()
        logger.debug(f'REFERENCE query for {cls.__name__}: {reference_params!r}')
        if reference_params[0] != SQL_DO_NOTHING:
            reference_status = await conn.execute(*reference_params)
            logger.debug(f'REFERENCEed {cls.__name__}: {reference_status}')
        else:
            logger.debug(f'REFERENCEed {cls.__name__}: No need to do anything.')
        # end def
    # end if

    @classmethod
    async def create_connection(cls, database_url) -> Connection:
        # https://magicstack.github.io/asyncpg/current/usage.html#example-automatic-json-conversion
        conn = await asyncpg.connect(database_url)
        return await cls._set_up_connection(conn=conn)
    # end def

    @classmethod
    async def create_connection_pool(cls, database_url) -> Pool:
        # https://magicstack.github.io/asyncpg/current/usage.html#example-automatic-json-conversion
        return await asyncpg.create_pool(database_url, setup=cls._set_up_connection)

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

    @classmethod
    async def get_connection(cls, database_url):
        """
        Deprecated, use `FastORM.create_connection(…)` instead.
        """
        logger.warning(
            'The use of FastORM.get_connection(…) is deprecated, use FastORM.create_connection(…)` instead.',
            exc_info=True,
        )
        return await cls.create_connection(database_url=database_url)
    # end def
# end class
