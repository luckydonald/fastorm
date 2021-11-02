#!/usr/bin/env python3
# -*- coding: utf-8 -*-
__author__ = 'luckydonald'

import asyncio
from datetime import datetime, timedelta
from typing import Optional, Union
from enum import Enum

from asyncpg import Connection
from luckydonaldUtils.typing import JSONType

from fastorm import FastORM


class State(str, Enum):
    RUNNING = 'running'
    ABORTED = 'stopped'
    COMPLETED = 'done'
# end class


class User(FastORM):
    _ignored_fields = []
    _primary_keys = ['id']
    _automatic_fields = ['id']
    _table_name = 'user'

    id: Optional[int]  # Optional because automatically filled (_automatic_fields)
    name: str
# end class


class Auction(FastORM):
    _ignored_fields = []
    _primary_keys = ['id']
    _automatic_fields = ['id']
    _table_name = 'auction'

    id: Optional[int]  # Optional because automatically filled (_automatic_fields)
    owner: Union[int, User]  # can be provided by int or the native object
    previous_owner: Optional[User]  # Optional because nullable
    state: State  # Enum support :D
    title: str
    subtitle: Optional[str]  # nullable
    description: str
    start_date: datetime  # datetime support
    end_date: datetime
    metadata: JSONType
    deleted: bool
    chat_id: int
# end class


async def run():
    conn = await FastORM.create_connection('postgresql://user:password@postgres_host/database')

    await migrate_database(conn)  # a function (below) which prepares the database for use, creates tables and manages schema changes.

    owner_user = User(id=1234, name="The new auction owner.")
    await owner_user.insert(conn=conn, ignore_setting_automatic_fields=True)

    previous_owner = User(id=None, name="The previous auction owner.")
    await previous_owner.insert(conn=conn, ignore_setting_automatic_fields=False)

    print(f'Inserted owner user as id={owner_user.id}.')
    print(f'Inserted previous owner user as id={previous_owner.id}.')

    auction = Auction(
        id=None,  # gonna be automatic if `insert(…, ignore_setting_automatic_fields=False)` (default).
        owner=1234,  # by numeric id, aka. the actual field value
        previous_owner=previous_owner,  # use the id field (`User._primary_keys`) to determine the actual integer value.
        state=State.RUNNING,  # enum will be a string in the database
        title="I sell my soul", subtitle="Slightly used",
        description="You only get a share though.",
        start_date=datetime.now(), end_date=datetime.now() + timedelta(days=5),  # datetimes just works
        metadata={"just": ["json", "stuff", 111, datetime.now()]},  # will be native JSONB. You can have datetimes and your own classes in there as well, see `FastORM._set_up_connection`.
        deleted=False,
        chat_id=9223372036854775807,  # note, this database field must be BIGINT for such large numbers
    )

    await auction.insert(conn=conn, ignore_setting_automatic_fields=True, on_conflict_upsert_field_list=None,  write_back_automatic_fields=True)
    print(f'Inserted auction as id={auction.id}.')
    auction_id = auction.id

    # now let's retrieve some data from the database!

    db_auctions_by_user = await Auction.select(conn=conn, owner=1234)  # you can provide everything after the `conn=conn` to identify your entries. Returns a list, which can be empty.
    db_auction = await Auction.get(conn=conn, id=auction_id)  # same as select, but only allows a single result. None if not found. Raises an error if it's more than one match.
    print(db_auction.title)

    # let's change something.
    db_auction.state = State.COMPLETED
    await db_auction.update(conn=conn)

    # run custom queries

    # e.g. joining the users to that result and loading them in a single query
    # basically we use a prepared statement for variables.
    fetch_params = (
        f'SELECT '
        f' {Auction.get_select_fields(namespace="auction")}'  # first all fields from the auction
        f', {User.get_select_fields(namespace="owner")}'  # then all fields from the owner
        f' FROM'
        f'  {Auction.get_table()} AS "auction",'  # notice this is the same as the `namespace=…` above
        f'  {User.get_table()} AS "owner",'  # notice this is the same as the `namespace=…` above
        # now the join condition for those table
        f'WHERE "owner"."id" = "auction"."owner"'  # join user table with auction for owner
        # and more general condition we are interested right now
        f' AND "auction"."end_date" > CURRENT_TIMESTAMP'  # still running
        f' AND ("auction"."state" = $1 OR "auction"."id" = $2)'  # I don't know, but two parameters so I can demostrate that.
        f' ORDER BY "auction"."end_date"'  # ascending, so we have the soonest to end first
        ,
        State.COMPLETED,
        1234,
    )  # so to make it clear, syntax is ["SQL", $1, $2, …], list or tuple
    # now run that query
    fetch_results = await conn.fetch(*fetch_params)
    # now we can deconstruct that into rows
    auction_fields_len = Auction.get_select_fields_len()
    user_fields_len = User.get_select_fields_len()
    for row in fetch_results:
        # split the results into the proper lists for both elements
        auction_fields = row[0:auction_fields_len]
        user_fields = row[auction_fields_len:auction_fields_len + user_fields_len]

        # generate objects from it
        database_auction = Auction.from_row(auction_fields)
        database_user = User.from_row(user_fields)
        print(f'Auction {database_auction.title} from user {database_user.name}')
    # end for
# end def


async def migrate_database(conn: Connection):
    """
    Database migration, to set up all the tables we need. With versions so you can simply add a new version if you modify your models.
    Usually a separate migration file, just in the same one here for the sake of the tutorial being a single file.
    """
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS "version" (
          "version" SERIAL PRIMARY KEY,
          "date" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          "message" TEXT NOT NULL,
          "details" TEXT NOT NULL,
          "updated_from_version" INTEGER
        )
        """
    )
    # Select the version from the table and handle the table being just created as version 0.
    now = datetime.now()
    row = await conn.fetchrow('SELECT "version", "date", "message", "details" FROM "version" ORDER BY "version" DESC')
    if row is None:
        version = None
        print(f'database no version yet, assuming version 0.')
    else:
        version, date, message, details = row
        print(f'database has version {version} from {date}: {message} ({details})')
    # end if
    updated_from_version = version

    if version < 1:
        version = 1
        message = "Create a first update, with a user table"
        details = "Just an user for now and the auction table later for the purpose of a tutorial"
        await conn.execute(
            """
            CREATE TABLE "user" (
                "id" SERIAL PRIMARY KEY,
                "name" TEXT NOT NULL
            )
            """
        )
        await conn.execute(
            '''INSERT INTO version(version, date, message, details, updated_from_version) VALUES($1, $2, $3, $4, $5)''',
            version, now, message, details, updated_from_version
        )
    # end if

    if version < 2:
        version = 2
        message = "Create a second update, with the auction table"
        details = None
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS "auction" (

              "id" SERIAL PRIMARY KEY,  -- id: Optional[int]  # Optional because automaticly filled (_automatic_fields)
              "category" TEXT NOT NULL,
              "owner" INTEGER NOT NULL,  -- owner: Union[int, User]  # can be provided by int or the native object
              "previous_owner" INTEGER,  -- previous_owner: Optional[User]  # Optional because nullable
              "state" TEXT NOT NULL, -- state: State  # Enum support :D
              "title" TEXT NOT NULL, -- title: str
              "subtitle" TEXT, -- subtitle: Optional[str]  # nullable
              "description" TEXT NOT NULL, -- description: str
              "start_date" TIMESTAMP NOT NULL, -- start_date: datetime  # datetime support
              "end_date" TIMESTAMP NOT NULL, -- end_date: datetime
              "metadata" JSONB NOT NULL, -- metadata: JSONType
              "deleted" BOOLEAN NOT NULL, -- deleted: bool
              "chat_id" BIGINT NOT NULL  -- chat_id: int
            );
            """
        )
        await conn.execute(
            '''INSERT INTO version(version, date, message, details, updated_from_version) VALUES($1, $2, $3, $4, $5)''',
            version, now, message, details, updated_from_version
        )
    # end if
    print(f'Updated database from {updated_from_version} to {version}')
# end def


# Spin up a quick and simple event loop
# and run until completed
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(run())
finally:
    loop.close()
# end try
