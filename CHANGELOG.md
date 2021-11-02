> ### Known bugs:
> - In Python versions **before Python 3.10**, the automated **CREATE TABLE statement** (`build_sql_create(…)` thus also `create_table(…)`) will have problems with referencing other tables and list types:     
>   That means, `list[int]` will be missing an array at the end (`BIGINT[]` will be only `BIGINT`, `BIGINT[][][]` will be only `BIGINT[][]`) and
>   referencing other tables as part of an optional segment will result in an `JSONB` field instead of the referencing fields.   
>   **Workaround**: Change the type of the field after creation in python versions if you must use both 3.10 and the `create_table(…)` (or `build_sql_create(…)`) function
> - That's all we know of

# v0.0.7
- Fix for CREATE table statement not allowing for a prepared statement, and thus needing the escaping of values.
    - Needs psycopg2 installed for complex types (everything other than None, bool, int, and pure ascii strings)
- Added support for `PRIMARY KEY`(`S`).
     
# v0.0.6
- Handle `Optional[Table, <Table Primary Key Type>]` for references.
    - Works for referencing other tables with a multi-field primary key, too. 
- Added lots of unit tests and also fix bugs found by this
    - added support for multiple primary keys in most places now
    - fixes for errors/wonkyness introduced by using pydantic's BaseModel.

# v0.0.5
- Added `create_table(cls, conn: Connection, if_not_exists: bool = False)`.
- Added boolean `if_not_exists: bool = False` to the  `build_sql_create(…)`


# v0.0.4
- Added `has_changes()` and `get_changes()`. 


# v0.0.3
- Added `FastORM.build_sql_create()` with works quite well for simple database types.
    - This is a beta version of creating tables, and will be changed, modified and improved in the future.
    - That means if you use it, you man need to do manual migrations for a later version.
    - Refer to this changelog and search for `build_sql_create` to find more.
- Fixed uninstallable dependency    

    
