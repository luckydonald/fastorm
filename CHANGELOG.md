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

    
