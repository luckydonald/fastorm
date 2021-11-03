# FastORM
### ORM for async postgres
##### Beta

FastORM is a modern, fast (async), database library for projects with Python 3.10+ based on standard Python type hints.

The key features are:
 - Async postgres
 - Tested to work with Python 3.10

#### Example
> See [example.py](example.py) for more examples.

Let's define some tables, to show off the capabilities.

```py
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
```

Now you can quickly write classes to the database:

```py
conn = await FastORM.create_connection('postgresql://user:password@postgres_host/database')


user = User(id=None, name="hunter24")  # id will be filled by the database
await owner_user.insert(conn=conn)  # set's the id, too.

auction = Auction(
    id=None,  # gonna be automatic if `insert(…, ignore_setting_automatic_fields=False)` (default).
    # two ways of setting references to other tables:
    # by the actual value, in this case the numeric id
    owner=user.id,  
    # or via a different object,
    # it will use the id field (internally set by `User._primary_keys`) to determine the actual values.
    previous_owner=user,  
    state=State.RUNNING,  # enum will be a string in the database
    title="I sell my soul", subtitle="Slightly used",
    description="You only get a share though, others claim ownership, too.",
    start_date=datetime.now(), end_date=datetime.now() + timedelta(days=5),  # datetimes just works
    metadata={"just": ["json", "stuff", 111, datetime.now()]},  # will be native JSONB. You can have datetimes and your own classes in there as well, see `FastORM._set_up_connection`.
    deleted=False,
    chat_id=9223372036854775807,  # note, this database field must be BIGINT for such large numbers
)
await auction.insert(conn=conn)
```

Basic lookups are easy, too.

```py
# single lookup, returns one element or None
user = await User.get(name="hunter24")
user = await User.get(id=1234)

# list of results (list can have length 0)
all_running_auctions = Auction.select(state=State.RUNNING)
```

Of course updating and deleting is possible too.

```py
auction.state = State.COMPLETED
await auction.update()
```
```py
await user.delete()
```
