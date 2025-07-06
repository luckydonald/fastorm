# Python Types Table
# | Python Type         | Example             |
# |---------------------|---------------------|
# | int                 | 1                   |
# | float               | 1.0                 |
# | str                 | "hello"             |
# | bool                | True                |
# | bytes               | b"abc"              |
# | list                | [1, 2, 3]           |
# | tuple               | (1, 2, 3)           |
# | set                 | {1, 2, 3}           |
# | frozenset           | frozenset([1, 2])   |
# | dict                | {"a": 1}            |
# | NoneType            | None                |
# | complex             | 1+2j                |
# | range               | range(5)            | NOT SUPPORTED IN Pydantic
# | memoryview          | memoryview(b"abc")  | MOT SUPPORTED IN Pydantic
# | bytearray           | bytearray(b"abc")   | USE bytes INSTEAD.
# | object              | object()            |
# | type                | type(1)             |
# | callable            | lambda x: x         | NOT SUPPORTED IN Pydantic
# | Any (typing)        | -                   |
# | Optional (typing)   | Optional[int]       |
# | Union (typing)      | Union[int, str]     |
# | List (typing)       | List[int]           |
# | Dict (typing)       | Dict[str, int]      |
# | Set (typing)        | Set[int]            |
# | Tuple (typing)      | Tuple[int, ...]     |
# | Literal (typing)    | Literal[1, 2, 3]    |
# | Enum                | Enum('A', 'B')      |
# | datetime            | datetime.datetime() |
# | date                | datetime.date()     |
# | time                | datetime.time()     |
# | timedelta           | datetime.timedelta(hours=2, minutes=3) |
# | Decimal             | Decimal("1.23")     |
from datetime import datetime, timedelta, date, time
from enum import Enum
from typing import Literal, Union
from decimal import Decimal

from pydantic import BaseModel, JsonValue


class ExampleStrEnum(str, Enum):
    A = "A"
    B = "B"
    C = "C"

class ExampleIntEnum(int, Enum):
    A = 1
    B = 2
    C = 3

class ExampleFloatEnum(float, Enum):
    A = 6.9
    B = 44.58

class ExampleMixedEnum(Enum):
    A = "A"
    B = 2
    C = True
    D = None

# ints, byte and unicode strings, bools, Enum values, None
LiteralStrings = Literal["A", "B", "C"]
LiteralInts = Literal[1, 2, 3]
LiteralBool = Literal[True, True]
LiteralStrEnum = Literal[ExampleStrEnum.A, ExampleStrEnum.B]
LiteralIntEnum = Literal[ExampleIntEnum.A, ExampleIntEnum.B]
LiteralFloatEnum = Literal[ExampleFloatEnum.A]
LiteralMixedEnum = Literal[ExampleMixedEnum.A, ExampleMixedEnum.B, ExampleMixedEnum.D]
LiteralMixed = Literal[1, "A", True, None, ExampleStrEnum.A, ExampleIntEnum.B, ExampleFloatEnum.A, ExampleMixedEnum.C]
LiteralRecursiveLiteral = Literal[LiteralStrings, LiteralInts]
LiteralRecursiveLiteralLevel2 = Literal[LiteralRecursiveLiteral, LiteralBool]

class ExampleBaseModel(BaseModel):
    example_str: str
    example_bool: bool

class ReferencedTable(BaseModel):
    id: int
    name: str
    description: str


class TableToTest(BaseModel):
    id: int
    name: str
    description: str
    int_field: int
    float_field: float
    str_field: str
    bool_field: bool
    bytes_field: bytes
    list_field: list
    tuple_field: tuple
    set_field: set
    frozenset_field: frozenset
    dict_field: dict
    nonetype_field: type(None)
    complex_field: complex
    # range_field: range
    # memoryview_field: memoryview
    bytearray_field: bytes
    object_field: object
    type_field: type
    # callable_field: callable
    # Typing fields
    any_field: object
    optional_field: int | None
    union_field: int | str
    list_typing_field: list[int]
    dict_typing_field: dict[str, int]
    set_typing_field: set[int]
    tuple_typing_field: tuple[int, ...]
    literal_field: int
    enum_field: str
    datetime_field: datetime
    date_field: date
    time_field: time
    decimal_field: Decimal
    # Literal, Enum
    literal_strings_field: LiteralStrings
    literal_ints_field: LiteralInts
    literal_bool_field: LiteralBool
    literal_str_enum_field: LiteralStrEnum
    literal_int_enum_field: LiteralIntEnum
    literal_float_enum_field: LiteralFloatEnum
    literal_mixed_enum_field: LiteralMixedEnum
    literal_mixed_field: LiteralMixed
    literal_recursive_literal_field: LiteralRecursiveLiteral
    literal_recursive_literal_level2_field: LiteralRecursiveLiteralLevel2
    example_str_enum_field: ExampleStrEnum
    example_int_enum_field: ExampleIntEnum
    example_float_enum_field: LiteralFloatEnum
    example_mixed_enum_field: ExampleMixedEnum
    # stuff I can think of:
    timedelta_field: timedelta
    other_reference_required: ReferencedTable
    other_reference_optional: ReferencedTable | None
    self_reference: Union["TableToTest", None]
    json_field: JsonValue
    pydantic_basemodel: ExampleBaseModel
    pydantic_generic_basemodel: BaseModel
    int_list: list[int]
    float_list: list[float]
    str_list: list[str]
    tuple_list: list[tuple[int, ...]]
    datetime_list: list[datetime]
    timedelta_list: list[timedelta]
    # dicts
    int_dict: dict[str, int]
    float_dict: dict[str, float]
    dict_dict: dict[str, dict[str, int | str]]
    mixed_type_str_int: str | int
    mixed_type_bool_str: bool | str
    optional_str: str | None
    optional_int: int | None
    optional_float: float | None
    optional_mixed: int | str | None


def test_insert_row():
    import datetime
    from decimal import Decimal
    table = TableToTest(
        id=1,
        name="Test Name",
        description="Test Description",
        int_field=42,
        float_field=3.14,
        str_field="hello",
        bool_field=True,
        bytes_field=b"abc",
        list_field=[1, 2, 3],
        tuple_field=(1, 2, 3),
        set_field={1, 2, 3},
        frozenset_field=frozenset([1, 2]),
        dict_field={"a": 1},
        nonetype_field=None,
        complex_field=1+2j,
        # range_field=range(5),
        # memoryview_field=memoryview(b"abc"),
        bytearray_field=bytearray(b"abc"),
        object_field=object(),
        type_field=int,
        # callable_field=lambda x: x,
        any_field="anything",
        optional_field=None,
        union_field="union",
        list_typing_field=[4, 5, 6],
        dict_typing_field={"b": 2},
        set_typing_field={4, 5, 6},
        tuple_typing_field=(4, 5, 6),
        literal_field=1,
        enum_field="A",
        datetime_field=datetime.datetime.now(),
        date_field=datetime.date.today(),
        time_field=datetime.datetime.now().time(),
        decimal_field=Decimal("1.23"),
        literal_strings_field="A",
        literal_ints_field=1,
        literal_bool_field=True,
        literal_str_enum_field=ExampleStrEnum.A,
        literal_int_enum_field=ExampleIntEnum.A,
        literal_float_enum_field=ExampleFloatEnum.A,
        literal_mixed_enum_field=ExampleMixedEnum.A,
        literal_mixed_field=1,
        literal_recursive_literal_field="A",
        literal_recursive_literal_level2_field="A",
        example_str_enum_field=ExampleStrEnum.A,
        example_int_enum_field=ExampleIntEnum.A,
        example_float_enum_field=ExampleFloatEnum.A,
        example_mixed_enum_field=ExampleMixedEnum.A,
        timedelta_field=datetime.timedelta(hours=2, minutes=3),
        other_reference_required=ReferencedTable(id=2, name="Ref Name", description="Ref Desc"),
        other_reference_optional=None,
        self_reference=None,
        json_field={"key": "value"},
        pydantic_basemodel=ExampleBaseModel(example_str="abc", example_bool=False),
        pydantic_generic_basemodel=ExampleBaseModel(example_str="def", example_bool=True),
        int_list=[1, 2, 3],
        float_list=[1.1, 2.2, 3.3],
        str_list=["a", "b", "c"],
        tuple_list=[(1, 2), (3, 4)],
        datetime_list=[datetime.datetime.now()],
        timedelta_list=[datetime.timedelta(minutes=5)],
        int_dict={"a": 1, "b": 2},
        float_dict={"a": 1.1, "b": 2.2},
        dict_dict={"a": {"x": 1, "y": "foo"}},
        mixed_type_str_int="string",
        mixed_type_bool_str=True,
        optional_str=None,
        optional_int=None,
        optional_float=None,
        optional_mixed=None,
    )

from hypothesis import given, strategies as st

@given(
    id=st.integers(),
    name=st.text(),
    description=st.text(),
    int_field=st.integers(),
    float_field=st.floats(allow_nan=False, allow_infinity=False),
    str_field=st.text(),
    bool_field=st.booleans(),
    bytes_field=st.binary(),
    list_field=st.lists(st.integers()),
    tuple_field=st.tuples(st.integers(), st.integers(), st.integers()),
    set_field=st.sets(st.integers()),
    frozenset_field=st.builds(frozenset, st.lists(st.integers())),
    dict_field=st.dictionaries(st.text(), st.integers()),
    nonetype_field=st.none(),
    complex_field=st.complex_numbers(allow_nan=False, allow_infinity=False),
    bytearray_field=st.binary().map(bytearray),
    object_field=st.just(object()),
    type_field=st.just(int),
    any_field=st.one_of(st.text(), st.integers(), st.none()),
    optional_field=st.one_of(st.integers(), st.none()),
    union_field=st.one_of(st.integers(), st.text()),
    list_typing_field=st.lists(st.integers()),
    dict_typing_field=st.dictionaries(st.text(), st.integers()),
    set_typing_field=st.sets(st.integers()),
    tuple_typing_field=st.tuples(st.integers(), st.integers(), st.integers()),
    literal_field=st.just(1),
    enum_field=st.just("A"),
    datetime_field=st.datetimes(),
    date_field=st.dates(),
    time_field=st.times(),
    decimal_field=st.decimals(allow_nan=False, allow_infinity=False),
)
def test_insert_row_hypothesis(
    id, name, description, int_field, float_field, str_field, bool_field, bytes_field,
    list_field, tuple_field, set_field, frozenset_field, dict_field, nonetype_field,
    complex_field, bytearray_field, object_field, type_field, any_field, optional_field,
    union_field, list_typing_field, dict_typing_field, set_typing_field, tuple_typing_field,
    literal_field, enum_field, datetime_field, date_field, time_field, decimal_field
):
    # Only a subset of fields for demonstration; expand as needed.
    TableToTest(
        id=id,
        name=name,
        description=description,
        int_field=int_field,
        float_field=float_field,
        str_field=str_field,
        bool_field=bool_field,
        bytes_field=bytes_field,
        list_field=list_field,
        tuple_field=tuple_field,
        set_field=set_field,
        frozenset_field=frozenset_field,
        dict_field=dict_field,
        nonetype_field=nonetype_field,
        complex_field=complex_field,
        bytearray_field=bytearray_field,
        object_field=object_field,
        type_field=type_field,
        any_field=any_field,
        optional_field=optional_field,
        union_field=union_field,
        list_typing_field=list_typing_field,
        dict_typing_field=dict_typing_field,
        set_typing_field=set_typing_field,
        tuple_typing_field=tuple_typing_field,
        literal_field=literal_field,
        enum_field=enum_field,
        datetime_field=datetime_field,
        date_field=date_field,
        time_field=time_field,
        decimal_field=decimal_field,
        # The rest of the fields can be filled with defaults or skipped for this test.
        literal_strings_field="A",
        literal_ints_field=1,
        literal_bool_field=True,
        literal_str_enum_field=ExampleStrEnum.A,
        literal_int_enum_field=ExampleIntEnum.A,
        literal_float_enum_field=ExampleFloatEnum.A,
        literal_mixed_enum_field=ExampleMixedEnum.A,
        literal_mixed_field=1,
        literal_recursive_literal_field="A",
        literal_recursive_literal_level2_field="A",
        example_str_enum_field=ExampleStrEnum.A,
        example_int_enum_field=ExampleIntEnum.A,
        example_float_enum_field=ExampleFloatEnum.A,
        example_mixed_enum_field=ExampleMixedEnum.A,
        timedelta_field=timedelta(hours=2, minutes=3),
        other_reference_required=ReferencedTable(id=2, name="Ref Name", description="Ref Desc"),
        other_reference_optional=None,
        self_reference=None,
        json_field={"key": "value"},
        pydantic_basemodel=ExampleBaseModel(example_str="abc", example_bool=False),
        pydantic_generic_basemodel=ExampleBaseModel(example_str="def", example_bool=True),
        int_list=[1, 2, 3],
        float_list=[1.1, 2.2, 3.3],
        str_list=["a", "b", "c"],
        tuple_list=[(1, 2), (3, 4)],
        datetime_list=[datetime.now()],
        timedelta_list=[timedelta(minutes=5)],
        int_dict={"a": 1, "b": 2},
        float_dict={"a": 1.1, "b": 2.2},
        dict_dict={"a": {"x": 1, "y": "foo"}},
        mixed_type_str_int="string",
        mixed_type_bool_str=True,
        optional_str=None,
        optional_int=None,
        optional_float=None,
        optional_mixed=None,
    )
