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
from typing import Literal
from decimal import Decimal

from pydantic import BaseModel


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
    timedelta_field: timedelta


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
    )


if __name__ == "__main__":
    test_insert_row()