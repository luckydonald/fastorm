from datetime import datetime, timedelta, date, time
from enum import Enum
from typing import Literal, Union
from decimal import Decimal

from pydantic import BaseModel, JsonValue
from hypothesis import given, strategies as st


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


@st.composite
def example_base_model_strategy(draw):
    return ExampleBaseModel(
        example_str=draw(st.text()),
        example_bool=draw(st.booleans())
    )

@st.composite
def referenced_table_strategy(draw):
    return ReferencedTable(
        id=draw(st.integers()),
        name=draw(st.text()),
        description=draw(st.text())
    )

@st.composite
def table_to_test_strategy(draw):
    # For recursive/self-referencing fields, use None for simplicity
    return TableToTest(
        id=draw(st.integers()),
        name=draw(st.text()),
        description=draw(st.text()),
        int_field=draw(st.integers()),
        float_field=draw(st.floats(allow_nan=False, allow_infinity=False)),
        str_field=draw(st.text()),
        bool_field=draw(st.booleans()),
        bytes_field=draw(st.binary()),
        list_field=draw(st.lists(st.integers())),
        tuple_field=draw(st.tuples(st.integers(), st.integers(), st.integers())),
        set_field=draw(st.sets(st.integers())),
        frozenset_field=draw(st.builds(frozenset, st.lists(st.integers()))),
        dict_field=draw(st.dictionaries(st.text(), st.integers())),
        nonetype_field=None,
        complex_field=draw(st.complex_numbers(allow_nan=False, allow_infinity=False)),
        bytearray_field=draw(st.binary().map(bytearray)),
        object_field=object(),
        type_field=int,
        any_field=draw(st.one_of(st.text(), st.integers(), st.none())),
        optional_field=draw(st.one_of(st.integers(), st.none())),
        union_field=draw(st.one_of(st.integers(), st.text())),
        list_typing_field=draw(st.lists(st.integers())),
        dict_typing_field=draw(st.dictionaries(st.text(), st.integers())),
        set_typing_field=draw(st.sets(st.integers())),
        tuple_typing_field=draw(st.tuples(st.integers(), st.integers(), st.integers())),
        literal_field=1,
        enum_field="A",
        datetime_field=draw(st.datetimes()),
        date_field=draw(st.dates()),
        time_field=draw(st.times()),
        decimal_field=draw(st.decimals(allow_nan=False, allow_infinity=False)),
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
        timedelta_field=draw(st.timedeltas()),
        other_reference_required=draw(referenced_table_strategy()),
        other_reference_optional=draw(st.one_of(referenced_table_strategy(), st.none())),
        self_reference=None,
        json_field=draw(st.dictionaries(st.text(), st.integers())),
        pydantic_basemodel=draw(example_base_model_strategy()),
        pydantic_generic_basemodel=draw(example_base_model_strategy()),
        int_list=draw(st.lists(st.integers())),
        float_list=draw(st.lists(st.floats(allow_nan=False, allow_infinity=False))),
        str_list=draw(st.lists(st.text())),
        tuple_list=draw(st.lists(st.tuples(st.integers(), st.integers()))),
        datetime_list=draw(st.lists(st.datetimes())),
        timedelta_list=draw(st.lists(st.timedeltas())),
        int_dict=draw(st.dictionaries(st.text(), st.integers())),
        float_dict=draw(st.dictionaries(st.text(), st.floats(allow_nan=False, allow_infinity=False))),
        dict_dict=draw(st.dictionaries(st.text(), st.dictionaries(st.text(), st.one_of(st.integers(), st.text())))),
        mixed_type_str_int=draw(st.one_of(st.text(), st.integers())),
        mixed_type_bool_str=draw(st.one_of(st.booleans(), st.text())),
        optional_str=draw(st.one_of(st.text(), st.none())),
        optional_int=draw(st.one_of(st.integers(), st.none())),
        optional_float=draw(st.one_of(st.floats(allow_nan=False, allow_infinity=False), st.none())),
        optional_mixed=draw(st.one_of(st.integers(), st.text(), st.none())),
    )

@given(table=table_to_test_strategy())
def test_insert_row_hypothesis(table):
    # Just ensure instantiation works and fields are populated
    assert isinstance(table, TableToTest)
    assert isinstance(table.pydantic_basemodel, ExampleBaseModel)
    print(table.model_dump())

if __name__ == "__main__":
    test_insert_row_hypothesis()
