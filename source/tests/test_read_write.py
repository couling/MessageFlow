from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone
import pytz
import decimal

import pytest

import message_stream


@pytest.fixture()
def schema() -> message_stream.Schema:
    return message_stream.Schema()


@pytest.mark.parametrize('to_encode', [
    None, True, False, 0, 1, 10, 100, 10000, 1000000000000000000000, 0.9, '', 'y', 'hello', b'some bytes',
    decimal.Decimal("1.2345"), decimal.Decimal("-600.54321"),
    datetime.now(), datetime.now(timezone.utc), datetime.now().replace(microsecond=0),
    datetime(2021, 1, 30, 10, 21, 1, 123, timezone.utc).astimezone(pytz.timezone('America/New_York')),
    datetime(2021, 6, 30, 10, 21, 1, 0, timezone.utc).astimezone(pytz.timezone('America/New_York')),
    [1, 2, 3], ('x', 'y', 'zee'), {'x': 1, 1: 'y'}, {'', 'b', 'bee'}
], ids=lambda x: f"'{x}'")
def test_basic_round_trip(schema: message_stream.Schema, to_encode):
    encoded_value = schema.dump_bytes(to_encode)
    decoded_value = schema.load_bytes(encoded_value)
    assert to_encode == decoded_value
    assert type(to_encode) == type(decoded_value)


def test_simple_schema_dataclass(schema):
    @schema.define_structure()
    @dataclass()
    class Simple:
        a_string: str
        an_int: int

    test_basic_round_trip(schema, Simple(a_string="hello", an_int=5))


def test_complex_schema_dataclass(schema: message_stream.Schema):
    @dataclass()
    class Child_1:
        name: str
        value: int

    @dataclass()
    class Child_2:
        first: str
        second: str

    @dataclass()
    class Parent:
        some_list: List[int]
        some_tuple: Tuple[str, str]
        some_dict: Dict[int, str]
        some_child: Child_1
        some_optional_set: Optional[Child_2]
        some_optional_not_set: Optional[Child_2]

    example_object = Parent(
        some_list=[9, 8, 7],
        some_tuple=('foo', 'bar'),
        some_dict={1: 'true'},
        some_child=Child_1(name='bob', value=10),
        some_optional_set=Child_2(first='foo', second='bar'),
        some_optional_not_set=None,
    )
    schema.define_structure(Parent)
    test_basic_round_trip(schema, example_object)
