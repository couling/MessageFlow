# pylint: disable=redefined-outer-name,missing-class-docstring
import decimal
import io
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Optional, NamedTuple

import pytest
import pytz

import message_stream


@pytest.fixture()
def schema() -> message_stream.Schema:
    """
    A simple schema
    """
    return message_stream.Schema()


BASE_TYPE_EXAMPLES = [
     None,
     ...,
     True,
     False,
     0,
     1,
     10,
     100,
     10000,
     1000000000000000000000,
     2 ** 33,
     0.0,
     0.9,
     '',  # zero chars
     'y',  # one byte one char
     '£',  # two bytes one char
     '✓',  # three bytes one char
     '👍',  # four bytes one char
     'hello',  # bytes == chars
     'Hey 👍',  # bytes > chars
     b'some bytes',
     decimal.Decimal("1.2345"),
     decimal.Decimal("-600.54321"),
     datetime.now(),
     datetime.now(timezone.utc), datetime.now().replace(microsecond=0),
     datetime(2021, 1, 30, 10, 21, 1, 123, timezone.utc).astimezone(pytz.timezone('America/New_York')),
     datetime(2021, 6, 30, 10, 21, 1, 0, timezone.utc).astimezone(pytz.timezone('America/New_York')),
     [1, 2, 3],
     ('x', 'y', 'zee'),
     {'x': 1, 1: 'y'},
     {'', 'b', 'bee'},
]


def raw_example_ids(example):
    """
    Returns friendly names for all RAW_EXAMPLES
    """
    if isinstance(example, bytes):
        return str([f"0x{hex(i)}" for i in example])
    if example == '':
        return "<empty string>"
    return str(example)


@pytest.mark.parametrize('to_encode', BASE_TYPE_EXAMPLES, ids=raw_example_ids)
def test_raw_round_trip(to_encode):
    """
    Test encoding an basic objects through the default schema and decoding them back results in identical objects
    """
    encoded_value = message_stream.dump_bytes(to_encode)
    decoded_value = message_stream.load_bytes(encoded_value)
    assert to_encode == decoded_value
    # pylint: disable=unidiomatic-typecheck
    assert type(to_encode) == type(decoded_value)


@pytest.mark.parametrize('to_encode', BASE_TYPE_EXAMPLES, ids=raw_example_ids)
def test_basic_round_trip(schema: message_stream.Schema, to_encode):
    """
    Test that encoding basic object through a fresh schema then decoding them back results in identical objects
    """
    encoded_value = schema.dump_bytes(to_encode)
    decoded_value = schema.load_bytes(encoded_value)
    assert to_encode == decoded_value
    # pylint: disable=unidiomatic-typecheck
    assert type(to_encode) == type(decoded_value)


def test_simple_schema_dataclass(schema):
    """
    Test a simple schema can be encoded and decoded
    """

    @schema.define_structure()
    @dataclass()
    class Simple:
        a_string: str
        an_int: int

    test_basic_round_trip(schema, Simple(a_string="hello", an_int=5))


def test_complex_schema_dataclass(schema: message_stream.Schema):
    """
    Test a complex schema (with children) can be encoded and decoded.
    """
    @dataclass()
    class Child1:
        name: str
        value: int

    @dataclass()
    class Child2:
        first: str
        second: str

    @dataclass()
    class Parent:
        some_list: List[int]
        some_tuple: Tuple[str, str]
        some_dict: Dict[int, str]
        some_child: Child1
        some_optional_set: Optional[Child2]
        some_optional_not_set: Optional[Child2]

    example_object = Parent(
        some_list=[9, 8, 7],
        some_tuple=('foo', 'bar'),
        some_dict={1: 'true'},
        some_child=Child1(name='bob', value=10),
        some_optional_set=Child2(first='foo', second='bar'),
        some_optional_not_set=None,
    )
    schema.define_structure(Parent)
    test_basic_round_trip(schema, example_object)


def test_complex_schema_named_tuple(schema: message_stream.Schema):
    """
    Test complex schema of named tuples can be encoded and decoded.
    """
    class Child1(NamedTuple):
        name: str
        value: int

    class Child2(NamedTuple):
        first: str
        second: str

    class Parent(NamedTuple):
        some_list: List[int]
        some_tuple: Tuple[str, str]
        some_dict: Dict[int, str]
        some_child: Child1
        some_optional_set: Optional[Child2]
        some_optional_not_set: Optional[Child2]

    example_object = Parent(
        some_list=[9, 8, 7],
        some_tuple=('foo', 'bar'),
        some_dict={1: 'true'},
        some_child=Child1(name='bob', value=10),
        some_optional_set=Child2(first='foo', second='bar'),
        some_optional_not_set=None,
    )
    schema.define_structure(Parent)
    test_basic_round_trip(schema, example_object)


def test_back_references_are_used(schema: message_stream.Schema):
    """
    Certain conditions should cause the encoder to encode a backref.  This includes large strings.
    """
    value = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."
    result_1 = schema.dump_bytes((value,))
    result_2 = schema.dump_bytes((value, value,))
    assert len(result_2) == len(result_1) + 2

    parsed_1, parsed_2 = schema.load_bytes(result_2)
    assert parsed_1 is parsed_2
    assert parsed_2 is not value
    assert parsed_2 == value


def test_sequence_of_objects(schema: message_stream.Schema):
    """
    Test that encoding a sequence of top level objects can be read back.
    """
    sequence = (1, 2, 2.0, 'hello', '👍', False, ...)
    buffer = io.BytesIO()

    encoder = schema.encoder(buffer)
    for item in sequence:
        encoder(item)

    buffer.seek(0, os.SEEK_SET)

    decoder = schema.decoder(buffer)
    result = tuple(decoder)

    assert result == sequence
