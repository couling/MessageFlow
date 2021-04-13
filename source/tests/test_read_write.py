from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import decimal

import pytest

import message_stream


@pytest.mark.parametrize('to_send', [None, True, False, 0, 1, 10, 100, 10000, 1000000000000000000000, '', 'y', 'hello',
                                     decimal.Decimal("1.2345"), decimal.Decimal("-600.54321"),
                                     [1, 2, 3], ('x', 'y', 'zee'), {'x': 1, 1: 'y'}, {'', 'b', 'bee'}],
                         ids=lambda x: f"'{x}'")
def test_basic_write(to_send):
    schema = message_stream.Schema()
    value = schema.dump_bytes(to_send)
    result = schema.load_bytes(value)
    assert to_send == result
    assert type(to_send) == type(result)


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


def TestSchemaDataclass():
    example_object = Parent(
        some_list=[9, 8, 7],
        some_tuple=('foo', 'bar'),
        some_dict={1: 'true'},
        some_child=Child_1(name='bob', value=10),
        some_optional_set=Child_2(first='foo', second='bar'),
        some_optional_not_set=None,
    )
    schema = message_stream.Schema()
    schema.auto_struct(Parent)

    value = schema.dump_bytes(example_object)
    result = schema.load_bytes(value)
    assert result == example_object
