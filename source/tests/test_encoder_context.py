# pylint: disable=redefined-outer-name
from io import BytesIO

import pytest

from message_stream.encoder_decoder_context import EncoderContext
from message_stream.encoder_decoder import SentinelEncoder


@pytest.fixture()
def encoded_data() -> BytesIO:
    return BytesIO()


@pytest.fixture()
def encoder_context(encoded_data) -> EncoderContext:
    return EncoderContext({type(None): (SentinelEncoder(None), {None: 1})}, {}, encoded_data)


@pytest.mark.parametrize(('value', 'expected_size'), [
    (0x00, 1),
    (0x01, 1),
    (0x7F, 1),
    (0x0080, 2),
    (0x3FFF, 2),
    (0x00004000, 4),
    (0x1FFFFFFF, 4),
    (0x20000000, 8),
    (0x0FFFFFFFFFFFFFFF, 8),
])
def test_encode_variable_int(value: int, expected_size: int, encoder_context: EncoderContext, encoded_data: BytesIO):
    encoder_context.encode_variable_int(value)
    assert encoded_data.tell() == expected_size


def test_encode_variable_int_overflow(encoder_context: EncoderContext):
    with pytest.raises(ValueError):
        encoder_context.encode_variable_int(0x1000000000000000)


@pytest.mark.parametrize(('value', 'expected_result'), [
    ('', '\0'.encode("UTF-8")),
    ('hello', '\5hello'.encode("UTF-8")),
    ('£', '\2£'.encode("UTF-8")),
])
def test_encode_string(value: str, expected_result: bytes, encoder_context: EncoderContext, encoded_data: BytesIO):
    encoder_context.encode_string(value)
    result = encoded_data.getvalue()
    assert result == expected_result
